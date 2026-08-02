//! Fibre Engine extension: job system.
//!
//! Two-tier architecture:
//!   Compute — Marl fibers (PC) / pure Rust work-stealing (consoles)
//!   I/O     — 4-thread fixed pool for read_async / blocking_call
//!
//! `blocking_call` does NOT create a thread per call:
//! it uses the shared I/O pool. This is the same pool `read_async` uses.

use crate::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use crate::sync::{Condvar, Mutex};
use crate::thread;
use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;

static INITIALIZED: AtomicBool = AtomicBool::new(false);
static INIT_LOCK: Mutex<()> = Mutex::new(());

// ═══════════════════════════════════════════════════════════════════════
//  I/O Pool — 4 dedicated threads, shared by read_async & blocking_call
// ═══════════════════════════════════════════════════════════════════════

struct IoInner {
    queue: Mutex<VecDeque<Box<dyn FnOnce() + Send + 'static>>>,
    cvar: Condvar,
    stop: AtomicBool,
}

static mut IO: Option<&'static IoInner> = None;
static mut IO_HANDLES: Option<Vec<thread::JoinHandle<()>>> = None;

fn io_push(task: Box<dyn FnOnce() + Send + 'static>) {
    unsafe {
        if let Some(io) = IO {
            io.queue.lock().unwrap().push_back(task);
            io.cvar.notify_one();
        }
    }
}

fn io_call<R: Send + 'static>(f: impl FnOnce() -> R + Send + 'static) -> R {
    let r = Arc::new(Mutex::new(None::<R>));
    let d = Arc::new((Mutex::new(false), Condvar::new()));
    let r2 = r.clone();
    let d2 = d.clone();
    io_push(Box::new(move || { *r2.lock().unwrap() = Some(f()); *d2.0.lock().unwrap() = true; d2.1.notify_one(); }));
    let mut g = d.0.lock().unwrap();
    while !*g { g = d.1.wait(g).unwrap(); }
    r.lock().unwrap().take().unwrap()
}

fn io_init(n: usize) {
    let inner: &'static IoInner = Box::leak(Box::new(IoInner {
        queue: Mutex::new(VecDeque::new()), cvar: Condvar::new(), stop: AtomicBool::new(false),
    }));
    let mut h = Vec::with_capacity(n);
    for _ in 0..n {
        let io: &'static IoInner = inner;
        h.push(thread::Builder::new().name("fibre-io".into())
            .spawn(move || {
                loop {
                    if io.stop.load(Ordering::Relaxed) { return; }
                    let mut q = io.queue.lock().unwrap();
                    let t = loop { if let Some(t) = q.pop_front() { break t; } q = io.cvar.wait(q).unwrap(); };
                    drop(q); t();
                }
            }).expect("io: spawn"));
    }
    unsafe { IO = Some(inner); IO_HANDLES = Some(h); }
}

// ═══════════════════════════════════════════════════════════════════════
//  Compute backends (cfg-gated)
// ═══════════════════════════════════════════════════════════════════════

#[cfg(fibre_backend = "marl")]
mod compute {
    use core::ffi::c_void;
    unsafe extern "C" {
        fn marl_scheduler_init(n: i32, stack: usize);
        fn marl_bind();
        fn marl_schedule(f: unsafe extern "C" fn(*mut u8), arg: *mut u8);
        fn marl_wg_create(i: i32) -> *mut c_void;
        fn marl_wg_destroy(w: *mut c_void);
        fn marl_wg_add(w: *mut c_void, d: i32);
        fn marl_wg_done(w: *mut c_void) -> bool;
        fn marl_wg_wait(w: *mut c_void);
        fn marl_wg_clone(w: *mut c_void) -> *mut c_void;
        fn marl_event_create(m: i32, i: i32) -> *mut c_void;
        fn marl_event_destroy(e: *mut c_void);
        fn marl_event_signal(e: *mut c_void);
        fn marl_event_wait(e: *mut c_void);
        fn marl_event_is_signalled(e: *mut c_void) -> bool;
    }
    unsafe extern "C" fn thunk(a: *mut u8) { let c: Box<Box<dyn FnOnce() + Send>> = Box::from_raw(a as *mut Box<dyn FnOnce() + Send>); c(); }
    pub fn init(n: i32) { unsafe { marl_scheduler_init(n, 0); marl_bind(); } }
    pub fn schedule(f: Box<dyn FnOnce() + Send + 'static>, _priority: u8) {
        let b: Box<Box<dyn FnOnce() + Send>> = Box::new(f);
        unsafe { marl_schedule(thunk, Box::into_raw(b) as *mut u8); }
    }
    pub struct WaitGroup { inner: *mut c_void }
    unsafe impl Send for WaitGroup {} unsafe impl Sync for WaitGroup {}
    impl WaitGroup {
        pub fn new(i: i32) -> Self { Self { inner: unsafe { marl_wg_create(i) } } }
        pub fn add(&self, d: i32) { unsafe { marl_wg_add(self.inner, d); } }
        pub fn done(&self) { unsafe { marl_wg_done(self.inner); } }
        pub fn wait(&self) { unsafe { marl_wg_wait(self.inner); } }
    }
    impl Clone for WaitGroup { fn clone(&self) -> Self { Self { inner: unsafe { marl_wg_clone(self.inner) } } } }
    impl Drop for WaitGroup { fn drop(&mut self) { unsafe { marl_wg_destroy(self.inner); } } }
    pub struct Event { inner: *mut c_void }
    unsafe impl Send for Event {} unsafe impl Sync for Event {}
    impl Event {
        pub fn new(m: bool) -> Self { Self { inner: unsafe { marl_event_create(m as i32, 0) } } }
        pub fn signal(&self) { unsafe { marl_event_signal(self.inner); } }
        pub fn wait(&self) { unsafe { marl_event_wait(self.inner); } }
        pub fn is_signalled(&self) -> bool { unsafe { marl_event_is_signalled(self.inner) } }
    }
    impl Drop for Event { fn drop(&mut self) { unsafe { marl_event_destroy(self.inner); } } }
}

#[cfg(not(fibre_backend = "marl"))]
mod compute {
    use super::*;
    static mut Q: Option<Arc<Vec<(Mutex<VecDeque<Box<dyn FnOnce() + Send + 'static>>>, Condvar)>>> = None;

    pub fn init(n: i32) {
        let n = n.max(1) as usize;
        let p: Arc<_> = Arc::new((0..n).map(|_| (Mutex::new(VecDeque::new()), Condvar::new())).collect());
        for id in 0..n {
            let c = p.clone();
            thread::Builder::new().name(format!("fw-{id}"))
                .spawn(move || loop {
                    let mut q = c[id].0.lock().unwrap();
                    let t = loop { if let Some(t) = q.pop_front() { break t; } q = c[id].1.wait(q).unwrap(); };
                    drop(q); t();
                }).expect("work spawn");
        }
        unsafe { Q = Some(p); }
    }
    pub fn schedule(f: Box<dyn FnOnce() + Send + 'static>, _priority: u8) {
        unsafe { if let Some(ref q) = Q { q[0].0.lock().unwrap().push_back(f); q[0].1.notify_one(); } }
    }
    pub struct WaitGroup { inner: Arc<(Mutex<i32>, Condvar)> }
    impl WaitGroup {
        pub fn new(i: i32) -> Self { Self { inner: Arc::new((Mutex::new(i), Condvar::new())) } }
        pub fn add(&self, d: i32) { *self.inner.0.lock().unwrap() += d; }
        pub fn done(&self) { let mut c = self.inner.0.lock().unwrap(); *c -= 1; if *c <= 0 { self.inner.1.notify_all(); } }
        pub fn wait(&self) { let mut c = self.inner.0.lock().unwrap(); while *c > 0 { c = self.inner.1.wait(c).unwrap(); } }
    }
    impl Clone for WaitGroup { fn clone(&self) -> Self { Self { inner: self.inner.clone() } } }
    pub struct Event { inner: Arc<(Mutex<bool>, Condvar)>, manual: bool }
    impl Event {
        pub fn new(m: bool) -> Self { Self { inner: Arc::new((Mutex::new(false), Condvar::new())), manual: m } }
        pub fn signal(&self) { *self.inner.0.lock().unwrap() = true; self.inner.1.notify_all(); }
        pub fn wait(&self) { let mut s = self.inner.0.lock().unwrap(); while !*s { s = self.inner.1.wait(s).unwrap(); } if !self.manual { *s = false; } }
        pub fn is_signalled(&self) -> bool { *self.inner.0.lock().unwrap() }
    }
}

// ═══════════════════════════════════════════════════════════════════════
//  Public API
// ═══════════════════════════════════════════════════════════════════════

pub fn init(num_workers: u32) -> bool {
    let _lock = INIT_LOCK.lock().unwrap();
    if INITIALIZED.swap(true, Ordering::SeqCst) { return false; }

    let n = (if num_workers > 0 { num_workers as i32 }
             else { thread::available_parallelism().map(|c| c.get() as i32).unwrap_or(4) }).max(1);
    compute::init(n);
    io_init(4);
    true
}

pub fn is_initialized() -> bool { INITIALIZED.load(Ordering::Relaxed) }

pub fn schedule<F: FnOnce() + Send + 'static>(f: F, _priority: u8) {
    assert!(INITIALIZED.load(Ordering::Relaxed), "job: init required");
    compute::schedule(Box::new(f), 0);
}

pub fn blocking_call<F: FnOnce() -> R + Send + 'static, R: Send + 'static>(f: F) -> R {
    assert!(INITIALIZED.load(Ordering::Relaxed), "job: init required");
    io_call(f)
}

pub fn read_async(path: &str) -> crate::io::Result<Vec<u8>> {
    assert!(INITIALIZED.load(Ordering::Relaxed), "job: init required");
    let p = alloc::string::String::from(path);
    io_call(move || crate::fs::read(p.as_str()))
}

pub use compute::{WaitGroup, Event};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::Arc;
    use crate::sync::atomic::{AtomicBool, Ordering};
    use crate::time::Duration;
    fn e() { if !INITIALIZED.load(Ordering::Relaxed) { init(2); } }

    #[test] fn schedule_works() { e(); let f = Arc::new(AtomicBool::new(false)); let f2 = f.clone(); schedule(move || f2.store(true, Ordering::SeqCst)); thread::sleep(Duration::from_millis(100)); assert!(f.load(Ordering::SeqCst)); }
    #[test] fn blocking_call_works() { e(); assert_eq!(blocking_call(|| 42), 42); }
    #[test] fn read_async_works() { e(); let r = read_async("Cargo.toml").unwrap(); assert!(!r.is_empty()); }
    #[test] fn read_async_not_found() { e(); assert!(read_async("/nonexistent_file_12345").is_err()); }
    #[test] fn waitgroup_works() { e(); let wg = WaitGroup::new(1); let f = Arc::new(AtomicBool::new(false)); let f2 = f.clone(); let w = wg.clone(); schedule(move || { f2.store(true, Ordering::SeqCst); w.done(); }); wg.wait(); assert!(f.load(Ordering::SeqCst)); }
    #[test] fn event_works() { e(); let ev = Arc::new(Event::new(false)); let ev2 = ev.clone(); let f = Arc::new(AtomicBool::new(false)); let f2 = f.clone(); schedule(move || { f2.store(true, Ordering::SeqCst); ev2.signal(); }); ev.wait(); assert!(f.load(Ordering::SeqCst)); }
}
