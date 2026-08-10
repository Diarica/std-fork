//! Fibre Engine extension: job system.
//!
//! Two-tier architecture:
//!   Compute — Marl fibers (PC) / pure Rust work-stealing (consoles)
//!   I/O     — 4-thread fixed pool for schedule_io / blocking_call
//!
//! `blocking_call` does NOT create a thread per call:
//! it uses the shared I/O pool. This is the same pool `schedule_io` uses.
//!
//! 异步读文件不提供 `read_async` 包装：用 `schedule_io` 投递闭包，闭包内自行
//! `std::fs::read`（fire-and-forget，无等待语义——需要结果就同步 read，不需要
//! 为 "async" 名称付线程调度开销）。

use crate::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use crate::sync::{Condvar, Mutex};
use crate::thread;
use crate::time::Duration;
use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::sync::Arc;
use alloc::vec::Vec;

static INITIALIZED: AtomicBool = AtomicBool::new(false);
static INIT_LOCK: Mutex<()> = Mutex::new(());
static IO_READY: AtomicBool = AtomicBool::new(false);

// ═══════════════════════════════════════════════════════════════════════
//  I/O Pool — 4 dedicated threads, shared by schedule_io & blocking_call
// ═══════════════════════════════════════════════════════════════════════

/// 三优先级 I/O 队列（0=URGENT, 1=NORMAL, 2=LOW——Sakura JobQueue 同款）。
/// worker 永远先取高优先级：URGENT 先行，LOW 在普通流量空闲时执行。
struct IoInner {
    queues: Mutex<[VecDeque<Box<dyn FnOnce() + Send + 'static>>; 3]>,
    cvar: Condvar,
    stop: AtomicBool,
}

static mut IO: Option<&'static IoInner> = None;
static mut IO_HANDLES: Option<Vec<thread::JoinHandle<()>>> = None;

fn io_push(task: Box<dyn FnOnce() + Send + 'static>, priority: usize) {
    unsafe {
        if let Some(io) = IO {
            io.queues.lock().unwrap()[priority].push_back(task);
            io.cvar.notify_one();
        }
    }
}

fn io_call<R: Send + 'static>(f: impl FnOnce() -> R + Send + 'static) -> R {
    let r = Arc::new(Mutex::new(None::<R>));
    let d = Arc::new((Mutex::new(false), Condvar::new()));
    let r2 = r.clone();
    let d2 = d.clone();
    io_push(Box::new(move || { *r2.lock().unwrap() = Some(f()); *d2.0.lock().unwrap() = true; d2.1.notify_one(); }), 1);
    let mut g = d.0.lock().unwrap();
    while !*g { g = d.1.wait(g).unwrap(); }
    r.lock().unwrap().take().unwrap()
}

fn io_init(n: usize) {
    let inner: &'static IoInner = Box::leak(Box::new(IoInner {
        queues: Mutex::new([VecDeque::new(), VecDeque::new(), VecDeque::new()]),
        cvar: Condvar::new(), stop: AtomicBool::new(false),
    }));
    let mut h = Vec::with_capacity(n);
    for _ in 0..n {
        let io: &'static IoInner = inner;
        h.push(thread::Builder::new().name("fibre-io".into())
            .spawn(move || {
                loop {
                    if io.stop.load(Ordering::Relaxed) { return; }
                    let mut qs = io.queues.lock().unwrap();
                    let t = 'outer: loop {
                        // 高优先级优先：0 → 1 → 2
                        for q in qs.iter_mut() {
                            if let Some(t) = q.pop_front() { break 'outer t; }
                        }
                        qs = io.cvar.wait(qs).unwrap();
                    };
                    drop(qs);
                    // M9：闭包 panic 不能杀死 worker 线程（IO 池资源/任务丢失——
                    // 任务内的 panic 是调用方错误；worker 必须存活继续服务）
                    if let Err(_) = crate::panic::catch_unwind(crate::panic::AssertUnwindSafe(t)) {
                        eprintln!("[std::job] io worker task panicked (task dropped)");
                    }
                }
            }).expect("io: spawn"));
    unsafe { IO = Some(inner); IO_HANDLES = Some(h); }
}

// ═══════════════════════════════════════════════════════════════════════
//  Compute backends (cfg-gated)
// ═══════════════════════════════════════════════════════════════════════

#[cfg(fibre_backend = "marl")]
mod compute {
    use core::ffi::c_void;
    use crate::time::Duration;
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
        fn marl_scheduler_shutdown();
        fn marl_scheduler_is_bound() -> i32;
        fn marl_unbind();
        fn marl_schedule_ex(f: unsafe extern "C" fn(*mut u8), arg: *mut u8, flags: i32);
        fn marl_event_try_wait(e: *mut c_void, timeout_ns: i64) -> bool;
        fn marl_event_clear(e: *mut c_void);
    }
    unsafe extern "C" fn thunk(a: *mut u8) { let c: Box<Box<dyn FnOnce() + Send>> = Box::from_raw(a as *mut Box<dyn FnOnce() + Send>); let _ = crate::panic::catch_unwind(crate::panic::AssertUnwindSafe(|| c())); }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn init(n: i32) { unsafe { marl_scheduler_init(n, 0); marl_bind(); } }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn shutdown() { unsafe { marl_scheduler_shutdown(); } }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn bind() { unsafe { marl_bind(); } }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn unbind() { unsafe { marl_unbind(); } }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn is_bound() -> bool { unsafe { marl_scheduler_is_bound() != 0 } }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn schedule(f: Box<dyn FnOnce() + Send + 'static>, _priority: u8) {
        let b: Box<Box<dyn FnOnce() + Send>> = Box::new(f);
        unsafe { marl_schedule(thunk, Box::into_raw(b) as *mut u8); }
    }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn schedule_same_thread(f: Box<dyn FnOnce() + Send + 'static>) {
        let b: Box<Box<dyn FnOnce() + Send>> = Box::new(f);
        unsafe { marl_schedule_ex(thunk, Box::into_raw(b) as *mut u8, 1); }
    }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub struct WaitGroup { inner: *mut c_void }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    unsafe impl Send for WaitGroup {} #[stable(feature = "fibre_job", since = "1.99.0")]
    unsafe impl Sync for WaitGroup {}
    #[stable(feature = "fibre_job", since = "1.99.0")]
    impl WaitGroup {
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn new(i: i32) -> Self { Self { inner: unsafe { marl_wg_create(i) } } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn add(&self, d: i32) { unsafe { marl_wg_add(self.inner, d); } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn done(&self) -> bool { unsafe { marl_wg_done(self.inner) } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn wait(&self) { unsafe { marl_wg_wait(self.inner); } }
    }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    impl Clone for WaitGroup { fn clone(&self) -> Self { Self { inner: unsafe { marl_wg_clone(self.inner) } } } }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    impl Drop for WaitGroup { fn drop(&mut self) { unsafe { marl_wg_destroy(self.inner); } } }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub struct Event { inner: *mut c_void }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    unsafe impl Send for Event {} #[stable(feature = "fibre_job", since = "1.99.0")]
    unsafe impl Sync for Event {}
    #[stable(feature = "fibre_job", since = "1.99.0")]
    impl Event {
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn new_with_state(manual: bool, initial: bool) -> Self { Self { inner: unsafe { marl_event_create(manual as i32, initial as i32) } } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn new(m: bool) -> Self { Self::new_with_state(m, false) }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn signal(&self) { unsafe { marl_event_signal(self.inner); } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn wait(&self) { unsafe { marl_event_wait(self.inner); } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn is_signalled(&self) -> bool { unsafe { marl_event_is_signalled(self.inner) } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn clear(&self) { unsafe { marl_event_clear(self.inner); } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn try_wait(&self, timeout: Option<Duration>) -> bool {
            let ns = timeout.map(|d| d.as_nanos() as i64).unwrap_or(0);
            unsafe { marl_event_try_wait(self.inner, ns) }
        }
    }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    impl Drop for Event { fn drop(&mut self) { unsafe { marl_event_destroy(self.inner); } } }
}

#[cfg(not(fibre_backend = "marl"))]
mod compute {
    use super::*;
    use crate::time::Duration;
    static mut Q: Option<Arc<Vec<(Mutex<VecDeque<Box<dyn FnOnce() + Send + 'static>>>, Condvar)>>> = None;

    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn init(n: i32) {
        let n = n.max(1) as usize;
        let p: Arc<_> = Arc::new((0..n).map(|_| (Mutex::new(VecDeque::<Box<dyn FnOnce() + Send + 'static>>::new()), Condvar::new())).collect::<Vec<_>>());
        for id in 0..n {
            let c = p.clone();
            thread::Builder::new().name(format!("fw-{id}"))
                .spawn(move || loop {
                    let mut q = c[id].0.lock().unwrap();
                    let t = loop { if let Some(t) = q.pop_front() { break t; } q = c[id].1.wait(q).unwrap(); };
                    drop(q); let _ = crate::panic::catch_unwind(crate::panic::AssertUnwindSafe(t));
                }).expect("work spawn");
        }
        unsafe { Q = Some(p); }
    }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn shutdown() {}
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn bind() {}
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn unbind() {}
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn is_bound() -> bool { true }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn schedule(f: Box<dyn FnOnce() + Send + 'static>, _priority: u8) {
        unsafe { if let Some(ref q) = Q { q[0].0.lock().unwrap().push_back(f); q[0].1.notify_one(); } }
    }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub fn schedule_same_thread(f: Box<dyn FnOnce() + Send + 'static>) { schedule(f, 0) }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub struct WaitGroup { inner: Arc<(Mutex<i32>, Condvar)> }
    impl WaitGroup {
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn new(i: i32) -> Self { Self { inner: Arc::new((Mutex::new(i), Condvar::new())) } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn add(&self, d: i32) { *self.inner.0.lock().unwrap() += d; }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn done(&self) -> bool { let mut c = self.inner.0.lock().unwrap(); *c -= 1; if *c <= 0 { self.inner.1.notify_all(); true } else { false } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn wait(&self) { let mut c = self.inner.0.lock().unwrap(); while *c > 0 { c = self.inner.1.wait(c).unwrap(); } }
    }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    impl Clone for WaitGroup { fn clone(&self) -> Self { Self { inner: self.inner.clone() } } }
    #[stable(feature = "fibre_job", since = "1.99.0")]
    pub struct Event { inner: Arc<(Mutex<bool>, Condvar)>, manual: bool }
    impl Event {
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn new_with_state(manual: bool, initial: bool) -> Self { Self { inner: Arc::new((Mutex::new(initial), Condvar::new())), manual } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn new(m: bool) -> Self { Self::new_with_state(m, false) }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn signal(&self) { *self.inner.0.lock().unwrap() = true; self.inner.1.notify_all(); }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn wait(&self) { let mut s = self.inner.0.lock().unwrap(); while !*s { s = self.inner.1.wait(s).unwrap(); } if !self.manual { *s = false; } }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn is_signalled(&self) -> bool { *self.inner.0.lock().unwrap() }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn clear(&self) { *self.inner.0.lock().unwrap() = false; }
        #[stable(feature = "fibre_job", since = "1.99.0")]
        pub fn try_wait(&self, timeout: Option<Duration>) -> bool {
            let mut s = self.inner.0.lock().unwrap();
            if *s { return true; }
            match timeout {
                Some(d) => { let (g, _) = self.inner.1.wait_timeout(s, d).unwrap(); *g }
                None => false,
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
//  Public API
// ═══════════════════════════════════════════════════════════════════════

#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn init(num_workers: u32) -> bool {
    let _lock = INIT_LOCK.lock().unwrap();
    if INITIALIZED.swap(true, Ordering::SeqCst) { return false; }

    let n = (if num_workers > 0 { num_workers as i32 }
             else { thread::available_parallelism().map(|c| c.get() as i32).unwrap_or(4) }).max(1);
    compute::init(n);
    io_init(4);
    IO_READY.store(true, Ordering::SeqCst);
    true
}

#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn is_initialized() -> bool { INITIALIZED.load(Ordering::Relaxed) }

#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn schedule<F: FnOnce() + Send + 'static>(f: F, _priority: u8) {
    assert!(INITIALIZED.load(Ordering::Relaxed), "job: init required");
    compute::schedule(Box::new(f), 0);
}

#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn shutdown() { compute::shutdown(); }
#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn bind() { compute::bind(); }
#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn unbind() { compute::unbind(); }
#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn is_bound() -> bool { compute::is_bound() }
#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn schedule_same_thread<F: FnOnce() + Send + 'static>(f: F) {
    assert!(INITIALIZED.load(Ordering::Relaxed), "job: init required");
    compute::schedule_same_thread(Box::new(f));
}
#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn blocking_call<F: FnOnce() -> R + Send + 'static, R: Send + 'static>(f: F) -> R {
    assert!(INITIALIZED.load(Ordering::Relaxed), "job: init required");
    io_call(f)
}

/// 启动专用 I/O 池（4 线程，"fibre-io"）。幂等：重复调用无副作用。
///
/// 与 [`init`] 不同，`init_io` 只初始化 I/O 池，不初始化 compute 调度器——
/// 供已自行管理 compute（如 fibre-job 的 marl 后端）的宿主使用，避免双计算池。
#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn init_io() {
    let _lock = INIT_LOCK.lock().unwrap();
    if IO_READY.swap(true, Ordering::SeqCst) { return; }
    io_init(4);
}

/// 将闭包投递到专用 I/O 池线程执行（fire-and-forget，不等待结果）。
///
/// I/O 池与计算调度器隔离：阻塞 I/O（磁盘读、网络等）在固定 4 线程池上执行，
/// 不占用计算线程。完成回调需自行安排（如 [`crate::job::dispatch_main`] 之类的桥）。
#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn schedule_io<F: FnOnce() + Send + 'static>(f: F) {
    assert!(IO_READY.load(Ordering::Relaxed), "job: init_io required");
    io_push(Box::new(f), 1); // NORMAL
}

/// URGENT 优先级 I/O：玩家触发的关键加载（ref_package）——插队到普通流量之前。
#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn schedule_io_urgent<F: FnOnce() + Send + 'static>(f: F) {
    assert!(IO_READY.load(Ordering::Relaxed), "job: init_io required");
    io_push(Box::new(f), 0); // URGENT
}

/// LOW 优先级 I/O：预加载/流送（非关键）——普通流量空闲时执行。
#[stable(feature = "fibre_job", since = "1.99.0")]
pub fn schedule_io_low<F: FnOnce() + Send + 'static>(f: F) {
    assert!(IO_READY.load(Ordering::Relaxed), "job: init_io required");
    io_push(Box::new(f), 2); // LOW
}

#[stable(feature = "fibre_job", since = "1.99.0")]
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
    #[test] fn waitgroup_works() { e(); let wg = WaitGroup::new(1); let f = Arc::new(AtomicBool::new(false)); let f2 = f.clone(); let w = wg.clone(); schedule(move || { f2.store(true, Ordering::SeqCst); w.done(); }); wg.wait(); assert!(f.load(Ordering::SeqCst)); }
    #[test] fn event_works() { e(); let ev = Arc::new(Event::new(false)); let ev2 = ev.clone(); let f = Arc::new(AtomicBool::new(false)); let f2 = f.clone(); schedule(move || { f2.store(true, Ordering::SeqCst); ev2.signal(); }); ev.wait(); assert!(f.load(Ordering::SeqCst)); }
}
