// marl_c_api.cpp — C wrapper implementation.
#include "marl_c_api.h"

#include <marl/debug.h>
#include <marl/scheduler.h>
#include <marl/task.h>
#include <marl/waitgroup.h>
#include <marl/event.h>
#include <marl/blockingcall.h>
#include <marl/parallelize.h>

#include <atomic>
#include <cstdlib>
#include <new>

// Global scheduler pointer, so we can bind() from any thread.
static std::atomic<marl::Scheduler*> g_scheduler{nullptr};

// ─── Scheduler ────────────────────────────────────────────────────────────

int marl_scheduler_init(int num_worker_threads, size_t fiber_stack_size) {
    // Guard: prevent double initialization.
    if (g_scheduler.load(std::memory_order_acquire) != nullptr) {
        return 0;
    }

    auto cfg = marl::Scheduler::Config::allCores();
    if (num_worker_threads > 0) {
        cfg.setWorkerThreadCount(num_worker_threads);
    }
    if (fiber_stack_size > 0) {
        cfg.setFiberStackSize(fiber_stack_size);
    }
    auto* s = new marl::Scheduler(cfg);
    g_scheduler.store(s, std::memory_order_release);
    s->bind();
    return 1;
}

void marl_scheduler_shutdown(void) {
    auto* s = g_scheduler.exchange(nullptr, std::memory_order_acq_rel);
    if (s) {
        marl::Scheduler::unbind();
        delete s;
    }
}

int marl_scheduler_is_bound(void) {
    return (marl::Scheduler::get() != nullptr) ? 1 : 0;
}

void marl_bind(void) {
    auto* s = g_scheduler.load(std::memory_order_acquire);
    if (s && marl::Scheduler::get() == nullptr) {
        s->bind();
    }
}

void marl_unbind(void) {
    marl::Scheduler::unbind();
}

// ─── Schedule ─────────────────────────────────────────────────────────────

void marl_schedule(marl_task_fn fn, void* user_data) {
    marl::schedule([=]() {
        fn(user_data);
    });
}

void marl_schedule_ex(marl_task_fn fn, void* user_data, int flags) {
    marl::Task::Flags f = marl::Task::Flags::None;
    if (flags & 1) {
        f = marl::Task::Flags::SameThread;
    }
    marl::Task task([=]() { fn(user_data); }, f);
    marl::schedule(std::move(task));
}

// ─── WaitGroup ────────────────────────────────────────────────────────────
//
// marl::WaitGroup has a const member (shared_ptr), which makes it
// non-copy-assignable. We heap-allocate the WaitGroup and store a
// pointer to avoid the issue.

struct marl_wait_group {
    marl::WaitGroup* impl;
};

marl_wait_group_t* marl_wg_create(int initial_count) {
    auto* wg = new marl_wait_group_t();
    wg->impl = new marl::WaitGroup(static_cast<unsigned int>(initial_count));
    return wg;
}

void marl_wg_destroy(marl_wait_group_t* wg) {
    delete wg->impl;
    delete wg;
}

void marl_wg_add(marl_wait_group_t* wg, int delta) {
    wg->impl->add(static_cast<unsigned int>(delta));
}

bool marl_wg_done(marl_wait_group_t* wg) {
    return wg->impl->done();
}

void marl_wg_wait(marl_wait_group_t* wg) {
    wg->impl->wait();
}

marl_wait_group_t* marl_wg_clone(marl_wait_group_t* wg) {
    auto* clone = new marl_wait_group_t();
    clone->impl = new marl::WaitGroup(*wg->impl); // copy constructor (shared_ptr bump)
    return clone;
}

// ─── Event ────────────────────────────────────────────────────────────────
//
// Same issue as WaitGroup: const shared_ptr member.

struct marl_event {
    marl::Event* impl;
};

marl_event_t* marl_event_create(int manual_reset, int initial_state) {
    auto mode = manual_reset ? marl::Event::Mode::Manual : marl::Event::Mode::Auto;
    auto* e = new marl_event_t();
    e->impl = new marl::Event(mode, initial_state != 0);
    return e;
}

void marl_event_destroy(marl_event_t* e) {
    delete e->impl;
    delete e;
}

void marl_event_wait(marl_event_t* e) {
    e->impl->wait();
}

bool marl_event_try_wait(marl_event_t* e, int64_t timeout_ns) {
    if (timeout_ns < 0) {
        e->impl->wait();
        return true;
    }
    return e->impl->wait_for(std::chrono::nanoseconds(timeout_ns));
}

void marl_event_signal(marl_event_t* e) {
    e->impl->signal();
}

void marl_event_clear(marl_event_t* e) {
    e->impl->clear();
}

bool marl_event_is_signalled(marl_event_t* e) {
    return e->impl->isSignalled();
}

// ─── Blocking Call ────────────────────────────────────────────────────────

void marl_blocking_call(marl_task_fn fn, void* user_data) {
    marl::blocking_call([=]() {
        fn(user_data);
    });
}

// ─── Parallelize ──────────────────────────────────────────────────────────

void marl_parallelize(marl_task_fn* tasks, void** tasks_arg, int count) {
    if (count <= 0) return;
    if (count == 1) {
        tasks[0](tasks_arg[0]);
        return;
    }

    marl::WaitGroup wg(static_cast<unsigned int>(count - 1));
    for (int i = 1; i < count; i++) {
        marl_task_fn fn = tasks[i];
        void* arg = tasks_arg[i];
        marl::schedule([=, &wg]() {
            fn(arg);
            wg.done();
        });
    }
    tasks[0](tasks_arg[0]);
    wg.wait();
}
