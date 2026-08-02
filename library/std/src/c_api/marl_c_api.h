// marl_c_api.h — C wrapper around Google's Marl C++ fiber-based task scheduler.
// Licensed under Apache 2.0.
//
// This is the minimal C API needed by the fibre Engine's job system.
// Marl is at: https://github.com/Diarica/marl

#ifndef MARL_C_API_H
#define MARL_C_API_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// ─── Task function signature ──────────────────────────────────────────────
// A task is a function pointer + opaque user data.
typedef void (*marl_task_fn)(void* user_data);

// ─── Scheduler ────────────────────────────────────────────────────────────
// Initialize the global scheduler with the given number of worker threads.
// Returns 1 on success, 0 on failure.
int marl_scheduler_init(int num_worker_threads, size_t fiber_stack_size);

// Shut down the scheduler. Blocks until all tasks complete.
void marl_scheduler_shutdown(void);

// Bind this thread to the scheduler (required before schedule() on that thread).
void marl_bind(void);

// Unbind this thread from the scheduler.
void marl_unbind(void);

// Returns 1 if a scheduler is bound to this thread, 0 otherwise.
int marl_scheduler_is_bound(void);

// Schedule ─────────────────────────────────────────────────────────────
// Queue a task for asynchronous execution.
void marl_schedule(marl_task_fn fn, void* user_data);

// Queue a task with additional flags (e.g. SameThread).
// flags: 0 = None, 1 = SameThread
void marl_schedule_ex(marl_task_fn fn, void* user_data, int flags);

// ─── WaitGroup ────────────────────────────────────────────────────────────
// Fiber-aware counter-based synchronization primitive.
typedef struct marl_wait_group marl_wait_group_t;

marl_wait_group_t* marl_wg_create(int initial_count);
void marl_wg_destroy(marl_wait_group_t* wg);
void marl_wg_add(marl_wait_group_t* wg, int delta);
bool marl_wg_done(marl_wait_group_t* wg);
void marl_wg_wait(marl_wait_group_t* wg);

// Returns a new handle pointing to the same underlying WaitGroup.
marl_wait_group_t* marl_wg_clone(marl_wait_group_t* wg);

// ─── Event ────────────────────────────────────────────────────────────────
// Fiber-aware event for signaling between tasks.
typedef struct marl_event marl_event_t;

marl_event_t* marl_event_create(int manual_reset, int initial_state);
void marl_event_destroy(marl_event_t* e);
void marl_event_wait(marl_event_t* e);
bool marl_event_try_wait(marl_event_t* e, int64_t timeout_ns);
void marl_event_signal(marl_event_t* e);
void marl_event_clear(marl_event_t* e);
bool marl_event_is_signalled(marl_event_t* e);

// ─── Blocking Call ────────────────────────────────────────────────────────
// Run a blocking function on a dedicated thread, yielding the current fiber
// to other tasks until the function returns.
void marl_blocking_call(marl_task_fn fn, void* user_data);

// ─── Parallelize ──────────────────────────────────────────────────────────
// Run count tasks in parallel and wait for all to complete.
// tasks[i] receives tasks_arg[i] as user_data. Both arrays must be length
// count. The first task (index 0) runs on the calling thread; the rest are
// scheduled.
void marl_parallelize(marl_task_fn* tasks, void** tasks_arg, int count);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // MARL_C_API_H
