/*
 * threadpool.h - fixed-size worker pool fed by a bounded FIFO work queue.
 *
 * The queue is a ring buffer guarded by one mutex; idle workers sleep on a
 * condition variable until a job arrives or the pool is shut down.
 */
#ifndef KV_THREADPOOL_H
#define KV_THREADPOOL_H

#include <stddef.h>

typedef void (*tp_fn)(void *arg);
typedef struct threadpool threadpool_t;

/* Start nthreads workers with room for queue_cap pending jobs. Workers run
 * with all signals blocked. Returns NULL on failure. */
threadpool_t *tp_create(size_t nthreads, size_t queue_cap);

/* Enqueue fn(arg) without blocking. Returns 0, or -1 if the queue is full or
 * the pool is shutting down (the caller still owns arg). */
int tp_submit(threadpool_t *p, tp_fn fn, void *arg);

/* Number of jobs waiting (not yet picked up by a worker). */
size_t tp_pending(threadpool_t *p);

/* Refuse new jobs, let workers drain every queued job, join them, and free
 * the pool. */
void tp_shutdown(threadpool_t *p);

#endif
