#include "threadpool.h"

#include <pthread.h>
#include <signal.h>
#include <stdlib.h>

typedef struct {
    tp_fn fn;
    void *arg;
} job_t;

struct threadpool {
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    job_t *ring;
    size_t cap;
    size_t head;  /* index of the oldest job */
    size_t count; /* jobs in the ring */
    int stopping;
    pthread_t *threads;
    size_t nthreads;
};

static void *worker_main(void *arg)
{
    threadpool_t *p = arg;

    for (;;) {
        pthread_mutex_lock(&p->lock);
        while (p->count == 0 && !p->stopping)
            pthread_cond_wait(&p->not_empty, &p->lock);
        if (p->count == 0) { /* stopping and fully drained */
            pthread_mutex_unlock(&p->lock);
            return NULL;
        }
        job_t job = p->ring[p->head];
        p->head = (p->head + 1) % p->cap;
        p->count--;
        pthread_mutex_unlock(&p->lock);

        job.fn(job.arg);
    }
}

static void join_all(threadpool_t *p, size_t n)
{
    pthread_mutex_lock(&p->lock);
    p->stopping = 1;
    pthread_cond_broadcast(&p->not_empty);
    pthread_mutex_unlock(&p->lock);
    for (size_t i = 0; i < n; i++)
        pthread_join(p->threads[i], NULL);
}

static void destroy(threadpool_t *p)
{
    pthread_cond_destroy(&p->not_empty);
    pthread_mutex_destroy(&p->lock);
    free(p->threads);
    free(p->ring);
    free(p);
}

threadpool_t *tp_create(size_t nthreads, size_t queue_cap)
{
    if (nthreads == 0 || queue_cap == 0)
        return NULL;
    threadpool_t *p = calloc(1, sizeof *p);
    if (!p)
        return NULL;
    p->ring = calloc(queue_cap, sizeof *p->ring);
    p->threads = calloc(nthreads, sizeof *p->threads);
    if (!p->ring || !p->threads) {
        free(p->ring);
        free(p->threads);
        free(p);
        return NULL;
    }
    p->cap = queue_cap;
    pthread_mutex_init(&p->lock, NULL);
    pthread_cond_init(&p->not_empty, NULL);

    /* Threads inherit the creator's signal mask: block everything so async
     * signals are only ever handled by the thread that asked for them. */
    sigset_t all, old;
    sigfillset(&all);
    pthread_sigmask(SIG_SETMASK, &all, &old);
    size_t started = 0;
    for (; started < nthreads; started++)
        if (pthread_create(&p->threads[started], NULL, worker_main, p) != 0)
            break;
    pthread_sigmask(SIG_SETMASK, &old, NULL);

    if (started < nthreads) {
        join_all(p, started);
        destroy(p);
        return NULL;
    }
    p->nthreads = nthreads;
    return p;
}

int tp_submit(threadpool_t *p, tp_fn fn, void *arg)
{
    int rc = -1;
    pthread_mutex_lock(&p->lock);
    if (!p->stopping && p->count < p->cap) {
        p->ring[(p->head + p->count) % p->cap] = (job_t){ fn, arg };
        p->count++;
        pthread_cond_signal(&p->not_empty);
        rc = 0;
    }
    pthread_mutex_unlock(&p->lock);
    return rc;
}

size_t tp_pending(threadpool_t *p)
{
    pthread_mutex_lock(&p->lock);
    size_t n = p->count;
    pthread_mutex_unlock(&p->lock);
    return n;
}

void tp_shutdown(threadpool_t *p)
{
    if (!p)
        return;
    join_all(p, p->nthreads);
    destroy(p);
}
