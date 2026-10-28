/* Unit tests for the fixed-size thread pool. */
#include "threadpool.h"
#include "test.h"

#include <stdatomic.h>
#include <unistd.h>

static atomic_int done;

static void count_job(void *arg)
{
    (void)arg;
    atomic_fetch_add(&done, 1);
}

static void slow_job(void *arg)
{
    (void)arg;
    usleep(20000);
    atomic_fetch_add(&done, 1);
}

static void test_runs_all_jobs(void)
{
    atomic_store(&done, 0);
    threadpool_t *p = tp_create(4, 10000);
    CHECK(p != NULL);
    for (int i = 0; i < 10000; i++)
        CHECK(tp_submit(p, count_job, NULL) == 0);
    tp_shutdown(p);
    CHECK(atomic_load(&done) == 10000);
}

static void test_full_queue_rejects_and_shutdown_drains(void)
{
    atomic_store(&done, 0);
    threadpool_t *p = tp_create(1, 3);
    int accepted = 0;
    /* at most one running + three queued; the rest must be refused */
    for (int i = 0; i < 10; i++)
        accepted += tp_submit(p, slow_job, NULL) == 0;
    CHECK(accepted >= 3 && accepted <= 4);
    CHECK(tp_pending(p) <= 3);
    tp_shutdown(p); /* must wait for every accepted job */
    CHECK(atomic_load(&done) == accepted);
}

int main(void)
{
    RUN(test_runs_all_jobs);
    RUN(test_full_queue_rejects_and_shutdown_drains);
    return test_summary("test_threadpool");
}
