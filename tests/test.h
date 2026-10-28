/* Minimal test harness shared by the C unit tests. */
#ifndef KV_TEST_H
#define KV_TEST_H

#include <stdio.h>

static int test_failures;
static int test_checks;

#define CHECK(cond)                                                       \
    do {                                                                  \
        test_checks++;                                                    \
        if (!(cond)) {                                                    \
            test_failures++;                                              \
            fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                 \
    } while (0)

#define RUN(fn)                                                           \
    do {                                                                  \
        int before_ = test_failures;                                      \
        fn();                                                             \
        printf("  %-28s %s\n", #fn, test_failures == before_ ? "ok" : "FAILED"); \
    } while (0)

static inline int test_summary(const char *name)
{
    printf("%s: %d checks, %d failures\n", name, test_checks, test_failures);
    return test_failures ? 1 : 0;
}

#endif
