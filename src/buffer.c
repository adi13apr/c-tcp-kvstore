#include "buffer.h"

#include <stdlib.h>
#include <string.h>

#define BUF_MIN_CAP 256

void buf_init(buf_t *b)
{
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
}

void buf_free(buf_t *b)
{
    free(b->data);
    buf_init(b);
}

int buf_reserve(buf_t *b, size_t extra)
{
    if (b->cap - b->len >= extra)
        return 0;
    if (extra > (size_t)-1 / 2 - b->len)
        return -1;

    size_t need = b->len + extra;
    size_t cap = b->cap ? b->cap : BUF_MIN_CAP;
    while (cap < need)
        cap *= 2;

    char *p = realloc(b->data, cap);
    if (!p)
        return -1;
    b->data = p;
    b->cap = cap;
    return 0;
}

int buf_append(buf_t *b, const void *p, size_t n)
{
    if (n == 0)
        return 0;
    if (buf_reserve(b, n) < 0)
        return -1;
    memcpy(b->data + b->len, p, n);
    b->len += n;
    return 0;
}

void buf_consume(buf_t *b, size_t n)
{
    if (n >= b->len) {
        b->len = 0;
        return;
    }
    memmove(b->data, b->data + n, b->len - n);
    b->len -= n;
}
