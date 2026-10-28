/*
 * net.h - small socket helpers shared by the servers.
 */
#ifndef KV_NET_H
#define KV_NET_H

#include <stddef.h>

/* Create a TCP listening socket bound to addr:port (IPv4 dotted quad).
 * The socket is non-blocking and close-on-exec. Returns fd or -1. */
int net_listen(const char *addr, int port, int backlog);

/* Write all n bytes to a blocking socket, retrying on partial writes and
 * EINTR. Never raises SIGPIPE. Returns 0, or -1 with errno set. */
int net_send_all(int fd, const char *p, size_t n);

int net_set_nonblock(int fd);
void net_set_nodelay(int fd);

/* Parse a decimal integer in [min, max]; returns -1 if invalid. */
long net_parse_long(const char *s, long min, long max);

#endif
