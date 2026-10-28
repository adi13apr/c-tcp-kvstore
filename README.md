# c-tcp-kvstore

A multithreaded in-memory key-value server in C11 (Linux, pthreads), built
twice over the same storage and protocol code:

| Binary         | Concurrency model                                                         |
|----------------|---------------------------------------------------------------------------|
| `server`       | Fixed-size **thread pool** + bounded work queue (mutex + condition variable); blocking I/O, one worker per connection |
| `epoll_server` | **Event-driven**: level-triggered `epoll` loop(s) with non-blocking sockets; 1 loop by default, N with `-t N` |
| `client`       | C load generator: N connections, GET/SET mix, optional pipelining; reports req/s, latency percentiles and time-to-first-reply |

Both servers share a thread-safe hash table (striped read-write locks), a
line protocol module, partial-read/partial-write handling, SIGPIPE
immunity, and a graceful SIGINT/SIGTERM shutdown. Both run clean under valgrind
(zero leaks of any kind, including still-reachable) and ThreadSanitizer.

```
src/
  hashtable.[ch]    striped-rwlock chained hash table with online resize
  protocol.[ch]     request framing, parsing and execution (shared by both servers)
  buffer.[ch]       growable byte buffer
  net.[ch]          listen socket, send_all, small socket helpers
  threadpool.[ch]   fixed worker pool, bounded ring-buffer queue, mutex + condvar
  server.c          thread-pool server
  epoll_server.c    epoll server
  client.c          load-test client
tests/
  test_*.c          unit tests (hash table incl. concurrent resize, protocol, pool)
  functional_test.py  end-to-end tests run against either server
  valgrind_check.sh   everything under valgrind, incl. SIGINT during load
bench/
  run_bench.py      benchmark matrix -> Markdown table
  results.md        raw output of the run quoted below
```

## Architecture

### Thread-pool server (`server`)

```
                              +------------------------------------------------+
  clients ---TCP---> listen fd |  MAIN THREAD (acceptor)                        |
                               |   poll([signalfd, listen fd])                  |
  SIGINT/SIGTERM --> signalfd  |     |                                          |
                               |     | accept4() -> TCP_NODELAY, SO_RCV/SNDTIMEO |
                               |     v                                          |
                               |   tp_submit(fd) --full--> "ERR server busy",   |
                               |     |                     close (load shedding)|
                               |     v                                          |
                               |  +------------------------------------------+  |
                               |  | bounded ring buffer of jobs (FIFO)       |  |
                               |  | pthread_mutex_t lock                     |  |
                               |  | pthread_cond_t  not_empty                |  |
                               |  +------------------------------------------+  |
                               |     | pop (pthread_cond_wait while empty)      |
                               |     +-----------+-----------+-----------+      |
                               |     v           v           v           v      |
                               |  worker 0    worker 1     ...     worker N-1   |
                               |  each owns ONE connection until it closes:     |
                               |  blocking recv -> proto_process -> send_all    |
                               |     |                                          |
                               |     |  active-connection registry (mutex):     |
                               |     |  on shutdown, shutdown(fd, SHUT_RD)      |
                               |     |  wakes every blocked recv()              |
                               |     +--------------------+                     |
                               |                          v                     |
                               |   +-----------------------------------------+  |
                               |   | hash table: buckets[], 64 striped       |  |
                               |   | rwlocks, bucket i -> stripe i & 63      |  |
                               |   +-----------------------------------------+  |
                               +------------------------------------------------+
```

### Epoll server (`epoll_server -t N`)

```
  SIGINT/SIGTERM --> MAIN THREAD: sigwait() --write--> stop eventfd (watched by every loop)

  +------------------- loop 0 -------------------+     +------------ loop k (1..N-1) ------------+
  | epoll_wait over:                             |     | epoll_wait over:                        |
  |   listen fd (non-blocking)                   |     |   stop eventfd                          |
  |   stop eventfd, own wake eventfd             |     |   own wake eventfd                      |
  |   its own connections                        |     |   its own connections                   |
  |                                              |     |                                         |
  | accept4() --round robin--+-> conn_add(self)  |     |   wake -> drain inbox -> conn_add()     |
  |                          +-> inbox[k] (mutex)| ==> |                                         |
  |                              + write(wake k) |     |                                         |
  +----------------------------------------------+     +-----------------------------------------+
           per connection, on one loop for its whole life (no locking of connection state):

    recv (one read per EPOLLIN) -> in buf -> proto_process -> out buf -> send (non-blocking)
       ^                                                                    |
       |  EPOLLIN  only while peer may send AND pending output < 256 KiB    |  partial write:
       |           (backpressure for clients that don't read)               |  keep offset,
       +--- EPOLLOUT only while output is pending <-------------------------+  enable EPOLLOUT

           all loops ---> shared hash table (same striped-rwlock table as above)
```

## Build and run

Requirements: Linux, gcc or clang with C11, GNU make, Python 3 (tests and
bench only), valgrind (for `make valgrind`).

```sh
make                 # builds ./server ./epoll_server ./client (all targets)
make server          # individual targets
make epoll_server
make client

make test            # unit tests + functional tests against both servers
make valgrind        # everything under valgrind; fails on any error or leak
make tsan            # ThreadSanitizer builds + the same tests
make bench           # benchmark matrix (needs an otherwise idle machine)
make clean
```

Run a server (Ctrl-C shuts it down gracefully):

```sh
./server       -p 7379 -t 8 -q 1024 -i 60   # 8 workers, queue 1024, 60 s idle timeout
./epoll_server -p 7379 -t 4                 # 4 event loops
```

| Flag | `server` | `epoll_server` | Default |
|---|---|---|---|
| `-a addr` | bind address | bind address | `127.0.0.1` |
| `-p port` | TCP port | TCP port | `7379` |
| `-t n`    | worker threads | event-loop threads | 8 / 1 |
| `-q n`    | pending-connection queue capacity | - | 1024 |
| `-i sec`  | per-connection idle/send timeout (0 = none) | - | 60 |

Talk to it with any line-based tool:

```sh
$ printf 'PING\nSET greeting hello world\nGET greeting\nDEL greeting\nGET greeting\n' | nc -N 127.0.0.1 7379
PONG
OK
VALUE hello world
DELETED
NOT_FOUND
```

Load test:

```sh
./client -p 7379 -c 64 -n 10000 -P 1 -r 80 -k 100000 -d 32
#  -c connections  -n requests per connection  -P pipeline depth
#  -r % GETs  -k keyspace  -d value bytes  -I extra idle connections  -W skip prepopulate
```

## Protocol

A request is one line terminated by `\n`; a `\r` immediately before it is
stripped, so `telnet`/`nc -C` work. Responses are one line each, in request
order. Clients may pipeline any number of requests without waiting.

```
request  = command *( SEP arg ) [ "\r" ] "\n"
SEP      = 1*( " " / "\t" )
key      = 1*( any byte except SP, TAB, CR, LF )
value    = *( any byte except LF )        ; everything after the single separator that follows the key
```

| Request | Success response | Other responses |
|---|---|---|
| `PING` | `PONG` | |
| `SET <key> <value>` | `OK` | |
| `GET <key>` | `VALUE <value>` | `NOT_FOUND` |
| `DEL <key>` | `DELETED` | `NOT_FOUND` |
| `QUIT` | `BYE`, then the server closes the connection | |

Details:

* Commands are case-insensitive; keys and values are case-sensitive and binary-safe apart from
  the framing bytes above.
* `SET` stores the rest of the line verbatim, so values may contain spaces
  (including leading and trailing ones), and `SET k ` (trailing separator) stores an empty value.
  `SET k` (no separator after the key) is an argument error.
* Blank lines are ignored.
* Errors are `ERR <reason>`, and the connection stays usable:
  `ERR unknown command`, `ERR wrong number of arguments`.
* Fatal errors are followed by the server closing the connection:
  `ERR line too long` (line > 65,536 bytes, excluding the terminator),
  `ERR out of memory`.
* `ERR server busy` + close: thread-pool server only, when its pending-connection queue is full.
* Because the protocol is line-framed, a value cannot contain `\n` or end in `\r`.

## Testing

* **Unit tests** (`tests/test_*.c`): hash table semantics, growth to
  50k keys, 8 threads doing mixed SET/GET/DEL on private and shared keys while the
  table resizes underneath them (starting from 1 bucket); protocol framing and
  limits (exact-limit and limit+1 lines, CRLF, pipelining, partial lines, output
  high-water); thread pool drain and full-queue rejection.
* **Functional tests** (`tests/functional_test.py`) run against each server
  over real sockets: every command, a request sent **one byte per TCP
  segment**, 5,000 pipelined requests in one write, max-size values, a client
  that **disconnects mid-command**, a client that requests ~120 MB of responses and
  **resets the connection** without reading (the SIGPIPE path), half-close
  (`shutdown(SHUT_WR)`), 6 concurrent clients, output **backpressure resume**,
  and graceful shutdown with one connection in service and one still queued
  (both must be answered, and the process must exit 0).
* **`make valgrind`** runs all of the above under valgrind with
  `--errors-for-leak-kinds=all --error-exitcode=99`, then sends **SIGINT to each
  server while 16 clients are mid-load** (with `-t 4`, so the pool server has
  connections both in service and queued). Any leak of any kind fails the run.
* **`make tsan`** runs the multithreaded tests and both servers (epoll with 4
  loops) under ThreadSanitizer.

## Benchmark results

Reproduce with `make bench` (`python3 bench/run_bench.py --reps 5` produced
the numbers below; raw output is in [`bench/results.md`](bench/results.md)).

**Setup.** 4-vCPU VM (Intel Xeon @ 2.8 GHz, Linux 6.18, gcc 13.3 `-O2`),
loopback TCP, **client and server on the same 4 vCPUs**, so the absolute
numbers are low and only the comparisons mean anything. Workload: 80% GET / 20% SET over
100k prepopulated keys, 32-byte values, 400k requests per run, each connection
closed-loop (send a batch of `pipeline` requests, wait for all replies). Each cell
is the median of 5 runs, on a fresh server per cell. The thread-pool server ran with
`-t 8 -i 2` (8 workers, 2 s idle timeout).

* **RTT**: time for one batch round trip, per connection.
* **worst first reply**: time from the start until the *last* connection got its
  first response. It is the column that shows connections left waiting in the
  pool's queue, because each connection contributes only one such sample, so the waits
  barely move p99.

| Workload | Server | Throughput (req/s) | range over 5 runs | p50 RTT | p99 RTT | worst first reply |
|---|---|---:|---:|---:|---:|---:|
| 8 conns | thread pool (8 workers) | **292,975** | 163k - 301k | 0.016 ms | 0.074 ms | 0.4 ms |
| 8 conns | epoll (1 loop) | **47,064** | 47k - 47k | 0.142 ms | 0.351 ms | 0.3 ms |
| 8 conns | epoll (4 loops) | **114,023** | 101k - 131k | 0.029 ms | 0.185 ms | 0.4 ms |
| 64 conns | thread pool (8 workers) | **296,201** | 275k - 304k | 0.018 ms | 0.096 ms | **1119.6 ms** |
| 64 conns | epoll (1 loop) | **46,945** | 46k - 48k | 1.316 ms | 2.135 ms | 2.7 ms |
| 64 conns | epoll (4 loops) | **133,538** | 130k - 143k | 0.340 ms | 1.853 ms | 8.2 ms |
| 256 conns | thread pool (8 workers) | **217,626** | 155k - 316k | 0.021 ms | 0.179 ms | **1722.6 ms** |
| 256 conns | epoll (1 loop) | **42,415** | 41k - 45k | 5.769 ms | 7.916 ms | 10.1 ms |
| 256 conns | epoll (4 loops) | **142,669** | 133k - 154k | 1.391 ms | 5.170 ms | 4.1 ms |
| 64 conns, pipeline 16 | thread pool (8 workers) | **1,109,851** | 1.02M - 3.32M | 0.097 ms | 0.322 ms | 325.8 ms |
| 64 conns, pipeline 16 | epoll (1 loop) | **505,474** | 496k - 522k | 1.918 ms | 3.901 ms | 3.5 ms |
| 64 conns, pipeline 16 | epoll (4 loops) | **1,368,704** | 1.18M - 1.52M | 0.580 ms | 2.918 ms | 4.4 ms |
| 8 conns + 16 idle | thread pool (8 workers) | **72,831** | 66k - 73k | 0.014 ms | 0.078 ms | **4032.6 ms** |
| 8 conns + 16 idle | epoll (1 loop) | **45,613** | 44k - 47k | 0.149 ms | 0.381 ms | 0.3 ms |
| 8 conns + 16 idle | epoll (4 loops) | **120,978** | 115k - 326k | 0.037 ms | 0.199 ms | 0.2 ms |

**What the numbers say**

1. **When active connections ≤ workers, the thread pool has the best raw
   throughput** (293k vs 114k req/s at 8 connections). Each request is a
   blocking `recv` and a `send` on a thread that the kernel wakes directly, with no
   readiness step in between. With one connection in ping-pong the two servers
   are identical (about 19k req/s, 47 µs RTT each), so the gap at 8 connections is about
   how the work spreads over CPUs, not about per-request cost. Each pool thread
   serves one client and tends to share a CPU with it. Each epoll loop serves
   several clients, which costs cross-CPU wakeups (expensive in a VM).
2. **With more connections than workers, the pool's throughput number hides
   starvation.** At 64 and 256 connections it still reports about 220-300k req/s,
   but only 8 connections are ever in service. The rest sit in the queue, and the
   last one gets its first byte after **1.1-1.7 s, i.e. essentially the whole run**.
   Epoll serves every connection from the start: worst first reply ≤ 10 ms, and
   latency grows smoothly with load (p50 0.34 → 1.39 ms going from 64 to 256 connections on 4 loops).
3. **Idle connections are the thread pool's worst case.** 16 idle clients took
   all 8 workers until the idle timeout fired twice (2 x 2 s), so active clients waited
   **4 s** for a first reply, and throughput fell to 73k. With the default `-i 60`
   that stall would be 2 minutes. Epoll doesn't notice: an idle connection costs a
   struct in a list and no thread.
4. **One epoll loop is CPU-bound on a single core at about 45k req/s.** `strace -c`
   confirms the loop does only one `recvfrom` + one `sendto` per request.
   The time is kernel networking, which on loopback includes delivering each reply
   to the peer, all charged to that one core. More loops scale it out (3x with 4 loops),
   and so does **pipelining**: 16 requests per round trip amortize the syscalls and
   wakeups, taking one loop to 505k and 4 loops to **1.37M req/s**, the best
   median in the table. The pool's pipelined number swings 1.0-3.3M between runs, and its
   queued connections still wait 326 ms.
5. **Variance.** Cells whose range is wide (pool at 8 and 256 conns, epoll-4 with idle
   connections) depend on where the scheduler places 8-16 busy threads on 4
   vCPUs shared with the client. Single-loop epoll is the most repeatable configuration
   (±3%) because it has the fewest threads to place.

**Bottom line:** the thread pool is a good fit for a small, known number of
busy clients (e.g. a fixed set of application servers). The event loop is the
right design when the number of connections is large, unknown or mostly idle,
because it serves every client fairly and its cost grows with *activity*, not
with *connections*.

## Design decisions

### Thread pool: connection is the unit of work, and the queue is bounded

With blocking sockets, a worker can only do useful work on a connection that has
data, and finding out which connections have data *is* the event-loop problem.
So in the pool server a job is a whole connection: the worker loops
`recv -> execute -> send` until the client leaves. That keeps workers simple
and fast. Each one is just straight-line blocking code, and each wakeup goes
straight to the thread that will handle the data. The cost is that at most `-t`
clients are served at once. The others wait in the FIFO queue, as the benchmark
shows. Three things keep that wait bounded and visible rather than silent:

* **Bounded queue + non-blocking submit.** `tp_submit` never blocks. When the
  queue is full the acceptor answers `ERR server busy` and closes. A blocking
  submit would leave the acceptor asleep on a condition variable where it could not
  notice SIGINT, and an unbounded queue just turns overload into unbounded
  memory growth and latency.
* **Idle and send timeouts** (`SO_RCVTIMEO`/`SO_SNDTIMEO`, `-i`): a client that goes
  quiet, or stops reading its responses, gives its worker back after `-i`
  seconds instead of holding it forever.
* **Load shedding is explicit**: the client gets an error line, not a hang.

### Work queue: ring buffer, one mutex, one condition variable

The queue is a fixed array used as a ring (no allocation per job) protected by
one mutex. Workers wait on a single `not_empty` condition variable inside a
`while` loop (spurious wakeups, and so a woken worker re-checks the stop
flag). A submit wakes one worker with `pthread_cond_signal`; shutdown uses
`pthread_cond_broadcast`. There is no `not_full` condition variable because
the producer never waits (see above). Critical sections are a few loads and
stores, so a single lock is not a bottleneck at the rate connections arrive.
The pool creates its threads with **all signals blocked**, so no worker can
ever be picked to run a signal handler.

### Hash table: striped read-write locks

* **Striped, not global.** A single `pthread_rwlock_t` makes every reader do an
  atomic update to the same cache line, so even read-only traffic serializes on that line.
  The table has 64 stripes. Bucket `i` belongs to stripe `i & 63`, and each lock
  is padded to its own 64-byte cache line to avoid false sharing.
* **Read-write, not mutex.** GET is the common operation and copies the value
  (up to 64 KiB) *while holding the lock*, directly into the connection's
  output buffer via a callback, so there is no extra malloc+copy per GET. Concurrent
  GETs of keys on the same stripe run in parallel. SET/DEL take the stripe in
  write mode.
* **Writer preference.** glibc rwlocks prefer readers by default, which lets a
  GET-heavy workload starve SETs indefinitely. Locks are created with
  `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`.
* **Short critical sections.** SET allocates the new entry *before* taking the
  lock and frees the replaced entry *after* releasing it. The element count
  is a C11 atomic, so it needs no lock.
* **Online resize without a global lock on the fast path.** Both counts are powers
  of two and `nbuckets >= nstripes`, so a key's stripe (`hash & 63`) never
  depends on the table size. When the load factor passes 1, the inserting thread
  takes **every stripe in index order** (the only multi-lock path, so lock
  ordering makes deadlock impossible), re-checks that nobody grew the table
  first, rehashes using the stored hashes, and swaps the array. Threads waiting
  on a stripe simply see the new array once they get the lock.
* Hash: FNV-1a with a murmur3 finalizer so the masked low bits are well mixed.
  It is not seeded, so it is not hash-flooding resistant (see Limitations).

### Signals: synchronous delivery only

SIGINT/SIGTERM are blocked in every thread before any thread starts, then
received *synchronously*: through a `signalfd` polled next to the listening
socket (pool server) or by `sigwait()` in an otherwise idle main thread (epoll
server). There are no asynchronous handlers, so there are no async-signal-safety
constraints, no `EINTR` races, and no question of which thread the kernel picks.

**SIGPIPE** is ignored process-wide *and* every `send` passes `MSG_NOSIGNAL`. A
peer that resets mid-response becomes an `EPIPE`/`ECONNRESET` return value that
closes that connection only (covered by `t_abrupt_disconnects`).

### Graceful shutdown: `shutdown(fd, SHUT_RD)` as the wake-up

The hard part of shutting down a blocking-I/O server is waking a worker that is
asleep in `recv()` on an idle client. The options considered:

| Option | Problem |
|---|---|
| `pthread_cancel` | Can cancel a worker mid-`malloc` or while it holds a hash table lock |
| `pthread_kill` + `EINTR` | Racy: a signal that lands just *before* `recv()` is lost and the worker sleeps forever |
| `poll()` with a timeout before every `recv` | An extra syscall per request, plus shutdown latency up to the timeout |
| **`shutdown(fd, SHUT_RD)`** | Chosen |

On Linux, `shutdown(SHUT_RD)` wakes a blocked `recv()`, which **still returns any
bytes already received**, then returns 0 (I verified this with a test
program before relying on it). So a worker treats shutdown like a normal client
EOF: it answers every request the client already sent, flushes, and closes. To
find the live fds, workers register their connection in a small registry
guarded by a mutex that also guards the `stopping` flag. Either the main thread
sees the registration or the worker sees the flag, so no connection is missed. A
worker unregisters *before* `close()`, so shutdown can never hit a recycled
fd number.

The resulting sequence for the pool server is:
stop accepting -> `SHUT_RD` every live connection -> workers drain the
queue (queued connections get `SHUT_RD` immediately, so their pending
requests are answered too) -> join -> free the table, registry, and pool.
The epoll server does the same per loop, with a 3 s grace deadline for clients
that are not reading their final responses.

### Partial reads and writes

* **Reads**: each connection has an input buffer. `proto_process` executes
  only complete lines and reports how much it consumed, and the incomplete tail is
  kept for the next read, so a request split across any number of TCP segments,
  or many requests in one segment, both just work. The longest accepted line is 64 KiB, so
  a read buffer of 64 KiB + 2 always holds one full request, and a client
  cannot make the server buffer unbounded input.
* **Writes**: the pool server's `net_send_all` loops on short writes and
  `EINTR`. The epoll server keeps an offset into its output buffer, enables
  `EPOLLOUT` only while bytes are pending, and disables it once drained.
* **Output high-water mark (256 KiB)**: `proto_process` stops once that much
  output is pending, so `GET big` x 10,000 in one write cannot balloon memory.
  The epoll server also *stops reading* from a connection above the mark,
  which is per-client backpressure.

One bug found during testing is worth recording. When a high-water pause ends
because `EPOLLOUT` drained the output, the loop must go back and parse the
requests still sitting in the input buffer. A client that sent everything up front
will never trigger another `EPOLLIN`, so there is no second chance. The first
version returned early and hung such clients intermittently. `t_backpressure_resume`
reproduces it deterministically.

### epoll: level-triggered, one read per event

Edge-triggered epoll requires draining every fd to `EAGAIN` on each
notification, or the fd never fires again. That conflicts directly with backpressure,
where the point is to *stop* reading with data still queued. Level-triggered
mode plus toggling interest (`EPOLLIN` off above the high-water mark, `EPOLLOUT`
only while output is pending) expresses that directly, and it is the mode where
forgetting a piece of state degrades into an extra wakeup rather than a hang.
Each `EPOLLIN` gets one `recv` of up to 16 KiB, so a single busy client cannot
starve the others on its loop. Under load, `strace -c` shows exactly one `recvfrom`
and one `sendto` per request, about 8 events per `epoll_wait`, and no
`epoll_ctl` calls in steady state.

### epoll with N loops: accept on one loop, distribute round-robin

Each loop owns its connections for their whole life, so connection state is
never shared or locked. The first version had every loop watch the shared listener
with `EPOLLEXCLUSIVE`. Benchmarking showed that **all 8 connections of a burst
landed on whichever loop woke first** (it accepts in a batch, and level-triggering
keeps reporting the listener to it), so `-t 4` performed like `-t 1`.
`SO_REUSEPORT` was rejected too: it hashes connections across per-loop listeners, which is uneven at
small connection counts (8 connections over 4 loops can easily split 4/2/1/1).
The final design is the classic main-reactor/sub-reactor pattern: loop 0
accepts and deals fds round-robin into each loop's mutex-protected inbox, then
pokes that loop's `eventfd`. The split is exactly even, and the only lock is
taken once per new connection. With that change, `-t 4` at 8 connections went
from the same 46k req/s as one loop to 114k (median of 5 runs, range 101-131k).

### Smaller choices

* **One protocol module for both servers**, so the servers behave identically and every
  functional test runs against both.
* **`TCP_NODELAY`** on every connection, because responses are tiny and Nagle's algorithm would
  delay them waiting for an ACK.
* **Bind to 127.0.0.1 by default.** There is no authentication, so exposing it is an
  explicit `-a 0.0.0.0`.
* **`EMFILE` handling.** Out of fds, a level-triggered listener would spin forever.
  The epoll server keeps a spare fd that it closes to accept and drop one client,
  then reopens. The pool server stops polling the listener for 100 ms.
* Idle epoll connections release buffers larger than 64 KiB, so 10k idle
  clients do not each keep a burst-sized allocation.

## Limitations

* IPv4 only, no persistence, no auth, no TTL/eviction, and no memory limit.
* The hash function is unseeded, so an attacker who can choose keys can force collisions
  (a seeded SipHash would fix this).
* In the pool server, concurrency is capped at `-t` connections (by design, see above).
* Text framing: values cannot contain `\n`. A length-prefixed binary command
  would lift that.
