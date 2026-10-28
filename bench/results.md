8 conns                  thread pool (8 workers)       292975 req/s (163195-301006)  p50   0.016 ms  p99    0.074 ms  first-reply worst     0.373 ms  err 0
8 conns                  epoll (1 loop)                 47064 req/s (46522-47321)  p50   0.142 ms  p99    0.351 ms  first-reply worst     0.317 ms  err 0
8 conns                  epoll (4 loops)               114023 req/s (100805-131148)  p50   0.029 ms  p99    0.185 ms  first-reply worst     0.363 ms  err 0
64 conns                 thread pool (8 workers)       296201 req/s (274634-304080)  p50   0.018 ms  p99    0.096 ms  first-reply worst  1119.602 ms  err 0
64 conns                 epoll (1 loop)                 46945 req/s (45999-48151)  p50   1.316 ms  p99    2.135 ms  first-reply worst     2.707 ms  err 0
64 conns                 epoll (4 loops)               133538 req/s (129993-143370)  p50   0.340 ms  p99    1.853 ms  first-reply worst     8.194 ms  err 0
256 conns                thread pool (8 workers)       217626 req/s (154638-316424)  p50   0.021 ms  p99    0.179 ms  first-reply worst  1722.599 ms  err 0
256 conns                epoll (1 loop)                 42415 req/s (41072-44507)  p50   5.769 ms  p99    7.916 ms  first-reply worst    10.101 ms  err 0
256 conns                epoll (4 loops)               142669 req/s (132678-154346)  p50   1.391 ms  p99    5.170 ms  first-reply worst     4.066 ms  err 0
64 conns, pipeline 16    thread pool (8 workers)      1109851 req/s (1017522-3324007)  p50   0.097 ms  p99    0.322 ms  first-reply worst   325.777 ms  err 0
64 conns, pipeline 16    epoll (1 loop)                505474 req/s (495983-521560)  p50   1.918 ms  p99    3.901 ms  first-reply worst     3.534 ms  err 0
64 conns, pipeline 16    epoll (4 loops)              1368704 req/s (1175382-1520910)  p50   0.580 ms  p99    2.918 ms  first-reply worst     4.402 ms  err 0
8 conns + 16 idle        thread pool (8 workers)        72831 req/s (66109-73109)  p50   0.014 ms  p99    0.078 ms  first-reply worst  4032.646 ms  err 0
8 conns + 16 idle        epoll (1 loop)                 45613 req/s (43588-47403)  p50   0.149 ms  p99    0.381 ms  first-reply worst     0.279 ms  err 0
8 conns + 16 idle        epoll (4 loops)               120978 req/s (115378-325973)  p50   0.037 ms  p99    0.199 ms  first-reply worst     0.239 ms  err 0

| Workload | Server | Throughput (req/s) | range over 5 runs | p50 RTT | p99 RTT | worst first reply |
|---|---|---:|---:|---:|---:|---:|
| 8 conns | thread pool (8 workers) | **292,975** | 163k - 301k | 0.016 ms | 0.074 ms | 0.4 ms |
| 8 conns | epoll (1 loop) | **47,064** | 47k - 47k | 0.142 ms | 0.351 ms | 0.3 ms |
| 8 conns | epoll (4 loops) | **114,023** | 101k - 131k | 0.029 ms | 0.185 ms | 0.4 ms |
| 64 conns | thread pool (8 workers) | **296,201** | 275k - 304k | 0.018 ms | 0.096 ms | 1119.6 ms |
| 64 conns | epoll (1 loop) | **46,945** | 46k - 48k | 1.316 ms | 2.135 ms | 2.7 ms |
| 64 conns | epoll (4 loops) | **133,538** | 130k - 143k | 0.340 ms | 1.853 ms | 8.2 ms |
| 256 conns | thread pool (8 workers) | **217,626** | 155k - 316k | 0.021 ms | 0.179 ms | 1722.6 ms |
| 256 conns | epoll (1 loop) | **42,415** | 41k - 45k | 5.769 ms | 7.916 ms | 10.1 ms |
| 256 conns | epoll (4 loops) | **142,669** | 133k - 154k | 1.391 ms | 5.170 ms | 4.1 ms |
| 64 conns, pipeline 16 | thread pool (8 workers) | **1,109,851** | 1.02M - 3.32M | 0.097 ms | 0.322 ms | 325.8 ms |
| 64 conns, pipeline 16 | epoll (1 loop) | **505,474** | 496k - 522k | 1.918 ms | 3.901 ms | 3.5 ms |
| 64 conns, pipeline 16 | epoll (4 loops) | **1,368,704** | 1.18M - 1.52M | 0.580 ms | 2.918 ms | 4.4 ms |
| 8 conns + 16 idle | thread pool (8 workers) | **72,831** | 66k - 73k | 0.014 ms | 0.078 ms | 4032.6 ms |
| 8 conns + 16 idle | epoll (1 loop) | **45,613** | 44k - 47k | 0.149 ms | 0.381 ms | 0.3 ms |
| 8 conns + 16 idle | epoll (4 loops) | **120,978** | 115k - 326k | 0.037 ms | 0.199 ms | 0.2 ms |
