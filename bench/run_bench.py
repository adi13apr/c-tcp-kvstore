#!/usr/bin/env python3
"""Benchmark both servers with ./client and print a Markdown table.

Each (server, workload) cell starts a fresh server, runs the client
--reps times and reports the run with the median throughput, plus the
min-max throughput range across runs.

The thread-pool server runs with a 2 s idle timeout (-i 2) so the idle-
connection workload finishes in seconds rather than the default 60 s.

    make && python3 bench/run_bench.py            # full matrix
    python3 bench/run_bench.py --quick            # fewer requests, 1 rep
"""
import argparse
import os
import re
import signal
import socket
import subprocess
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SERVERS = [
    ("thread pool (8 workers)", ["./server", "-t", "8", "-i", "2"]),
    ("epoll (1 loop)", ["./epoll_server", "-t", "1"]),
    ("epoll (4 loops)", ["./epoll_server", "-t", "4"]),
]

# (label, active connections, pipeline depth, idle connections)
WORKLOADS = [
    ("8 conns", 8, 1, 0),
    ("64 conns", 64, 1, 0),
    ("256 conns", 256, 1, 0),
    ("64 conns, pipeline 16", 64, 16, 0),
    ("8 conns + 16 idle", 8, 1, 16),
]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start(cmd, port):
    proc = subprocess.Popen(cmd + ["-p", str(port)], cwd=ROOT,
                            stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=1).close()
            return proc
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("server did not start: %r" % cmd)


def run_client(port, conns, pipeline, idle, total, get_pct):
    per_conn = max(1, total // conns)
    out = subprocess.run(
        ["./client", "-p", str(port), "-c", str(conns), "-n", str(per_conn),
         "-P", str(pipeline), "-r", str(get_pct), "-k", "100000", "-d", "32",
         "-I", str(idle)],
        cwd=ROOT, capture_output=True, text=True, check=False).stdout
    m = re.search(r"RESULT (.*)", out)
    if not m:
        raise RuntimeError("client failed:\n" + out)
    return {k: float(v) for k, v in (kv.split("=") for kv in m.group(1).split())}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--total", type=int, default=400000, help="requests per run")
    ap.add_argument("--get-pct", type=int, default=80)
    args = ap.parse_args()
    if args.quick:
        args.reps, args.total = 1, 100000

    results = {}
    for wl_label, conns, depth, idle in WORKLOADS:
        for srv_label, cmd in SERVERS:
            port = free_port()
            proc = start(cmd, port)
            runs = [run_client(port, conns, depth, idle, args.total, args.get_pct)
                    for _ in range(args.reps)]
            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=30)
            runs.sort(key=lambda r: r["rps"])
            r = dict(runs[len(runs) // 2], lo=runs[0]["rps"], hi=runs[-1]["rps"])
            results[(wl_label, srv_label)] = r
            print("%-24s %-26s %9.0f req/s (%.0f-%.0f)  p50 %7.3f ms  p99 %8.3f ms  "
                  "first-reply worst %9.3f ms  err %d"
                  % (wl_label, srv_label, r["rps"], r["lo"], r["hi"], r["p50_us"] / 1e3,
                     r["p99_us"] / 1e3, r["first_max_us"] / 1e3, r["errors"]), flush=True)

    def k(x):
        return "%.0fk" % (x / 1e3) if x < 1e6 else "%.2fM" % (x / 1e6)

    print()
    print("| Workload | Server | Throughput (req/s) | range over %d runs | p50 RTT | p99 RTT "
          "| worst first reply |" % args.reps)
    print("|---|---|---:|---:|---:|---:|---:|")
    for wl_label, _, _, _ in WORKLOADS:
        for srv_label, _ in SERVERS:
            r = results[(wl_label, srv_label)]
            print("| %s | %s | **%s** | %s - %s | %.3f ms | %.3f ms | %.1f ms |"
                  % (wl_label, srv_label, format(int(r["rps"]), ","), k(r["lo"]), k(r["hi"]),
                     r["p50_us"] / 1e3, r["p99_us"] / 1e3, r["first_max_us"] / 1e3))


if __name__ == "__main__":
    main()
