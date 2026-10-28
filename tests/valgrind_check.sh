#!/usr/bin/env bash
# Run everything under valgrind and fail on any memory error or leak of any
# kind (definite, indirect, possible, or still-reachable).
#
#   1. unit tests
#   2. the functional suite against each server configuration
#   3. load + SIGINT *while clients are mid-request*, so the shutdown path
#      frees live connections, queued work and a populated table
set -u
cd "$(dirname "$0")/.."

VG=(valgrind -q --leak-check=full --show-leak-kinds=all
    --errors-for-leak-kinds=all --error-exitcode=99)
fail=0

step() { printf '\n== %s\n' "$*"; }
check() { # name, exit code
    if [ "$2" -eq 0 ]; then echo "   PASS $1"; else echo "   FAIL $1 (exit $2)"; fail=1; fi
}

step "unit tests"
for t in build/test_hashtable build/test_protocol build/test_threadpool; do
    "${VG[@]}" "$t" > /dev/null
    check "$t" $?
done

step "functional tests"
for cfg in "./server" "./server --extra=-t2" "./epoll_server" "./epoll_server --extra=-t4"; do
    # shellcheck disable=SC2086
    python3 tests/functional_test.py --valgrind --server $cfg | tail -1
    check "$cfg" "${PIPESTATUS[0]}"
done

step "SIGINT under load"
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
for bin in ./server ./epoll_server; do
    port=$((port + 1))
    log=$(mktemp)
    "${VG[@]}" --log-file="$log" "$bin" -p "$port" -t 4 2>/dev/null &
    pid=$!
    for _ in $(seq 100); do  # wait until it accepts connections
        python3 -c "import socket; socket.create_connection(('127.0.0.1',$port),1)" 2>/dev/null && break
        sleep 0.1
    done
    # 16 clients, more than the pool has workers, so some are queued too.
    ./client -p "$port" -c 16 -n 1000000 -P 4 -k 5000 > /dev/null 2>&1 &
    cpid=$!
    sleep 3
    kill -INT "$pid"
    wait "$pid"
    rc=$?
    wait "$cpid" 2>/dev/null  # the client sees its connections closed; expected
    if [ -s "$log" ]; then cat "$log"; fi
    rm -f "$log"
    check "$bin interrupted under load" "$rc"
done

echo
if [ "$fail" -eq 0 ]; then echo "valgrind: all clean"; else echo "valgrind: FAILURES"; fi
exit "$fail"
