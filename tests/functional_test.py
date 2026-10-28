#!/usr/bin/env python3
"""End-to-end tests for either server binary.

Starts the server on a free port, exercises the protocol over real TCP
sockets (including the awkward cases: split writes, pipelining, abrupt
disconnects, half-close, concurrency), then sends SIGINT and requires a
clean exit. With --valgrind the server runs under valgrind and any leak or
memory error fails the run.

    python3 tests/functional_test.py --server ./server
    python3 tests/functional_test.py --server ./epoll_server --valgrind
"""
import argparse
import os
import signal
import socket
import subprocess
import sys
import threading
import time

ARGS = None
MAX_LINE = 64 * 1024


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, extra=()):
        self.port = free_port()
        cmd = [ARGS.server, "-p", str(self.port), *extra]
        if ARGS.valgrind:
            cmd = ["valgrind", "-q", "--leak-check=full", "--show-leak-kinds=all",
                   "--errors-for-leak-kinds=all", "--error-exitcode=99"] + cmd
        self.proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True)
        deadline = time.time() + (30 if ARGS.valgrind else 5)
        while True:
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=1).close()
                return
            except OSError:
                if self.proc.poll() is not None or time.time() > deadline:
                    raise RuntimeError("server failed to start: " + self.proc.stderr.read())
                time.sleep(0.05)

    def stop(self):
        """SIGINT the server and return (exit code, stderr)."""
        self.proc.send_signal(signal.SIGINT)
        try:
            _, err = self.proc.communicate(timeout=60 if ARGS.valgrind else 10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            _, err = self.proc.communicate()
            return -1, err + "\n(timed out waiting for graceful shutdown)"
        return self.proc.returncode, err


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.buf = b""

    def send(self, data):
        self.sock.sendall(data)

    def line(self):
        while b"\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError("connection closed; partial=%r" % self.buf[:80])
            self.buf += chunk
        ln, self.buf = self.buf.split(b"\n", 1)
        return ln

    def cmd(self, data):
        self.send(data + b"\n")
        return self.line()

    def at_eof(self):
        """True if the server closed the connection (after any buffered data)."""
        try:
            return self.sock.recv(1) == b""
        except ConnectionResetError:
            return True

    def close(self):
        self.sock.close()


# ---------------------------------------------------------------- tests ----

def t_basic(port):
    c = Client(port)
    assert c.cmd(b"PING") == b"PONG"
    assert c.cmd(b"GET missing") == b"NOT_FOUND"
    assert c.cmd(b"SET k v") == b"OK"
    assert c.cmd(b"GET k") == b"VALUE v"
    assert c.cmd(b"SET k two words ") == b"OK"
    assert c.cmd(b"GET k") == b"VALUE two words "
    assert c.cmd(b"DEL k") == b"DELETED"
    assert c.cmd(b"DEL k") == b"NOT_FOUND"
    assert c.cmd(b"set Mixed\tcase") == b"OK"
    assert c.cmd(b"get Mixed\r") == b"VALUE case"
    assert c.cmd(b"NOPE") == b"ERR unknown command"
    assert c.cmd(b"GET") == b"ERR wrong number of arguments"
    assert c.cmd(b"QUIT") == b"BYE"
    assert c.at_eof()


def t_split_writes(port):
    """Every byte in its own TCP segment: server must reassemble lines."""
    c = Client(port)
    c.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    for b in b"SET split value-sent-byte-by-byte\r\nGET split\n":
        c.send(bytes([b]))
        time.sleep(0.001)
    assert c.line() == b"OK"
    assert c.line() == b"VALUE value-sent-byte-by-byte"
    c.close()


def t_pipelining(port):
    """Thousands of requests in one write; responses must stay in order."""
    c = Client(port)
    n = 5000
    reqs = b"".join(b"SET p%d %d\nGET p%d\n" % (i, i, i) for i in range(n))
    threading.Thread(target=c.send, args=(reqs,)).start()  # don't deadlock on full buffers
    for i in range(n):
        assert c.line() == b"OK"
        assert c.line() == b"VALUE %d" % i
    c.close()


def t_big_values(port):
    c = Client(port)
    val = b"x" * (MAX_LINE - len(b"SET big "))
    assert c.cmd(b"SET big " + val) == b"OK"
    assert c.cmd(b"GET big") == b"VALUE " + val
    # Many large responses pipelined: exercises output high-water/backpressure.
    threading.Thread(target=c.send, args=(b"GET big\n" * 200,)).start()
    for _ in range(200):
        assert c.line() == b"VALUE " + val
    c.close()


def t_line_too_long(port):
    c = Client(port)
    c.send(b"SET k " + b"y" * (MAX_LINE + 10))
    assert c.line() == b"ERR line too long"
    assert c.at_eof()


def t_abrupt_disconnects(port):
    """Half a command then close; and a reader that vanishes mid-response."""
    c = Client(port)
    c.send(b"SET half")
    c.close()

    c = Client(port)
    c.cmd(b"SET big " + b"z" * 60000)
    c.send(b"GET big\n" * 2000)  # ~120 MB of responses we never read
    c.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\0\0\0\0\0\0\0")
    c.close()  # RST: server's next send hits EPIPE/ECONNRESET, not SIGPIPE

    time.sleep(0.2)
    c = Client(port)
    assert c.cmd(b"PING") == b"PONG"  # still alive
    c.close()


def t_half_close(port):
    """Client sends everything then shuts down its write side."""
    c = Client(port)
    c.send(b"SET hc 1\nGET hc\nPING\n")
    c.sock.shutdown(socket.SHUT_WR)
    assert [c.line(), c.line(), c.line()] == [b"OK", b"VALUE 1", b"PONG"]
    assert c.at_eof()


def t_concurrent(port):
    errors = []

    def worker(tid):
        try:
            c = Client(port)
            for i in range(300):
                k = b"c%d:%d" % (tid, i)
                assert c.cmd(b"SET " + k + b" v" + k) == b"OK"
                assert c.cmd(b"GET " + k) == b"VALUE v" + k
                assert c.cmd(b"SET shared " + k) == b"OK"
                assert c.cmd(b"GET shared").startswith(b"VALUE c")
                if i % 2:
                    assert c.cmd(b"DEL " + k) == b"DELETED"
            c.close()
        except Exception as e:  # noqa: BLE001 - report every failure
            errors.append("thread %d: %r" % (tid, e))

    # Fewer clients than the default worker count so the pool server serves
    # them all at once; the epoll server doesn't care.
    ths = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    assert not errors, errors
    c = Client(port)
    assert c.cmd(b"GET c3:298") == b"VALUE vc3:298"
    assert c.cmd(b"GET c3:299") == b"NOT_FOUND"
    c.close()


def t_graceful_shutdown():
    """SIGINT with one busy connection and one waiting connection.

    With a single worker the pool server has `busy` in service and `waiting`
    still in the queue; both must be answered/closed and the process must
    exit 0. (The epoll server serves both immediately; same expectations.)
    """
    srv = Server(["-t", "1"])
    busy = Client(srv.port)
    assert busy.cmd(b"SET before-shutdown 1") == b"OK"
    waiting = Client(srv.port)
    waiting.send(b"PING\nGET before-shutdown\n")
    time.sleep(0.3)
    code, err = srv.stop()
    assert waiting.line() == b"PONG"
    assert waiting.line() == b"VALUE 1"
    assert waiting.at_eof()
    assert busy.at_eof()
    assert code == 0, "exit code %d\n%s" % (code, err)


TESTS = [t_basic, t_split_writes, t_pipelining, t_big_values, t_line_too_long,
         t_abrupt_disconnects, t_half_close, t_concurrent]


def main():
    global ARGS
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--valgrind", action="store_true")
    ARGS = ap.parse_args()
    name = os.path.basename(ARGS.server) + (" (valgrind)" if ARGS.valgrind else "")
    failed = 0

    srv = Server()
    for t in TESTS:
        try:
            t(srv.port)
            print("  %-24s ok" % t.__name__)
        except Exception as e:  # noqa: BLE001
            failed += 1
            print("  %-24s FAILED: %r" % (t.__name__, e))
    code, err = srv.stop()
    if code != 0:
        failed += 1
        print("  %-24s FAILED: exit code %d\n%s" % ("clean_exit", code, err))
    else:
        print("  %-24s ok" % "clean_exit")

    try:
        t_graceful_shutdown()
        print("  %-24s ok" % "t_graceful_shutdown")
    except Exception as e:  # noqa: BLE001
        failed += 1
        print("  %-24s FAILED: %r" % ("t_graceful_shutdown", e))

    print("%s: %d failures" % (name, failed))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
