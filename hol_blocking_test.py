#!/usr/bin/env python3
"""
Demonstrates head-of-line blocking (or its absence) concretely:

  Client A: sends SLOWCOMPUTE 5000 (a ~5 second CPU-bound command) ONCE.
  Client B: sends PING every 200ms for the whole test, and we time
            each round trip precisely.

If Client B's PING latency spikes to ~5 seconds while A's command runs,
the server is single-threaded and A is blocking B (epoll/event-loop mode).
If Client B's PING latency stays flat around a few milliseconds the whole
time, A and B are running on independent threads (thread-per-connection mode).
"""
import argparse
import socket
import threading
import time


def resp_encode(*parts):
    cmd = f"*{len(parts)}\r\n"
    for p in parts:
        cmd += f"${len(p)}\r\n{p}\r\n"
    return cmd.encode()


def client_a_slow_command(host, port, millis, start_barrier):
    s = socket.create_connection((host, port))
    start_barrier.wait()  # synchronize so A fires right as B's pinging begins
    t0 = time.time()
    s.sendall(resp_encode("SLOWCOMPUTE", str(millis)))
    reply = s.recv(1024)
    elapsed = time.time() - t0
    print(f"[Client A] SLOWCOMPUTE {millis}ms finished in {elapsed:.3f}s, reply={reply!r}")
    s.close()


def client_b_pinger(host, port, duration_seconds, start_barrier, results):
    s = socket.create_connection((host, port))
    start_barrier.wait()
    end_time = time.time() + duration_seconds
    while time.time() < end_time:
        t0 = time.time()
        s.sendall(resp_encode("PING"))
        reply = s.recv(1024)
        latency_ms = (time.time() - t0) * 1000
        results.append((time.time(), latency_ms))
        time.sleep(0.2)
    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--slow-millis", type=int, default=5000)
    parser.add_argument("--test-duration", type=int, default=8)
    args = parser.parse_args()

    barrier = threading.Barrier(2)
    b_results = []

    t_a = threading.Thread(target=client_a_slow_command,
                            args=(args.host, args.port, args.slow_millis, barrier))
    t_b = threading.Thread(target=client_b_pinger,
                            args=(args.host, args.port, args.test_duration, barrier, b_results))

    t_a.start()
    t_b.start()
    t_a.join()
    t_b.join()

    print(f"\n[Client B] {len(b_results)} pings sent over {args.test_duration}s. Latencies:")
    for ts, latency_ms in b_results:
        marker = " <-- SPIKE" if latency_ms > 100 else ""
        print(f"  {latency_ms:8.1f} ms{marker}")

    max_latency = max(l for _, l in b_results) if b_results else 0
    avg_latency = sum(l for _, l in b_results) / len(b_results) if b_results else 0
    print(f"\nClient B summary: avg={avg_latency:.1f}ms, max={max_latency:.1f}ms")


if __name__ == "__main__":
    main()
