#!/usr/bin/env python3
"""
Load simulator for the Redis clone.

WHY A CUSTOM SCRIPT AND NOT wrk/ab: tools like wrk and ab speak HTTP.
Our server speaks raw RESP over TCP, so we need something that opens plain
TCP sockets and holds them open -- which is also, honestly, the more
educational choice: you can see exactly what "1000 idle clients" means at
the socket level.

What this does:
  1. Opens `--connections` TCP connections to the server.
  2. Sends one PING on each to prove the connection is alive and registered
     with the server (so it shows up in the server's connection/thread count).
  3. Holds all connections open and idle for `--hold-seconds`, simulating
     real clients that connected but aren't actively issuing commands --
     the exact scenario from the epoll diagram (1000s idle, a couple active).
  4. Reports how many connections actually succeeded vs failed.

Usage:
  python3 load_simulator.py --connections 2000 --hold-seconds 30 --port 6380
"""
import argparse
import socket
import threading
import time


def make_connection(host, port, results, index, lock):
    try:
        s = socket.create_connection((host, port), timeout=5)
        # RESP-encoded PING: *1\r\n$4\r\nPING\r\n
        s.sendall(b"*1\r\n$4\r\nPING\r\n")
        reply = s.recv(64)
        if reply != b"+PONG\r\n":
            raise RuntimeError(f"unexpected reply: {reply!r}")
        with lock:
            results["succeeded"] += 1
        return s
    except Exception as e:
        with lock:
            results["failed"] += 1
            if results["failed"] <= 5:  # don't spam if thousands fail the same way
                print(f"  connection {index} failed: {e}")
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--connections", type=int, default=1000)
    parser.add_argument("--hold-seconds", type=int, default=20)
    parser.add_argument("--ramp-batch", type=int, default=100,
                         help="open this many connections at a time, to avoid overwhelming the accept queue")
    args = parser.parse_args()

    results = {"succeeded": 0, "failed": 0}
    lock = threading.Lock()
    sockets = []

    print(f"Opening {args.connections} connections to {args.host}:{args.port} "
          f"in batches of {args.ramp_batch}...")

    start = time.time()
    for batch_start in range(0, args.connections, args.ramp_batch):
        batch_end = min(batch_start + args.ramp_batch, args.connections)
        threads = []
        batch_results = [None] * (batch_end - batch_start)

        def worker(i, idx):
            batch_results[idx] = make_connection(args.host, args.port, results, i, lock)

        for i in range(batch_start, batch_end):
            t = threading.Thread(target=worker, args=(i, i - batch_start))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        sockets.extend(s for s in batch_results if s is not None)
        print(f"  progress: {batch_end}/{args.connections} attempted "
              f"({results['succeeded']} up, {results['failed']} failed)")

    elapsed = time.time() - start
    print(f"\nDone opening connections in {elapsed:.1f}s: "
          f"{results['succeeded']} succeeded, {results['failed']} failed")
    print(f"Holding all {len(sockets)} connections open (idle) for {args.hold_seconds}s...")
    print("While this runs, sample the server process with monitor_server.sh in another terminal.\n")

    time.sleep(args.hold_seconds)

    print("Closing all connections...")
    for s in sockets:
        try:
            s.close()
        except Exception:
            pass
    print("Done.")


if __name__ == "__main__":
    main()
