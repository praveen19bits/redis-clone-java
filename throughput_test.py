#!/usr/bin/env python3
"""
Throughput benchmark: N persistent client connections, each firing commands
as fast as possible (no delay) for a fixed duration. Measures aggregate
ops/sec -- this directly tests whether the shared KeyValueStore becomes
the bottleneck in both modes equally, or whether they diverge.

CONNECTIONS ARE RAMPED IN BATCHES, not opened all at once. A burst of many
simultaneous connect() calls can overwhelm a TCP accept backlog (or, on
Docker Desktop for Windows, the host<->container port-forwarding layer)
before the server's accept loop can drain it, causing the OS or the proxy
to actively refuse the excess connections. Ramping avoids that regardless
of which layer's queue was actually the bottleneck.
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


def connect_one(host, port, client_id, sockets, idx, errors):
    try:
        s = socket.create_connection((host, port), timeout=10)
        sockets[idx] = s
    except Exception as e:
        errors.append((idx, str(e)))


def fire_commands(sock, duration_seconds, client_id, command, counters, idx, start_barrier):
    key = f"k{client_id}"
    start_barrier.wait()  # all clients begin the timed window together
    end_time = time.time() + duration_seconds
    count = 0
    while time.time() < end_time:
        if command == "SET":
            sock.sendall(resp_encode("SET", key, "v"))
        else:
            sock.sendall(resp_encode("GET", key))
        sock.recv(256)
        count += 1
    counters[idx] = count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--duration", type=int, default=5)
    parser.add_argument("--command", choices=["SET", "GET"], default="SET")
    parser.add_argument("--ramp-batch", type=int, default=100,
                         help="open this many connections at a time before the timed run starts")
    args = parser.parse_args()

    sockets = [None] * args.clients
    errors = []

    print(f"Opening {args.clients} connections in batches of {args.ramp_batch}...")
    for batch_start in range(0, args.clients, args.ramp_batch):
        batch_end = min(batch_start + args.ramp_batch, args.clients)
        threads = []
        for i in range(batch_start, batch_end):
            t = threading.Thread(target=connect_one, args=(args.host, args.port, i, sockets, i, errors))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        print(f"  {batch_end}/{args.clients} connected ({len(errors)} failed so far)")

    if errors:
        print(f"\n{len(errors)} connections FAILED to establish. First few:")
        for idx, msg in errors[:5]:
            print(f"  client {idx}: {msg}")
        connected_indices = [i for i in range(args.clients) if sockets[i] is not None]
        if not connected_indices:
            print("No connections succeeded at all -- aborting.")
            return
        print(f"Proceeding with {len(connected_indices)} successfully connected clients.\n")
    else:
        connected_indices = list(range(args.clients))
        print("All connections established cleanly.\n")

    counters = [0] * args.clients
    barrier = threading.Barrier(len(connected_indices))
    threads = []
    for i in connected_indices:
        t = threading.Thread(target=fire_commands,
                              args=(sockets[i], args.duration, i, args.command, counters, i, barrier))
        threads.append(t)
        t.start()

    start = time.time()
    for t in threads:
        t.join()
    elapsed = time.time() - start

    for i in connected_indices:
        try:
            sockets[i].close()
        except Exception:
            pass

    total_ops = sum(counters)
    print(f"{len(connected_indices)} clients, {args.command}, {elapsed:.2f}s elapsed")
    print(f"Total ops: {total_ops}, throughput: {total_ops/elapsed:.0f} ops/sec")


if __name__ == "__main__":
    main()
