# redis-clone-java

A Redis server clone built from scratch in Java — not a production Redis replacement, but a learning
project for distributed systems architecture and design patterns. The goal is to actually implement
Redis's internals (protocol, event loop, concurrency model, and eventually replication and sharding)
rather than just use Redis as a black box.

## What this focuses on

- **The reactor pattern.** A single-threaded NIO event loop (`Selector`), the same architecture real
  Redis and nginx use to multiplex thousands of connections on one OS thread via epoll/kqueue/IOCP.
- **Concurrency models, compared head-to-head.** The same protocol parser, command dispatcher, and
  data store are shared, unchanged, by two swappable server implementations: a single-threaded event
  loop (`--mode=epoll`) and a traditional thread-per-connection server (`--mode=thread`). Both are
  benchmarked against each other for memory footprint, throughput, and head-of-line blocking behavior.
- **The RESP wire protocol.** Hand-rolled parsing/serialization of Redis's actual protocol, including
  commands split across multiple TCP reads and pipelined commands in a single read.
- **Concurrency correctness.** Where locking is actually required (and where it's pure overhead),
  proven with a real SET/EXPIRE race condition rather than just reasoned about.
- **Design patterns as they naturally fit**, not bolted on: Command (one class per Redis command),
  Strategy/Observer/State/Template Method (planned for later phases), Singleton, Factory.

Every claim above is backed by a real, reproducible experiment (load tests, throughput benchmarks, a
head-of-line-blocking demonstration) — see
[redis-clone-learning-notes.md](redis-clone-learning-notes.md) for the full write-up, measured numbers,
and the reasoning behind each architectural decision. Read it before making architectural changes.

## Current status

**Phase 1 (done):** single-node, in-memory string store with `PING`, `ECHO`, `SET`, `GET`, `DEL`,
`EXISTS`, `EXPIRE`, and `SLOWCOMPUTE` (a deliberately CPU-bound command used to demonstrate
head-of-line blocking in epoll mode).

**Planned next:** durability (AOF/RDB), pub/sub and transactions, leader-follower replication, then
the distributed-systems payoff — consistent hashing, gossip membership, and a minimal Raft
implementation. Full roadmap in [redis-clone-learning-notes.md](redis-clone-learning-notes.md#17-next-phases--the-roadmap-from-here).

## Quick start

Build with Maven:
```
mvn package
```

Run either concurrency model:
```
java -cp target/classes com.praveen.redisclone.RedisServer --mode=epoll  [--port=6380]
java -cp target/classes com.praveen.redisclone.RedisServer --mode=thread [--port=6380]
```
`--mode` defaults to `epoll`, `--port` defaults to `6380`.

Talk to it with any RESP client, e.g. `redis-cli -p 6380`.

## Architecture at a glance

Protocol parsing, command dispatch, and storage are fully decoupled from the concurrency model — only
how connections are scheduled differs between the two server implementations.

| Component | Responsibility |
|---|---|
| `RedisServer` | Entry point; parses flags, launches the chosen server |
| `EventLoopServer` | Single-threaded NIO reactor — one OS thread total, regardless of connection count |
| `ThreadPerConnectionServer` | One blocking thread per connected client |
| `RespParser` | Parses raw bytes into commands; handles partial and pipelined reads |
| `RespWriter` | Formats RESP replies |
| `Command` + `commands/*` | One class per Redis command |
| `CommandDispatcher` | Routes a parsed command to its `Command` implementation |
| `KeyValueStore` | The in-memory data engine — the one place concurrent access matters |

See [CLAUDE.md](CLAUDE.md) for contributor-facing details (build/test workflow, experiment scripts,
locking rationale, and conventions for adding new commands).

## Experiments included

- `load_simulator.py` — demonstrates epoll's flat memory/thread cost vs thread-per-connection's linear
  cost, at thousands of idle connections.
- `throughput_test.py` — aggregate ops/sec benchmark comparing `--mode=thread` vs `--mode=epoll`.
- `hol_blocking_test.py` — measures head-of-line blocking by racing a slow command against concurrent
  pings.
- `monitor_server.sh` / `monitor_server.ps1` — resource monitoring (threads, memory) while the above
  run.
- A `Dockerfile` for running the same experiments under constrained/multi-core hardware.
