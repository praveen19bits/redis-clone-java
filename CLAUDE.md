# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

A from-scratch Redis server clone in Java, built as a learning project for distributed systems
architecture and design patterns (not a production Redis replacement). It is currently in Phase 1
(single-node, in-memory string store, two swappable concurrency models). The full roadmap and a large
amount of design rationale — why Java was chosen, epoll/reactor internals, head-of-line blocking
experiments, locking correctness arguments, measured thread-vs-epoll benchmarks — lives in
[redis-clone-learning-notes.md](redis-clone-learning-notes.md). Read that file before making
architectural changes; it explains *why* things are built the way they are, not just what the code does.

## Build, run, test

Build with Maven:
```
mvn package
```
Produces `target/redis-clone.jar` (`mainClass` is `com.praveen.redisclone.RedisServer`, set via
`maven-jar-plugin` in [pom.xml](pom.xml)).

Run directly:
```
java -cp target/classes com.praveen.redisclone.RedisServer --mode=epoll  [--port=6380]
java -cp target/classes com.praveen.redisclone.RedisServer --mode=thread [--port=6380]
```
`--mode` defaults to `epoll` if omitted; `--port` defaults to `6380`. VS Code launch configs for both
modes exist in [.vscode/launch.json](.vscode/launch.json).

There is no test suite (no `src/test`, no JUnit dependency) — correctness is currently verified by the
Python scripts below, run manually against a live server. When adding tests, wire in a test framework
via `pom.xml` first.

### Manual protocol / behavior verification
Talk to the running server with raw RESP over TCP (`redis-cli` works fine, or a raw socket) on whatever
`--port` it was started with.

### Experiment / load scripts (Python, no dependencies beyond stdlib)
- `python3 load_simulator.py --connections 2000 --hold-seconds 30 --port 6380` — opens N idle
  connections to demonstrate epoll's flat memory/thread cost vs thread-per-connection's linear cost.
- `python3 throughput_test.py --clients 8 --duration 5 --command SET|GET --port 6380` — aggregate
  ops/sec benchmark, used to compare `--mode=thread` vs `--mode=epoll`.
- `python3 hol_blocking_test.py --slow-millis 5000 --port 6380` — proves/measures head-of-line blocking
  by racing a `SLOWCOMPUTE` command against concurrent `PING`s.

### Resource monitoring while testing
- Linux: `monitor_server.sh` (reads `/proc/<pid>/status`)
- Windows: `monitor_server.ps1 -ProcessId <pid> [-IntervalSeconds 2]` (reads `Get-Process` thread
  count / working set)

### Docker (for multi-core / resource-constrained experiments)
```
docker build -t redis-clone-java .
docker run --rm -p 6380:6380 --cpus=1 --memory=1g redis-clone-java --mode=epoll --port=6380
```
Note the Dockerfile compiles with plain `javac` (not Maven) in its build stage, and runs on a full JDK
(not JRE) specifically so `jcmd`/`jstack`/`jstat` work via `docker exec`. Keep the Dockerfile and
`pom.xml` build in sync manually if source layout changes.

## Architecture

Package root: `com.praveen.redisclone`.

**The central design decision:** protocol parsing, command dispatch, and data storage are fully
decoupled from the concurrency model. `RespParser`, `RespWriter`, `CommandDispatcher`, and
`KeyValueStore` are shared unchanged by both server implementations — only how connections are
scheduled differs. When changing shared code, verify both `--mode=epoll` and `--mode=thread` still work.

| Component | Responsibility | Lifecycle |
|---|---|---|
| `RedisServer` | Entry point; parses `--mode`/`--port`, launches the chosen server | n/a |
| `EventLoopServer` | Single-threaded NIO reactor (`Selector`) — one OS thread total, regardless of connection count | n/a |
| `ThreadPerConnectionServer` | One blocking `java.lang.Thread` per connected client | n/a |
| `RespParser` | Buffers/parses raw bytes into a command (`List<String>`); handles commands split across multiple TCP reads and multiple pipelined commands per read | **Per-connection** (stateful) |
| `RespWriter` | Formats RESP reply types (simple string, error, integer, bulk string, null bulk) | Shared, stateless |
| `Command` (interface) + `commands/*` | One class per command (`PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`, `EXPIRE`, `SLOWCOMPUTE`) | Shared, stateless |
| `CommandDispatcher` | Uppercases command name, looks it up in a `Map<String, Command>`, invokes it | Shared, stateless |
| `KeyValueStore` | In-memory `ConcurrentHashMap` + TTL map, guarded by a `ReentrantReadWriteLock` | Shared — the one place concurrent access matters |

### Adding a new command
Add a class in `commands/` implementing `Command.execute(List<String> args, KeyValueStore store)`
returning a fully RESP-encoded reply (use `RespWriter`), then register it in
`CommandDispatcher`'s constructor. No other code needs to change — this is the point of using the
Command pattern here.

### `KeyValueStore` locking — do not remove the lock casually
`ConcurrentHashMap`'s own thread-safety only guarantees single-operation atomicity, not atomicity across
a sequence of operations. `set()` touches two maps (value + TTL removal); without the
`ReentrantReadWriteLock` wrapping both, a concurrent `EXPIRE` from another thread (only possible in
`--mode=thread`) can race with a `SET` and silently lose the TTL. In `--mode=epoll` the lock is pure
overhead (only one thread ever touches the store) but is kept so one `KeyValueStore` class correctly
serves both server modes. See §10 of the learning notes for the full argument before changing this.

### Known architectural trade-off: head-of-line blocking in epoll mode
Because `EventLoopServer` is single-threaded, a slow/CPU-bound command handler (see
`SlowComputeCommand`, kept specifically to demonstrate this) blocks every other client for its entire
duration. This is intentional/expected behavior for this phase, not a bug — see §9 of the learning
notes. Every command handler is expected to be fast and non-blocking; don't add a handler that does
blocking I/O or heavy computation without accounting for this.

### Uncaught exceptions in `EventLoopServer`
The reactor loop's per-key `try/catch` is deliberately careful: an uncaught exception on the single
reactor thread kills the entire process (no other non-daemon thread exists to keep it alive), so any new
code added inside the `select()` loop must not let exceptions escape uncaught, and cleanup on error must
type-check the channel (`ServerSocketChannel` for the accept key vs `SocketChannel` for client keys)
before casting.
