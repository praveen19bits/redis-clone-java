# Redis Clone: Learning Journal & Interview Reference

Everything covered so far while building a Redis server from scratch in Java, for learning distributed systems architecture and design patterns.

---

## 1. Why this project, and why Java

**Goal:** learn distributed systems architecture and design patterns end-to-end by building a Redis clone from scratch — not just use Redis, actually implement its internals.

**Language decision — Java vs Rust vs Go:**
- **Go** was the initial "fastest path" recommendation — goroutines map naturally to concurrency, closest to real distributed tooling (etcd, Kubernetes).
- **Rust** would teach memory/concurrency safety the hard way (closest to real Redis's C internals), but the borrow checker fight over shared mutable state (the KV store) risks eating the whole timeline before reaching the interesting distributed-systems material.
- **Java was chosen** because: existing fluency means faster progress through the phases that matter (Raft, gossip, sharding); NIO/`Selector` gives an explicit, teachable model of the event-loop/reactor pattern (arguably *more* explicit than Go, where the runtime hides it); GC pauses are a real downside for latency-sensitive work, but that gap itself becomes a worthwhile side-lesson (why Redis avoids GC'd languages).
- Rust remains a good **second pass** later — re-implement just the storage engine in Rust once the Java version is complete, to feel the memory-management difference directly.

---

## 2. Project architecture (Phase 1, built and running)

**Package:** `com.praveen.redisclone`

| Component | Responsibility | Shared or per-connection? |
|---|---|---|
| `RedisServer` | Launcher — reads `--mode=thread\|epoll` and `--port` flags, starts the right server | n/a |
| `EventLoopServer` | Single-threaded NIO reactor implementation | n/a |
| `ThreadPerConnectionServer` | Traditional one-thread-per-client implementation | n/a |
| `RespParser` | Parses raw bytes into a command list, e.g. `["SET","foo","1"]` | **Per-connection** — stateful, because commands can arrive split across multiple reads |
| `RespWriter` | Formats replies into RESP wire format | Shared — pure stateless helpers |
| `Command` (interface) + `commands/*` | One class per Redis command (PING, ECHO, SET, GET, DEL, EXISTS, EXPIRE, SLOWCOMPUTE) | Shared — stateless logic |
| `CommandDispatcher` | Maps command name string → `Command` object, routes execution | Shared — stateless routing |
| `KeyValueStore` | The actual data engine (in-memory map + TTL tracking) | Shared — the ONE place concurrent access is a real concern |

**Key design insight proven in practice:** protocol parsing (`RespParser`) is completely decoupled from the concurrency model. The exact same parser class is fed by a non-blocking `Selector` loop in `EventLoopServer` and by a blocking per-thread `read()` loop in `ThreadPerConnectionServer`, unchanged. This is what let the whole concurrency model be swapped via a flag without touching protocol or command-dispatch code.

---

## 3. RESP protocol (the wire format)

Redis clients send commands as RESP Arrays of Bulk Strings:
```
*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
```
- `*3` → array of 3 elements
- `$3\r\nSET\r\n` → bulk string, length 3, value "SET"

Five core reply types: Simple String (`+OK\r\n`), Error (`-ERR ...\r\n`), Integer (`:1\r\n`), Bulk String (`$3\r\nbar\r\n`), Null Bulk String (`$-1\r\n`, the "key not found" reply).

**Hard part solved:** commands can arrive split across multiple TCP reads. `RespParser` buffers incoming bytes and only returns a parsed command once a complete one is available, rewinding cleanly on partial data. Verified working via:
- Raw byte-level testing against every command
- **Pipelining test** — 3 commands sent in a single TCP write, all 3 parsed and answered correctly (`+OK+OK$1\r\n1`), proving the parser correctly drains multiple buffered commands per read.

---

## 4. Design patterns used, and why each one fits

| Pattern | Where | Why it fits |
|---|---|---|
| **Command** | `Command` interface + `commands/*` + `CommandDispatcher` | Adding a new command = one new class, zero changes elsewhere. Also sets up MULTI/EXEC later — a transaction is just a queued `List<Command>`. Mirrors real Redis's `commands.c` dispatch table structure. |
| **Strategy** | Planned for eviction policies (LRU/LFU/TTL) | Swappable algorithm behind one interface |
| **Observer** | Planned for Pub/Sub | Subscribers react to published events |
| **State** | Planned for connection/replication states | |
| **Template Method** | Planned for startup/shutdown lifecycle | |
| **Singleton** | `KeyValueStore` instance | One shared store |
| **Factory** | Command parsing/lookup in `CommandDispatcher` | |
| **Proxy / Decorator** | Not yet built — natural place for cross-cutting concerns (logging, metrics, auth) wrapped around `Command` objects without touching command logic itself |

---

## 5. The Reactor Pattern — the architectural core

Real Redis is single-threaded: one thread runs an event loop multiplexing thousands of connections via OS-level I/O notification (**epoll** on Linux, **kqueue** on macOS/BSD, **IOCP** on Windows). It never blocks on any one client — it asks the OS "which sockets are ready right now?" and only touches those. Java's `java.nio.channels.Selector` is the portable equivalent, picking whichever native mechanism matches the running OS.

**Flow per loop iteration (`EventLoopServer`):**
1. `selector.select()` blocks until ≥1 channel is ready
2. For each ready channel: accept new connections, or read/write data
3. Repeat forever

**Mapping to raw epoll syscalls:**
- `serverChannel.register(selector, OP_ACCEPT)` → `epoll_ctl()` (register interest, once)
- `selector.select()` → `epoll_wait()` (block until ready)
- `selector.selectedKeys()` → the kernel handing back just the ready FDs, not all of them

---

## 6. What is a socket, what is a file descriptor (FD)

- A **socket** is a kernel object representing one connection endpoint: connection state, a receive buffer, a send buffer.
- A **file descriptor (FD)** is a small integer your process holds that indexes into a table pointing at that kernel object. `accept()` returns an FD (e.g. `7`); Java's `SocketChannel` is a thin wrapper around it.

**One packet's full journey (the mechanism behind epoll):**
1. Packet arrives on the NIC for FD 7
2. Hardware interrupt fires — CPU jumps into the kernel
3. Kernel copies the data into FD 7's receive buffer
4. **epoll callback runs** (registered once via `epoll_ctl`) — appends FD 7 to a separate **ready list**. This is O(1), incremental, paid once per packet.
5. `epoll_wait()` returns — hands the waiting thread exactly `[7]`

**epoll keeps two structures:**
- **Interest list** — built once per connection via `epoll_ctl()` ("watch these sockets")
- **Ready list** — built incrementally by interrupt-time callbacks; `epoll_wait()` just reads this, never re-scans everything

**Contrast with `select()`/`poll()`:** no ready list exists — every call re-hands the kernel the *entire* socket list and the kernel re-scans all of them, an O(n) cost paid on every single call, even when 998 of 1000 sockets haven't changed. epoll's entire trick: pay the "who's ready" cost once, at interrupt time, in the kernel — never re-pay it in your own loop.

---

## 7. The C10K problem, and epoll's place in history

- **The problem:** a server with 1,000 clients where only 2 are active at any instant. How do you find the 2 without wasting effort on the 998?
- **Thread-per-connection:** 1,000 OS threads × ~1MB stack reservation, plus OS scheduler context-switch overhead between all of them. This is the actual **C10K problem**, coined ~1999 — this model collapses from pure overhead well before ~10,000 connections.
- **`select()`/`poll()`:** fixes the "no thread pool" problem but is O(n) per call — you re-describe your whole connection list to the kernel every iteration.
- **epoll — the fix:** invented by **Davide Libenzi**, merged into mainline Linux kernel **2.5.45, October 2002**. Not the first of its kind: Windows' IOCP predates it (1994), BSD's kqueue predates it by 2 years (July 2000) — each OS solved C10K independently. epoll itself is still evolving (`EPOLLEXCLUSIVE` added in kernel 4.5, March 2016).
- **First major adopter:** **nginx** (Igor Sysoev, first released 2004), built specifically to solve C10K for high-traffic sites — small fixed number of worker processes, each running an epoll event loop handling thousands of connections apiece. Architecturally near-identical to this project's `EventLoopServer`.

---

## 8. Real measured numbers (not estimates) — thread mode vs epoll mode

### Linux, 4,000 connections
| | Baseline | At 4,000 connections | Per-connection cost |
|---|---|---|---|
| Thread mode | 13 threads, 40,336 KB | 4,013 threads, 298,596 KB | **~64.6 KB/connection** |
| Epoll mode | 13 threads, 40,932 KB | 13 threads, 80,556 KB | **~9.9 KB/connection** |

### Windows, 1,500 connections
| | Baseline | At 1,500 connections | Per-connection cost |
|---|---|---|---|
| Thread mode | 31 threads, 80,080 KB | 1,531 threads, 193,208 KB | **~75.4 KB/connection** |
| Epoll mode | 22 threads, 47,192 KB | 24 threads (JVM housekeeping only), 76,072 KB | **~19.25 KB/connection** |

**Cross-platform takeaway:** exact numbers differ (different OS, memory manager, JVM build), but the *shape* is identical on both: thread mode scales memory and thread count linearly with connections; epoll stays essentially flat. Thread mode used 3.9×–6.5× more memory per idle connection than epoll across both platforms.

**The hard wall:** this Linux container's kernel thread ceiling was **31,841 total threads for the whole machine**. At ~100,000 target connections, thread mode is not just inefficient — it's **structurally impossible** on hardware like this; epoll has no such ceiling since thread count never depends on connection count.

**Why per-thread cost (~65-75KB) is much less than the 1MB `-Xss` stack reservation:** Linux reserves 1MB of *virtual* address space per thread but only commits physical RAM pages actually touched. An idle thread blocked in `read()` has shallow call depth, so it uses only a handful of pages.

**Tools used to measure this (the actual point of the exercise):**
- Linux: `/proc/<pid>/status` (`Threads:`, `VmRSS:`) — the same source `top`/`htop`/`ps` read from
- Windows: PowerShell `Get-Process -Id <pid> | Select Threads, WorkingSet` (`WorkingSet` = Windows' equivalent of `VmRSS`)
- Custom Python load simulator (`load_simulator.py`) — needed because the protocol is raw RESP over TCP, not HTTP, so generic tools like `wrk`/`ab` don't apply
- `jcmd <pid> Thread.print` / `jstack` — full thread dump, would show hundreds of threads all blocked at the identical line in thread mode
- `top -H -p <pid>` — live per-thread CPU view

---

## 9. Head-of-line blocking — the trade-off in the other direction

Epoll's efficiency comes at a real cost: since there's only one thread, a slow command blocks *every* other client. Proven with a real experiment:

**Setup:** added a genuinely CPU-bound `SLOWCOMPUTE <ms>` command (a busy-loop, not `sleep`, so it can't be optimized away as idle waiting). Client A sends `SLOWCOMPUTE 5000`; Client B sends `PING` every 200ms throughout.

**Results:**
| | Epoll mode | Thread mode |
|---|---|---|
| Client A's compute duration | 5.02s | 5.01s (identical work) |
| Client B's worst ping | **4,810 ms** | **17.3 ms** |
| Client B's average ping | 301.7 ms | 0.9 ms |
| Pings B completed in 8s | 16 | 40 |

**Why:** in epoll mode, B's ping got queued behind A's compute in the single reactor thread's sequential processing — B had to wait almost the entire remaining duration of A's work. In thread mode, A and B run on independent OS threads; the OS scheduler shares real CPU cores between them, so B is never forced to wait for A's unrelated work.

**Important nuance:** this isn't "epoll failed" — it's epoll behaving exactly as designed. Epoll was built to solve *idle-connection* overhead, never to parallelize *active* command execution. The failure mode only appears when a handler violates the architecture's core assumption: **every command handler must be fast**. Real Redis's `KEYS *` and `FLUSHALL` are the canonical production examples of this exact problem.

---

## 10. Locking and concurrency correctness

**In epoll mode:** the lock around `KeyValueStore` is pure overhead — only one thread ever touches the store, so there's no race to prevent. Small but real wasted CPU cost per command, buying zero correctness benefit.

**In thread mode:** the lock is necessary, and `ConcurrentHashMap`'s own thread-safety is *not* sufficient by itself. `ConcurrentHashMap` only guarantees individual operations are atomic — not a *sequence* of operations across two maps. `KeyValueStore.set()` does two map operations (put the value, clear any old TTL) — without an external lock wrapping both, a concurrent `EXPIRE` from another thread could interleave between them: Thread-A's `SET` could silently erase a TTL that Thread-B's concurrent `EXPIRE` just set, with the client believing `EXPIRE` succeeded. This is a genuine correctness bug that would only surface under real concurrent load, not casual testing.

**Honest engineering conclusion:** if targeting epoll-only deployment, the lock should be dropped in favor of `ConcurrentHashMap`'s native guarantees. Keeping it serves both modes correctly from one shared class, at a small performance cost — a reasonable trade for this project, worth revisiting for a single-target deployment.

---

## 11. Does the shared store bottleneck both modes equally? (throughput experiment)

**The doubt:** eventually both modes share the same `KeyValueStore` — doesn't that make the latency impact the same regardless of mode?

**Partly true, partly not — measured with a real throughput benchmark** (`throughput_test.py`, N persistent clients firing commands as fast as possible for a fixed duration):

On this sandbox (single CPU core):
| | Thread mode | Epoll mode |
|---|---|---|
| SET (write-heavy), 8 clients, 5s | 61,773 ops/sec | **73,264 ops/sec** |
| GET (read-heavy), 8 clients, 5s | 65,543 ops/sec | **76,365 ops/sec** |

**Where the "same bottleneck" intuition is correct:** for *writes*, both modes do converge to serializing around the shared store — thread mode via the write lock (store-wide, not per-key — two threads writing unrelated keys still queue behind each other), epoll mode via natural single-thread ordering. Structurally similar.

**Where it breaks down:**
1. **Window size differs.** In thread mode, the lock is held only for the actual map operation (~50–200ns) — parsing, dispatch, and the socket write still happen in parallel across threads. In epoll mode, the *entire* command (parse + dispatch + store access + write) is serialized, because there's nothing else to run it concurrently with. This is why `SLOWCOMPUTE` stalled another client for 4.8 seconds — a store-only lock would never cause a stall that large.
2. **Reads are not equivalent.** `KeyValueStore` uses a `ReadWriteLock` — multiple threads can hold the read lock *simultaneously*, so on real multi-core hardware, thread mode allows genuinely parallel GETs across cores. Epoll mode can never do this — one thread, full stop, regardless of core count.
3. **Single-core result was misleading in an instructive way:** epoll won *both* benchmarks here because thread mode paid real lock/context-switch overhead while getting zero benefit from "parallel reads" — there was no second core for a second read to land on. The expectation on genuinely multi-core hardware: epoll's throughput stays roughly flat (still one thread, one core, regardless of how many cores exist), while thread mode's GET throughput should climb with core count. This flip couldn't be proven in a 1-core sandbox — it's an open experiment to run on real multi-core hardware (see Section 13, resource-constrained testing).

**One-line answer:** the shared store is a real bottleneck in both modes, but only thread mode has any mechanism to push past it for reads — and that mechanism needs multiple cores to matter at all.

---

## 12. How a single thread routes responses to the *correct* client (FD demultiplexing)

**The doubt:** with only one thread, how does the server avoid mixing up which reply goes to which client?

**The resolving insight:** the kernel already knows which bytes belong to which client, before the single thread ever runs — demultiplexing is a TCP/IP-stack fact, not something the event loop thread has to manage.

**The 4-tuple:** a TCP connection is uniquely identified by (source IP, source port, destination IP, destination port) together. The server listens on one port (6380), but that's only half of each connection's identity — Client A (`192.168.1.5:54321`) and Client B (`192.168.1.9:60110`) have completely different 4-tuples even though both hit the same server port. The kernel creates a **separate socket (separate FD) per accepted connection**, each with its own independent send/receive buffers.

**The write call is inherently scoped:** in `handleRead()`, the line `client.write(...)` isn't "send this to whoever" — `client` is the specific `SocketChannel` object bound to one particular FD from `accept()`. The JVM's write syscall targets that exact FD number; the kernel copies bytes into *that* FD's send buffer only, then routes them out using that FD's already-known 4-tuple. FD 8's buffer is never touched by a write aimed at FD 7.

**Full sequence, two clients:**
1. Client A's command arrives → kernel appends FD 7 to the ready list
2. `select()` wakes with FD 7's `SelectionKey` — the thread already knows exactly which `SocketChannel` to read from and reply to
3. Thread computes the reply, calls `client.write(...)` where `client` is unambiguously the FD-7 channel
4. Kernel writes into FD 7's send buffer only, sends it toward `192.168.1.5:54321`
5. Thread loops back to `select()`; later FD 8 becomes ready, same thread repeats the cycle with the FD-8 channel, routing to `192.168.1.9:60110` — Client A never sees any of it

**One-line answer:** the FD carries the client's identity, baked in by the kernel at `accept()` time. One thread juggling many FDs is choosing *scheduling order* among already-separated connections, not resolving ambiguity about whose data is whose.

---

## 13. Resource-constrained testing (Docker)

Added to enable exactly this experiment on real multi-core hardware, cleanly and reproducibly:

- **`Dockerfile`** — multi-stage build (JDK to compile, JRE to run)
- **`RedisServer.java`** now logs `JVM sees N CPU core(s) available, max heap M MB` on startup — confirms a `--cpus`/`--memory` constraint actually took effect (modern JVMs read Docker's cgroup limits automatically as of JDK 10+)

**Commands:**
```
docker build -t redis-clone-java .
docker run --rm -p 6380:6380 --cpus=1 --memory=1g redis-clone-java --mode=epoll --port=6380
docker run --rm -p 6380:6380 --cpus=8 --memory=4g redis-clone-java --mode=epoll --port=6380
```
Then run `throughput_test.py` against each, for both `--mode=thread` and `--mode=epoll`, to see whether thread mode's GET throughput actually climbs with core count while epoll's stays flat — the direct test of Section 11's open question.

**Tuning notes:** use more client threads than cores (e.g. `--clients 32`) to keep the server saturated; expect JIT warm-up variance on short (5s) runs; try `--memory=256m` to observe GC-pressure latency spikes under a harder memory constraint.

---

## 14. When to use each model — the decision framework

**Thread-per-connection fits when:**
1. Connections are few, each doing real sustained CPU or blocking work (e.g., a scientific computing service) — true OS-level isolation between clients, no single point of stalling
2. Blocking libraries can't be avoided (legacy JDBC, blocking SDKs) — blocking one thread doesn't freeze the whole server
3. Code simplicity matters more than raw scale — linear blocking code is easier to write, debug, and stack-trace than event-driven callback code

**Epoll/event-loop fits when:**
1. Connection count is large and mostly idle (chat servers, WebSocket/SSE push, pub/sub, API gateways, IoT fleets)
2. Per-request work is genuinely fast and non-blocking (Redis's hash lookups, nginx serving static files/reverse-proxying)
3. Resource efficiency at scale matters more than per-client isolation — fewer servers, lower cost, less fleet-coordination complexity

**The practical middle ground — real systems hybridize:**
- **nginx**: event loop for connections, separate thread pool just for blocking disk reads (`aio threads`)
- **Node.js**: single-threaded JS event loop for app code, but libuv runs a background thread pool for filesystem/DNS work
- **Modern Redis (6.0+)**: a few extra I/O threads just for socket byte-shuffling, but command *execution* stays strictly single-threaded on purpose — this is what lets Redis guarantee every command is atomic with zero locks anywhere in its core data structures

**Quick gut-check:** "At peak, will most connections be idle most of the time, and is per-request work fast and non-blocking?" Yes to both → epoll. Either "no" → thread-per-connection or the hybrid pattern.

---

## 15. Real-world parallel: Spring Boot + HikariCP + PostgreSQL

Two different pooling patterns stacked, not one pattern repeated:

- **Tomcat** (HTTP layer): a **bounded, reusable** worker thread pool (default max 200). Each request borrows a thread, returns it to the pool when done — never unbounded thread creation like the naive `ThreadPerConnectionServer`.
- **HikariCP** (DB layer): a small, **fixed** pool of pre-opened connections (default sizing ≈ `(core_count × 2) + effective_spindle_count`, often as low as 10) — connections are borrowed and returned, not opened per-request.
- **PostgreSQL server side**: for every one of those pooled connections, Postgres forks a **dedicated OS process** (MySQL uses a thread instead) that lives for the connection's entire lifetime — this is the same "one dedicated worker per connection" cost profile measured earlier, just relocated to the database machine.
- **Why the DB pool is kept deliberately small:** each connection costs a real OS process on a shared, often harder-to-scale machine. Sizing it to match Tomcat's 200 would reproduce the exact resource-exhaustion pattern measured with 4,000 Java threads — except now it's the database paying the cost.
- **Failure mode when exceeded:** threads block waiting for a pooled connection to free up; if none frees up in time, `SQLTransientConnectionException: Connection is not available, request timed out` — same *category* of failure as hitting the OS thread ceiling, surfacing at a different layer.

---

## 16. Interview talking points (condensed Q&A)

**Q: What problem does epoll solve, and how?**
A: The C10K problem — tracking readiness across thousands of connections without O(n) rescanning or one-thread-per-connection overhead. epoll splits registration (`epoll_ctl`, once) from waiting (`epoll_wait`), and the kernel maintains a ready list incrementally via interrupt-time callbacks, so `epoll_wait` never scans — it just reads whatever's already in the ready list.

**Q: Why is Redis single-threaded?**
A: Command execution is fast enough (microseconds) that serializing it costs little, and staying single-threaded for execution buys atomicity guarantees with zero locks in the core data structures — much simpler to reason about correctness. (Redis 6.0+ still adds I/O threads, but only for socket read/write, never command execution.)

**Q: What's the trade-off epoll makes that people miss?**
A: It optimizes for idle-connection overhead, not command parallelism. A single slow handler blocks every other client, because there's only one thread. This is the head-of-line blocking risk — measured directly with the `SLOWCOMPUTE` experiment (4.8s stall on an unrelated PING).

**Q: When would you NOT use an event loop?**
A: Few connections doing sustained CPU/blocking work, or when you're stuck with blocking libraries you can't replace with non-blocking equivalents — thread-per-connection gives real OS-level isolation there.

**Q: Why does `ConcurrentHashMap` alone not make `KeyValueStore` thread-safe?**
A: It only guarantees single-operation atomicity, not atomicity across a sequence of operations spanning two maps (the SET/EXPIRE TTL race is the concrete example).

**Q: How does this relate to Spring Boot / HikariCP?**
A: Same "dedicated worker per connection" cost pattern, applied to the database layer via Postgres's per-connection process model — which is exactly why HikariCP pools are kept deliberately small rather than matching the web tier's thread pool size.

**Q: With only one thread, how does epoll mode avoid mixing up replies between clients?**
A: It never has to resolve that ambiguity — the kernel already separates connections by their unique 4-tuple (src IP, src port, dst IP, dst port) at `accept()` time, giving each one its own FD and its own send/receive buffers. A `write()` call is scoped to one specific FD; the single thread only decides *scheduling order* among already-separated sockets, not which bytes belong to whom.

**Q: Doesn't the shared `KeyValueStore` make both modes bottleneck equally?**
A: For writes, yes — both modes serialize around the store (a store-wide lock in thread mode, natural single-thread ordering in epoll mode). But the *window* differs (thread mode's lock covers only the map operation; epoll's serialization covers the whole command), and reads are not equivalent — thread mode's `ReadWriteLock` allows genuinely parallel GETs across cores, which epoll structurally cannot do regardless of core count.

---

## 17. Next phases — the roadmap from here

**Phase 2 — Durability (not yet built):**
- AOF (append-only file) logging
- RDB-style snapshotting (fork/copy-on-write semantics)
- Recovery on restart

**Phase 3 — Concurrency & pub/sub (not yet built):**
- Multiplexed connection handling refinements
- Pub/Sub (Observer pattern)
- Transactions: MULTI/EXEC/WATCH (optimistic locking) — natural fit given the Command pattern already in place

**Phase 4 — Replication (not yet built):**
- Leader-follower replication (async first, then semi-sync)
- Replication log/offset tracking
- Failover detection (heartbeat/health checks)

**Phase 5 — Distributed systems layer (not yet built, the core payoff):**
- Consistent hashing for sharding across nodes
- Gossip protocol for cluster membership (Redis Cluster style)
- Minimal Raft implementation for leader election
- CAP theorem tradeoffs — deliberately choosing and justifying consistency vs. availability at each layer

**Phase 6 — Production concerns (not yet built):**
- Client connection pooling, backpressure
- Metrics/observability (Decorator pattern around `Command` execution)
- Config loading, graceful shutdown (Template Method pattern)

**Concrete near-term suggestions:**
1. Try adding a new command yourself (e.g. `INCR`) to test whether the Command pattern has fully clicked
2. Strip the lock from `KeyValueStore` temporarily and reproduce the SET/EXPIRE race live under thread mode, to see the correctness bug rather than just reason about it
3. Consider a hybrid worker-pool experiment: keep the epoll accept/read loop, but hand `SLOWCOMPUTE`-style work off to a small separate thread pool instead of running it on the reactor thread — this is the nginx/Node.js pattern, and building a minimal version of it would directly demonstrate the fix for the head-of-line blocking problem measured in Section 9
4. Run the Docker-based throughput experiment from Section 13 on real multi-core hardware (`--cpus=1` vs `--cpus=8`) to resolve the open question from Section 11: does thread mode's GET throughput actually climb with core count while epoll's stays flat?
5. Optionally: a second-pass storage engine written in Rust, once Phase 1–3 are solid in Java, to feel the memory-management contrast directly
