---
name: verify
description: How to build, launch, and drive redis-clone-java to verify a change at runtime (not via tests/typecheck).
---

# Verifying redis-clone-java at runtime

Surface: a raw TCP socket speaking RESP. Drive it with `redis-cli` for
normal commands and raw Python sockets for malformed/edge-case protocol
input (redis-cli won't let you send garbage bytes).

## Build

```bash
mvn -q package
```

## Launch (pick free ports so you don't collide with a dev instance on 6380)

```bash
java -cp target/classes com.praveen.redisclone.RedisServer --mode=epoll  --port=6480 > /tmp/epoll.log  2>&1 &
java -cp target/classes com.praveen.redisclone.RedisServer --mode=thread --port=6481 > /tmp/thread.log 2>&1 &
```

Always test **both modes** — they share `RespParser`/`CommandDispatcher`/
`KeyValueStore` but have independent connection-handling code paths
(`EventLoopServer` vs `ThreadPerConnectionServer`), and bugs (e.g. a
handler thread dying silently) only show up in one of them.

`redis-cli` is at
`/c/dev-work/softwares/Redis-8.10.1-Windows-x64-msys2/redis-cli` on this
machine.

## Drive normal commands

```bash
redis-cli -p 6480 SET foo bar
redis-cli -p 6480 GET foo
redis-cli -p 6480 EXPIRE foo 100
redis-cli -p 6480 EXISTS foo
redis-cli -p 6480 DEL foo
```

## Drive protocol edge cases (raw socket, not redis-cli)

Use a small Python `socket.create_connection` script. Things worth
probing whenever `RespParser.java` changes:
- Oversized declared array/bulk length (`*2000000000\r\n`) — should
  close the connection cleanly, not crash/hang the server.
- Integer-overflow-inducing length (`$2147483647\r\n`).
- Negative bulk length (`$-5\r\n`).
- Garbage first byte / wrong marker byte.
- `*0\r\n` (empty array) immediately followed by a real pipelined
  command in the same buffer — regression check for buffer
  compaction after an early-return path.
- A command split across two separate `sendall()` calls with a sleep
  between them — regression check for partial-read handling.
- Multiple commands pipelined in one `sendall()` — regression check
  for multi-command draining per read.
- After sending garbage: reconnect and PING to confirm the *server*
  (not just that connection) is still alive.

**Gotcha:** when hand-building a RESP bulk string, the declared `$N`
length must exactly match the byte length of the value — an off-by-one
here looks exactly like a parser bug but is your own test payload
being malformed (the parser doesn't validate the trailing `\r\n` bytes
it skips, so a wrong length silently desyncs the rest of the buffer).
Always compute length as `len(value)` programmatically, never by hand.

## Drive concurrency (thread mode specifically)

Spin up N Python threads, each opening its own connection and hammering
`SET` on a distinct key many times; assert every reply is `+OK\r\n`.
This is the regression check for `KeyValueStore`'s locking — a global
lock would still pass this, but it's the right test to extend with
timing/throughput if `KeyValueStore` changes again.

## Cleanup

```bash
kill %1 %2  # or the PIDs you saved
```
Check both log files' tails for stack traces before declaring success —
a crashed reactor thread in epoll mode doesn't always announce itself
to the client, only to stderr.
