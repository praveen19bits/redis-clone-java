package com.praveen.redisclone;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The actual data engine. Phase 1 supports strings only (lists/hashes/sets
 * come in a later phase).
 *
 * LOCKING: a single ConcurrentHashMap<String, Entry> holds both the value
 * and its TTL per key, updated via compute()/computeIfPresent(). Those
 * methods hold ConcurrentHashMap's own per-bucket lock for the duration of
 * the remapping function, so "read the current entry, decide the new one,
 * write it back" is atomic per key -- with no external lock:
 *   - In --mode=epoll, only the single reactor thread ever calls in here, so
 *     there was never a race to prevent. The ReentrantReadWriteLock this
 *     replaced was pure overhead paid on every single command for zero
 *     correctness benefit.
 *   - In --mode=thread, that lock was one GLOBAL lock shared by every key --
 *     unrelated keys (e.g. SET user:1 from one client, SET user:2 from
 *     another) fully serialized behind each other even though nothing about
 *     them conflicts. Per-key atomicity via compute() lets unrelated keys
 *     proceed truly in parallel, using ConcurrentHashMap's existing
 *     per-bucket locking -- strictly better concurrency, not just a smaller
 *     lock.
 * This also halves the map operations per command: SET/GET/DEL/EXPIRE used
 * to each touch two separate maps (value + TTL); now they touch one.
 *
 * TTL/expiry: each entry carries its own expiry timestamp (0 = none). We do
 * LAZY expiry (checked on read) here; Phase 2 will add ACTIVE expiry (a
 * background sweep), which is what real Redis does in addition to lazy
 * expiry.
 */
public class KeyValueStore {

    private record Entry(String value, long expiryAtMillis) {
        boolean isExpired() {
            return expiryAtMillis != 0 && System.currentTimeMillis() >= expiryAtMillis;
        }
    }

    private final ConcurrentHashMap<String, Entry> data = new ConcurrentHashMap<>();

    public void set(String key, String value) {
        // A plain put() replaces any previous entry -- including whatever
        // TTL it carried -- in one map write, matching real Redis's "SET
        // clears any previous TTL" behavior without a separate TTL-map remove.
        data.put(key, new Entry(value, 0));
    }

    public String get(String key) {
        // computeIfPresent runs under the same per-key bin lock as any
        // concurrent SET/EXPIRE on this exact key, so "check expiry, evict if
        // stale" can't race with another thread's update to the same key --
        // returning null from the remapping function atomically removes the
        // mapping.
        Entry entry = data.computeIfPresent(key, (k, e) -> e.isExpired() ? null : e);
        return entry == null ? null : entry.value();
    }

    public boolean del(String key) {
        return data.remove(key) != null;
    }

    public boolean exists(String key) {
        return get(key) != null;
    }

    public void expire(String key, long seconds) {
        // No-op if the key is absent (computeIfPresent skips the function
        // entirely), matching the original "only set a TTL if the key
        // exists" behavior.
        data.computeIfPresent(key, (k, e) ->
                new Entry(e.value(), System.currentTimeMillis() + seconds * 1000));
    }
}
