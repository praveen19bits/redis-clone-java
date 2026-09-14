package com.praveen.redisclone;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The actual data engine. Phase 1 supports strings only (lists/hashes/sets
 * come in a later phase).
 *
 * DESIGN NOTE: real Redis is single-threaded, so it never needs locks around
 * its data structures at all -- that's WHY it's so fast. We're using an NIO
 * single-threaded event loop too (see RedisServer), so this store is only
 * ever touched by one thread. We still use ConcurrentHashMap + a lock here
 * defensively, and to leave the door open for a multi-threaded worker pool
 * later if you want to experiment with that tradeoff yourself.
 *
 * TTL/expiry: each key can have an optional expiry timestamp (epoch millis).
 * We do LAZY expiry (checked on read) here; Phase 2 will add ACTIVE expiry
 * (a background sweep), which is what real Redis does in addition to lazy
 * expiry.
 */
public class KeyValueStore {

    private final ConcurrentHashMap<String, String> data = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> expiryTimestamps = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void set(String key, String value) {
        lock.writeLock().lock();
        try {
            data.put(key, value);
            expiryTimestamps.remove(key); // SET clears any previous TTL, like real Redis
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String get(String key) {
        lock.readLock().lock();
        try {
            if (isExpired(key)) return null;
            return data.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean del(String key) {
        lock.writeLock().lock();
        try {
            expiryTimestamps.remove(key);
            return data.remove(key) != null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean exists(String key) {
        lock.readLock().lock();
        try {
            if (isExpired(key)) return false;
            return data.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void expire(String key, long seconds) {
        lock.writeLock().lock();
        try {
            if (data.containsKey(key)) {
                expiryTimestamps.put(key, System.currentTimeMillis() + seconds * 1000);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Lazy expiry: if a key's TTL has passed, evict it right now, on this read. */
    private boolean isExpired(String key) {
        Long expiry = expiryTimestamps.get(key);
        if (expiry == null) return false;
        if (System.currentTimeMillis() >= expiry) {
            data.remove(key);
            expiryTimestamps.remove(key);
            return true;
        }
        return false;
    }
}
