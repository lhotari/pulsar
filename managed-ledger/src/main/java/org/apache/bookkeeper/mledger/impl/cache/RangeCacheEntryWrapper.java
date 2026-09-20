/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.impl.cache;

import io.netty.util.Recycler;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReferenceCountedEntry;

/**
 * Wrapper around the value to store in Map. This is needed to ensure that a specific instance can be removed from
 * the map by calling the {@link Map#remove(Object, Object)} method. Certain race conditions could result in the
 * wrong value being removed from the map. The instances of this class are recycled to avoid creating new objects.
 */
@CustomLog
class RangeCacheEntryWrapper {
    private static final VarHandle VERSION;
    private static final int SPIN_LIMIT = 256;

    static {
        try {
            VERSION = MethodHandles.lookup().findVarHandle(RangeCacheEntryWrapper.class, "version", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Recycler.Handle<RangeCacheEntryWrapper> recyclerHandle;
    private static final Recycler<RangeCacheEntryWrapper> RECYCLER = new Recycler<RangeCacheEntryWrapper>() {
        @Override
        protected RangeCacheEntryWrapper newObject(Handle<RangeCacheEntryWrapper> recyclerHandle) {
            return new RangeCacheEntryWrapper(recyclerHandle);
        }
    };
    // Even values denote a stable state and odd values a mutation. Keeping the state in the
    // wrapper avoids allocating a separate lock for every recycler miss.
    private volatile int version;
    Position key;
    ReferenceCountedEntry value;
    RangeCache rangeCache;
    long size;
    long timestampNanos;
    int requeueCount;
    volatile boolean accessed;

    private RangeCacheEntryWrapper(Recycler.Handle<RangeCacheEntryWrapper> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    static <R> R withNewInstance(RangeCache rangeCache, Position key, ReferenceCountedEntry value, long size,
                                 Function<RangeCacheEntryWrapper, R> function) {
        RangeCacheEntryWrapper entryWrapper = RECYCLER.get();
        return entryWrapper.withWriteLock(wrapper -> {
            entryWrapper.rangeCache = rangeCache;
            entryWrapper.key = key;
            entryWrapper.value = value;
            entryWrapper.size = size;
            // Set the timestamp to the current time in nanoseconds
            // This is used for time-based eviction of entries
            entryWrapper.timestampNanos = System.nanoTime();
            return function.apply(entryWrapper);
        });
    }

    /**
     * Get the value associated with the key. Returns null if the key does not match the key.
     *
     * @param key the key to match
     * @return the value associated with the key, or null if the value has already been recycled or the key does not
     * match
     */
    ReferenceCountedEntry getValue(Position key) {
        return getValueInternal(key, false);
    }

    /**
     * Get the value associated with the Map.Entry's key and value. Exact instance of the key is required to match.
     *
     * @param entry the entry which contains the key and {@link RangeCacheEntryWrapper} value to get the value from
     * @return the value associated with the key, or null if the value has already been recycled or the key does not
     * exactly match the same instance
     */
    static ReferenceCountedEntry getValueMatchingMapEntry(Map.Entry<Position, RangeCacheEntryWrapper> entry) {
        return entry.getValue().getValueInternal(entry.getKey(), true);
    }

    /**
     * Get the value associated with the key. Returns null if the key does not match the key associated with the
     * value.
     *
     * @param key                    the key to match
     * @param requireSameKeyInstance when true, the matching will be restricted to exactly the same instance of the
     *                               key as the one stored in the wrapper. This is used to avoid any races
     *                               when retrieving or removing the entries from the cache when the key and value
     *                               instances are available.
     * @return the value associated with the key, or null if the key does not match
     */
    private ReferenceCountedEntry getValueInternal(Position key, boolean requireSameKeyInstance) {
        int attempts = 0;
        Position localKey;
        ReferenceCountedEntry localValue;
        while (true) {
            int stamp = (int) VERSION.getAcquire(this);
            if ((stamp & 1) != 0) {
                pauseAfterFailedAttempt(++attempts);
                continue;
            }
            localKey = this.key;
            localValue = this.value;
            if ((int) VERSION.getAcquire(this) == stamp) {
                break;
            }
            pauseAfterFailedAttempt(++attempts);
        }
        // check that the given key matches the key associated with the value in the entry
        // this is used to detect if the entry has already been recycled and contains another key
        // when requireSameKeyInstance is true, the key must be exactly the same instance as the one stored in the
        // entry to match
        if (localKey != key && (requireSameKeyInstance || localKey == null || !localKey.equals(key))) {
            return null;
        }
        accessed = true;
        return localValue;
    }

    /**
     * Marks the entry as removed if the key and value match the current key and value.
     * This method should only be called while holding the write lock within {@link #withWriteLock(Function)}.
     * @param key the expected key of the entry
     * @param value the expected value of the entry
     * @return the size of the entry if the entry was removed, -1 otherwise
     */
    long markRemoved(Position key, ReferenceCountedEntry value) {
        if (this.key != key || this.value != value) {
            return -1;
        }
        rangeCache = null;
        this.key = null;
        this.value = null;
        long removedSize = size;
        size = 0;
        timestampNanos = 0;
        requeueCount = 0;
        return removedSize;
    }

    <R> R withWriteLock(Function<RangeCacheEntryWrapper, R> function) {
        int stamp = acquireWriteLock();
        try {
            return function.apply(this);
        } finally {
            VERSION.setRelease(this, stamp + 2);
        }
    }

    private int acquireWriteLock() {
        int attempts = 0;
        while (true) {
            int stamp = (int) VERSION.getAcquire(this);
            if ((stamp & 1) == 0 && VERSION.compareAndSet(this, stamp, stamp + 1)) {
                return stamp;
            }
            pauseAfterFailedAttempt(++attempts);
        }
    }

    private static void pauseAfterFailedAttempt(int attempts) {
        if (attempts <= SPIN_LIMIT) {
            Thread.onSpinWait();
        } else {
            LockSupport.parkNanos(1L);
        }
    }

    void markRequeued() {
        timestampNanos = System.nanoTime();
        accessed = false;
        requeueCount++;
    }

    void recycle() {
        rangeCache = null;
        key = null;
        value = null;
        size = 0;
        timestampNanos = 0;
        requeueCount = 0;
        accessed = false;
        recyclerHandle.recycle(this);
    }
}
