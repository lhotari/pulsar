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
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReferenceCountedEntry;
import org.apache.bookkeeper.mledger.impl.EntryImpl;

/**
 * Wrapper around the value to store in a {@link RangeCache} slot. This is needed to ensure that a specific instance
 * can be removed from its slot with a compare-and-set. Certain race conditions could result in the wrong value being
 * removed from the cache. The instances of this class are recycled to avoid creating new objects.
 */
@CustomLog
class RangeCacheEntryWrapper {
    private final Recycler.Handle<RangeCacheEntryWrapper> recyclerHandle;
    private static final Recycler<RangeCacheEntryWrapper> RECYCLER = new Recycler<RangeCacheEntryWrapper>() {
        @Override
        protected RangeCacheEntryWrapper newObject(Handle<RangeCacheEntryWrapper> recyclerHandle) {
            return new RangeCacheEntryWrapper(recyclerHandle);
        }
    };
    private final StampedLock lock = new StampedLock();
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
        StampedLock lock = entryWrapper.lock;
        long stamp = lock.writeLock();
        try {
            entryWrapper.rangeCache = rangeCache;
            entryWrapper.key = key;
            entryWrapper.value = value;
            entryWrapper.size = size;
            // Set the timestamp to the current time in nanoseconds
            // This is used for time-based eviction of entries
            entryWrapper.timestampNanos = System.nanoTime();
            return function.apply(entryWrapper);
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    /**
     * Get the value associated with the key. Returns null if the key does not match the key.
     *
     * @param key the key to match
     * @return the value associated with the key, or null if the value has already been recycled or the key does not
     * match
     */
    ReferenceCountedEntry getValue(Position key) {
        return getValue(key.getLedgerId(), key.getEntryId());
    }

    /**
     * Get the value of the entry at the given ledger ID and entry ID, such as the position of a cache slot.
     *
     * @return the value associated with the position, or null if the value has already been recycled or the wrapper
     * holds another position
     */
    ReferenceCountedEntry getValue(long ledgerId, long entryId) {
        return getValue(ledgerId, entryId, true);
    }

    /**
     * Get the value of the entry at the given position, marking the entry accessed for the eviction only when
     * {@code markAccessed} is set.
     */
    ReferenceCountedEntry getValue(long ledgerId, long entryId, boolean markAccessed) {
        long stamp = lock.tryOptimisticRead();
        Position localKey = this.key;
        ReferenceCountedEntry localValue = this.value;
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            localKey = this.key;
            localValue = this.value;
            lock.unlockRead(stamp);
        }
        // check that the position matches the key associated with the value in the entry
        // this is used to detect if the entry has already been recycled and contains another key
        if (localKey == null || localKey.compareTo(ledgerId, entryId) != 0) {
            return null;
        }
        markAccessed(markAccessed);
        return localValue;
    }

    // Writes the flag only when it isn't set: concurrent readers of an entry would otherwise contend on the line
    private void markAccessed(boolean markAccessed) {
        if (markAccessed && !accessed) {
            accessed = true;
        }
    }

    /**
     * Copies the value of the entry at the given position, as {@link EntryImpl#create(EntryImpl)} does, without
     * retaining the value. The cache removes and releases a value under the wrapper's write lock, so when no write lock
     * was taken between reading the value and copying it, the cache held the value meanwhile and the copy retained a
     * live buffer. Returns null when the copy can't be made that way: the wrapper was written meanwhile, it holds
     * another position or no {@link EntryImpl}, or the value's message metadata has to be initialized first; the
     * caller then retains the value to copy it.
     *
     * @param requireMessageMetadata whether the value must have its message metadata, which the copy shares
     * @return the copy, which the caller owns, or null
     */
    EntryImpl copyValue(long ledgerId, long entryId, boolean requireMessageMetadata) {
        long stamp = lock.tryOptimisticRead();
        if (stamp == 0L) {
            return null;
        }
        Position localKey = this.key;
        ReferenceCountedEntry localValue = this.value;
        if (localKey == null || localKey.compareTo(ledgerId, entryId) != 0
                || !(localValue instanceof EntryImpl entry)
                || (requireMessageMetadata && entry.getMessageMetadata() == null)
                || !lock.validate(stamp)) {
            return null;
        }
        EntryImpl copy;
        try {
            copy = EntryImpl.create(entry);
        } catch (RuntimeException e) {
            if (lock.validate(stamp)) {
                throw e;
            }
            // the value was released and recycled meanwhile
            return null;
        }
        if (!lock.validate(stamp)) {
            // The copy may hold a buffer that was released and reused meanwhile. It wasn't read, so releasing it
            // doesn't count as a read of the expected read count that it shares.
            copy.setDecreaseReadCountOnRelease(false);
            copy.release();
            return null;
        }
        markAccessed(true);
        return copy;
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
        long stamp = lock.writeLock();
        try {
            return function.apply(this);
        } finally {
            lock.unlockWrite(stamp);
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
