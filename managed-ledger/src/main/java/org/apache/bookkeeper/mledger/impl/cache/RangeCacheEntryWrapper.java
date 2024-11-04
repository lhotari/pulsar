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
import java.util.Map;
import java.util.concurrent.locks.StampedLock;

/**
 * Wrapper around the value to store in Map. This is needed to ensure that a specific instance can be removed from
 * the map by calling the {@link Map#remove(Object, Object)} method. Certain race conditions could result in the
 * wrong value being removed from the map. The instances of this class are recycled to avoid creating new objects.
 */
class RangeCacheEntryWrapper<K, V> {
    private final Recycler.Handle<RangeCacheEntryWrapper> recyclerHandle;
    private static final Recycler<RangeCacheEntryWrapper> RECYCLER = new Recycler<RangeCacheEntryWrapper>() {
        @Override
        protected RangeCacheEntryWrapper newObject(Handle<RangeCacheEntryWrapper> recyclerHandle) {
            return new RangeCacheEntryWrapper(recyclerHandle);
        }
    };
    private final StampedLock lock = new StampedLock();
    private K key;
    private V value;
    long size;
    long timestampNanos;

    private RangeCacheEntryWrapper(Recycler.Handle<RangeCacheEntryWrapper> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    static <K, V> RangeCacheEntryWrapper<K, V> create(K key, V value, long size) {
        RangeCacheEntryWrapper<K, V> entryWrapper = RECYCLER.get();
        long stamp = entryWrapper.lock.writeLock();
        entryWrapper.key = key;
        entryWrapper.value = value;
        entryWrapper.size = size;
        entryWrapper.timestampNanos = System.nanoTime();
        entryWrapper.lock.unlockWrite(stamp);
        return entryWrapper;
    }

    K getKey() {
        long stamp = lock.tryOptimisticRead();
        K localKey = key;
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            localKey = key;
            lock.unlockRead(stamp);
        }
        return localKey;
    }

    V getValue(K key) {
        long stamp = lock.tryOptimisticRead();
        K localKey = this.key;
        V localValue = this.value;
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            localKey = this.key;
            localValue = this.value;
            lock.unlockRead(stamp);
        }
        if (localKey != key) {
            return null;
        }
        return localValue;
    }

    long getSize() {
        long stamp = lock.tryOptimisticRead();
        long localSize = size;
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            localSize = size;
            lock.unlockRead(stamp);
        }
        return localSize;
    }

    long getTimestampNanos() {
        long stamp = lock.tryOptimisticRead();
        long localTimestampNanos = timestampNanos;
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            localTimestampNanos = timestampNanos;
            lock.unlockRead(stamp);
        }
        return localTimestampNanos;
    }

    void markRemoved() {
        long stamp = lock.writeLock();
        key = null;
        value = null;
        size = 0;
        timestampNanos = 0;
        lock.unlockWrite(stamp);
    }

    void recycle() {
        key = null;
        value = null;
        size = 0;
        timestampNanos = 0;
        recyclerHandle.recycle(this);
    }
}
