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

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.cache.RangeCache.ValueWithKeyValidation;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Special type of cache where get() and delete() operations can be done over a range of keys.
 * The implementation avoids locks and synchronization by relying on ConcurrentSkipListMap for storing the entries.
 * Since there are no locks, it's necessary to ensure that a single entry in the cache is removed exactly once.
 * Removing an entry multiple times could result in the entries of the cache being released multiple times,
 * even while they are still in use. This is prevented by using a custom wrapper around the value to store in the map
 * that ensures that the value is removed from the map only if the exact same instance is present in the map.
 * There's also a check that ensures that the value matches the key. This is used to detect races without impacting
 * consistency.
 *
 * @param <Key>
 *            Cache key. Needs to be Comparable
 * @param <Value>
 *            Cache value
 */
@Slf4j
public class RangeCache<Key extends Comparable<Key>, Value extends ValueWithKeyValidation<Key>> {
    private final RangeCacheRemovalQueue<Key, Value> removalQueue;

    public interface ValueWithKeyValidation<T> extends ReferenceCounted {
        boolean matchesKey(T key);
    }

    // Map from key to nodes inside the linked list
    private final ConcurrentNavigableMap<Key, RangeCacheEntryWrapper<Key, Value>> entries;
    private AtomicLong size; // Total size of values stored in cache
    private final Weighter<Value> weighter; // Weighter object used to extract the size from values

    /**
     * Construct a new RangeLruCache with default Weighter.
     */
    public RangeCache(RangeCacheRemovalQueue<Key, Value> removalQueue) {
        this(new DefaultWeighter<>(), removalQueue);
    }

    /**
     * Construct a new RangeLruCache.
     *
     * @param weighter a custom weighter to compute the size of each stored value
     */
    public RangeCache(Weighter<Value> weighter,
                      RangeCacheRemovalQueue<Key, Value> removalQueue) {
        this.removalQueue = removalQueue;
        this.size = new AtomicLong(0);
        this.entries = new ConcurrentSkipListMap<>();
        this.weighter = weighter;
    }

    /**
     * Insert.
     *
     * @param key
     * @param value ref counted value with at least 1 ref to pass on the cache
     * @return whether the entry was inserted in the cache
     */
    public boolean put(Key key, Value value) {
        // retain value so that it's not released before we put it in the cache and calculate the weight
        value.retain();
        try {
            if (!value.matchesKey(key)) {
                throw new IllegalArgumentException("Value '" + value + "' does not match key '" + key + "'");
            }
            long entrySize = weighter.getSize(value);
            RangeCacheEntryWrapper<Key, Value> newWrapper = RangeCacheEntryWrapper.create(this, key, value, entrySize);
            if (entries.putIfAbsent(key, newWrapper) == null) {
                this.size.addAndGet(entrySize);
                removalQueue.addEntry(newWrapper);
                return true;
            } else {
                // recycle the new wrapper as it was not used
                newWrapper.recycle();
                return false;
            }
        } finally {
            value.release();
        }
    }

    public boolean exists(Key key) {
        return key != null ? entries.containsKey(key) : true;
    }

    /**
     * Get the value associated with the key and increment the reference count of it.
     * The caller is responsible for releasing the reference.
     */
    public Value get(Key key) {
        return getValueFromWrapper(key, entries.get(key));
    }

    private Value getValueFromWrapper(Key key, RangeCacheEntryWrapper<Key, Value> valueWrapper) {
        if (valueWrapper == null) {
            return null;
        } else {
            Value value = valueWrapper.getValue(key);
            return getRetainedValueMatchingKey(key, value);
        }
    }

    /**
     * @apiNote the returned value must be released if it's not null
     */
    private Value getValueMatchingEntry(Map.Entry<Key, RangeCacheEntryWrapper<Key, Value>> entry) {
        Value valueMatchingEntry = RangeCacheEntryWrapper.getValueMatchingMapEntry(entry);
        return getRetainedValueMatchingKey(entry.getKey(), valueMatchingEntry);
    }

    // validates that the value matches the key and that the value has not been recycled
    // which are possible due to the lack of exclusive locks in the cache and the use of reference counted objects
    /**
     * @apiNote the returned value must be released if it's not null
     */
    private Value getRetainedValueMatchingKey(Key key, Value value) {
        if (value == null) {
            // the wrapper has been recycled and contains another key
            return null;
        }
        try {
            value.retain();
        } catch (IllegalReferenceCountException e) {
            // Value was already deallocated
            return null;
        }
        // check that the value matches the key and that there's at least 2 references to it since
        // the cache should be holding one reference and a new reference was just added in this method
        if (value.refCnt() > 1 && value.matchesKey(key)) {
            return value;
        } else {
            // Value or IdentityWrapper was recycled and already contains another value
            // release the reference added in this method
            value.release();
            return null;
        }
    }

    /**
     *
     * @param first
     *            the first key in the range
     * @param last
     *            the last key in the range (inclusive)
     * @return a collections of the value found in cache
     */
    public Collection<Value> getRange(Key first, Key last) {
        List<Value> values = new ArrayList();

        // Return the values of the entries found in cache
        for (Map.Entry<Key, RangeCacheEntryWrapper<Key, Value>> entry : entries.subMap(first, true, last, true)
                .entrySet()) {
            Value value = getValueMatchingEntry(entry);
            if (value != null) {
                values.add(value);
            }
        }

        return values;
    }

    /**
     *
     * @param first
     * @param last
     * @param lastInclusive
     * @return an pair of ints, containing the number of removed entries and the total size
     */
    public Pair<Integer, Long> removeRange(Key first, Key last, boolean lastInclusive) {
        if (log.isDebugEnabled()) {
            log.debug("Removing entries in range [{}, {}], lastInclusive: {}", first, last, lastInclusive);
        }
        RangeCacheRemovalCounters counters = RangeCacheRemovalCounters.create();
        Map<Key, RangeCacheEntryWrapper<Key, Value>> subMap = entries.subMap(first, true, last, lastInclusive);
        for (Map.Entry<Key, RangeCacheEntryWrapper<Key, Value>> entry : subMap.entrySet()) {
            removeEntryWithWriteLock(entry.getKey(), entry.getValue(), counters);
        }
        return handleRemovalResult(counters);
    }

    boolean removeEntryWithWriteLock(Key expectedKey, RangeCacheEntryWrapper<Key, Value> entryWrapper,
                                          RangeCacheRemovalCounters counters) {
        return entryWrapper.withWriteLock(e -> {
            if (e.key == null || e.key != expectedKey) {
                // entry has already been removed
                return false;
            }
            return removeEntry(e.key, e.value, e, counters, false);
        });
    }

    /**
     * Remove the entry from the cache. This must be called within a function passed to
     * {@link RangeCacheEntryWrapper#withWriteLock(Function)}.
     * @param key the expected key of the entry
     * @param value the expected value of the entry
     * @param entryWrapper the entry wrapper instance
     * @param counters the removal counters
     * @return true if the entry was removed, false otherwise
     */
    boolean removeEntry(Key key, Value value, RangeCacheEntryWrapper<Key, Value> entryWrapper,
                        RangeCacheRemovalCounters counters, boolean updateSize) {
        // always remove the entry from the map
        entries.remove(key, entryWrapper);
        if (value == null) {
            // the wrapper has already been recycled and contains another key
            return false;
        }
        try {
            // add extra retain to avoid value being released while we are removing it
            value.retain();
        } catch (IllegalReferenceCountException e) {
            return false;
        }
        try {
            if (!value.matchesKey(key)) {
                return false;
            }
            long removedSize = entryWrapper.markRemoved(key, value);
            if (removedSize > -1) {
                counters.entryRemoved(removedSize);
                if (updateSize) {
                    size.addAndGet(-removedSize);
                }
                if (value.refCnt() > 1) {
                    // remove the cache reference
                    value.release();
                } else {
                    log.info("Unexpected refCnt {} for key {}, removed entry without releasing the value",
                            value.refCnt(), key);
                }
                return true;
            } else {
                return false;
            }
        } finally {
            // remove the extra retain
            value.release();
        }
    }

    private Pair<Integer, Long> handleRemovalResult(RangeCacheRemovalCounters counters) {
        size.addAndGet(-counters.removedSize);
        Pair<Integer, Long> result = Pair.of(counters.removedEntries, counters.removedSize);
        counters.recycle();
        return result;
    }

    /**
     * Just for testing. Getting the number of entries is very expensive on the conncurrent map
     */
    protected long getNumberOfEntries() {
        return entries.size();
    }

    public long getSize() {
        return size.get();
    }

    /**
     * Remove all the entries from the cache.
     *
     * @return size of removed entries
     */
    public Pair<Integer, Long> clear() {
        if (log.isDebugEnabled()) {
            log.debug("Clearing the cache with {} entries and size {}", entries.size(), size.get());
        }
        RangeCacheRemovalCounters counters = RangeCacheRemovalCounters.create();
        while (!Thread.currentThread().isInterrupted()) {
            Map.Entry<Key, RangeCacheEntryWrapper<Key, Value>> entry = entries.firstEntry();
            if (entry == null) {
                break;
            }
            removeEntryWithWriteLock(entry.getKey(), entry.getValue(), counters);
        }
        return handleRemovalResult(counters);
    }

    /**
     * Interface of a object that is able to the extract the "weight" (size/cost/space) of the cached values.
     *
     * @param <ValueT>
     */
    public interface Weighter<ValueT> {
        long getSize(ValueT value);
    }

    /**
     * Default cache weighter, every value is assumed the same cost.
     *
     * @param <Value>
     */
    private static class DefaultWeighter<Value> implements Weighter<Value> {
        @Override
        public long getSize(Value value) {
            return 1;
        }
    }
}
