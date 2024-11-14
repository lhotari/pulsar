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

import static com.google.common.base.Preconditions.checkArgument;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.apache.commons.lang3.tuple.Pair;
import org.jctools.queues.MpscUnboundedArrayQueue;

class RangeCacheRemovalQueue {
    // The removal queue is unbounded, but we allocate memory in chunks to avoid frequent memory allocations.
    private static final int REMOVAL_QUEUE_CHUNK_SIZE = 128 * 1024;
    private final Queue<RangeCacheEntryWrapper> removalQueue = new MpscUnboundedArrayQueue<>(
            REMOVAL_QUEUE_CHUNK_SIZE);
    private final RangeCacheRemovalQueueStash stash = new RangeCacheRemovalQueueStash();

    public Pair<Integer, Long> evictLEntriesBeforeTimestamp(long timestampNanos) {
        return evictEntries(
                (e, c) -> e.timestampNanos < timestampNanos ? EvictionResult.REMOVE : EvictionResult.STASH_AND_STOP);
    }

    public Pair<Integer, Long> evictLeastAccessedEntries(long sizeToFree) {
        checkArgument(sizeToFree > 0);
        return evictEntries(
                (e, c) -> c.removedSize < sizeToFree ? EvictionResult.REMOVE : EvictionResult.STASH_AND_STOP);
    }

    public boolean addEntry(RangeCacheEntryWrapper newWrapper) {
        return removalQueue.offer(newWrapper);
    }

    class RangeCacheRemovalQueueStash {
        List<RangeCacheEntryWrapper> entries = new ArrayList<>();
        int size = 0;
        int removed = 0;

        public void add(RangeCacheEntryWrapper entry) {
            entries.add(entry);
            size++;
        }

        public boolean evictEntries(EvictionPredicate evictionPredicate, RangeCacheRemovalCounters counters) {
            boolean continueEviction = doEvictEntries(evictionPredicate, counters);
            maybeTrim();
            return continueEviction;
        }

        private boolean doEvictEntries(EvictionPredicate evictionPredicate, RangeCacheRemovalCounters counters) {
            for (int i = 0; i < entries.size(); i++) {
                RangeCacheEntryWrapper entry = entries.get(i);
                if (entry == null) {
                    continue;
                }
                EvictionResult evictionResult = handleEviction(evictionPredicate, entry, counters);
                if (!evictionResult.shouldStash()) {
                    entries.set(i, null);
                    removed++;
                }
                if (!evictionResult.isContinueEviction() || Thread.currentThread().isInterrupted()) {
                    return false;
                }
            }
            return true;
        }

        void maybeTrim() {
            if (size > 1000 && removed > size / 2) {
                List<RangeCacheEntryWrapper> newEntries = new ArrayList<>(size - removed);
                for (RangeCacheEntryWrapper entry : entries) {
                    if (entry != null) {
                        newEntries.add(entry);
                    }
                }
                entries = newEntries;
                size = entries.size();
                removed = 0;
            }
        }
    }

    enum EvictionResult {
        REMOVE, STASH, STASH_AND_STOP, MISSING;

        boolean isContinueEviction() {
            return this != STASH_AND_STOP;
        }

        boolean shouldStash() {
            return this == STASH || this == STASH_AND_STOP;
        }

        boolean shouldRecycle() {
            return this == REMOVE;
        }
    }

    interface EvictionPredicate {
        EvictionResult test(RangeCacheEntryWrapper entry, RangeCacheRemovalCounters counters);
    }

    /**
     * Evict entries from the removal queue based on the provided eviction predicate.
     * This method is synchronized to prevent multiple threads from removing entries simultaneously.
     * An MPSC (Multiple Producer Single Consumer) queue is used as the removal queue, which expects a single consumer.
     *
     * @param evictionPredicate the predicate to determine if an entry should be evicted
     * @return the number of entries and the total size removed from the cache
     */
    private synchronized Pair<Integer, Long> evictEntries(
            EvictionPredicate evictionPredicate) {
        RangeCacheRemovalCounters counters = RangeCacheRemovalCounters.create();
        boolean continueEviction = stash.evictEntries(evictionPredicate, counters);
        if (continueEviction) {
            while (!Thread.currentThread().isInterrupted()) {
                RangeCacheEntryWrapper entry = removalQueue.poll();
                if (entry == null) {
                    break;
                }
                EvictionResult evictionResult = handleEviction(evictionPredicate, entry, counters);
                if (evictionResult.shouldStash()) {
                    stash.add(entry);
                }
                if (!evictionResult.isContinueEviction()) {
                    break;
                }
            }
        }
        return handleRemovalResult(counters);
    }

    private EvictionResult handleEviction(EvictionPredicate evictionPredicate, RangeCacheEntryWrapper entry,
                                          RangeCacheRemovalCounters counters) {
        EvictionResult evictionResult = entry.withWriteLock(e -> {
            if (e.key == null) {
                // entry has been removed
                return EvictionResult.MISSING;
            }
            EvictionResult result = evictionPredicate.test(e, counters);
            if (result == EvictionResult.REMOVE) {
                e.rangeCache.removeEntry(e.key, e.value, e, counters, true);
            }
            return result;
        });
        if (evictionResult.shouldRecycle()) {
            // recycle the entry after it has been removed from the queue
            entry.recycle();
        }
        return evictionResult;
    }

    private Pair<Integer, Long> handleRemovalResult(RangeCacheRemovalCounters counters) {
        Pair<Integer, Long> result = Pair.of(counters.removedEntries, counters.removedSize);
        counters.recycle();
        return result;
    }
}
