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
import java.util.Queue;
import java.util.function.BiPredicate;
import org.apache.commons.lang3.tuple.Pair;
import org.jctools.queues.MpscUnboundedArrayQueue;

class RangeCacheRemovalQueue {
    // The removal queue is unbounded, but we allocate memory in chunks to avoid frequent memory allocations.
    private static final int REMOVAL_QUEUE_CHUNK_SIZE = 128 * 1024;
    private final Queue<RangeCacheEntryWrapper> removalQueue = new MpscUnboundedArrayQueue<>(
            REMOVAL_QUEUE_CHUNK_SIZE);
    // TODO: add an array list (optimized solution would be a linked chunked array list implementation)
    //  to store the entries that are skipped during eviction
    // this will allow us to evict entries in a single pass without peeking the message passing queue
    // this datastructure doesn't have to be thread safe since it's a single thread reading and writing to it.

    public Pair<Integer, Long> evictLEntriesBeforeTimestamp(long timestampNanos) {
        return evictEntries((e, c) -> e.timestampNanos < timestampNanos);
    }

    public Pair<Integer, Long> evictLeastAccessedEntries(long sizeToFree) {
        checkArgument(sizeToFree > 0);
        return evictEntries((e, c) -> c.removedSize < sizeToFree);
    }

    public boolean addEntry(RangeCacheEntryWrapper newWrapper) {
        return removalQueue.offer(newWrapper);
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
            BiPredicate<RangeCacheEntryWrapper, RangeCacheRemovalCounters> evictionPredicate) {
        RangeCacheRemovalCounters counters = RangeCacheRemovalCounters.create();
        while (!Thread.currentThread().isInterrupted()) {
            // TODO: replace peeking by taking the entry from the queue and adding it to the skipped entries list
            RangeCacheEntryWrapper entry = removalQueue.peek();
            if (entry == null) {
                break;
            }
            boolean removeFromQueue = entry.withWriteLock(e -> {
                if (e.key == null) {
                    // entry has been removed
                    return true;
                }
                if (evictionPredicate.test(e, counters)) {
                    e.rangeCache.removeEntry(e.key, e.value, e, counters, true);
                    return true;
                } else {
                    return false;
                }
            });

            if (removeFromQueue) {
                // remove peeked entry
                removalQueue.poll();
                // recycle the entry after it has been removed from the queue
                entry.recycle();
            } else {
                // stop removing entries
                break;
            }
        }
        return handleRemovalResult(counters);
    }

    private Pair<Integer, Long> handleRemovalResult(RangeCacheRemovalCounters counters) {
        Pair<Integer, Long> result = Pair.of(counters.removedEntries, counters.removedSize);
        counters.recycle();
        return result;
    }
}
