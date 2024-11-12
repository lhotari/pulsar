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
import org.jctools.queues.MpscChunkedArrayQueue;

class RangeCacheRemovalQueue<Key extends Comparable<Key>, Value extends RangeCache.ValueWithKeyValidation<Key>> {
    private final Queue<RangeCacheEntryWrapper<Key, Value>> removalQueue = new MpscChunkedArrayQueue<>(1024);

    public Pair<Integer, Long> evictLEntriesBeforeTimestamp(long timestampNanos) {
        return evictEntries((e, c) -> e.timestampNanos < timestampNanos);
    }

    public Pair<Integer, Long> evictLeastAccessedEntries(long sizeToFree) {
        checkArgument(sizeToFree > 0);
        return evictEntries((e, c) -> c.removedSize < sizeToFree);
    }

    public boolean addEntry(RangeCacheEntryWrapper<Key, Value> newWrapper) {
        return removalQueue.offer(newWrapper);
    }

    private synchronized Pair<Integer, Long> evictEntries(
            BiPredicate<RangeCacheEntryWrapper<Key, Value>, RangeCacheRemovalCounters> evictionPredicate) {
        RangeCacheRemovalCounters counters = RangeCacheRemovalCounters.create();
        while (!Thread.currentThread().isInterrupted()) {
            RangeCacheEntryWrapper<Key, Value> entry = removalQueue.peek();
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
