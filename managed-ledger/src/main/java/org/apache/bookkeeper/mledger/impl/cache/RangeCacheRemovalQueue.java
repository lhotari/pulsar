package org.apache.bookkeeper.mledger.impl.cache;

import java.util.Queue;
import org.apache.commons.lang3.tuple.Pair;
import org.jctools.queues.MpscChunkedArrayQueue;

class RangeCacheRemovalQueue<Key extends Comparable<Key>, Value extends RangeCache.ValueWithKeyValidation<Key>> {
    private final Queue<RangeCacheEntryWrapper<Key, Value>> removalQueue = new MpscChunkedArrayQueue<>(1024);

    public Pair<Integer, Long> evictLEntriesBeforeTimestamp(long timestampNanos) {
        return null;
    }

    public Pair<Integer, Long> evictLeastAccessedEntries(long sizeToFree) {
        return null;
    }
}
