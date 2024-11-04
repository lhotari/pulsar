package org.apache.bookkeeper.mledger.impl.cache;

import java.util.Queue;
import org.jctools.queues.MpscChunkedArrayQueue;

class RangeCacheRemovalQueue<Key extends Comparable<Key>, Value extends RangeCache.ValueWithKeyValidation<Key>> {
    private final Queue<RangeCacheEntryWrapper<Key, Value>> removalQueue = new MpscChunkedArrayQueue<>(1024);
}
