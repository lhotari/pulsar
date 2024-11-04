package org.apache.bookkeeper.mledger.impl.cache;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.mledger.impl.cache.RangeEntryCacheManagerImpl.MB;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class RangeEntryCacheManagerEvictionHandler implements EntryCachesEvictionHandler {

    private final RangeEntryCacheManagerImpl manager;
    private final RangeCacheRemovalQueue<Position, EntryImpl> rangeCacheRemovalQueue;

    public RangeEntryCacheManagerEvictionHandler(RangeEntryCacheManagerImpl manager,
                                                 RangeCacheRemovalQueue<Position, EntryImpl> rangeCacheRemovalQueue) {
        this.manager = manager;
        this.rangeCacheRemovalQueue = rangeCacheRemovalQueue;
    }

    @Override
    public void invalidateEntriesBeforeTimestampNanos(long timestamp) {
        Pair<Integer, Long> evictedPair = rangeCacheRemovalQueue.evictLEntriesBeforeTimestamp(timestamp);
        manager.entriesRemoved(evictedPair.getRight(), evictedPair.getLeft());
    }

    @Override
    public Pair<Integer, Long> evictEntries(long sizeToFree) {
        checkArgument(sizeToFree > 0);
        Pair<Integer, Long> evicted = rangeCacheRemovalQueue.evictLeastAccessedEntries(sizeToFree);
        int evictedEntries = evicted.getLeft();
        long evictedSize = evicted.getRight();
        if (log.isDebugEnabled()) {
            log.debug(
                    "Doing cache eviction of at least {} Mb -- Deleted {} entries - Total size deleted: {} Mb "
                            + " -- Current Size: {} Mb",
                    sizeToFree / MB, evictedEntries, evictedSize / MB, manager.getSize() / MB);
        }
        manager.entriesRemoved(evictedSize, evictedEntries);
        return evicted;
    }
}
