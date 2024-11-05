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
