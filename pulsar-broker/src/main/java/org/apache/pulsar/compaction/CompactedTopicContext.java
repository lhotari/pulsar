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
package org.apache.pulsar.compaction;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.MessageIdData;

@Getter
public class CompactedTopicContext {
    // A reader reads the compacted ledger in order, so each read starts where its previous one ended
    static final int MAX_NEXT_READ_INDEXES = 10_000;

    final LedgerHandle ledger;
    final AsyncLoadingCache<Long, MessageIdData> cache;
    // The compacted ledger's entry that follows a read's last entry, by the position after that entry's original
    // position, where the reader's cursor continues: the reader's next read starts at that entry without searching
    // the compacted ledger for it, which takes a ledger read for each step of the search that the small cache misses
    private final Cache<Position, Long> nextReadIndexes =
            Caffeine.newBuilder().maximumSize(MAX_NEXT_READ_INDEXES).build();

    public CompactedTopicContext(LedgerHandle ledger, AsyncLoadingCache<Long, MessageIdData> cache) {
        this.ledger = ledger;
        this.cache = cache;
    }

    /** The compacted ledger's entry where a read from the position starts, when a previous read ended before it. */
    Long nextReadIndex(Position position) {
        return nextReadIndexes.getIfPresent(PositionFactory.create(position.getLedgerId(), position.getEntryId()));
    }

    /** Records that a read ending at the compacted ledger's entry {@code index - 1} continues at the position. */
    void recordNextReadIndex(Position position, long index) {
        nextReadIndexes.put(PositionFactory.create(position.getLedgerId(), position.getEntryId()), index);
    }
}
