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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.netty.buffer.ByteBuf;

/**
 * The entries of compacted ledgers that the broker read, shared by its compacted topics. A compacted ledger's snapshot
 * is read whole, in order, by every reader that reads the topic compacted from the start, and twice by every
 * compaction, which reads the previous snapshot in both of its phases; without a cache, each of these reads it from
 * the bookies. The entries are kept as heap copies, so that a hit needs no reference counting against the eviction.
 * Compacted ledgers' ids aren't reused, so a deleted ledger's entries are evicted as the least recently used. A cache
 * serves the ledgers of one BookKeeper cluster, whose ledger ids identify them.
 */
public final class CompactedLedgerEntryCache {
    /** A cache that keeps nothing. */
    public static final CompactedLedgerEntryCache DISABLED = new CompactedLedgerEntryCache(0);
    // The heap that an entry takes beyond its bytes: the key, the array header and the cache's node
    private static final int ENTRY_OVERHEAD_BYTES = 96;

    private record Key(long ledgerId, long entryId) {
    }

    private final Cache<Key, byte[]> entries;

    /**
     * @param maxBytes the most bytes that the entries take on the heap; 0 for a cache that keeps nothing
     */
    public CompactedLedgerEntryCache(long maxBytes) {
        entries = maxBytes > 0 ? Caffeine.newBuilder()
                .maximumWeight(maxBytes)
                .<Key, byte[]>weigher((key, bytes) -> bytes.length + ENTRY_OVERHEAD_BYTES)
                .build() : null;
    }

    public boolean isEnabled() {
        return entries != null;
    }

    /** The bytes of the compacted ledger's entry, or null when it isn't cached. */
    byte[] get(long ledgerId, long entryId) {
        return entries != null ? entries.getIfPresent(new Key(ledgerId, entryId)) : null;
    }

    /** Keeps a copy of the compacted ledger's entry, without changing the buffer's reader index or reference count. */
    void put(long ledgerId, long entryId, ByteBuf entry) {
        byte[] bytes = copyIfEnabled(entry);
        if (bytes != null) {
            entries.put(new Key(ledgerId, entryId), bytes);
        }
    }

    /** Keeps the bytes of the compacted ledger's entry, which the caller doesn't change afterwards. */
    void put(long ledgerId, long entryId, byte[] entry) {
        if (entries != null) {
            entries.put(new Key(ledgerId, entryId), entry);
        }
    }

    /** A copy of the entry's readable bytes when the cache keeps entries, or null. */
    byte[] copyIfEnabled(ByteBuf entry) {
        if (entries == null) {
            return null;
        }
        byte[] bytes = new byte[entry.readableBytes()];
        entry.getBytes(entry.readerIndex(), bytes);
        return bytes;
    }

    /** The bytes that the cached entries take, an estimate. */
    long size() {
        return entries != null ? entries.policy().eviction().map(eviction -> eviction.weightedSize().orElse(0))
                .orElse(0L) : 0;
    }
}
