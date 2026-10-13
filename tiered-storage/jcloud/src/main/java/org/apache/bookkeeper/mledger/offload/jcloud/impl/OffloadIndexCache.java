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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.OffloadedReadCacheConfig;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.net.BookieId;

/**
 * The parsed indexes of offloaded ledgers, shared by every read handle of the offloader factory, so that reopening the
 * read handle of a ledger, which the managed ledger does after it has closed the handle of a ledger that it no longer
 * read, doesn't read the ledger's index object from the blob store again. An open of a ledger whose index is being
 * read waits for that read instead of reading it again. The indexes are kept within an estimated size budget and
 * dropped when no read handle has been opened with them for a while.
 *
 * <p>A shared index is never closed, which would recycle it while read handles still use it: the read handles that
 * are opened with it don't close it, and the garbage collector frees it once neither the cache nor a read handle holds
 * it.
 */
public class OffloadIndexCache {
    /** The index object of an offloaded ledger. */
    record Key(String bucket, String indexKey) {
    }

    /** Reads and parses an index object from the blob store. */
    @FunctionalInterface
    interface Loader {
        OffloadIndexBlock load() throws IOException, BKException;
    }

    // The estimated memory of a parsed index: the objects of the index and of the ledger's metadata, each entry of the
    // index with its node in the index's map, and the variable parts of the metadata: each custom metadata value, with
    // its key's characters, and each ensemble, with its bookies' ids
    private static final int INDEX_BYTES = 2048;
    private static final int INDEX_ENTRY_BYTES = 128;
    private static final int MAP_ENTRY_BYTES = 64;
    private static final int BOOKIE_ID_BYTES = 96;
    /**
     * How long an open waits for another open's read of the same index, after which it reads the index itself: a read
     * that never completes, which the cache keeps since it neither weighs nor expires a read in progress, would
     * otherwise keep every later open of the ledger waiting.
     */
    static final long DEFAULT_LOAD_WAIT_MILLIS = TimeUnit.MINUTES.toMillis(1);

    // The reads in progress are in the cache too, as futures that later opens wait for, which weigh nothing until
    // they complete; a read that fails is removed, so that the next open reads the index again
    private final AsyncCache<Key, OffloadIndexBlock> indexes;
    private final LongAdder hits = new LongAdder();
    private final LongAdder joinedLoads = new LongAdder();
    private final LongAdder loads = new LongAdder();
    private final LongAdder abandonedLoads = new LongAdder();
    private final long loadWaitMillis;

    public OffloadIndexCache(OffloadedReadCacheConfig config) {
        this(config.indexCacheSizeBytes(), config.indexCacheExpirySeconds());
    }

    @VisibleForTesting
    OffloadIndexCache(long maxSizeBytes, int ttlSeconds) {
        this(maxSizeBytes, ttlSeconds, DEFAULT_LOAD_WAIT_MILLIS);
    }

    @VisibleForTesting
    OffloadIndexCache(long maxSizeBytes, int ttlSeconds, long loadWaitMillis) {
        this.loadWaitMillis = loadWaitMillis;
        this.indexes = Caffeine.newBuilder()
                // With 0, an index is dropped once its read completes, after the opens that waited for it got it
                .maximumWeight(Math.max(0, maxSizeBytes))
                .<Key, OffloadIndexBlock>weigher((key, index) -> estimatedSize(index))
                .expireAfterAccess(Math.max(1, ttlSeconds), TimeUnit.SECONDS)
                // The cache's maintenance runs on the threads that use it
                .executor(Runnable::run)
                .buildAsync();
    }

    static int estimatedSize(OffloadIndexBlock index) {
        long size = INDEX_BYTES + (long) index.getEntryCount() * INDEX_ENTRY_BYTES;
        LedgerMetadata metadata = index.getLedgerMetadata();
        if (metadata != null) {
            Map<String, byte[]> customMetadata = metadata.getCustomMetadata();
            if (customMetadata != null) {
                for (Map.Entry<String, byte[]> entry : customMetadata.entrySet()) {
                    size += MAP_ENTRY_BYTES + 2L * entry.getKey().length()
                            + (entry.getValue() != null ? entry.getValue().length : 0);
                }
            }
            Map<Long, ? extends List<BookieId>> ensembles = metadata.getAllEnsembles();
            if (ensembles != null) {
                for (List<BookieId> ensemble : ensembles.values()) {
                    size += MAP_ENTRY_BYTES + (long) ensemble.size() * BOOKIE_ID_BYTES;
                }
            }
        }
        return (int) Math.min(Integer.MAX_VALUE, size);
    }

    /**
     * Returns the parsed index of an index object: from the cache, from a read of it in progress, or from a read that
     * this call makes with {@code loader} on the calling thread. The caller must not close the index.
     */
    OffloadIndexBlock get(String bucket, String indexKey, Loader loader) throws IOException, BKException {
        Key key = new Key(bucket, indexKey);
        for (;;) {
            CompletableFuture<OffloadIndexBlock> existing = indexes.getIfPresent(key);
            if (existing == null) {
                CompletableFuture<OffloadIndexBlock> load = new CompletableFuture<>();
                existing = indexes.asMap().putIfAbsent(key, load);
                if (existing == null) {
                    loads.increment();
                    try {
                        OffloadIndexBlock index = loader.load();
                        load.complete(index);
                        return index;
                    } catch (Throwable t) {
                        load.completeExceptionally(t);
                        throw t;
                    }
                }
            }
            if (existing.isDone()) {
                if (existing.isCompletedExceptionally()) {
                    // A failed read that the cache hasn't removed yet: read the index again
                    indexes.asMap().remove(key, existing);
                    continue;
                }
                hits.increment();
            } else {
                joinedLoads.increment();
            }
            OffloadIndexBlock index = await(existing);
            if (index != null) {
                return index;
            }
            // The read didn't complete in time: drop it, unless another open already has, and read the index again
            abandonedLoads.increment();
            indexes.asMap().remove(key, existing);
        }
    }

    // The index that the read completes with, or null when it doesn't complete within the wait
    private OffloadIndexBlock await(CompletableFuture<OffloadIndexBlock> pending) throws IOException, BKException {
        try {
            return pending.get(loadWaitMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("Interrupted while waiting for the read"
                    + " of an offloaded ledger's index");
            interrupted.initCause(e);
            throw interrupted;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof BKException bkException) {
                throw bkException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IOException(cause);
        }
    }

    /** The opens whose index was cached. */
    @VisibleForTesting
    long hits() {
        return hits.sum();
    }

    /** The opens that waited for another open's read of the index. */
    @VisibleForTesting
    long joinedLoads() {
        return joinedLoads.sum();
    }

    /** The reads of other opens that an open stopped waiting for, since they didn't complete in time. */
    @VisibleForTesting
    long abandonedLoads() {
        return abandonedLoads.sum();
    }

    /** The reads of index objects from the blob store. */
    @VisibleForTesting
    long loads() {
        return loads.sum();
    }

    /** The indexes in the cache, the reads in progress among them. */
    @VisibleForTesting
    long size() {
        indexes.synchronous().cleanUp();
        return indexes.synchronous().estimatedSize();
    }
}
