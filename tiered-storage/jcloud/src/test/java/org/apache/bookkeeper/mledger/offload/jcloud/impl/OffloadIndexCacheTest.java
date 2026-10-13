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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.testng.annotations.Test;

public class OffloadIndexCacheTest {
    private static OffloadIndexBlock index(int entries) {
        OffloadIndexBlock index = mock(OffloadIndexBlock.class);
        when(index.getEntryCount()).thenReturn(entries);
        return index;
    }

    @Test
    public void reusesTheIndexOfAnObject() throws Exception {
        OffloadIndexCache cache = new OffloadIndexCache(1 << 20, 60);
        OffloadIndexBlock index = index(10);
        AtomicInteger reads = new AtomicInteger();
        OffloadIndexCache.Loader loader = () -> {
            reads.incrementAndGet();
            return index;
        };

        assertThat(cache.get("bucket", "ledger-1-index", loader)).isSameAs(index);
        assertThat(cache.get("bucket", "ledger-1-index", loader)).isSameAs(index);
        assertThat(reads).hasValue(1);
        assertThat(cache.loads()).isEqualTo(1);
        assertThat(cache.hits()).isEqualTo(1);
        // another object, or the same key in another bucket, is read
        cache.get("bucket", "ledger-2-index", loader);
        cache.get("other", "ledger-1-index", loader);
        assertThat(reads).hasValue(3);
    }

    @Test
    public void opensOfAnObjectThatIsBeingReadWaitForTheRead() throws Exception {
        for (long size : new long[] {1 << 20, 0}) {
            OffloadIndexCache cache = new OffloadIndexCache(size, 60);
            OffloadIndexBlock index = index(10);
            CountDownLatch reading = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger reads = new AtomicInteger();
            OffloadIndexCache.Loader loader = () -> {
                reads.incrementAndGet();
                reading.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
                return index;
            };
            CompletableFuture<OffloadIndexBlock> first = CompletableFuture.supplyAsync(() -> get(cache, loader));
            assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();
            CompletableFuture<OffloadIndexBlock> second = CompletableFuture.supplyAsync(() -> get(cache, loader));
            // the second open waits for the first one's read
            assertThatWaits(cache);
            release.countDown();

            assertThat(first.get(10, TimeUnit.SECONDS)).isSameAs(index);
            assertThat(second.get(10, TimeUnit.SECONDS)).isSameAs(index);
            assertThat(reads).as("reads with a cache of %d bytes", size).hasValue(1);
            assertThat(cache.joinedLoads()).isEqualTo(1);
            // without a budget, the index isn't kept once read
            cache.get("bucket", "index", () -> index);
            assertThat(cache.loads()).isEqualTo(size > 0 ? 1 : 2);
        }
    }

    // Waits until the second open has joined the read in progress
    private static void assertThatWaits(OffloadIndexCache cache) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (cache.joinedLoads() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(cache.joinedLoads()).isEqualTo(1);
    }

    private static OffloadIndexBlock get(OffloadIndexCache cache, OffloadIndexCache.Loader loader) {
        try {
            return cache.get("bucket", "index", loader);
        } catch (IOException | BKException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void readsAnIndexAgainAfterAFailedRead() throws Exception {
        OffloadIndexCache cache = new OffloadIndexCache(1 << 20, 60);
        assertThatThrownBy(() -> cache.get("bucket", "index", () -> {
            throw new IOException("unavailable");
        })).isInstanceOf(IOException.class).hasMessage("unavailable");
        assertThatThrownBy(() -> cache.get("bucket", "index", () -> {
            throw new BKException.BKNoSuchLedgerExistsException();
        })).isInstanceOf(BKException.BKNoSuchLedgerExistsException.class);

        OffloadIndexBlock index = index(10);
        assertThat(cache.get("bucket", "index", () -> index)).isSameAs(index);
        assertThat(cache.loads()).isEqualTo(3);
    }

    @Test
    public void keepsTheIndexesWithinTheBudget() throws Exception {
        // room for about two indexes of 10 entries
        OffloadIndexCache cache = new OffloadIndexCache(2L * OffloadIndexCache.estimatedSize(index(10)), 60);
        for (int i = 0; i < 10; i++) {
            OffloadIndexBlock index = index(10);
            cache.get("bucket", "index-" + i, () -> index);
        }
        assertThat(cache.size()).isLessThanOrEqualTo(2);
    }
}
