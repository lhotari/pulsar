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
import java.io.InterruptedIOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerMetadata;
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

    // A read that waits until it's released, and then returns the index or fails
    private static final class BlockingLoader implements OffloadIndexCache.Loader {
        final CountDownLatch reading = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final OffloadIndexBlock index;
        final IOException failure;

        BlockingLoader(OffloadIndexBlock index, IOException failure) {
            this.index = index;
            this.failure = failure;
        }

        @Override
        public OffloadIndexBlock load() throws IOException {
            reading.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            if (failure != null) {
                throw failure;
            }
            return index;
        }
    }

    @Test
    public void opensThatWaitForAFailedReadFailWithIt() throws Exception {
        OffloadIndexCache cache = new OffloadIndexCache(1 << 20, 60);
        BlockingLoader failing = new BlockingLoader(null, new IOException("unavailable"));
        CompletableFuture<OffloadIndexBlock> first = CompletableFuture.supplyAsync(() -> get(cache, failing));
        assertThat(failing.reading.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<OffloadIndexBlock> second = CompletableFuture.supplyAsync(() -> get(cache, () -> {
            throw new AssertionError("joins the read in progress");
        }));
        assertThatWaits(cache);
        failing.release.countDown();

        assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("unavailable");
        assertThatThrownBy(() -> second.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("unavailable");
        // the next open reads the index again
        OffloadIndexBlock index = index(10);
        assertThat(cache.get("bucket", "index", () -> index)).isSameAs(index);
        assertThat(cache.loads()).isEqualTo(2);
    }

    @Test
    public void anOpenStopsWaitingForAReadThatDoesntComplete() throws Exception {
        OffloadIndexCache cache = new OffloadIndexCache(1 << 20, 60, 200);
        BlockingLoader stalled = new BlockingLoader(index(10), null);
        CompletableFuture<OffloadIndexBlock> first = CompletableFuture.supplyAsync(() -> get(cache, stalled));
        assertThat(stalled.reading.await(10, TimeUnit.SECONDS)).isTrue();

        // reads the index itself, after waiting for the stalled read
        OffloadIndexBlock index = index(10);
        assertThat(cache.get("bucket", "index", () -> index)).isSameAs(index);
        assertThat(cache.abandonedLoads()).isEqualTo(1);
        assertThat(cache.get("bucket", "index", () -> {
            throw new AssertionError("cached");
        })).isSameAs(index);

        // the stalled read completes for its own open, without replacing the cached index
        stalled.release.countDown();
        assertThat(first.get(10, TimeUnit.SECONDS)).isSameAs(stalled.index);
        assertThat(cache.get("bucket", "index", () -> {
            throw new AssertionError("cached");
        })).isSameAs(index);
    }

    @Test
    public void anInterruptedOpenStopsWaitingAndTheReadGoesOn() throws Exception {
        OffloadIndexCache cache = new OffloadIndexCache(1 << 20, 60);
        BlockingLoader loader = new BlockingLoader(index(10), null);
        CompletableFuture<OffloadIndexBlock> first = CompletableFuture.supplyAsync(() -> get(cache, loader));
        assertThat(loader.reading.await(10, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<Throwable> interrupted = new CompletableFuture<>();
        Thread waiter = new Thread(() -> {
            try {
                cache.get("bucket", "index", () -> {
                    throw new AssertionError("joins the read in progress");
                });
                interrupted.complete(null);
            } catch (Throwable t) {
                boolean wasInterrupted = Thread.currentThread().isInterrupted();
                interrupted.complete(wasInterrupted ? t : new AssertionError("not interrupted"));
            }
        });
        waiter.start();
        assertThatWaits(cache);
        waiter.interrupt();
        assertThat(interrupted.get(10, TimeUnit.SECONDS)).isInstanceOf(InterruptedIOException.class);

        loader.release.countDown();
        assertThat(first.get(10, TimeUnit.SECONDS)).isSameAs(loader.index);
        assertThat(cache.get("bucket", "index", () -> {
            throw new AssertionError("cached");
        })).isSameAs(loader.index);
    }

    @Test
    public void weighsTheLedgersMetadata() {
        OffloadIndexBlock small = index(1);
        OffloadIndexBlock withMetadata = index(1);
        LedgerMetadata metadata = mock(LedgerMetadata.class);
        when(metadata.getCustomMetadata()).thenReturn(Map.of("key", new byte[64 * 1024]));
        when(withMetadata.getLedgerMetadata()).thenReturn(metadata);
        assertThat(OffloadIndexCache.estimatedSize(withMetadata))
                .isGreaterThan(OffloadIndexCache.estimatedSize(small) + 64 * 1024);
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
