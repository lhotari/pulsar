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

import io.opentelemetry.api.OpenTelemetry;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** Measures real entry-copy/release and read-permit accounting with current or fused batch callbacks. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ReadBatchCallbackBenchmark {
    private static final long LIMIT_BYTES = 16 * 1024 * 1024;
    private static final ReadEntriesCallback NOOP_CALLBACK = new ReadEntriesCallback() {
        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
        }
    };

    @Param({"1", "100", "1000"})
    public int batchSize;

    @Param
    public CallbackImplementation callbackImplementation;

    private EntryImpl[] cachedEntries;
    private EntryImpl[] readEntries;
    private List<Entry> readEntryList;
    private InflightReadsLimiter limiter;
    private ScheduledExecutorService executor;

    @Setup
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        limiter = new InflightReadsLimiter(LIMIT_BYTES, 1, 0, executor, OpenTelemetry.noop());
        cachedEntries = new EntryImpl[batchSize];
        readEntries = new EntryImpl[batchSize];
        readEntryList = Arrays.asList(readEntries);
        for (int i = 0; i < batchSize; i++) {
            cachedEntries[i] = EntryImpl.create(1, i, new byte[128]);
            cachedEntries[i].getPosition();
        }
    }

    @Benchmark
    public long readAndReleaseBatch() {
        InflightReadsLimiter batchLimiter = limiter;
        InflightReadsLimiter.Handle handle = batchLimiter.acquire(batchSize * 128L, null).orElseThrow();
        for (int i = 0; i < batchSize; i++) {
            EntryImpl entry = EntryImpl.create(cachedEntries[i]);
            readEntries[i] = entry;
        }
        completeRead(callbackImplementation.createCallback(batchLimiter, handle, NOOP_CALLBACK), readEntryList);
        for (int i = batchSize - 1; i >= 0; i--) {
            readEntries[i].release();
            readEntries[i] = null;
        }
        return batchLimiter.getRemainingBytes();
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    private static void completeRead(ReadEntriesCallback callback, List<Entry> entries) {
        callback.readEntriesComplete(entries, null);
    }

    public enum CallbackImplementation {
        CURRENT {
            @Override
            ReadEntriesCallback createCallback(InflightReadsLimiter limiter, InflightReadsLimiter.Handle handle,
                                               ReadEntriesCallback originalCallback) {
                return new ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        AtomicInteger remainingCount = new AtomicInteger(entries.size());
                        Runnable releasePermits = () -> {
                            if (remainingCount.decrementAndGet() <= 0) {
                                limiter.release(handle);
                            }
                        };
                        for (Entry entry : entries) {
                            ((EntryImpl) entry).onDeallocate(releasePermits);
                        }
                        originalCallback.readEntriesComplete(entries, ctx);
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        limiter.release(handle);
                        originalCallback.readEntriesFailed(exception, ctx);
                    }
                };
            }
        },
        FUSED {
            @Override
            ReadEntriesCallback createCallback(InflightReadsLimiter limiter, InflightReadsLimiter.Handle handle,
                                               ReadEntriesCallback originalCallback) {
                return new FusedReadEntriesCallback(limiter, handle, originalCallback);
            }
        };

        abstract ReadEntriesCallback createCallback(InflightReadsLimiter limiter,
                                                     InflightReadsLimiter.Handle handle,
                                                     ReadEntriesCallback originalCallback);
    }

    private static final class FusedReadEntriesCallback implements ReadEntriesCallback, Runnable {
        private static final AtomicIntegerFieldUpdater<FusedReadEntriesCallback> REMAINING_ENTRIES_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(FusedReadEntriesCallback.class, "remainingEntries");

        private final InflightReadsLimiter limiter;
        private final InflightReadsLimiter.Handle handle;
        private ReadEntriesCallback originalCallback;
        private volatile int remainingEntries;

        private FusedReadEntriesCallback(InflightReadsLimiter limiter, InflightReadsLimiter.Handle handle,
                                         ReadEntriesCallback originalCallback) {
            this.limiter = limiter;
            this.handle = handle;
            this.originalCallback = originalCallback;
        }

        @Override
        public void readEntriesComplete(List<Entry> entries, Object ctx) {
            if (!REMAINING_ENTRIES_UPDATER.compareAndSet(this, 0, entries.size() + 1)) {
                return;
            }
            for (Entry entry : entries) {
                ((EntryImpl) entry).onDeallocate(this);
            }
            ReadEntriesCallback callback = originalCallback;
            try {
                callback.readEntriesComplete(entries, ctx);
            } finally {
                originalCallback = null;
            }
        }

        @Override
        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            if (!REMAINING_ENTRIES_UPDATER.compareAndSet(this, 0, 1)) {
                return;
            }
            limiter.release(handle);
            ReadEntriesCallback callback = originalCallback;
            try {
                callback.readEntriesFailed(exception, ctx);
            } finally {
                originalCallback = null;
            }
        }

        @Override
        public void run() {
            if (REMAINING_ENTRIES_UPDATER.decrementAndGet(this) == 1) {
                limiter.release(handle);
            }
        }
    }

    @TearDown
    public void tearDown() {
        try {
            if (limiter.getRemainingBytes() != LIMIT_BYTES) {
                throw new IllegalStateException("Read permits were not released exactly once per batch");
            }
        } finally {
            for (EntryImpl entry : cachedEntries) {
                entry.release();
            }
            limiter.close();
            executor.shutdownNow();
        }
    }
}
