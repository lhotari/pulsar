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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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

/**
 * Measures copying a range of cached entries for a read, as a cache hit does, with entries in pooled buffers: a copy
 * that retains the buffer while the cached entry is retained, or a copy that takes over the cached entry's reference.
 * The default is one reader; run it also with {@code -t 4} to have several readers copy the same entries, as
 * subscriptions reading together do, which is the case that the shared copy targets.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class RangeCacheCopyBenchmark {
    @Param({"100"})
    public int batchSize;

    @Param({"128"})
    public int entrySize;

    private RangeCache cache;
    private Position first;
    private Position last;

    @Setup
    public void setup() {
        cache = new RangeCache(new RangeCacheRemovalQueue(0, false));
        first = PositionFactory.create(1, 0);
        last = PositionFactory.create(1, batchSize - 1);
        for (int i = 0; i < batchSize; i++) {
            ByteBuf data = PooledByteBufAllocator.DEFAULT.directBuffer(entrySize, entrySize).writeZero(entrySize);
            // as the cache inserts an entry, with a retained duplicate of its buffer
            EntryImpl entry = EntryImpl.createWithRetainedDuplicate(PositionFactory.create(1, i), data, 0);
            data.release();
            if (!cache.put(entry.getPosition(), entry)) {
                entry.release();
                throw new IllegalStateException("Failed to populate range cache");
            }
        }
    }

    @Benchmark
    public int copyRetainingTheBuffer() {
        RangeEntryCacheImpl.CachedEntries result = new RangeEntryCacheImpl.CachedEntries(0, batchSize, null);
        cache.forEachInRange(first, last, result);
        return release(result);
    }

    @Benchmark
    public int copySharingTheCachedEntry() {
        RangeEntryCacheImpl.CachedEntries result = new RangeEntryCacheImpl.CachedEntries(0, batchSize, null);
        cache.forEachRetainedInRange(first, last, result::acceptRetained);
        return release(result);
    }

    private static int release(RangeEntryCacheImpl.CachedEntries result) {
        int count = 0;
        for (Entry entry : result.entries) {
            if (entry != null) {
                count += entry.getLength();
                entry.release();
            }
        }
        return count;
    }

    @TearDown
    public void tearDown() {
        cache.clear();
    }
}
