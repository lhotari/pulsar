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

import java.util.concurrent.ThreadLocalRandom;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures cached range reads of the same entries from several threads, as the dispatchers of several subscriptions
 * read the entries at a topic's tail: each read copies 100 consecutive cached entries and releases the copies, either
 * retaining and releasing each cached entry around its copy or copying it under its wrapper's optimistic read.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(4)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
public class RangeCacheFanoutReadBenchmark {
    private static final int CACHED_ENTRIES = 1_000;
    private static final int READ_SIZE = 100;

    private RangeCache cache;

    @Setup
    public void setup() {
        cache = new RangeCache(new RangeCacheRemovalQueue(0, false));
        RangeCache.Inserter inserter = cache.newInserter();
        for (int i = 0; i < CACHED_ENTRIES; i++) {
            EntryImpl entry = EntryImpl.create(0, i, new byte[64]);
            if (!inserter.put(entry.getPosition(), entry, entry.getLength())) {
                entry.release();
                throw new IllegalStateException("Failed to populate range cache");
            }
        }
    }

    @Benchmark
    public int retainAndCopy() {
        int first = ThreadLocalRandom.current().nextInt(CACHED_ENTRIES - READ_SIZE);
        RangeEntryCacheImpl.CachedEntries result = new RangeEntryCacheImpl.CachedEntries(first, READ_SIZE, null);
        cache.forEachInRange(position(first), position(first + READ_SIZE - 1), result);
        return release(result);
    }

    @Benchmark
    public int copyUnderOptimisticRead() {
        int first = ThreadLocalRandom.current().nextInt(CACHED_ENTRIES - READ_SIZE);
        RangeEntryCacheImpl.CachedEntries result = new RangeEntryCacheImpl.CachedEntries(first, READ_SIZE, null);
        cache.forEachCopyInRange(position(first), position(first + READ_SIZE - 1), null, result::acceptCopy);
        return release(result);
    }

    private static Position position(long entryId) {
        return PositionFactory.create(0, entryId);
    }

    private static int release(RangeEntryCacheImpl.CachedEntries result) {
        int count = 0;
        for (Entry entry : result.entries) {
            if (entry != null) {
                count++;
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
