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
package org.apache.bookkeeper.mledger.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
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
import org.openjdk.jmh.annotations.Warmup;

/** Models the cursor's read-lock/range checks, allowing escape analysis in both predicate forms. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ReadSkipPredicateBenchmark {
    @Param({"0", "128", "10000"})
    public int deletedRanges;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Position markDeletePosition;
    private PositionRangeSet ranges;
    private Predicate<Position> legacy;
    private PositionPredicate primitive;
    private int nextEntry;

    @Setup
    public void setup() {
        markDeletePosition = PositionFactory.create(1, 0);
        ranges = new PositionRangeSet(PositionFactory::create, false);
        for (int i = 0; i < deletedRanges; i++) {
            ranges.addOpenClosed(1, 2L * i, 1, 2L * i + 1);
        }
        legacy = this::isDeleted;
        primitive = this::isDeleted;
    }

    private boolean isDeleted(Position position) {
        lock.readLock().lock();
        try {
            return position.compareTo(markDeletePosition) <= 0
                    || ranges.contains(position.getLedgerId(), position.getEntryId());
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean isDeleted(long ledgerId, long entryId) {
        lock.readLock().lock();
        try {
            return markDeletePosition.compareTo(ledgerId, entryId) >= 0 || ranges.contains(ledgerId, entryId);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Benchmark
    public boolean positionLookup() {
        return legacy.test(PositionFactory.create(1, nextEntry++ & 32767));
    }

    @Benchmark
    public boolean primitiveLookup() {
        return PositionPredicate.test(primitive, 1, nextEntry++ & 32767);
    }
}
