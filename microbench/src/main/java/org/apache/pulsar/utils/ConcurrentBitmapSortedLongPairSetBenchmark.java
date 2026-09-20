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
package org.apache.pulsar.utils;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** Measures replay-position set operations through the production implementation. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class ConcurrentBitmapSortedLongPairSetBenchmark {
    private static final int ENTRY_COUNT = 4096;

    private ConcurrentBitmapSortedLongPairSet updateSet;
    private ConcurrentBitmapSortedLongPairSet readSet;
    private int entryId;

    @Setup
    public void setup() {
        updateSet = new ConcurrentBitmapSortedLongPairSet();
        updateSet.add(1, 0);
        readSet = new ConcurrentBitmapSortedLongPairSet();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            readSet.add(1, i);
        }
    }

    @Benchmark
    public void addAndRemove() {
        int id = 1 + entryId++ % (ENTRY_COUNT - 1);
        updateSet.add(1, id);
        updateSet.remove(1, id);
    }

    @Benchmark
    public boolean contains() {
        return readSet.contains(1, entryId++ % ENTRY_COUNT);
    }

    @Benchmark
    public int size() {
        return readSet.size();
    }
}
