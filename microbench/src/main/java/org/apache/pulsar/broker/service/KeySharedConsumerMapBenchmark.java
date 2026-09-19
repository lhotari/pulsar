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
package org.apache.pulsar.broker.service;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.mutable.MutableInt;
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
import org.openjdk.jmh.infra.Blackhole;

/** Models the two consumer-keyed maps built for each Key_Shared dispatch batch. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class KeySharedConsumerMapBenchmark {
    @Param({"HASH_MAP", "OPEN_HASH_MAP"})
    public String implementation;

    @Param({"10"})
    public int consumerCount;

    @Param({"1000"})
    public int entryCount;

    private Key[] keys;
    private Object[] entries;

    @Setup
    public void setup() {
        keys = new Key[entryCount];
        entries = new Object[entryCount];
        Key[] consumers = new Key[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            consumers[i] = new Key(i);
        }
        for (int i = 0; i < entryCount; i++) {
            keys[i] = consumers[i % consumerCount];
            entries[i] = new Object();
        }
    }

    @Benchmark
    public void groupEntries(Blackhole blackhole) {
        Map<Key, List<Object>> grouped;
        Map<Key, MutableInt> permits;
        if ("OPEN_HASH_MAP".equals(implementation)) {
            grouped = new Object2ObjectOpenHashMap<>(consumerCount);
            permits = new Object2ObjectOpenHashMap<>(consumerCount);
        } else {
            grouped = new HashMap<>();
            permits = new HashMap<>();
        }
        for (int i = 0; i < entryCount; i++) {
            Key key = keys[i];
            MutableInt available = permits.computeIfAbsent(key, ignored -> new MutableInt(100));
            if (available.intValue() > 0) {
                available.decrement();
                grouped.computeIfAbsent(key, ignored -> new ArrayList<>()).add(entries[i]);
            }
        }
        blackhole.consume(grouped);
        blackhole.consume(permits);
    }

    private record Key(int id) {
    }
}
