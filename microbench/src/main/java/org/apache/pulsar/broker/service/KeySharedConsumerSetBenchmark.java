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

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

/** Models the consumer sets built for each Key_Shared dispatch batch. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class KeySharedConsumerSetBenchmark {
    @Param({"HASH_SET", "OPEN_HASH_SET"})
    public String implementation;

    @Param({"10"})
    public int consumerCount;

    @Param({"1000"})
    public int entryCount;

    private Key[] keys;

    @Setup
    public void setup() {
        keys = new Key[entryCount];
        Key[] consumers = new Key[consumerCount];
        for (int i = 0; i < consumerCount; i++) {
            consumers[i] = new Key(i);
        }
        for (int i = 0; i < entryCount; i++) {
            keys[i] = consumers[i % consumerCount];
        }
    }

    @Benchmark
    public void collectConsumers(Blackhole blackhole) {
        Set<Key> consumers;
        Set<Key> blockedConsumers;
        if ("OPEN_HASH_SET".equals(implementation)) {
            consumers = new ObjectOpenHashSet<>(consumerCount);
            blockedConsumers = new ObjectOpenHashSet<>(consumerCount);
        } else {
            consumers = new HashSet<>();
            blockedConsumers = new HashSet<>();
        }
        for (Key key : keys) {
            consumers.add(key);
        }
        blackhole.consume(consumers);
        blackhole.consume(blockedConsumers);
    }

    private record Key(int id) {
    }
}
