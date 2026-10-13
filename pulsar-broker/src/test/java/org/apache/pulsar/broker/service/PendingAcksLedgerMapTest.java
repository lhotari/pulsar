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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.testng.annotations.Test;

public class PendingAcksLedgerMapTest {

    @Test
    public void keepsEntriesInOrderWithAppendsInsertsAndRemovals() {
        PendingAcksLedgerMap map = new PendingAcksLedgerMap();
        assertThat(map.put(5, value(1))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(map.put(7, value(2))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        // out of order, as a redelivery adds it
        assertThat(map.put(6, value(3))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(map.put(1, value(4))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(map.put(6, value(5))).isEqualTo(value(3));
        assertThat(entries(map)).containsExactly(1L, 5L, 6L, 7L);
        assertThat(map.get(6)).isEqualTo(value(5));
        assertThat(map.get(2)).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);

        assertThat(map.remove(5)).isEqualTo(value(1));
        assertThat(map.remove(5)).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(map.containsKey(5)).isFalse();
        // a removed entry can come back
        assertThat(map.put(5, value(6))).isEqualTo(PendingAckValues.PACKED_NOT_FOUND);
        assertThat(entries(map)).containsExactly(1L, 5L, 6L, 7L);

        List<Long> removed = new ArrayList<>();
        assertThat(map.hasEntriesUpTo(0)).isFalse();
        assertThat(map.hasEntriesUpTo(6)).isTrue();
        assertThat(map.removeUpTo(6, (entryId, v) -> removed.add(entryId))).isEqualTo(3);
        assertThat(removed).containsExactly(1L, 5L, 6L);
        assertThat(entries(map)).containsExactly(7L);
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.remove(7)).isEqualTo(value(2));
        assertThat(map.isEmpty()).isTrue();
        assertThat(map.hasEntriesUpTo(Long.MAX_VALUE)).isFalse();
    }

    @Test
    public void behavesAsASortedMap() {
        // random operations in the patterns of dispatch and acks: mostly increasing entry IDs, some redeliveries,
        // removals near the start and range removals up to a mark-delete position
        Random random = new Random(1);
        for (int round = 0; round < 50; round++) {
            PendingAcksLedgerMap map = new PendingAcksLedgerMap();
            TreeMap<Long, Long> reference = new TreeMap<>();
            long nextEntryId = 0;
            for (int step = 0; step < 5000; step++) {
                int operation = random.nextInt(100);
                if (operation < 45) {
                    long entryId = random.nextInt(10) == 0 && nextEntryId > 0
                            ? random.nextLong(nextEntryId) : nextEntryId++;
                    long v = value(random.nextInt(1000));
                    Long previous = reference.put(entryId, v);
                    assertThat(map.put(entryId, v))
                            .isEqualTo(previous != null ? previous : PendingAckValues.PACKED_NOT_FOUND);
                } else if (operation < 90) {
                    long entryId = reference.isEmpty() || random.nextInt(4) == 0
                            ? random.nextLong(nextEntryId + 1) : nthKey(reference, random.nextInt(
                                    Math.min(reference.size(), 20)));
                    Long previous = reference.remove(entryId);
                    assertThat(map.remove(entryId))
                            .isEqualTo(previous != null ? previous : PendingAckValues.PACKED_NOT_FOUND);
                } else if (operation < 95) {
                    long upTo = reference.isEmpty() ? nextEntryId : reference.firstKey() + random.nextInt(30);
                    Map<Long, Long> head = new TreeMap<>(reference.headMap(upTo, true));
                    List<Long> removedIds = new ArrayList<>();
                    List<Long> removedValues = new ArrayList<>();
                    assertThat(map.hasEntriesUpTo(upTo)).isEqualTo(!head.isEmpty());
                    assertThat(map.removeUpTo(upTo, (entryId, v) -> {
                        removedIds.add(entryId);
                        removedValues.add(v);
                    })).isEqualTo(head.size());
                    assertThat(removedIds).containsExactlyElementsOf(head.keySet());
                    assertThat(removedValues).containsExactlyElementsOf(head.values());
                    reference.headMap(upTo, true).clear();
                } else {
                    long entryId = random.nextLong(nextEntryId + 1);
                    Long expected = reference.get(entryId);
                    assertThat(map.get(entryId))
                            .isEqualTo(expected != null ? expected : PendingAckValues.PACKED_NOT_FOUND);
                    assertThat(map.containsKey(entryId)).isEqualTo(expected != null);
                }
                assertThat(map.size()).isEqualTo(reference.size());
            }
            List<Long> ids = new ArrayList<>();
            List<Long> values = new ArrayList<>();
            map.forEach((entryId, v) -> {
                ids.add(entryId);
                values.add(v);
            });
            assertThat(ids).containsExactlyElementsOf(reference.keySet());
            assertThat(values).containsExactlyElementsOf(reference.values());
        }
    }

    private static long value(int remainingUnacked) {
        return PendingAckValues.pack(remainingUnacked, remainingUnacked * 31);
    }

    private static long nthKey(TreeMap<Long, Long> map, int n) {
        int i = 0;
        for (Long key : map.keySet()) {
            if (i++ == n) {
                return key;
            }
        }
        throw new IllegalArgumentException();
    }

    private static List<Long> entries(PendingAcksLedgerMap map) {
        List<Long> ids = new ArrayList<>();
        map.forEach((entryId, v) -> ids.add(entryId));
        return ids;
    }
}
