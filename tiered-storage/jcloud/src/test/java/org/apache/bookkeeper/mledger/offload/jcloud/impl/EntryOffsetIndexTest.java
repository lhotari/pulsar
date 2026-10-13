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
import java.util.List;
import org.testng.annotations.Test;

public class EntryOffsetIndexTest {
    // Entries of 100 bytes, header included, from offset 128
    private static long[] offsets(long firstEntryId, int count) {
        long[] offsets = new long[count];
        for (int i = 0; i < count; i++) {
            offsets[i] = 128 + (firstEntryId + i) * 100;
        }
        return offsets;
    }

    private static void add(EntryOffsetIndex.ObjectOffsets offsets, long firstEntryId, int count) {
        offsets.add(firstEntryId, offsets(firstEntryId, count), count, 128 + (firstEntryId + count) * 100);
    }

    private static List<List<Long>> runs(EntryOffsetIndex.ObjectOffsets offsets) {
        return offsets.runs().stream().map(run -> List.of(run[0], run[1])).toList();
    }

    @Test
    public void findsTheNearestKnownOffset() {
        EntryOffsetIndex index = new EntryOffsetIndex(1 << 20, 60);
        EntryOffsetIndex.ObjectOffsets offsets = index.offsets("bucket", "object");
        assertThat(offsets.floor(5)).isNull();
        add(offsets, 10, 10);
        assertThat(offsets.floor(5)).isNull();
        assertThat(offsets.floor(10)).isEqualTo(new EntryOffsetIndex.Position(10, 128 + 1000));
        assertThat(offsets.floor(19)).isEqualTo(new EntryOffsetIndex.Position(19, 128 + 1900));
        // after the run, where the entry after its last one starts
        assertThat(offsets.floor(1000)).isEqualTo(new EntryOffsetIndex.Position(20, 128 + 2000));
        // the same object's offsets for another read handle
        assertThat(index.offsets("bucket", "object")).isSameAs(offsets);
        assertThat(index.offsets("bucket", "other").floor(19)).isNull();
    }

    @Test
    public void joinsRunsThatOverlapOrContinueEachOther() {
        EntryOffsetIndex.ObjectOffsets offsets = new EntryOffsetIndex(1 << 20, 60).offsets("bucket", "object");
        add(offsets, 0, 10);
        add(offsets, 20, 10);
        assertThat(runs(offsets)).containsExactly(List.of(0L, 9L), List.of(20L, 29L));
        // continues the first byte for byte
        add(offsets, 10, 5);
        assertThat(runs(offsets)).containsExactly(List.of(0L, 14L), List.of(20L, 29L));
        // overlaps both, and fills the gap
        add(offsets, 12, 10);
        assertThat(runs(offsets)).containsExactly(List.of(0L, 29L));
        assertThat(offsets.floor(25)).isEqualTo(new EntryOffsetIndex.Position(25, 128 + 2500));
    }

    @Test
    public void keepsRunsOnBothSidesOfTheEndOfADataBlockApart() {
        EntryOffsetIndex.ObjectOffsets offsets = new EntryOffsetIndex(1 << 20, 60).offsets("bucket", "object");
        add(offsets, 0, 10);
        // the next block's first entry starts after 64 bytes of padding and the next block's header
        long[] nextBlock = {128 + 1000 + 64 + 128};
        offsets.add(10, nextBlock, 1, nextBlock[0] + 100);
        assertThat(runs(offsets)).containsExactly(List.of(0L, 9L), List.of(10L, 10L));
        assertThat(offsets.floor(10)).isEqualTo(new EntryOffsetIndex.Position(10, nextBlock[0]));
    }

    @Test
    public void dropsTheLeastRecentlyUsedObjectsBeyondItsBudget() {
        // room for the offsets of about 2,000 entries
        EntryOffsetIndex index = new EntryOffsetIndex(8 * 1024, 60);
        EntryOffsetIndex.ObjectOffsets first = index.offsets("bucket", "first");
        add(first, 0, 1000);
        EntryOffsetIndex.ObjectOffsets second = index.offsets("bucket", "second");
        add(second, 0, 1000);
        assertThat(index.weight()).isLessThanOrEqualTo(8 * 1024);
        index.offsets("bucket", "second");
        EntryOffsetIndex.ObjectOffsets third = index.offsets("bucket", "third");
        add(third, 0, 1000);
        assertThat(index.weight()).isLessThanOrEqualTo(8 * 1024);
        // the first object, used least recently, is dropped, and starts again empty
        assertThat(index.offsets("bucket", "first")).isNotSameAs(first);
        assertThat(index.offsets("bucket", "first").floor(10)).isNull();
    }
}
