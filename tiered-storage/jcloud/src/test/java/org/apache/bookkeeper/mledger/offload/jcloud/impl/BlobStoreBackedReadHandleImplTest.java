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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BlobStoreBackedReadHandleImplTest {

    private final EntryOffsetIndex entryOffsetIndex = new EntryOffsetIndex(16L * 1024 * 1024, 3600);

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

    @AfterClass
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @AfterClass
    public void clearCache() throws Exception {
        entryOffsetIndex.clear();
    }

    private String getExpectedEntryContent(int entryId) {
        return "Entry " + entryId;
    }

    private Pair<BlobStoreBackedReadHandleImpl, ByteBuf> createReadHandle(
            long ledgerId, int entries, boolean hasDirtyData) throws Exception {
        // Build data.
        List<Pair<Integer, Integer>> offsets = new ArrayList<>();
        int totalLen = 0;
        ByteBuf data = ByteBufAllocator.DEFAULT.heapBuffer(1024);
        data.writeInt(0);
        data.writerIndex(128);
        //data.readerIndex(128);
        for (int i = 0; i < entries; i++) {
            if (hasDirtyData && i == 1) {
                data.writeBytes("dirty data".getBytes(UTF_8));
            }
            offsets.add(Pair.of(i, data.writerIndex()));
            byte[] entryContent = getExpectedEntryContent(i).getBytes(UTF_8);
            totalLen += entryContent.length;
            data.writeInt(entryContent.length);
            data.writeLong(i);
            data.writeBytes(entryContent);
        }
        // Build metadata.
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("pwd".getBytes(UTF_8))
                .withClosedState()
                .withLastEntryId(entries)
                .withLength(totalLen)
                .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                .build();
        BackedInputStreamImpl inputStream = new BackedInputStreamImpl(data);
        // An index with every entry's offset, so that reads seek over the dirty data
        OffloadIndexBlock mockIndex = mock(OffloadIndexBlock.class);
        when(mockIndex.getLedgerMetadata()).thenReturn(metadata);
        for (Pair<Integer, Integer> pair : offsets) {
            when(mockIndex.getIndexEntryForEntry(pair.getLeft())).thenReturn(
                    OffloadIndexEntryImpl.of(pair.getLeft(), 0, pair.getRight(), 0));
        }
        // Build obj.
        return Pair.of(new BlobStoreBackedReadHandleImpl(ledgerId, mockIndex, inputStream, executor,
                () -> entryOffsetIndex.offsets("bucket", "ledger-" + ledgerId)), data);
    }

    private static class BackedInputStreamImpl extends BackedInputStream {

        private ByteBuf data;
        private long bytesRead;

        private BackedInputStreamImpl(ByteBuf data){
            this.data = data;
        }

        @Override
        public void seek(long position) {
            data.readerIndex((int) position);
        }

        @Override
        public void seekForward(long position) throws IOException {
            data.readerIndex((int) position);
        }

        @Override
        public long getCurrentPosition() {
            return data.readerIndex();
        }

        @Override
        public int read() throws IOException {
            if (data.readableBytes() == 0) {
                throw new EOFException("The input-stream has no bytes to read");
            }
            bytesRead++;
            // An unsigned byte, as specified by InputStream.read()
            return data.readByte() & 0xFF;
        }

        @Override
        public int available() throws IOException {
            return data.readableBytes();
        }
    }

    @DataProvider
    public Object[][] streamStartAt() {
        return new Object[][] {
            // It gives a 0 value of the entry length.
            { 0, false },
            // It gives a 0 value of the entry length.
            { 1, false },
            // The first entry starts at 128.
            { 128, false },
            // It gives a 0 value of the entry length.
            { 0, true },
            // It gives a 0 value of the entry length.
            { 1, true },
            // The first entry starts at 128.
            { 128, true }
        };
    }

    @Test(dataProvider = "streamStartAt")
    public void testRead(int streamStartAt, boolean hasDirtyData) throws Exception {
        int entryCount = 5;
        Pair<BlobStoreBackedReadHandleImpl, ByteBuf> ledgerDataPair =
                createReadHandle(1, entryCount, hasDirtyData);
        BlobStoreBackedReadHandleImpl ledger = ledgerDataPair.getLeft();
        ByteBuf data = ledgerDataPair.getRight();
        data.readerIndex(streamStartAt);
        // Teat read each entry.
        for (int i = 0; i < 5; i++) {
            LedgerEntries entries = ledger.read(i, i);
            assertEquals(new String(entries.iterator().next().getEntryBytes()), getExpectedEntryContent(i));
        }
        // Test read all entries.
        LedgerEntries entries1 = ledger.read(0, entryCount - 1);
        Iterator<LedgerEntry> iterator1 = entries1.iterator();
        for (int i = 0; i < entryCount; i++) {
            assertEquals(new String(iterator1.next().getEntryBytes()), getExpectedEntryContent(i));
        }
        // Test a special case.
        // 1. Read from 0 to "lac - 1".
        // 2. Any reading.
        LedgerEntries entries2 = ledger.read(0, entryCount - 2);
        Iterator<LedgerEntry> iterator2 = entries2.iterator();
        for (int i = 0; i < entryCount - 1; i++) {
            assertEquals(new String(iterator2.next().getEntryBytes()), getExpectedEntryContent(i));
        }
        LedgerEntries entries3 = ledger.read(0, entryCount - 1);
        Iterator<LedgerEntry> iterator3 = entries3.iterator();
        for (int i = 0; i < entryCount; i++) {
            assertEquals(new String(iterator3.next().getEntryBytes()), getExpectedEntryContent(i));
        }
        // cleanup.
        ledger.close();
    }

    @Test
    public void testGetIndexedEntryIdFloor() throws Exception {
        int entries = 5000;
        long[] indexedEntryIds = {0, 2500};
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(3)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("pwd".getBytes(UTF_8))
                .withClosedState()
                .withLastEntryId(entries - 1)
                .withLength(entries * 100L)
                .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                .build();
        // A sparse index with one index entry per data block, like the index of an offloaded ledger
        OffloadIndexBlock mockIndex = mock(OffloadIndexBlock.class);
        when(mockIndex.getLedgerMetadata()).thenReturn(metadata);
        when(mockIndex.getIndexEntryForEntry(anyLong())).thenAnswer(invocation -> {
            long entryId = invocation.getArgument(0);
            long floor = entryId >= indexedEntryIds[1] ? indexedEntryIds[1] : indexedEntryIds[0];
            return OffloadIndexEntryImpl.of(floor, 0, 128 + floor * 100, 0);
        });
        ByteBuf data = ByteBufAllocator.DEFAULT.heapBuffer(0);
        BlobStoreBackedReadHandleImpl ledger = new BlobStoreBackedReadHandleImpl(3, mockIndex,
                new BackedInputStreamImpl(data), executor, () -> entryOffsetIndex.offsets("bucket", "ledger-3"));
        try {
            assertThat(ledger.getIndexedEntryIdFloor(0)).isEqualTo(0);
            assertThat(ledger.getIndexedEntryIdFloor(2000)).as("floor in the first block").isEqualTo(0);
            assertThat(ledger.getIndexedEntryIdFloor(2500)).as("first entry of the second block").isEqualTo(2500);
            assertThat(ledger.getIndexedEntryIdFloor(4999)).as("floor in the second block").isEqualTo(2500);
            assertThat(ledger.getIndexedEntryIdCeiling(0)).isEqualTo(0);
            assertThat(ledger.getIndexedEntryIdCeiling(2000)).as("ceiling in the first block").isEqualTo(2500);
            assertThat(ledger.getIndexedEntryIdCeiling(2500)).as("first entry of the second block").isEqualTo(2500);
            assertThat(ledger.getIndexedEntryIdCeiling(4999)).as("no ceiling in the last block").isEqualTo(-1);
        } finally {
            ledger.close();
            data.release();
        }
    }

    private static final int ENTRY_SIZE = 20;
    private static final int HEADER_SIZE = 128;
    // The padding at the end of each data block but the last, which reads as a negative entry length
    private static final int PADDING_SIZE = 16;

    /**
     * A read handle over entries of {@link #ENTRY_SIZE} bytes, with 12 bytes of entry header, whose sparse index only
     * knows the offsets of the first entries of its data blocks, like the index of an offloaded ledger.
     */
    private final class SparselyIndexedLedger implements AutoCloseable {
        // An index of its own, whose offsets do not expire during the test
        private final EntryOffsetIndex entryOffsetIndex;
        private final ByteBuf data;
        private final long[] offsets;
        private final BackedInputStreamImpl inputStream;
        private final BlobStoreBackedReadHandleImpl handle;

        SparselyIndexedLedger(long ledgerId, int entryCount, long[] blockFirstEntryIds, boolean padded)
                throws Exception {
            this(ledgerId, entryCount, blockFirstEntryIds, padded, new EntryOffsetIndex(16L * 1024 * 1024, 3600));
        }

        SparselyIndexedLedger(long ledgerId, int entryCount, long[] blockFirstEntryIds, boolean padded,
                              EntryOffsetIndex entryOffsetIndex) throws Exception {
            this.entryOffsetIndex = entryOffsetIndex;
            data = ByteBufAllocator.DEFAULT.heapBuffer(HEADER_SIZE + entryCount * ENTRY_SIZE
                    + blockFirstEntryIds.length * PADDING_SIZE);
            data.writerIndex(HEADER_SIZE);
            offsets = new long[entryCount];
            for (int i = 0; i < entryCount; i++) {
                long entryId = i;
                if (padded && i > 0 && Arrays.stream(blockFirstEntryIds).anyMatch(first -> first == entryId)) {
                    for (int j = 0; j < PADDING_SIZE; j++) {
                        data.writeByte(0xFF);
                    }
                }
                offsets[i] = data.writerIndex();
                data.writeInt(ENTRY_SIZE - 12);
                data.writeLong(i);
                data.writeZero(ENTRY_SIZE - 12);
            }
            LedgerMetadata metadata = LedgerMetadataBuilder.create()
                    .withId(ledgerId)
                    .withEnsembleSize(1)
                    .withWriteQuorumSize(1)
                    .withAckQuorumSize(1)
                    .withDigestType(DigestType.CRC32C)
                    .withPassword("pwd".getBytes(UTF_8))
                    .withClosedState()
                    .withLastEntryId(entryCount - 1)
                    .withLength((long) entryCount * (ENTRY_SIZE - 12))
                    .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                    .build();
            OffloadIndexBlock mockIndex = mock(OffloadIndexBlock.class);
            when(mockIndex.getLedgerMetadata()).thenReturn(metadata);
            when(mockIndex.getIndexEntryForEntry(anyLong())).thenAnswer(invocation -> {
                long entryId = invocation.getArgument(0);
                long floor = 0;
                for (long blockFirstEntryId : blockFirstEntryIds) {
                    if (blockFirstEntryId <= entryId) {
                        floor = blockFirstEntryId;
                    }
                }
                return OffloadIndexEntryImpl.of(floor, 0, offsetOf(floor), 0);
            });
            inputStream = new BackedInputStreamImpl(data);
            handle = new BlobStoreBackedReadHandleImpl(ledgerId, mockIndex, inputStream, executor,
                    () -> entryOffsetIndex.offsets("bucket", "ledger-" + ledgerId));
        }

        long offsetOf(long entryId) {
            return offsets[(int) entryId];
        }

        /** The runs of entries with known offsets, as {first, last} pairs. */
        List<List<Long>> knownRuns() {
            return entryOffsetIndex.offsets("bucket", "ledger-" + handle.getId()).runs().stream()
                    .map(run -> List.of(run[0], run[1])).toList();
        }

        /**
         * Reads the given entries and returns the number of bytes read from the data object to read them.
         */
        long read(long firstEntry, long lastEntry) throws Exception {
            inputStream.bytesRead = 0;
            try (LedgerEntries entries = handle.read(firstEntry, lastEntry)) {
                long expectedEntryId = firstEntry;
                for (LedgerEntry entry : entries) {
                    assertThat(entry.getEntryId()).isEqualTo(expectedEntryId++);
                }
                assertThat(expectedEntryId).as("entries read").isEqualTo(lastEntry + 1);
            }
            return inputStream.bytesRead;
        }

        @Override
        public void close() throws Exception {
            handle.close();
            data.release();
        }
    }

    @Test
    public void testReadWalksFromTheNearestKnownOffset() throws Exception {
        // A single data block: only the first entry has an indexed offset
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(2, 5000, new long[] {0}, false)) {
            // The first read walks the block from its start, and records the offsets that it passes
            assertThat(ledger.read(2000, 2000)).as("bytes read by the first read").isEqualTo(2001L * ENTRY_SIZE);
            assertThat(ledger.knownRuns()).containsExactly(List.of(0L, 2000L));

            // A read further on walks from where the known offsets end, not from the start of the block
            assertThat(ledger.read(4999, 4999)).as("bytes read by the second read")
                    .isEqualTo((4999L - 2001 + 1) * ENTRY_SIZE);
            // A read of an entry whose offset is known reads only the entry
            assertThat(ledger.read(1000, 1000)).as("bytes read for a known entry").isEqualTo(ENTRY_SIZE);
            assertThat(ledger.knownRuns()).containsExactly(List.of(0L, 4999L));

            // Known offsets are not reported as indexed entries, so that searches do not depend on previous reads
            assertThat(ledger.handle.getIndexedEntryIdFloor(4999)).isEqualTo(0);
        }
    }

    @Test
    public void testReadWalksFromTheStartOfItsBlockWhenThatIsNearer() throws Exception {
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(4, 5000, new long[] {0, 2500}, false)) {
            ledger.read(2000, 2000);
            // An entry of the second block is walked to from the start of that block, which is nearer than the
            // offsets known in the first block
            assertThat(ledger.read(3000, 3000)).as("bytes read in the second block")
                    .isEqualTo((3000L - 2500 + 1) * ENTRY_SIZE);
            // An entry after the known offsets of the first block is walked to from them
            assertThat(ledger.read(2400, 2400)).as("bytes read after the known offsets")
                    .isEqualTo((2400L - 2001 + 1) * ENTRY_SIZE);
            assertThat(ledger.knownRuns()).containsExactly(List.of(0L, 2400L), List.of(2500L, 3000L));
        }
    }

    @Test
    public void testReadsAroundThePaddingAtTheEndOfABlock() throws Exception {
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(6, 5000, new long[] {0, 2500}, true)) {
            // Up to the last entry of the first block, whose known offsets end at the padding
            ledger.read(2499, 2499);
            // An entry of the second block is walked to from the start of that block, not across the padding
            assertThat(ledger.read(2600, 2600)).as("bytes read in the second block")
                    .isEqualTo((2600L - 2500 + 1) * ENTRY_SIZE);
            // The offsets on both sides of the padding stay apart
            assertThat(ledger.knownRuns()).containsExactly(List.of(0L, 2499L), List.of(2500L, 2600L));
            // A read across the end of the block reads its entries on both sides of the padding, which reads as a
            // negative entry length
            assertThat(ledger.read(2490, 2510)).as("bytes read across the padding")
                    .isEqualTo(4 + 21L * ENTRY_SIZE);
            // The entry after the padding is read from the index's offset, not from where the known offsets end
            assertThat(ledger.read(2500, 2500)).isEqualTo(ENTRY_SIZE);
        }
    }

    @Test
    public void testSequentialReadsReadOnlyTheirEntries() throws Exception {
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(7, 5000, new long[] {0}, false)) {
            for (long firstEntry = 0; firstEntry < 5000; firstEntry += 100) {
                assertThat(ledger.read(firstEntry, firstEntry + 99)).as("bytes read from entry %d", firstEntry)
                        .isEqualTo(100L * ENTRY_SIZE);
            }
            // The reads' offsets join into one run
            assertThat(ledger.knownRuns()).containsExactly(List.of(0L, 4999L));
        }
    }

    @Test
    public void testSearchProbesWalkLittleInAnObjectLargerThanTheBudget() throws Exception {
        // 300,000 entries of 20 bytes in one block, about 6 MiB, with a budget of 1 MiB: an object's share of 128 KiB
        // keeps the offsets of about 32,000 entries
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(9, 300_000, new long[] {0}, false,
                new EntryOffsetIndex(1024 * 1024, 3600))) {
            assertThat(ledger.read(299_999, 299_999)).isEqualTo(300_000L * ENTRY_SIZE);
            // probes of decreasing entries, as a search by timestamp makes, walk from the anchors, about 1 MiB of
            // data apart, or read known entries, instead of walking from the start of the block again
            long[] probes = {299_998, 150_000, 75_000, 225_000, 37_500, 299_000};
            for (long probe : probes) {
                assertThat(ledger.read(probe, probe)).as("bytes read for entry %d", probe)
                        .isLessThanOrEqualTo(EntryOffsetIndex.ANCHOR_INTERVAL_BYTES + ENTRY_SIZE);
            }
            // the runs of the latest probes have replaced the one around entry 299,998, which is walked to from the
            // nearest one that is left
            assertThat(ledger.read(299_998, 299_998)).isLessThanOrEqualTo(EntryOffsetIndex.ANCHOR_INTERVAL_BYTES);
            assertThat(ledger.read(299_998, 299_998)).as("a known entry").isEqualTo(ENTRY_SIZE);
        }
    }

    @Test
    public void testReadWalksFromTheBlockStartWithoutKnownOffsets() throws Exception {
        try (SparselyIndexedLedger ledger = new SparselyIndexedLedger(8, 5000, new long[] {0}, false)) {
            ledger.read(2000, 2000);
            ledger.entryOffsetIndex.clear();
            assertThat(ledger.read(4999, 4999)).isEqualTo(5000L * ENTRY_SIZE);
        }
    }
}
