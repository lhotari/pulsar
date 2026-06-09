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

import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.BatchedEntryDeletionIndexInfo;
import org.apache.bookkeeper.mledger.proto.LongListMap;
import org.apache.bookkeeper.mledger.proto.MessageRange;
import org.apache.bookkeeper.mledger.proto.PositionInfo;
import org.apache.pulsar.common.api.proto.CompressionType;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.util.collections.LongPairRangeSet;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Characterizes the serialized size of cursor acknowledgment state (individually deleted message ranges and
 * batch deletion indexes) under realistic and worst-case acknowledgment distributions, and how well LZ4 can
 * compress each. It validates that the configured persistence caps stay within the storage budgets:
 * <ul>
 *   <li>ZooKeeper znode: 10MB max (jute.maxbuffer); a ~5MB uncompressed budget is targeted. The metadata
 *       store persists individually deleted ranges using the {@link MessageRange} encoding.</li>
 *   <li>BookKeeper cursor ledger: a single entry is capped at {@code nettyMaxFrameSizeBytes =
 *       maxMessageSize + MESSAGE_SIZE_FRAME_PADDING} (~5MB by default). The cursor ledger persists ranges
 *       using the compact {@link LongListMap} (RoaringBitmap long-array) encoding when
 *       {@code managedLedgerPersistIndividualAckAsLongArray=true} (the default).</li>
 * </ul>
 *
 * <p>The exact encoders from {@link ManagedCursorImpl} are reused via {@link ManagedCursorMetadataUtils}, so the
 * measured sizes match what the broker actually persists. The test logs a report and asserts the budget bounds.
 */
public class ManagedCursorAckStateSizeTest {

    private static final Logger log = LoggerFactory.getLogger(ManagedCursorAckStateSizeTest.class);

    // Storage budgets (bytes).
    private static final int MB = 1024 * 1024;
    private static final long ZK_ZNODE_MAX = 10L * MB;            // jute.maxbuffer default
    private static final long ZK_TARGET = 5L * MB;                // accepted worst-case uncompressed ZK state
    private static final long BK_ENTRY_MAX = 5L * MB + 10 * 1024; // nettyMaxFrameSizeBytes = maxMessageSize + padding

    // Realistic ledger layout: ledgerId base + up to ENTRIES_PER_LEDGER entries per ledger.
    private static final long BASE_LEDGER = 3_000_000L;
    private static final int ENTRIES_PER_LEDGER = 50_000; // managedLedgerMaxEntriesPerLedger default

    // Caps to characterize. The chosen defaults should be validated against the budgets at these sizes.
    private static final int[] CAPS = {10_000, 50_000, 100_000, 200_000, 500_000, 1_000_000};
    private static final int GENERATED_RANGES = 1_000_000;

    private static final long SEED = 42L;

    private static final LongPairRangeSet.LongPairConsumer<Position> CONVERTER = PositionFactory::create;
    private static final LongPairRangeSet.RangeBoundConsumer<Position> REVERSE_CONVERTER =
            position -> new LongPairRangeSet.LongPair(position.getLedgerId(), position.getEntryId());

    private static RangeSetWrapper<Position> newRangeSet() {
        // Mirrors ManagedCursorImpl's individualDeletedMessages with the default config
        // (unackedRangesOpenCacheSetEnabled=true -> RoaringBitmap-backed OpenLongPairRangeSet).
        return new RangeSetWrapper<>(CONVERTER, REVERSE_CONVERTER, true, false);
    }

    private static int lz4Bytes(byte[] data) {
        CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(CompressionType.LZ4);
        ByteBuf out = codec.encode(Unpooled.wrappedBuffer(data));
        try {
            return out.readableBytes();
        } finally {
            out.release();
        }
    }

    /**
     * PROPOSED encoding (PIP-485): per-ledger {@link RoaringBitmap#serialize} size (run-optimized), summed, plus a
     * small per-ledger framing allowance. Unlike the current dense {@code toLongArray()} encoding, this depends on
     * the number/shape of ack holes, NOT on the backlog (entry-id span).
     */
    /**
     * Proposed on-disk payload: per ledger, the ledgerId followed by the (run-optimized) RoaringBitmap portable
     * serialization. This is the byte stream the cursor ledger / metadata store would persist.
     */
    private static byte[] roaringSerializedToBytes(RangeSetWrapper<Position> set, int maxRanges) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            for (Map.Entry<Long, long[]> e : set.toRanges(maxRanges).entrySet()) {
                RoaringBitmap rb = roaringFromWords(e.getValue()); // runOptimize() applied in roaringFromWords
                dos.writeLong(e.getKey());
                rb.serialize(dos);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return bos.toByteArray();
    }

    private static long roaringSerializedBytes(RangeSetWrapper<Position> set, int maxRanges) {
        return roaringSerializedToBytes(set, maxRanges).length;
    }

    /**
     * Transient heap allocated by the CURRENT encoding's {@code RoaringBitSet.toLongArray()} dense conversion (and
     * by the symmetric deserialization). This is the per-flush garbage that drives GC pressure; it scales with the
     * backlog (highest set entry-id per ledger), not with the number of ack holes.
     */
    private static long denseLongArrayHeapBytes(RangeSetWrapper<Position> set, int maxRanges) {
        long words = 0;
        for (long[] arr : set.toRanges(maxRanges).values()) {
            words += arr.length;
        }
        return words * Long.BYTES;
    }

    private static RoaringBitmap roaringFromWords(long[] words) {
        // Build the RoaringBitmap from the dense words in bulk (much faster than per-bit add at large scale).
        RoaringBitmap rb = org.roaringbitmap.BitSetUtil.bitmapOf(words);
        rb.runOptimize();
        return rb;
    }

    // The encoders below mirror ManagedCursorImpl.buildIndividualDeletedMessageRanges (MessageRange),
    // buildLongPropertiesMap (LongListMap) and buildBatchEntryDeletionIndexInfoList (BatchedEntryDeletionIndexInfo)
    // exactly, so the measured sizes match what the broker persists today. They operate on the real
    // RoaringBitmap-backed RangeSetWrapper and the generated LightProto messages.

    private static List<MessageRange> buildMessageRanges(RangeSetWrapper<Position> set, int maxRanges) {
        List<MessageRange> rangeList = new ArrayList<>();
        set.forEachRawRange((lowerKey, lowerValue, upperKey, upperValue) -> {
            if (rangeList.size() >= maxRanges) {
                return false;
            }
            MessageRange messageRange = new MessageRange();
            messageRange.setLowerEndpoint().setLedgerId(lowerKey).setEntryId(lowerValue);
            messageRange.setUpperEndpoint().setLedgerId(upperKey).setEntryId(upperValue);
            rangeList.add(messageRange);
            return true;
        });
        return rangeList;
    }

    private static List<LongListMap> buildLongListMap(Map<Long, long[]> ranges) {
        List<LongListMap> longListMap = new ArrayList<>();
        ranges.forEach((id, values) -> {
            if (values == null || values.length == 0) {
                return;
            }
            LongListMap lm = new LongListMap().setKey(id);
            for (long value : values) {
                lm.addValue(value);
            }
            longListMap.add(lm);
        });
        return longListMap;
    }

    private static List<BatchedEntryDeletionIndexInfo> buildBatchedEntryDeletionIndexInfo(
            NavigableMap<Position, BitSet> indexes, int maxIndexes) {
        List<BatchedEntryDeletionIndexInfo> result = new ArrayList<>();
        for (Map.Entry<Position, BitSet> entry : indexes.entrySet()) {
            if (result.size() >= maxIndexes) {
                break;
            }
            BatchedEntryDeletionIndexInfo info = new BatchedEntryDeletionIndexInfo();
            info.setPosition().setLedgerId(entry.getKey().getLedgerId()).setEntryId(entry.getKey().getEntryId());
            for (long word : entry.getValue().toLongArray()) {
                info.addDeleteSet(word);
            }
            result.add(info);
        }
        return result;
    }

    /** Serialized size of the MessageRange (metadata-store / ZooKeeper) encoding for up to {@code maxRanges}. */
    private static byte[] messageRangeBytes(RangeSetWrapper<Position> set, int maxRanges) {
        PositionInfo pi = new PositionInfo().setLedgerId(BASE_LEDGER).setEntryId(0);
        pi.addAllIndividualDeletedMessages(buildMessageRanges(set, maxRanges));
        return pi.toByteArray();
    }

    /** Serialized size of the LongListMap (BookKeeper cursor-ledger, dense long[]) encoding for up to N ranges. */
    private static byte[] longListMapBytes(RangeSetWrapper<Position> set, int maxRanges) {
        PositionInfo pi = new PositionInfo().setLedgerId(BASE_LEDGER).setEntryId(0);
        pi.addAllIndividualDeletedMessageRanges(buildLongListMap(set.toRanges(maxRanges)));
        return pi.toByteArray();
    }

    /**
     * Adds {@code targetRanges} acknowledged ranges following a hole pattern: each acked run has the supplied
     * length, followed by a single unacked message (a "hole") that separates ranges. Runs that cross a ledger
     * boundary are split per ledger, exactly as the cursor stores them. Returns the backlog span (number of
     * messages from the first to the last acknowledged message, inclusive).
     */
    private static long populateWithHoles(RangeSetWrapper<Position> set, int targetRanges, int minRun, int maxRun,
                                          Random rnd) {
        long m = 0;
        long lastAckedMsg = 0;
        int rangesAdded = 0;
        while (rangesAdded < targetRanges) {
            int runLen = (minRun == maxRun) ? minRun : minRun + rnd.nextInt(maxRun - minRun + 1);
            long runEnd = m + runLen - 1;
            long s = m;
            while (s <= runEnd && rangesAdded < targetRanges) {
                long ledger = BASE_LEDGER + s / ENTRIES_PER_LEDGER;
                long ledgerLastMsg = (s / ENTRIES_PER_LEDGER + 1L) * ENTRIES_PER_LEDGER - 1;
                long segEnd = Math.min(runEnd, ledgerLastMsg);
                long entryLo = s % ENTRIES_PER_LEDGER;
                long entryHi = segEnd % ENTRIES_PER_LEDGER;
                set.addOpenClosed(ledger, entryLo - 1, ledger, entryHi);
                rangesAdded++;
                lastAckedMsg = segEnd;
                s = segEnd + 1;
            }
            m = runEnd + 2; // skip one unacked message (the hole)
        }
        return lastAckedMsg + 1;
    }

    /**
     * Acknowledge every message in [0, backlog) except one unacked "hole" roughly every {@code avgHoleEveryK}
     * messages, so ack holes are spread across the whole backlog. The gap between holes is drawn from a normal
     * distribution centered on {@code avgHoleEveryK} with ~50% variability (clamped to [0.5x, 1.5x], e.g.
     * 500..1500 for 1000), so the holes are not perfectly periodic while the average hole count is preserved.
     * Acked runs are split at ledger boundaries exactly as the cursor stores them. Returns the number of holes.
     */
    private static long populateToBacklog(RangeSetWrapper<Position> set, long backlog, int avgHoleEveryK,
                                          Random rnd) {
        long m = 0;
        long holes = 0;
        while (m < backlog) {
            int gap = gaussianGap(avgHoleEveryK, rnd);
            long runEnd = Math.min(m + gap - 2, backlog - 1); // (gap - 1) acked messages then 1 hole
            long s = m;
            while (s <= runEnd) {
                long ledger = BASE_LEDGER + s / ENTRIES_PER_LEDGER;
                long ledgerLastMsg = (s / ENTRIES_PER_LEDGER + 1L) * ENTRIES_PER_LEDGER - 1;
                long segEnd = Math.min(runEnd, ledgerLastMsg);
                set.addOpenClosed(ledger, (s % ENTRIES_PER_LEDGER) - 1, ledger, segEnd % ENTRIES_PER_LEDGER);
                s = segEnd + 1;
            }
            m = runEnd + 2; // the message at runEnd + 1 is the unacked hole
            holes++;
        }
        return holes;
    }

    /** Gap between holes ~ N(avgGap, 0.25*avgGap), clamped to [0.5x, 1.5x] (~50% variance). */
    private static int gaussianGap(int avgGap, Random rnd) {
        int lo = Math.max(1, avgGap / 2);
        int hi = avgGap + avgGap / 2;
        int gap = (int) Math.round(avgGap + rnd.nextGaussian() * 0.25 * avgGap);
        return Math.max(lo, Math.min(hi, gap));
    }

    /**
     * Build the batchDeletedIndexes map for a {@code backlog}-message subscription split into batched entries of
     * {@code batchSize} messages, with unacked "holes" spread across the backlog at ~1/{@code avgHoleEveryK}
     * frequency (Gaussian gap). Only partially-acknowledged batches (some acked AND some unacked) are tracked,
     * holding the per-batch unacked-index BitSet, exactly as the cursor does.
     */
    private static NavigableMap<Position, BitSet> buildBatchIndexesForBacklog(long backlog, int batchSize,
            int avgHoleEveryK, Random rnd) {
        NavigableMap<Position, BitSet> map = new TreeMap<>();
        long nextHole = gaussianGap(avgHoleEveryK, rnd);
        long entryNum = 0;
        for (long base = 0; base < backlog; base += batchSize, entryNum++) {
            int b = (int) Math.min(batchSize, backlog - base);
            BitSet unacked = new BitSet(b);
            while (nextHole < base + b) {
                unacked.set((int) (nextHole - base));
                nextHole += gaussianGap(avgHoleEveryK, rnd);
            }
            int holes = unacked.cardinality();
            if (holes > 0 && holes < b) { // a partially-acknowledged batch is tracked
                long ledger = BASE_LEDGER + entryNum / ENTRIES_PER_LEDGER;
                map.put(PositionFactory.create(ledger, entryNum % ENTRIES_PER_LEDGER), unacked);
            }
        }
        return map;
    }

    @Test
    public void reportUnackedRangeStateSize() {
        Random rnd = new Random(SEED);

        // Scenario A: realistic - an ack hole every 100..200 messages (mostly contiguous acks).
        RangeSetWrapper<Position> realistic = newRangeSet();
        long realisticBacklog = populateWithHoles(realistic, GENERATED_RANGES, 100, 200, rnd);

        // Scenario B: worst case - every 2nd message unacked (each acked message is its own range).
        RangeSetWrapper<Position> worst = newRangeSet();
        long worstBacklog = populateWithHoles(worst, GENERATED_RANGES, 1, 1, rnd);

        log.info("=== Individually deleted ranges: serialized size by persistence cap ===");
        log.info("ZK budget(target/max)={}MB/{}MB  BK entry cap={}KB",
                ZK_TARGET / MB, ZK_ZNODE_MAX / MB, BK_ENTRY_MAX / 1024);
        log.info("backlog coverage: realistic(holes 100-200)={} msgs over {} ranges (~{} msgs/range); "
                        + "worstCase(every 2nd)={} msgs over {} ranges (~{} msgs/range)",
                realisticBacklog, GENERATED_RANGES, realisticBacklog / GENERATED_RANGES,
                worstBacklog, GENERATED_RANGES, worstBacklog / GENERATED_RANGES);

        for (int cap : CAPS) {
            reportRangeRow("realistic", realistic, cap, realisticBacklog);
            reportRangeRow("worstCase", worst, cap, worstBacklog);
        }

        // Budget assertions at the community-recommended 200000 cap.
        int cap = 200_000;
        long worstZk = messageRangeBytes(worst, cap).length;
        long worstBk = longListMapBytes(worst, cap).length;
        long realisticZk = messageRangeBytes(realistic, cap).length;
        long realisticBk = longListMapBytes(realistic, cap).length;

        // The compact BookKeeper (RoaringBitmap) encoding must fit a single ~5MB cursor-ledger entry.
        assertTrue(worstBk <= BK_ENTRY_MAX,
                "worst-case BK encoding " + worstBk + " exceeds BK entry cap " + BK_ENTRY_MAX);
        assertTrue(realisticBk <= BK_ENTRY_MAX,
                "realistic BK encoding " + realisticBk + " exceeds BK entry cap " + BK_ENTRY_MAX);
        // For the metadata store (ZooKeeper, MessageRange encoding), report the largest cap whose uncompressed
        // size stays within the 5MB target; this is the upper bound for the metadata-store routing threshold.
        log.info("ZK uncompressed @200k: realistic={} bytes, worstCase={} bytes (5MB target={} bytes)",
                realisticZk, worstZk, ZK_TARGET);
        log.info("max ranges fitting {}MB ZK (uncompressed): realistic~={}, worstCase~={}",
                ZK_TARGET / MB,
                maxRangesWithinBudget(realistic, ZK_TARGET, true),
                maxRangesWithinBudget(worst, ZK_TARGET, true));
    }

    private void reportRangeRow(String scenario, RangeSetWrapper<Position> set, int cap, long fullBacklog) {
        byte[] zk = messageRangeBytes(set, cap);
        byte[] bk = longListMapBytes(set, cap);
        int bkLz4 = lz4Bytes(bk);
        long proposed = roaringSerializedBytes(set, cap);
        long transientHeap = denseLongArrayHeapBytes(set, cap);
        long backlogAtCap = fullBacklog * cap / GENERATED_RANGES;
        log.info(String.format(
                "%-10s cap=%-8d backlog~=%-11d | ZK(MessageRange) raw=%6.2fMB | "
                        + "BK current dense long[] raw=%6.2fMB lz4=%6.2fMB transientHeap=%6.2fMB | "
                        + "BK proposed Roaring.serialize=%8.1fKB",
                scenario, cap, backlogAtCap,
                zk.length / (double) MB,
                bk.length / (double) MB, bkLz4 / (double) MB, transientHeap / (double) MB,
                proposed / 1024.0));
    }

    private static int maxRangesWithinBudget(RangeSetWrapper<Position> set, long budgetBytes, boolean messageRange) {
        // Binary search the largest cap whose serialized size stays within the budget.
        int lo = 0;
        int hi = GENERATED_RANGES;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            long size = messageRange ? messageRangeBytes(set, mid).length : longListMapBytes(set, mid).length;
            if (size <= budgetBytes) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    @Test
    public void reportBacklogCeilingForBkEntry() {
        // How large can the backlog grow before the persisted cursor ack-state no longer fits the default
        // BookKeeper single-entry limit (nettyMaxFrameSizeBytes ~= 5 MB)? The serialized size scales linearly
        // with the backlog (see reportRepresentativeBacklogScenarios), so we measure at a reference backlog and
        // project to the limit. With the current dense encoding the BK ledger has NO compression, so the ceiling
        // is "dense raw" -- only ~30 M messages, regardless of how few ack holes there are.
        long ref = 10_000_000L;
        int[] frequencies = {1000, 100, 20, 10};
        String[] labels = {"0.1% unacked", "1% unacked", "5% unacked", "10% unacked"};
        // The BK cursor ledger is not compressed today, so only the current dense encoding applies to it.
        log.info("=== Max backlog (messages) whose ack-state fits the {} KB BK single-entry limit with the "
                + "current dense encoding (projected from a {} M reference backlog) ===",
                BK_ENTRY_MAX / 1024, ref / 1_000_000);
        log.info(String.format("%-14s | %s", "ack density", "max backlog (dense, current BK)"));
        for (int i = 0; i < frequencies.length; i++) {
            RangeSetWrapper<Position> set = newRangeSet();
            populateToBacklog(set, ref, frequencies[i], new Random(SEED));
            byte[] dense = longListMapBytes(set, Integer.MAX_VALUE);
            log.info(String.format("%-14s | %,18d", labels[i], ceilingBacklog(ref, dense.length)));
        }
    }

    private static long ceilingBacklog(long refBacklog, long sizeAtRefBytes) {
        return refBacklog * BK_ENTRY_MAX / Math.max(1, sizeAtRefBytes);
    }

    @Test
    public void reportRepresentativeBacklogScenarios() {
        // Ack holes (unacked messages) spread uniformly across the backlog at a fixed frequency. For each
        // representative backlog size, low/medium/high hole density is shown. The current dense long[] encoding
        // and its transient serialization heap track the BACKLOG (one 64-bit word per 64 entry-ids per ledger,
        // regardless of hole count); the proposed RoaringBitmap.serialize tracks the NUMBER OF ACK HOLES,
        // (regardless of how wide the backlog is).
        long[] backlogs =
                {10_000L, 50_000L, 100_000L, 500_000L, 1_000_000L, 5_000_000L, 10_000_000L, 100_000_000L};
        int[] frequencies = {1000, 100, 20, 10}; // one ack hole every K messages
        String[] labels = {"low (~0.1% unacked)", "medium (~1% unacked)", "moderate (~5% unacked)",
                "high (~10% unacked)"};
        for (int i = 0; i < frequencies.length; i++) {
            log.info("=== Ack-hole density {}: one ack hole every {} messages ===", labels[i], frequencies[i]);
            log.info(String.format("%-11s %-9s %-9s | %-11s %-11s %-14s | %-13s %-13s",
                    "backlog", "holes", "ranges", "dense raw", "dense LZ4", "transientHeap",
                    "Roaring", "Roaring+LZ4"));
            for (long backlog : backlogs) {
                RangeSetWrapper<Position> set = newRangeSet();
                long holes = populateToBacklog(set, backlog, frequencies[i], new Random(SEED));
                byte[] dense = longListMapBytes(set, Integer.MAX_VALUE);
                int denseLz4 = lz4Bytes(dense);
                long heap = denseLongArrayHeapBytes(set, Integer.MAX_VALUE);
                byte[] roaring = roaringSerializedToBytes(set, Integer.MAX_VALUE);
                int roaringLz4 = lz4Bytes(roaring);
                log.info(String.format("%-11d %-9d %-9d | %8.1f KB %8.1f KB %11.1f KB | %10.1f KB %10.1f KB",
                        backlog, holes, set.size(),
                        dense.length / 1024.0, denseLz4 / 1024.0, heap / 1024.0,
                        roaring.length / 1024.0, roaringLz4 / 1024.0));
            }
        }
    }

    @Test
    public void reportHighEntropyRangeBitmapSize() {
        // High-entropy worst case for the RoaringBitmap (BookKeeper) encoding: each message acknowledged with
        // ~50% probability, producing dense bitmap containers that LZ4 cannot meaningfully compress. This is the
        // case the maintainer flagged: unlike the low-entropy "holes every 100-200" or periodic "every 2nd"
        // patterns, here the compact BK encoding stays small in raw bytes but LZ4 achieves ~1x, while the
        // MessageRange (ZK) encoding is large but still LZ4-compressible.
        Random rnd = new Random(SEED);
        RangeSetWrapper<Position> set = newRangeSet();
        int ledgers = 8;
        for (int l = 0; l < ledgers; l++) {
            long ledger = BASE_LEDGER + l;
            for (int e = 0; e < ENTRIES_PER_LEDGER; e++) {
                if (rnd.nextBoolean()) {
                    set.addOpenClosed(ledger, e - 1, ledger, e);
                }
            }
        }
        int ranges = set.size();
        byte[] bk = longListMapBytes(set, Integer.MAX_VALUE);
        byte[] zk = messageRangeBytes(set, Integer.MAX_VALUE);
        int bkLz4 = lz4Bytes(bk);
        int zkLz4 = lz4Bytes(zk);
        log.info("=== High-entropy (50% random acks): RoaringBitmap worst case for LZ4 ===");
        log.info(String.format("ranges=%d over %d ledgers (%d entries each) | "
                        + "BK(RoaringBitmap) raw=%.2fMB lz4=%.2fMB ratio=%.2fx | "
                        + "ZK(MessageRange) raw=%.2fMB lz4=%.2fMB ratio=%.2fx",
                ranges, ledgers, ENTRIES_PER_LEDGER,
                bk.length / (double) MB, bkLz4 / (double) MB, bk.length / (double) Math.max(1, bkLz4),
                zk.length / (double) MB, zkLz4 / (double) MB, zk.length / (double) Math.max(1, zkLz4)));
    }

    @Test
    public void reportBatchDeletedIndexBkStorage() {
        // BookKeeper storage consumed by the batchDeletedIndex state for a 1 M-message backlog, as a function of
        // ack-hole density (rows) and producer batch size (cols). Only partially-acknowledged batches are stored;
        // smaller batches yield more batch entries (1M/batchSize), so more of them end up partially acked.
        long backlog = 1_000_000L;
        int[] frequencies = {1000, 100, 20, 10};
        String[] densityLabels = {"0.1%", "1%", "5%", "10%"};
        int[] batchSizes = {10, 25, 50, 75, 100};
        // NOTE: backlog is a MESSAGE count, not an entry count. The topic stores backlog/batchSize batched
        // entries (one BK entry per batch). Cell = BK size (and the number of partially-acked batches tracked).
        log.info("=== batchDeletedIndex BK storage (uncompressed) for a {} M-MESSAGE backlog "
                + "(rows = ack-hole density, cols = batch size) ===", backlog / 1_000_000);
        StringBuilder header = new StringBuilder(String.format("%-9s |", "density"));
        StringBuilder totals = new StringBuilder(String.format("%-9s |", "entries"));
        for (int bs : batchSizes) {
            header.append(String.format(" %18s", "B=" + bs));
            totals.append(String.format(" %18s", String.format("%,d", (backlog + bs - 1) / bs)));
        }
        log.info(header.toString());
        log.info(totals.toString()); // total batched entries in the backlog = backlog / batchSize
        for (int i = 0; i < frequencies.length; i++) {
            StringBuilder row = new StringBuilder(String.format("%-9s |", densityLabels[i]));
            for (int bs : batchSizes) {
                NavigableMap<Position, BitSet> idx =
                        buildBatchIndexesForBacklog(backlog, bs, frequencies[i], new Random(SEED));
                byte[] bytes = batchIndexBytes(idx, Integer.MAX_VALUE);
                row.append(String.format(" %8.1f KB(%,7d)", bytes.length / 1024.0, idx.size()));
            }
            log.info(row.toString());
        }
    }

    @Test
    public void reportBatchDeletedIndexStateSize() {
        Random rnd = new Random(SEED);
        log.info("=== Batch deletion indexes: serialized size by persistence cap (batch sizes 20..100) ===");
        for (int cap : new int[]{10_000, 100_000, 200_000}) {
            reportBatchRow("randomAcks", buildBatchIndexes(cap, rnd, false), cap);
            reportBatchRow("worstCase", buildBatchIndexes(cap, rnd, true), cap);
        }

        // 200000 batch indexes must fit a single ~5MB entry (they share the entry with ranges, so this is the
        // batch-only worst case).
        NavigableMap<Position, BitSet> worst = buildBatchIndexes(200_000, new Random(SEED), true);
        long worstBytes = batchIndexBytes(worst, 200_000).length;
        assertTrue(worstBytes <= BK_ENTRY_MAX,
                "worst-case batch-index encoding " + worstBytes + " exceeds entry cap " + BK_ENTRY_MAX);
        double bytesPerIndex = worstBytes / 200_000.0;
        log.info("batch-index worst-case @200k: {} bytes total, ~{} bytes/index", worstBytes,
                String.format("%.1f", bytesPerIndex));
        // Batch deleteSet for a <=100-message batch is a single long; the entry is position-dominated (~<30 bytes).
        assertTrue(bytesPerIndex < 30, "batch-index entry larger than expected: " + bytesPerIndex + " bytes");
    }

    private void reportBatchRow(String scenario, NavigableMap<Position, BitSet> indexes, int cap) {
        byte[] bytes = batchIndexBytes(indexes, cap);
        int lz4 = lz4Bytes(bytes);
        log.info(String.format("%-10s cap=%-9d | BatchedEntryDeletionIndexInfo raw=%6.2fMB lz4=%6.2fMB ratio=%.1fx"
                        + " (~%.1f bytes/index)",
                scenario, cap, bytes.length / (double) MB, lz4 / (double) MB,
                bytes.length / (double) Math.max(1, lz4), bytes.length / (double) cap));
    }

    private static byte[] batchIndexBytes(NavigableMap<Position, BitSet> indexes, int maxIndexes) {
        PositionInfo pi = new PositionInfo().setLedgerId(BASE_LEDGER).setEntryId(0);
        pi.addAllBatchedEntryDeletionIndexInfos(buildBatchedEntryDeletionIndexInfo(indexes, maxIndexes));
        return pi.toByteArray();
    }

    /** Builds {@code count} partially-acknowledged batches with sizes uniformly in [20, 100]. */
    private static NavigableMap<Position, BitSet> buildBatchIndexes(int count, Random rnd, boolean worstCase) {
        NavigableMap<Position, BitSet> map = new TreeMap<>();
        long m = 0;
        for (int i = 0; i < count; i++) {
            long ledger = BASE_LEDGER + m / ENTRIES_PER_LEDGER;
            long entry = m % ENTRIES_PER_LEDGER;
            int batchSize = 20 + rnd.nextInt(81); // 20..100
            BitSet bits = new BitSet(batchSize);
            if (worstCase) {
                // Every 2nd index acked -> highest set bit near batchSize, maximizing the single deleteSet long.
                for (int b = 0; b < batchSize; b += 2) {
                    bits.set(b);
                }
            } else {
                for (int b = 0; b < batchSize; b++) {
                    if (rnd.nextBoolean()) {
                        bits.set(b);
                    }
                }
            }
            if (bits.isEmpty()) {
                bits.set(0); // a tracked batch always has at least one acked index
            }
            map.put(PositionFactory.create(ledger, entry), bits);
            m += 1 + rnd.nextInt(50); // spread positions across the backlog
        }
        return map;
    }
}
