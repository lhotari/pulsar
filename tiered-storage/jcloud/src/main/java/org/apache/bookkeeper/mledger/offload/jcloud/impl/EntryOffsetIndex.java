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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * The offsets of the entries of offloaded data objects that reads have passed, shared by every read handle of the
 * offloader factory. The index of a data object only has the offset of the first entry of each data block; the
 * offset of any other entry is found by walking the entry headers from a known offset. For each data object it keeps:
 * <ul>
 *   <li>runs of consecutive entries, each run its first entry's offset and its entries' offsets relative to it, 4
 *   bytes per entry, at most an eighth of the size budget per object: when an object's runs exceed it, the runs
 *   farthest from the latest read are dropped, and of a longer run than that, its end;</li>
 *   <li>anchors: an entry's offset about every {@link #ANCHOR_INTERVAL_BYTES} of the walked data, which are kept when
 *   the runs around them are dropped, so that a read walks at most about that much from one; when they exceed the
 *   object's share too, every other one is dropped.</li>
 * </ul>
 * The objects are kept within the size budget and dropped when no read has used them for a while. A lookup finds the
 * known offset nearest to an entry: the entry's own, an anchor's, or where the entry after the run before it starts.
 */
public class EntryOffsetIndex {
    /** About how much data apart the anchors are. */
    static final long ANCHOR_INTERVAL_BYTES = 1024 * 1024;
    // A run's own fields, beyond the 4 bytes of each entry's relative offset
    private static final int RUN_OVERHEAD_BYTES = 64;
    // An anchor in a TreeMap with its boxed key and value
    private static final int ANCHOR_BYTES = 80;
    private static final ObjectOffsets NONE = new ObjectOffsets(null, null, 0);

    private final Cache<String, ObjectOffsets> objects;
    private final long maxObjectRunsBytes;

    public EntryOffsetIndex(long maxSizeBytes, int expirySeconds) {
        long maxSize = Math.max(0, maxSizeBytes);
        // At most 1 GiB, which keeps an object's weight within an int
        this.maxObjectRunsBytes = Math.min(maxSize / 8, 1L << 30);
        this.objects = maxSize == 0 ? null : Caffeine.newBuilder()
                .maximumWeight(maxSize)
                .<String, ObjectOffsets>weigher((key, offsets) -> offsets.weight())
                .expireAfterAccess(Math.max(1, expirySeconds), TimeUnit.SECONDS)
                // The index's maintenance runs on the threads that use it
                .executor(Runnable::run)
                .build();
    }

    /**
     * The known offsets of the data object, which a read handle updates as it reads; without a budget, none, and
     * nothing is recorded.
     */
    ObjectOffsets offsets(String bucket, String key) {
        if (objects == null) {
            return NONE;
        }
        String objectKey = bucket + "/" + key;
        return objects.get(objectKey, k -> new ObjectOffsets(this, objectKey, maxObjectRunsBytes));
    }

    // Weighs the object's offsets again after they have changed, unless they have been dropped
    private void reweigh(ObjectOffsets offsets) {
        objects.asMap().replace(offsets.objectKey, offsets, offsets);
    }

    public void clear() {
        if (objects != null) {
            objects.invalidateAll();
        }
    }

    @VisibleForTesting
    long weight() {
        return objects == null ? 0 : objects.asMap().values().stream().mapToLong(ObjectOffsets::weight).sum();
    }

    /** An entry's offset in the data object. */
    record Position(long entryId, long offset) {
    }

    /** The known entry offsets of a data object. */
    static final class ObjectOffsets {
        private final EntryOffsetIndex index;
        private final String objectKey;
        private final long maxRunsBytes;
        // By first entry id; guarded by this
        private final TreeMap<Long, Run> runs = new TreeMap<>();
        // Entry id to offset; guarded by this
        private final TreeMap<Long, Long> anchors = new TreeMap<>();
        // Guarded by this
        private long runsWeight;

        // The most entries that the runs' arrays can hold within the share, with the end offset of a run
        private final int maxRunEntries;

        private ObjectOffsets(EntryOffsetIndex index, String objectKey, long maxRunsBytes) {
            this.index = index;
            this.objectKey = objectKey;
            this.maxRunsBytes = maxRunsBytes;
            this.maxRunEntries = (int) Math.max(0, (maxRunsBytes - RUN_OVERHEAD_BYTES) / 4 - 1);
        }

        private synchronized int weight() {
            return (int) Math.min(Integer.MAX_VALUE, runsWeight + (long) anchors.size() * ANCHOR_BYTES);
        }

        /**
         * The known offset nearest to the entry, at or before it: the entry's own, an anchor's, or where the entry
         * after the run that ends before it starts, which may also be the padding at the end of a data block; null
         * when none.
         */
        synchronized Position floor(long entryId) {
            Position nearest = null;
            Map.Entry<Long, Run> floor = runs.floorEntry(entryId);
            if (floor != null) {
                Run run = floor.getValue();
                long last = run.lastEntryId();
                nearest = entryId <= last ? new Position(entryId, run.offsetOf(entryId))
                        : new Position(last + 1, run.endOffset());
            }
            Map.Entry<Long, Long> anchor = anchors.floorEntry(entryId);
            if (anchor != null && (nearest == null || anchor.getKey() > nearest.entryId())) {
                nearest = new Position(anchor.getKey(), anchor.getValue());
            }
            return nearest;
        }

        /**
         * Adds the offsets of consecutive entries from {@code firstEntryId}, each at {@code offsets[i]}, the last one
         * ending at {@code endOffset}, joining the runs that they overlap or continue.
         */
        void add(long firstEntryId, long[] offsets, int count, long endOffset) {
            if (count == 0 || index == null) {
                return;
            }
            boolean changed;
            synchronized (this) {
                changed = addAnchors(firstEntryId, offsets, count);
                changed |= addRun(firstEntryId, offsets, count, endOffset);
                if (changed) {
                    trimRuns(firstEntryId, firstEntryId + count - 1);
                }
            }
            if (changed) {
                index.reweigh(this);
            }
        }

        // Guarded by this
        private boolean addAnchors(long firstEntryId, long[] offsets, int count) {
            boolean added = false;
            Map.Entry<Long, Long> floor = anchors.floorEntry(firstEntryId);
            long previous = floor != null ? floor.getValue() : Long.MIN_VALUE / 2;
            Map.Entry<Long, Long> next = anchors.higherEntry(firstEntryId);
            for (int i = 0; i < count; i++) {
                long entryId = firstEntryId + i;
                while (next != null && next.getKey() <= entryId) {
                    previous = next.getValue();
                    next = anchors.higherEntry(next.getKey());
                }
                if (offsets[i] - previous >= ANCHOR_INTERVAL_BYTES
                        && (next == null || next.getValue() - offsets[i] >= ANCHOR_INTERVAL_BYTES)) {
                    anchors.put(entryId, offsets[i]);
                    previous = offsets[i];
                    added = true;
                }
            }
            if ((long) anchors.size() * ANCHOR_BYTES > maxRunsBytes) {
                // Twice as far apart, within the share, which also keeps the object's weight within an int
                Iterator<Long> entryIds = anchors.keySet().iterator();
                boolean drop = false;
                while (entryIds.hasNext()) {
                    entryIds.next();
                    if (drop) {
                        entryIds.remove();
                    }
                    drop = !drop;
                }
            }
            return added;
        }

        // Guarded by this
        private boolean addRun(long firstEntryId, long[] offsets, int count, long endOffset) {
            long lastEntryId = firstEntryId + count - 1;
            Map.Entry<Long, Run> floor = runs.floorEntry(firstEntryId);
            Run before = floor != null ? floor.getValue() : null;
            if (before != null && before.covers(firstEntryId, lastEntryId, offsets[0], endOffset)) {
                // Known already, such as a read of entries whose offsets were known
                return false;
            }
            Map.Entry<Long, Run> after = runs.higherEntry(firstEntryId);
            boolean overlapsAfter = after != null && after.getKey() <= lastEntryId + 1;
            if (before != null && !overlapsAfter && before.lastEntryId() + 1 == firstEntryId
                    && before.endOffset() == offsets[0] && before.canAppend(endOffset)
                    && before.count + count <= maxRunEntries) {
                // Continues the run, as sequential reads do
                runsWeight -= before.weight();
                before.append(offsets, count, endOffset, maxRunEntries + 1);
                runsWeight += before.weight();
                return true;
            }
            Run added = Run.of(firstEntryId, offsets, count, endOffset);
            // The runs that the new one overlaps, or continues byte for byte, become one
            List<Run> joined = new ArrayList<>();
            joined.add(added);
            if (before != null && before.joins(added)) {
                joined.add(before);
            }
            for (Run run : runs.subMap(firstEntryId, false, lastEntryId + 1, true).values()) {
                if (run.joins(added)) {
                    joined.add(run);
                }
            }
            for (Run run : joined) {
                if (run != added) {
                    runs.remove(run.firstEntryId);
                    runsWeight -= run.weight();
                }
            }
            Run merged = joined.size() == 1 ? added : Run.merge(joined);
            runs.put(merged.firstEntryId, merged);
            runsWeight += merged.weight();
            return true;
        }

        /**
         * Keeps the runs within the object's share of the budget: drops the runs farthest from the latest read, and
         * of a run longer than the share, keeps its end, where the read was.
         */
        // Guarded by this
        private void trimRuns(long readFirstEntryId, long readLastEntryId) {
            while (runsWeight > maxRunsBytes && runs.size() > 1) {
                Run farthest = null;
                long farthestDistance = -1;
                for (Run run : runs.values()) {
                    long distance = run.lastEntryId() < readFirstEntryId ? readFirstEntryId - run.lastEntryId()
                            : run.firstEntryId > readLastEntryId ? run.firstEntryId - readLastEntryId : 0;
                    if (distance > farthestDistance) {
                        farthest = run;
                        farthestDistance = distance;
                    }
                }
                runs.remove(farthest.firstEntryId);
                runsWeight -= farthest.weight();
            }
            if (runsWeight > maxRunsBytes && runs.size() == 1) {
                Run run = runs.firstEntry().getValue();
                runs.clear();
                runsWeight = 0;
                if (maxRunEntries > 0) {
                    // Half of the share, in an array of the whole share, so that the run can grow by the other half
                    // before it is trimmed again
                    Run end = run.end(Math.max(1, maxRunEntries / 2), maxRunEntries + 1);
                    runs.put(end.firstEntryId, end);
                    runsWeight = end.weight();
                }
            }
        }

        @VisibleForTesting
        synchronized List<long[]> runs() {
            List<long[]> result = new ArrayList<>();
            runs.values().forEach(run -> result.add(new long[] {run.firstEntryId, run.lastEntryId()}));
            return result;
        }

        @VisibleForTesting
        synchronized List<Long> anchorEntryIds() {
            return new ArrayList<>(anchors.keySet());
        }
    }

    /**
     * Consecutive entries from {@code firstEntryId}, the first at {@code firstOffset}, entry i at {@code firstOffset +
     * relativeOffsets[i]}, and where the last one ends at {@code firstOffset + relativeOffsets[count]}; the array may
     * have room for more. Guarded by its object's offsets.
     */
    private static final class Run {
        private final long firstEntryId;
        private final long firstOffset;
        private int[] relativeOffsets;
        private int count;

        private Run(long firstEntryId, long firstOffset, int[] relativeOffsets, int count) {
            this.firstEntryId = firstEntryId;
            this.firstOffset = firstOffset;
            this.relativeOffsets = relativeOffsets;
            this.count = count;
        }

        static Run of(long firstEntryId, long[] offsets, int count, long endOffset) {
            int[] relative = new int[count + 1];
            for (int i = 0; i < count; i++) {
                relative[i] = Math.toIntExact(offsets[i] - offsets[0]);
            }
            relative[count] = Math.toIntExact(endOffset - offsets[0]);
            return new Run(firstEntryId, offsets[0], relative, count);
        }

        long lastEntryId() {
            return firstEntryId + count - 1;
        }

        long offsetOf(long entryId) {
            return firstOffset + relativeOffsets[(int) (entryId - firstEntryId)];
        }

        long endOffset() {
            return firstOffset + relativeOffsets[count];
        }

        int weight() {
            return RUN_OVERHEAD_BYTES + 4 * relativeOffsets.length;
        }

        /** Whether the run has the entries, at the same offsets. */
        boolean covers(long first, long last, long firstOffset, long endOffset) {
            if (first < firstEntryId || last > lastEntryId() || offsetOf(first) != firstOffset) {
                return false;
            }
            return last == lastEntryId() ? endOffset() == endOffset
                    : offsetOf(last + 1) == endOffset;
        }

        boolean canAppend(long endOffset) {
            return endOffset - firstOffset <= Integer.MAX_VALUE;
        }

        /** Appends entries that continue the run byte for byte, in an array of at most {@code maxLength}. */
        void append(long[] offsets, int added, long endOffset, int maxLength) {
            int needed = count + added + 1;
            if (needed > relativeOffsets.length) {
                relativeOffsets = Arrays.copyOf(relativeOffsets,
                        Math.max(needed, Math.min(relativeOffsets.length * 2, maxLength)));
            }
            for (int i = 0; i < added; i++) {
                relativeOffsets[count + i] = (int) (offsets[i] - firstOffset);
            }
            count += added;
            relativeOffsets[count] = (int) (endOffset - firstOffset);
        }

        /** A run of the last {@code keep} entries of this one, in an array of {@code length} or as many as needed. */
        Run end(int keep, int length) {
            keep = Math.min(keep, count);
            int from = count - keep;
            long base = firstOffset + relativeOffsets[from];
            int[] relative = new int[Math.max(keep + 1, length)];
            for (int i = 0; i <= keep; i++) {
                relative[i] = relativeOffsets[from + i] - relativeOffsets[from];
            }
            return new Run(firstEntryId + from, base, relative, keep);
        }

        /**
         * Whether the runs share entries, or one continues the other byte for byte: the entries after the last of one
         * aren't across the padding at the end of a data block from it.
         */
        boolean joins(Run other) {
            long first = Math.min(firstOffset, other.firstOffset);
            long end = Math.max(endOffset(), other.endOffset());
            if (end - first > Integer.MAX_VALUE) {
                // A run's relative offsets are ints
                return false;
            }
            if (other.firstEntryId <= lastEntryId() + 1 && firstEntryId <= other.lastEntryId() + 1) {
                if (other.firstEntryId == lastEntryId() + 1) {
                    return other.firstOffset == endOffset();
                }
                if (firstEntryId == other.lastEntryId() + 1) {
                    return firstOffset == other.endOffset();
                }
                return true;
            }
            return false;
        }

        /** One run of the entries of runs that overlap or continue each other. */
        static Run merge(List<Run> runs) {
            long first = Long.MAX_VALUE;
            long last = Long.MIN_VALUE;
            Run lastRun = null;
            for (Run run : runs) {
                first = Math.min(first, run.firstEntryId);
                if (run.lastEntryId() > last) {
                    last = run.lastEntryId();
                    lastRun = run;
                }
            }
            int count = Math.toIntExact(last - first + 1);
            long[] offsets = new long[count];
            for (Run run : runs) {
                for (long entryId = run.firstEntryId; entryId <= run.lastEntryId(); entryId++) {
                    offsets[(int) (entryId - first)] = run.offsetOf(entryId);
                }
            }
            return of(first, offsets, count, lastRun.endOffset());
        }
    }
}
