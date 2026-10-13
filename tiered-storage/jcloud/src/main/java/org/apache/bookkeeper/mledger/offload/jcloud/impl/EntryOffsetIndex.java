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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * The offsets of the entries of offloaded data objects that reads have passed, shared by every read handle of the
 * offloader factory. The index of a data object only has the offset of the first entry of each data block; the
 * offset of any other entry is found by walking the entry headers from a known offset. Each object's known offsets are
 * runs of consecutive entries, each run its first entry's offset and its entries' offsets relative to it, 4 bytes per
 * entry, kept within a size budget and dropped when no read has used them for a while. A lookup finds the known offset
 * nearest to an entry: the entry's own, or where the entry after the run before it starts.
 */
public class EntryOffsetIndex {
    // A run's own fields, beyond the 4 bytes of each entry's relative offset
    private static final int RUN_OVERHEAD_BYTES = 64;

    private final Cache<String, ObjectOffsets> objects;

    public EntryOffsetIndex(long maxSizeBytes, int expirySeconds) {
        this.objects = CacheBuilder.newBuilder()
                // One segment, so that the budget and the least recently used order are the index's
                .concurrencyLevel(1)
                .maximumWeight(Math.max(0, maxSizeBytes))
                .<String, ObjectOffsets>weigher((key, offsets) -> offsets.weight())
                .expireAfterAccess(Math.max(1, expirySeconds), TimeUnit.SECONDS)
                .build();
    }

    /** The known offsets of the data object, which a read handle updates as it reads. */
    ObjectOffsets offsets(String bucket, String key) {
        String objectKey = bucket + "/" + key;
        try {
            return objects.get(objectKey, () -> new ObjectOffsets(this, objectKey));
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    // Weighs the object's offsets again after they have changed, unless they have been dropped
    private void reweigh(ObjectOffsets offsets) {
        objects.asMap().replace(offsets.objectKey, offsets, offsets);
    }

    public void clear() {
        objects.invalidateAll();
    }

    @VisibleForTesting
    long weight() {
        return objects.asMap().values().stream().mapToLong(ObjectOffsets::weight).sum();
    }

    /** An entry's offset in the data object. */
    record Position(long entryId, long offset) {
    }

    /** The known entry offsets of a data object. */
    static final class ObjectOffsets {
        private final EntryOffsetIndex index;
        private final String objectKey;
        // By first entry id; guarded by this
        private final TreeMap<Long, Run> runs = new TreeMap<>();
        // Guarded by this
        private int weight;

        private ObjectOffsets(EntryOffsetIndex index, String objectKey) {
            this.index = index;
            this.objectKey = objectKey;
        }

        private synchronized int weight() {
            return weight;
        }

        /**
         * The known offset nearest to the entry, at or before it: the entry's own, or where the entry after the run
         * that ends before it starts, which may also be the padding at the end of a data block; null when none.
         */
        synchronized Position floor(long entryId) {
            Map.Entry<Long, Run> floor = runs.floorEntry(entryId);
            if (floor == null) {
                return null;
            }
            Run run = floor.getValue();
            long last = run.lastEntryId();
            return entryId <= last ? new Position(entryId, run.offsetOf(entryId))
                    : new Position(last + 1, run.endOffset());
        }

        /**
         * Adds the offsets of consecutive entries from {@code firstEntryId}, each at {@code offsets[i]}, the last one
         * ending at {@code endOffset}, joining the runs that they overlap or continue.
         */
        void add(long firstEntryId, long[] offsets, int count, long endOffset) {
            if (count == 0) {
                return;
            }
            synchronized (this) {
                Run added = Run.of(firstEntryId, offsets, count, endOffset);
                // The runs that the new one overlaps, or continues byte for byte, become one
                List<Run> joined = new ArrayList<>();
                joined.add(added);
                Map.Entry<Long, Run> before = runs.floorEntry(firstEntryId);
                if (before != null && before.getValue().joins(added)) {
                    joined.add(before.getValue());
                }
                for (Run after : runs.subMap(firstEntryId, false, added.lastEntryId() + 1, true).values()) {
                    if (after.joins(added)) {
                        joined.add(after);
                    }
                }
                for (Run run : joined) {
                    if (run != added) {
                        runs.remove(run.firstEntryId);
                        weight -= run.weight();
                    }
                }
                Run merged = joined.size() == 1 ? added : Run.merge(joined);
                runs.put(merged.firstEntryId, merged);
                weight += merged.weight();
            }
            index.reweigh(this);
        }

        @VisibleForTesting
        synchronized List<long[]> runs() {
            List<long[]> result = new ArrayList<>();
            runs.values().forEach(run -> result.add(new long[] {run.firstEntryId, run.lastEntryId()}));
            return result;
        }
    }

    /**
     * Consecutive entries from {@code firstEntryId}, the first at {@code firstOffset}, entry i at {@code firstOffset +
     * relativeOffsets[i]}, and where the last one ends at {@code firstOffset + relativeOffsets[count]}.
     */
    private record Run(long firstEntryId, long firstOffset, int[] relativeOffsets) {
        static Run of(long firstEntryId, long[] offsets, int count, long endOffset) {
            int[] relative = new int[count + 1];
            for (int i = 0; i < count; i++) {
                relative[i] = Math.toIntExact(offsets[i] - offsets[0]);
            }
            relative[count] = Math.toIntExact(endOffset - offsets[0]);
            return new Run(firstEntryId, offsets[0], relative);
        }

        int count() {
            return relativeOffsets.length - 1;
        }

        long lastEntryId() {
            return firstEntryId + count() - 1;
        }

        long offsetOf(long entryId) {
            return firstOffset + relativeOffsets[(int) (entryId - firstEntryId)];
        }

        long endOffset() {
            return firstOffset + relativeOffsets[count()];
        }

        int weight() {
            return RUN_OVERHEAD_BYTES + 4 * relativeOffsets.length;
        }

        /**
         * Whether the runs share entries, or one continues the other byte for byte: the entries after the last of one
         * aren't across the padding at the end of a data block from it.
         */
        boolean joins(Run other) {
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
            Arrays.fill(offsets, -1);
            for (Run run : runs) {
                for (long entryId = run.firstEntryId; entryId <= run.lastEntryId(); entryId++) {
                    offsets[(int) (entryId - first)] = run.offsetOf(entryId);
                }
            }
            return of(first, offsets, count, lastRun.endOffset());
        }
    }
}
