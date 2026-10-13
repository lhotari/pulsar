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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * The pending acks of one ledger: a map from entry ID to a packed {@link PendingAckValues} value, in the order of the
 * entry IDs. Not thread-safe; {@link PendingAcksMap} guards it with its lock.
 *
 * <p>The entries are kept in a list of blocks, each a sorted array of up to {@value #BLOCK_CAPACITY} entries, in the
 * order of their entry IDs, with each block's entries in a window of its arrays. A consumer is sent a ledger's entries
 * mostly in increasing order and acks them mostly in the same order, so that adding an entry appends it to the last
 * block and removing one advances the first block's window; a lookup is a binary search over the blocks and over one
 * block's consecutive memory, without the pointer chasing of a tree. An entry added out of order, as a redelivery adds
 * it, shifts the entries of one block at most, which splits when it is full. Empty blocks are dropped and a block that
 * becomes sparse merges with its neighbor, so that the memory follows the number of entries.
 */
final class PendingAcksLedgerMap {
    static final int BLOCK_CAPACITY = 128;
    // A block with fewer entries than this merges with a neighbor when they fit in one block together
    private static final int MERGE_THRESHOLD = BLOCK_CAPACITY / 4;

    private static final class Block {
        final long[] entryIds = new long[BLOCK_CAPACITY];
        final long[] values = new long[BLOCK_CAPACITY];
        // the entries are in [start, end)
        int start;
        int end;

        int size() {
            return end - start;
        }

        long firstEntryId() {
            return entryIds[start];
        }

        long lastEntryId() {
            return entryIds[end - 1];
        }

        int indexOf(long entryId) {
            return Arrays.binarySearch(entryIds, start, end, entryId);
        }

        /** Inserts at the insertion point, which keeps the order; the block mustn't be full. */
        void insertAt(int insertionPoint, long entryId, long value) {
            boolean roomBefore = start > 0;
            boolean roomAfter = end < BLOCK_CAPACITY;
            if (roomBefore && (!roomAfter || insertionPoint - start < end - insertionPoint)) {
                // shift the entries before the insertion point to the left
                System.arraycopy(entryIds, start, entryIds, start - 1, insertionPoint - start);
                System.arraycopy(values, start, values, start - 1, insertionPoint - start);
                start--;
                entryIds[insertionPoint - 1] = entryId;
                values[insertionPoint - 1] = value;
            } else {
                System.arraycopy(entryIds, insertionPoint, entryIds, insertionPoint + 1, end - insertionPoint);
                System.arraycopy(values, insertionPoint, values, insertionPoint + 1, end - insertionPoint);
                end++;
                entryIds[insertionPoint] = entryId;
                values[insertionPoint] = value;
            }
        }

        void removeAt(int index) {
            if (index - start < end - 1 - index) {
                System.arraycopy(entryIds, start, entryIds, start + 1, index - start);
                System.arraycopy(values, start, values, start + 1, index - start);
                start++;
            } else {
                System.arraycopy(entryIds, index + 1, entryIds, index, end - 1 - index);
                System.arraycopy(values, index + 1, values, index, end - 1 - index);
                end--;
            }
            if (start == end) {
                start = 0;
                end = 0;
            }
        }

        /** Moves the entries to the start of the arrays. */
        void compact() {
            if (start > 0) {
                System.arraycopy(entryIds, start, entryIds, 0, size());
                System.arraycopy(values, start, values, 0, size());
                end -= start;
                start = 0;
            }
        }
    }

    /** A callback for each entry, in the order of the entry IDs. */
    interface EntryConsumer {
        void accept(long entryId, long value);
    }

    private final ArrayList<Block> blocks = new ArrayList<>(1);
    // the most blocks since the list's capacity was last trimmed, to release the capacity after the map shrinks
    private int peakBlocks;
    private int size;

    int size() {
        return size;
    }

    boolean isEmpty() {
        return size == 0;
    }

    @VisibleForTesting
    int blockCount() {
        return blocks.size();
    }

    /** The entry's value, or {@link PendingAckValues#PACKED_NOT_FOUND}. */
    long get(long entryId) {
        int blockIndex = blockIndexFor(entryId);
        if (blockIndex < 0) {
            return PendingAckValues.PACKED_NOT_FOUND;
        }
        Block block = blocks.get(blockIndex);
        int index = block.indexOf(entryId);
        return index >= 0 ? block.values[index] : PendingAckValues.PACKED_NOT_FOUND;
    }

    boolean containsKey(long entryId) {
        return !PendingAckValues.isNotFound(get(entryId));
    }

    /**
     * Sets the entry's value, which mustn't be {@link PendingAckValues#PACKED_NOT_FOUND}.
     *
     * @return the previous value, or {@link PendingAckValues#PACKED_NOT_FOUND}
     */
    long put(long entryId, long value) {
        Block last = blocks.isEmpty() ? null : blocks.get(blocks.size() - 1);
        if (last == null || entryId > last.lastEntryId()) {
            // the common case: an entry after the others
            if (last == null || last.end == BLOCK_CAPACITY) {
                if (last != null && last.size() < BLOCK_CAPACITY) {
                    last.compact();
                } else {
                    last = new Block();
                    blocks.add(last);
                    peakBlocks = Math.max(peakBlocks, blocks.size());
                }
            }
            last.entryIds[last.end] = entryId;
            last.values[last.end] = value;
            last.end++;
            size++;
            return PendingAckValues.PACKED_NOT_FOUND;
        }
        // the first block whose entries reach the entry ID
        int blockIndex = Math.max(blockIndexFor(entryId), 0);
        Block block = blocks.get(blockIndex);
        int index = block.indexOf(entryId);
        if (index >= 0) {
            long previous = block.values[index];
            block.values[index] = value;
            return previous;
        }
        if (block.size() == BLOCK_CAPACITY) {
            Block upper = split(blockIndex);
            if (entryId > block.lastEntryId()) {
                block = upper;
            }
            index = block.indexOf(entryId);
        }
        block.insertAt(-index - 1, entryId, value);
        size++;
        return PendingAckValues.PACKED_NOT_FOUND;
    }

    /**
     * Removes the entry.
     *
     * @return its value, or {@link PendingAckValues#PACKED_NOT_FOUND} if it wasn't in the map
     */
    long remove(long entryId) {
        int blockIndex = blockIndexFor(entryId);
        if (blockIndex < 0) {
            return PendingAckValues.PACKED_NOT_FOUND;
        }
        Block block = blocks.get(blockIndex);
        int index = block.indexOf(entryId);
        if (index < 0) {
            return PendingAckValues.PACKED_NOT_FOUND;
        }
        long previous = block.values[index];
        removeAt(blockIndex, block, index);
        return previous;
    }

    /** The first entry's ID; the map mustn't be empty. */
    long firstEntryId() {
        return blocks.get(0).firstEntryId();
    }

    /** The first entry's value; the map mustn't be empty. */
    long firstValue() {
        Block first = blocks.get(0);
        return first.values[first.start];
    }

    /** Removes the first entry; the map mustn't be empty. */
    void removeFirst() {
        Block first = blocks.get(0);
        removeAt(0, first, first.start);
    }

    /** Whether the map has an entry with an ID up to {@code maxEntryId}, inclusive. */
    boolean hasEntriesUpTo(long maxEntryId) {
        return size > 0 && firstEntryId() <= maxEntryId;
    }

    /** Calls the consumer for each entry, in the order of the entry IDs. */
    void forEach(EntryConsumer consumer) {
        for (int b = 0; b < blocks.size(); b++) {
            Block block = blocks.get(b);
            for (int i = block.start; i < block.end; i++) {
                consumer.accept(block.entryIds[i], block.values[i]);
            }
        }
    }

    /** The index of the last block whose first entry ID is at most the entry ID, or -1. */
    private int blockIndexFor(long entryId) {
        int low = 0;
        int high = blocks.size() - 1;
        int found = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (blocks.get(mid).firstEntryId() <= entryId) {
                found = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return found;
    }

    /** Splits a full block in two halves; returns the new upper half, which follows it in the list. */
    private Block split(int blockIndex) {
        Block block = blocks.get(blockIndex);
        Block upper = new Block();
        int half = block.size() / 2;
        int from = block.start + half;
        int count = block.end - from;
        System.arraycopy(block.entryIds, from, upper.entryIds, 0, count);
        System.arraycopy(block.values, from, upper.values, 0, count);
        upper.end = count;
        block.end = from;
        blocks.add(blockIndex + 1, upper);
        peakBlocks = Math.max(peakBlocks, blocks.size());
        return upper;
    }

    private void removeAt(int blockIndex, Block block, int index) {
        block.removeAt(index);
        size--;
        if (block.size() == 0) {
            // Removing the first block shifts the references to the others: draining N entries in order moves about
            // (N / BLOCK_CAPACITY)^2 / 2 references in all, a small fraction of the entries' own handling
            blocks.remove(blockIndex);
        } else if (block.size() < MERGE_THRESHOLD) {
            mergeWithNeighbor(blockIndex, block);
        }
        if (blocks.size() * 4 < peakBlocks) {
            // the list keeps the capacity of its largest size: release it once a quarter of it is in use
            blocks.trimToSize();
            peakBlocks = blocks.size();
        }
    }

    /** Merges a sparse block with the next or the previous one, when they fit in one block together. */
    private void mergeWithNeighbor(int blockIndex, Block block) {
        if (blockIndex + 1 < blocks.size() && block.size() + blocks.get(blockIndex + 1).size() <= BLOCK_CAPACITY / 2) {
            moveInto(block, blocks.get(blockIndex + 1));
            blocks.remove(blockIndex + 1);
        } else if (blockIndex > 0 && block.size() + blocks.get(blockIndex - 1).size() <= BLOCK_CAPACITY / 2) {
            moveInto(blocks.get(blockIndex - 1), block);
            blocks.remove(blockIndex);
        }
    }

    /** Appends the entries of the following block to the target block. */
    private static void moveInto(Block target, Block following) {
        target.compact();
        System.arraycopy(following.entryIds, following.start, target.entryIds, target.end, following.size());
        System.arraycopy(following.values, following.start, target.values, target.end, following.size());
        target.end += following.size();
    }
}
