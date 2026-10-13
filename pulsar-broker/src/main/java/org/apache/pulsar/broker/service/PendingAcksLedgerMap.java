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

import java.util.Arrays;

/**
 * The pending acks of one ledger: a map from entry ID to a packed {@link PendingAckValues} value, in the order of the
 * entry IDs. Not thread-safe; {@link PendingAcksMap} guards it with its lock.
 *
 * <p>The entry IDs are kept in sorted arrays: a consumer is sent a ledger's entries mostly in increasing order, so that
 * adding one appends it, and the acks remove them mostly in the same order, from the start. A removed entry leaves a
 * tombstone, a value of {@link PendingAckValues#PACKED_NOT_FOUND}, so that the arrays stay sorted; the live entries are
 * between {@code head} and {@code tail}, and the tombstones are compacted away when they outnumber the live entries.
 * Lookups are binary searches over consecutive memory, without the pointer chasing of a tree.
 */
final class PendingAcksLedgerMap {
    private static final int INITIAL_CAPACITY = 16;
    private long[] entryIds = new long[INITIAL_CAPACITY];
    private long[] values = new long[INITIAL_CAPACITY];
    // the slots in use are [head, tail); head is a live entry unless the map is empty
    private int head;
    private int tail;
    private int size;

    /** A callback for each entry, in the order of the entry IDs. */
    interface EntryConsumer {
        void accept(long entryId, long value);
    }

    int size() {
        return size;
    }

    boolean isEmpty() {
        return size == 0;
    }

    /** The entry's value, or {@link PendingAckValues#PACKED_NOT_FOUND}. */
    long get(long entryId) {
        int index = indexOf(entryId);
        return index >= 0 ? values[index] : PendingAckValues.PACKED_NOT_FOUND;
    }

    boolean containsKey(long entryId) {
        return indexOf(entryId) >= 0;
    }

    /**
     * Sets the entry's value, which mustn't be {@link PendingAckValues#PACKED_NOT_FOUND}.
     *
     * @return the previous value, or {@link PendingAckValues#PACKED_NOT_FOUND}
     */
    long put(long entryId, long value) {
        if (tail == head || entryId > entryIds[tail - 1]) {
            if (tail == head) {
                // the map is empty: reuse the arrays from the start
                head = 0;
                tail = 0;
            }
            ensureCapacityAtTail();
            entryIds[tail] = entryId;
            values[tail] = value;
            tail++;
            size++;
            return PendingAckValues.PACKED_NOT_FOUND;
        }
        int index = Arrays.binarySearch(entryIds, head, tail, entryId);
        if (index >= 0) {
            // the entry, or a tombstone that it left
            long previous = values[index];
            values[index] = value;
            if (PendingAckValues.isNotFound(previous)) {
                size++;
            }
            return previous;
        }
        int insertionPoint = -index - 1;
        if (insertionPoint == head && head > 0) {
            // before the first entry, where removed entries left room
            head--;
            entryIds[head] = entryId;
            values[head] = value;
        } else {
            ensureCapacityAtTail();
            // making room may have moved the entries
            insertionPoint = -Arrays.binarySearch(entryIds, head, tail, entryId) - 1;
            System.arraycopy(entryIds, insertionPoint, entryIds, insertionPoint + 1, tail - insertionPoint);
            System.arraycopy(values, insertionPoint, values, insertionPoint + 1, tail - insertionPoint);
            entryIds[insertionPoint] = entryId;
            values[insertionPoint] = value;
            tail++;
        }
        size++;
        return PendingAckValues.PACKED_NOT_FOUND;
    }

    /**
     * Removes the entry.
     *
     * @return its value, or {@link PendingAckValues#PACKED_NOT_FOUND} if it wasn't in the map
     */
    long remove(long entryId) {
        int index = indexOf(entryId);
        if (index < 0) {
            return PendingAckValues.PACKED_NOT_FOUND;
        }
        long previous = values[index];
        removeAt(index);
        return previous;
    }

    /** Calls the consumer for each entry, in the order of the entry IDs. */
    void forEach(EntryConsumer consumer) {
        for (int i = head; i < tail; i++) {
            long value = values[i];
            if (!PendingAckValues.isNotFound(value)) {
                consumer.accept(entryIds[i], value);
            }
        }
    }

    /** Whether the map has an entry with an ID up to {@code maxEntryId}, inclusive. */
    boolean hasEntriesUpTo(long maxEntryId) {
        return size > 0 && entryIds[head] <= maxEntryId;
    }

    /**
     * Removes the entries with IDs up to {@code maxEntryId}, inclusive, and calls the consumer for each of them before
     * it is removed, in the order of the entry IDs.
     *
     * @return the number of entries removed
     */
    int removeUpTo(long maxEntryId, EntryConsumer removedEntryConsumer) {
        int removed = 0;
        while (size > 0 && entryIds[head] <= maxEntryId) {
            if (removedEntryConsumer != null) {
                removedEntryConsumer.accept(entryIds[head], values[head]);
            }
            removeAt(head);
            removed++;
        }
        return removed;
    }

    private int indexOf(long entryId) {
        if (size == 0 || entryId < entryIds[head] || entryId > entryIds[tail - 1]) {
            return -1;
        }
        int index = Arrays.binarySearch(entryIds, head, tail, entryId);
        return index >= 0 && !PendingAckValues.isNotFound(values[index]) ? index : -1;
    }

    private void removeAt(int index) {
        values[index] = PendingAckValues.PACKED_NOT_FOUND;
        size--;
        if (size == 0) {
            head = 0;
            tail = 0;
            return;
        }
        // keep head and tail on live entries
        while (PendingAckValues.isNotFound(values[head])) {
            head++;
        }
        while (PendingAckValues.isNotFound(values[tail - 1])) {
            tail--;
        }
        if (tail - head > 2 * size + INITIAL_CAPACITY) {
            compact();
        }
    }

    private void ensureCapacityAtTail() {
        if (tail < entryIds.length) {
            return;
        }
        if (tail - head > 2 * size) {
            // tombstones take room: compacting frees it
            compact();
        }
        if (head > 0 && tail - head < entryIds.length / 2) {
            // slide the live window to the start of the arrays
            compact();
        }
        if (tail == entryIds.length) {
            int newCapacity = entryIds.length * 2;
            entryIds = Arrays.copyOf(entryIds, newCapacity);
            values = Arrays.copyOf(values, newCapacity);
        }
    }

    /** Moves the live entries to the start of the arrays, without tombstones. */
    private void compact() {
        int target = 0;
        for (int i = head; i < tail; i++) {
            if (!PendingAckValues.isNotFound(values[i])) {
                entryIds[target] = entryIds[i];
                values[target] = values[i];
                target++;
            }
        }
        head = 0;
        tail = target;
    }
}
