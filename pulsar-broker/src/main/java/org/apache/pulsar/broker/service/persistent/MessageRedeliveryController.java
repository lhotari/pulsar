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
package org.apache.pulsar.broker.service.persistent;

import static org.apache.pulsar.broker.service.StickyKeyConsumerSelector.STICKY_KEY_HASH_NOT_SET;
import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMaps;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * The positions that a dispatcher has to dispatch again, its replay queue, with the sticky key hash of each position
 * that has one.
 *
 * <p>Not thread-safe: the dispatcher calls it under its monitor. Only {@link #size()} and {@link #isEmpty()} may be
 * called from any thread, such as by the stats and the read rescheduling.
 *
 * <p>With an owner resolver, such as a Key_Shared dispatcher's consumer selector, the positions are also indexed by
 * the owner of their hash, so that {@link #selectForReplay} visits only the positions of the owners that can take
 * messages: a replay queue full of the positions of a consumer without permits costs nothing to select from. Entry
 * IDs are unsigned 32-bit values, as RoaringBitmap stores them.
 */
@NotThreadSafe
public class MessageRedeliveryController {

    /**
     * What a selection needs to know about the owners of the positions. Its methods must not change the controller,
     * which the selection is iterating.
     */
    public interface ReplaySelection {
        /** How many positions of the owner the selection may take; 0 or less skips the owner's positions. */
        int availablePermits(Object owner);

        /** Whether the owner can't take the hash's messages now, such as while another consumer drains the hash. */
        boolean isBlocked(Object owner, int stickyKeyHash);
    }

    private static final long MAX_ENTRY_ID = 0xFFFF_FFFFL;

    private final boolean allowOutOfOrderDelivery;
    // ledger ID -> the ledger's positions in the replay queue
    private final Long2ObjectRBTreeMap<LedgerPositions> ledgers = new Long2ObjectRBTreeMap<>();
    // read without the monitor by size() and isEmpty()
    private volatile int size;
    // hash -> how many positions have it
    private final Int2IntOpenHashMap hashRefCounts = new Int2IntOpenHashMap();
    // whether any position has had a hash: a Shared dispatcher's positions never have one
    private boolean hasHashes;

    // The index by owner, with an owner resolver: stale after the owners changed, and rebuilt by the next selection
    private IntFunction<Object> ownerResolver;
    private boolean ownersStale = true;
    private final Reference2ObjectOpenHashMap<Object, SortedPositions> positionsByOwner =
            new Reference2ObjectOpenHashMap<>();
    // each hash's owner's positions, so that adding and removing a position doesn't resolve its owner again
    private final Int2ObjectOpenHashMap<SortedPositions> ownerPositionsByHash = new Int2ObjectOpenHashMap<>();
    // the positions without a hash, which a selection takes as they come, since it can't tell their owner
    private final SortedPositions positionsWithoutHash = new SortedPositions();
    // the positions whose hash has no owner, which a selection skips
    private final SortedPositions positionsWithoutOwner = new SortedPositions();

    public MessageRedeliveryController(boolean allowOutOfOrderDelivery) {
        this.allowOutOfOrderDelivery = allowOutOfOrderDelivery;
    }

    /** A ledger's positions in the replay queue, and the hash of each that has one. */
    private static final class LedgerPositions {
        final RoaringBitmap entries = new RoaringBitmap();
        // entry ID -> hash, for the positions that have one; STICKY_KEY_HASH_NOT_SET, 0, when absent
        final Int2IntOpenHashMap hashes = new Int2IntOpenHashMap();
    }

    /** Positions in order: ledger ID -> its entry IDs. */
    private static final class SortedPositions {
        final Long2ObjectRBTreeMap<RoaringBitmap> ledgers = new Long2ObjectRBTreeMap<>();

        void add(long ledgerId, int entryId) {
            RoaringBitmap entries = ledgers.get(ledgerId);
            if (entries == null) {
                entries = new RoaringBitmap();
                ledgers.put(ledgerId, entries);
            }
            entries.add(entryId);
        }

        void remove(long ledgerId, int entryId) {
            RoaringBitmap entries = ledgers.get(ledgerId);
            if (entries != null) {
                entries.remove(entryId);
                if (entries.isEmpty()) {
                    ledgers.remove(ledgerId);
                }
            }
        }

        // removes the positions up to and including the ledger's entry
        void removeUpTo(long ledgerId, long entryId) {
            while (!ledgers.isEmpty() && ledgers.firstLongKey() < ledgerId) {
                ledgers.remove(ledgers.firstLongKey());
            }
            RoaringBitmap entries = ledgers.get(ledgerId);
            if (entries != null && entryId >= 0) {
                entries.remove(0L, entryId + 1);
                if (entries.isEmpty()) {
                    ledgers.remove(ledgerId);
                }
            }
        }

        boolean isEmpty() {
            return ledgers.isEmpty();
        }

        void clear() {
            ledgers.clear();
        }
    }

    /**
     * Sets the resolver of a hash's owner, which indexes the positions by owner for {@link #selectForReplay}. It
     * returns null for a hash without an owner.
     */
    public void setOwnerResolver(IntFunction<Object> ownerResolver) {
        this.ownerResolver = ownerResolver;
        ownersChanged();
    }

    /**
     * Tells that the owners of the hashes may have changed, such as after a consumer was added or removed: the next
     * selection indexes the positions by their owners again.
     */
    public void ownersChanged() {
        if (!ownersStale) {
            ownersStale = true;
            clearIndex();
        }
    }

    private void clearIndex() {
        positionsByOwner.clear();
        ownerPositionsByHash.clear();
        positionsWithoutHash.clear();
        positionsWithoutOwner.clear();
    }

    private boolean indexed() {
        return !ownersStale;
    }

    public void add(long ledgerId, long entryId) {
        LedgerPositions ledger = ledgerPositions(ledgerId);
        int entry = (int) entryId;
        if (ledger.entries.checkedAdd(entry)) {
            size++;
            if (indexed()) {
                positionsWithoutHash.add(ledgerId, entry);
            }
        }
    }

    public void add(long ledgerId, long entryId, long stickyKeyHash) {
        if (stickyKeyHash == STICKY_KEY_HASH_NOT_SET) {
            if (!allowOutOfOrderDelivery) {
                throw new IllegalArgumentException("Sticky key hash is not set. It is required.");
            }
            // Dispatchers normalize real sticky-key hashes away from the sentinel, so the sentinel denotes a replay
            // position without a known hash.
            add(ledgerId, entryId);
            return;
        }
        int hash = (int) stickyKeyHash;
        LedgerPositions ledger = ledgerPositions(ledgerId);
        int entry = (int) entryId;
        hasHashes = true;
        if (ledger.entries.checkedAdd(entry)) {
            size++;
        } else {
            int previousHash = ledger.hashes.get(entry);
            if (previousHash == hash) {
                return;
            }
            // the position had no hash or another one: it moves to its hash's owner
            unindex(ledgerId, entry, previousHash);
            if (previousHash != STICKY_KEY_HASH_NOT_SET) {
                decrementRefCount(previousHash);
            }
        }
        ledger.hashes.put(entry, hash);
        hashRefCounts.addTo(hash, 1);
        if (indexed()) {
            ownerPositions(hash).add(ledgerId, entry);
        }
    }

    private LedgerPositions ledgerPositions(long ledgerId) {
        LedgerPositions ledger = ledgers.get(ledgerId);
        if (ledger == null) {
            ledger = new LedgerPositions();
            ledgers.put(ledgerId, ledger);
        }
        return ledger;
    }

    // The hash's owner's positions, resolving the owner when the hash is new to the index
    private SortedPositions ownerPositions(int hash) {
        SortedPositions positions = ownerPositionsByHash.get(hash);
        if (positions == null) {
            Object owner = ownerResolver.apply(hash);
            if (owner == null) {
                positions = positionsWithoutOwner;
            } else {
                positions = positionsByOwner.get(owner);
                if (positions == null) {
                    positions = new SortedPositions();
                    positionsByOwner.put(owner, positions);
                }
            }
            ownerPositionsByHash.put(hash, positions);
        }
        return positions;
    }

    private void unindex(long ledgerId, int entry, int hash) {
        if (!indexed()) {
            return;
        }
        if (hash == STICKY_KEY_HASH_NOT_SET) {
            positionsWithoutHash.remove(ledgerId, entry);
        } else {
            SortedPositions positions = ownerPositionsByHash.get(hash);
            if (positions != null) {
                positions.remove(ledgerId, entry);
            }
        }
    }

    private void decrementRefCount(int hash) {
        if (hashRefCounts.addTo(hash, -1) <= 1) {
            hashRefCounts.remove(hash);
            // the index forgets the hash's owner with its last position, so it holds only the queued hashes
            ownerPositionsByHash.remove(hash);
        }
    }

    public void remove(long ledgerId, long entryId) {
        LedgerPositions ledger = ledgers.get(ledgerId);
        if (ledger == null) {
            return;
        }
        int entry = (int) entryId;
        if (!ledger.entries.checkedRemove(entry)) {
            return;
        }
        size--;
        int hash = ledger.hashes.remove(entry);
        unindex(ledgerId, entry, hash);
        if (hash != STICKY_KEY_HASH_NOT_SET) {
            decrementRefCount(hash);
        }
        if (ledger.entries.isEmpty()) {
            ledgers.remove(ledgerId);
        }
        trimIfEmpty();
    }

    // Shrinks the hash tables once the replay queue is empty, so that a burst of many hashes doesn't keep their
    // capacity for the dispatcher's life
    private void trimIfEmpty() {
        if (size == 0) {
            hashRefCounts.clear();
            hashRefCounts.trim();
            ownerPositionsByHash.clear();
            ownerPositionsByHash.trim();
            positionsByOwner.clear();
            positionsByOwner.trim();
        }
    }

    /** Whether the position is in the replay queue. */
    public boolean contains(long ledgerId, long entryId) {
        LedgerPositions ledger = ledgers.get(ledgerId);
        return ledger != null && ledger.entries.contains((int) entryId);
    }

    /** The position's sticky key hash, or {@code STICKY_KEY_HASH_NOT_SET} when it has none or isn't queued. */
    public int getStickyKeyHash(long ledgerId, long entryId) {
        LedgerPositions ledger = ledgers.get(ledgerId);
        return ledger != null ? ledger.hashes.get((int) entryId) : STICKY_KEY_HASH_NOT_SET;
    }

    public Long getHash(long ledgerId, long entryId) {
        int hash = getStickyKeyHash(ledgerId, entryId);
        return hash != STICKY_KEY_HASH_NOT_SET ? Long.valueOf(hash) : null;
    }

    /** Removes the positions up to and including the mark-delete position, in time proportional to them. */
    public void removeAllUpTo(long markDeleteLedgerId, long markDeleteEntryId) {
        // entry IDs are unsigned 32-bit values
        markDeleteEntryId = Math.min(markDeleteEntryId, MAX_ENTRY_ID);
        while (!ledgers.isEmpty() && ledgers.firstLongKey() <= markDeleteLedgerId) {
            long ledgerId = ledgers.firstLongKey();
            LedgerPositions ledger = ledgers.get(ledgerId);
            PeekableIntIterator entries = ledger.entries.getIntIterator();
            int removed = 0;
            while (entries.hasNext() && (ledgerId < markDeleteLedgerId
                    || Integer.toUnsignedLong(entries.peekNext()) <= markDeleteEntryId)) {
                int hash = ledger.hashes.remove(entries.next());
                if (hash != STICKY_KEY_HASH_NOT_SET) {
                    decrementRefCount(hash);
                }
                removed++;
            }
            size -= removed;
            if (ledgerId < markDeleteLedgerId || removed == ledger.entries.getCardinality()) {
                ledgers.remove(ledgerId);
            } else {
                if (removed > 0) {
                    ledger.entries.remove(0L, markDeleteEntryId + 1);
                }
                break;
            }
        }
        if (indexed()) {
            positionsWithoutHash.removeUpTo(markDeleteLedgerId, markDeleteEntryId);
            positionsWithoutOwner.removeUpTo(markDeleteLedgerId, markDeleteEntryId);
            for (SortedPositions positions : positionsByOwner.values()) {
                positions.removeUpTo(markDeleteLedgerId, markDeleteEntryId);
            }
        }
        trimIfEmpty();
    }

    public boolean isEmpty() {
        return size == 0;
    }

    // An index that was fresh stays fresh: it's empty, as the replay queue is
    public void clear() {
        ledgers.clear();
        size = 0;
        clearIndex();
        trimIfEmpty();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("{");
        for (Long2ObjectMap.Entry<LedgerPositions> ledger : ledgers.long2ObjectEntrySet()) {
            if (result.length() > 1) {
                result.append(", ");
            }
            result.append(ledger.getLongKey()).append('=').append(ledger.getValue().entries);
        }
        return result.append('}').toString();
    }

    public boolean containsStickyKeyHash(int stickyKeyHash) {
        return !allowOutOfOrderDelivery
                && stickyKeyHash != STICKY_KEY_HASH_NOT_SET && hashRefCounts.containsKey(stickyKeyHash);
    }

    public Optional<Position> getFirstPositionInReplay() {
        if (ledgers.isEmpty()) {
            return Optional.empty();
        }
        long ledgerId = ledgers.firstLongKey();
        return Optional.of(PositionFactory.create(ledgerId,
                Integer.toUnsignedLong(ledgers.get(ledgerId).entries.first())));
    }

    /**
     * Get the messages to replay now, visiting the positions in order from the first one.
     *
     * @param maxMessagesToRead
     *            the max messages to read
     * @param filter
     *            the filter to use to select the messages to replay
     * @return the messages to replay now
     */
    public NavigableSet<Position> getMessagesToReplayNow(int maxMessagesToRead, Predicate<Position> filter) {
        NavigableSet<Position> items = new TreeSet<>();
        for (Long2ObjectMap.Entry<LedgerPositions> ledger : ledgers.long2ObjectEntrySet()) {
            PeekableIntIterator entries = ledger.getValue().entries.getIntIterator();
            while (entries.hasNext() && items.size() < maxMessagesToRead) {
                Position position = PositionFactory.create(ledger.getLongKey(),
                        Integer.toUnsignedLong(entries.next()));
                if (filter.test(position)) {
                    items.add(position);
                }
            }
            if (items.size() >= maxMessagesToRead) {
                break;
            }
        }
        return items;
    }

    /**
     * Selects up to {@code maxMessagesToRead} positions to replay now, in the order of their positions among those
     * that can be dispatched: the positions without a hash, which the dispatcher checks once it has read them, and
     * those of the owners with permits, each owner taking at most its permits. A position of a blocked hash isn't
     * taken, and with ordering enforced neither are the later positions of its hash. It visits only the positions of
     * the owners with permits, so the positions of the owners without don't cost it anything.
     *
     * <p>Needs an owner resolver, see {@link #setOwnerResolver}.
     */
    public NavigableSet<Position> selectForReplay(int maxMessagesToRead, ReplaySelection selection) {
        NavigableSet<Position> selected = new TreeSet<>();
        if (size == 0 || maxMessagesToRead <= 0) {
            return selected;
        }
        Objects.requireNonNull(ownerResolver, "The selection needs an owner resolver");
        if (ownersStale) {
            reindex();
        }
        PriorityQueue<OwnerCursor> heads = new PriorityQueue<>();
        if (!positionsWithoutHash.isEmpty()) {
            addIfNotExhausted(heads, new OwnerCursor(null, positionsWithoutHash, Integer.MAX_VALUE, ledgers));
        }
        for (Reference2ObjectMap.Entry<Object, SortedPositions> owner
                : Reference2ObjectMaps.fastIterable(positionsByOwner)) {
            if (owner.getValue().isEmpty()) {
                continue;
            }
            int permits = selection.availablePermits(owner.getKey());
            if (permits > 0) {
                addIfNotExhausted(heads, new OwnerCursor(owner.getKey(), owner.getValue(), permits, ledgers));
            }
        }
        IntOpenHashSet blockedHashes = null;
        while (!heads.isEmpty() && selected.size() < maxMessagesToRead) {
            OwnerCursor cursor = heads.poll();
            boolean take = true;
            if (cursor.owner != null) {
                int hash = cursor.hashes.get(cursor.entry);
                if (blockedHashes != null && blockedHashes.contains(hash)) {
                    take = false;
                } else if (selection.isBlocked(cursor.owner, hash)) {
                    take = false;
                    if (!allowOutOfOrderDelivery) {
                        if (blockedHashes == null) {
                            blockedHashes = new IntOpenHashSet();
                        }
                        blockedHashes.add(hash);
                    }
                }
            }
            if (take) {
                selected.add(PositionFactory.create(cursor.ledgerId, Integer.toUnsignedLong(cursor.entry)));
                cursor.permits--;
            }
            if (cursor.permits > 0) {
                addIfNotExhausted(heads, cursor);
            }
        }
        return selected;
    }

    private static void addIfNotExhausted(PriorityQueue<OwnerCursor> heads, OwnerCursor cursor) {
        if (cursor.advance()) {
            heads.add(cursor);
        }
    }

    // Indexes the positions by the owners of their hashes; fresh only once complete, should the resolver fail
    private void reindex() {
        clearIndex();
        for (Long2ObjectMap.Entry<LedgerPositions> ledgerEntry : ledgers.long2ObjectEntrySet()) {
            long ledgerId = ledgerEntry.getLongKey();
            LedgerPositions ledger = ledgerEntry.getValue();
            PeekableIntIterator entries = ledger.entries.getIntIterator();
            while (entries.hasNext()) {
                int entry = entries.next();
                int hash = ledger.hashes.get(entry);
                if (hash == STICKY_KEY_HASH_NOT_SET) {
                    positionsWithoutHash.add(ledgerId, entry);
                } else {
                    ownerPositions(hash).add(ledgerId, entry);
                }
            }
        }
        ownersStale = false;
    }

    // An owner's next position in a selection; ordered by that position
    private static final class OwnerCursor implements Comparable<OwnerCursor> {
        final Object owner;
        int permits;
        private final ObjectBidirectionalIterator<Long2ObjectMap.Entry<RoaringBitmap>> ledgers;
        private final Long2ObjectRBTreeMap<LedgerPositions> queuedLedgers;
        private PeekableIntIterator entries;
        long ledgerId;
        int entry;
        // the hashes of the current ledger's positions
        Int2IntOpenHashMap hashes;

        OwnerCursor(Object owner, SortedPositions positions, int permits,
                    Long2ObjectRBTreeMap<LedgerPositions> queuedLedgers) {
            this.owner = owner;
            this.permits = permits;
            this.ledgers = positions.ledgers.long2ObjectEntrySet().iterator();
            this.queuedLedgers = queuedLedgers;
        }

        // moves to the owner's next position; false when it has no more
        boolean advance() {
            while (entries == null || !entries.hasNext()) {
                if (!ledgers.hasNext()) {
                    return false;
                }
                Long2ObjectMap.Entry<RoaringBitmap> ledger = ledgers.next();
                ledgerId = ledger.getLongKey();
                entries = ledger.getValue().getIntIterator();
                hashes = queuedLedgers.get(ledgerId).hashes;
            }
            entry = entries.next();
            return true;
        }

        @Override
        public int compareTo(OwnerCursor other) {
            int byLedger = Long.compare(ledgerId, other.ledgerId);
            return byLedger != 0 ? byLedger : Integer.compareUnsigned(entry, other.entry);
        }
    }

    /**
     * Get the number of messages registered for replay in the redelivery controller.
     *
     * @return number of messages
     */
    public int size() {
        return size;
    }

    // whether the controller keeps hashes: when ordering is enforced, or once a position had one
    @VisibleForTesting
    boolean isPositionToStickyKeyHashInitialized() {
        return !allowOutOfOrderDelivery || hasHashes;
    }

    @VisibleForTesting
    List<Object> getIndexedOwners() {
        return ownersStale ? List.of() : new ArrayList<>(positionsByOwner.keySet());
    }
}
