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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MessageRedeliveryControllerTest {
    @DataProvider(name = "allowOutOfOrderDelivery")
    public Object[][] dataProvider() {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testAddAndRemove(boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = new MessageRedeliveryController(allowOutOfOrderDelivery);

        assertEquals(controller.isPositionToStickyKeyHashInitialized(), !allowOutOfOrderDelivery);
        assertTrue(controller.isEmpty());
        assertEquals(controller.size(), 0);

        controller.add(1, 1);
        controller.add(1, 2);

        assertFalse(controller.isEmpty());
        assertEquals(controller.size(), 2);
        assertNull(controller.getHash(1, 1));
        assertNull(controller.getHash(1, 2));
        assertEquals(controller.isPositionToStickyKeyHashInitialized(), !allowOutOfOrderDelivery);

        controller.remove(1, 1);
        controller.remove(1, 2);

        assertTrue(controller.isEmpty());
        assertEquals(controller.size(), 0);

        controller.add(2, 1, 100);
        controller.add(2, 2, 101);
        controller.add(2, 3, 101);

        assertFalse(controller.isEmpty());
        assertEquals(controller.size(), 3);
        assertTrue(controller.isPositionToStickyKeyHashInitialized());
        assertEquals(controller.getHash(2, 1), Long.valueOf(100));
        assertEquals(controller.getHash(2, 2), Long.valueOf(101));
        assertEquals(controller.getHash(2, 3), Long.valueOf(101));
        if (!allowOutOfOrderDelivery) {
            assertTrue(controller.containsStickyKeyHash(100));
            assertTrue(controller.containsStickyKeyHash(101));
        }

        controller.remove(2, 1);
        controller.remove(2, 2);

        assertEquals(controller.size(), 1);
        assertNull(controller.getHash(2, 1));
        assertNull(controller.getHash(2, 2));
        assertEquals(controller.getHash(2, 3), Long.valueOf(101));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(100));
            assertTrue(controller.containsStickyKeyHash(101));
        }

        controller.clear();
        assertTrue(controller.isEmpty());
        assertEquals(controller.size(), 0);
        assertNull(controller.getHash(2, 3));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(101));
        }

        controller.add(2, 2, 201);
        controller.add(1, 3, 100);
        controller.add(3, 1, 300);
        controller.add(2, 1, 200);
        controller.add(3, 2, 301);
        controller.add(1, 2, 101);
        controller.add(1, 1, 100);

        controller.removeAllUpTo(1, 3);
        assertEquals(controller.size(), 4);
        assertNull(controller.getHash(1, 1));
        assertNull(controller.getHash(1, 2));
        assertNull(controller.getHash(1, 3));
        assertEquals(controller.getHash(2, 1), Long.valueOf(200));
        assertEquals(controller.getHash(2, 2), Long.valueOf(201));
        assertEquals(controller.getHash(3, 1), Long.valueOf(300));
        assertEquals(controller.getHash(3, 2), Long.valueOf(301));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(100));
            assertFalse(controller.containsStickyKeyHash(101));
            assertTrue(controller.containsStickyKeyHash(200));
            assertTrue(controller.containsStickyKeyHash(201));
            assertTrue(controller.containsStickyKeyHash(300));
            assertTrue(controller.containsStickyKeyHash(301));
        }

        controller.removeAllUpTo(3, 1);
        assertEquals(controller.size(), 1);
        assertNull(controller.getHash(2, 1));
        assertNull(controller.getHash(2, 2));
        assertNull(controller.getHash(3, 1));
        assertEquals(controller.getHash(3, 2), Long.valueOf(301));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(200));
            assertFalse(controller.containsStickyKeyHash(201));
            assertFalse(controller.containsStickyKeyHash(300));
            assertTrue(controller.containsStickyKeyHash(301));
        }

        controller.removeAllUpTo(5, 10);
        assertTrue(controller.isEmpty());
        assertEquals(controller.size(), 0);
        assertNull(controller.getHash(3, 2));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(301));
        }
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testRemoveAllUpToASparsePrefixRemovesItsHashes(boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = new MessageRedeliveryController(allowOutOfOrderDelivery);
        // a gap between the positions removed and the one kept, in the same ledger
        controller.add(1, 5, 100);
        controller.add(1, 100, 200);

        controller.removeAllUpTo(1, 10);

        assertEquals(controller.size(), 1);
        assertNull(controller.getHash(1, 5));
        assertEquals(controller.getHash(1, 100), Long.valueOf(200));
        assertFalse(controller.containsStickyKeyHash(100));
        if (!allowOutOfOrderDelivery) {
            assertTrue(controller.containsStickyKeyHash(200));
        }
    }

    @Test(timeOut = 10000)
    public void testOutOfOrderSentinelHashDoesNotInitializePositionHashMap() {
        MessageRedeliveryController controller = new MessageRedeliveryController(true);

        controller.add(1, 1, STICKY_KEY_HASH_NOT_SET);

        assertFalse(controller.isPositionToStickyKeyHashInitialized());
        assertEquals(controller.size(), 1);
        assertNull(controller.getHash(1, 1));

        controller.removeAllUpTo(1, 1);
        assertTrue(controller.isEmpty());
        assertFalse(controller.isPositionToStickyKeyHashInitialized());
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testContainsStickyKeyHash(boolean allowOutOfOrderDelivery) throws Exception {
        MessageRedeliveryController controller = new MessageRedeliveryController(allowOutOfOrderDelivery);
        controller.add(1, 1, 100);
        controller.add(1, 2, 101);
        controller.add(1, 3, 102);
        controller.add(2, 2, 103);
        controller.add(2, 1, 104);

        if (allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(100));
            assertFalse(controller.containsStickyKeyHash(101));
            assertFalse(controller.containsStickyKeyHash(104));
        } else {
            assertTrue(controller.containsStickyKeyHash(100));
            assertTrue(controller.containsStickyKeyHash(101));
            assertTrue(controller.containsStickyKeyHash(104));
        }

        assertFalse(controller.containsStickyKeyHash(99));
        assertFalse(controller.containsStickyKeyHash(105));
        assertFalse(controller.containsStickyKeyHash(106));
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testGetMessagesToReplayNow(boolean allowOutOfOrderDelivery) throws Exception {
        MessageRedeliveryController controller = new MessageRedeliveryController(allowOutOfOrderDelivery);
        controller.add(2, 2);
        controller.add(1, 3);
        controller.add(3, 1);
        controller.add(2, 1);
        controller.add(3, 2);
        controller.add(1, 2);
        controller.add(1, 1);

        if (allowOutOfOrderDelivery) {
            // The entries are sorted by ledger ID but not by entry ID
            Position[] actual1 = controller.getMessagesToReplayNow(3, item -> true).toArray(new Position[3]);
            Position[] expected1 = { PositionFactory.create(1, 1),
                    PositionFactory.create(1, 2), PositionFactory.create(1, 3) };
            assertEqualsNoOrder(actual1, expected1);
        } else {
            // The entries are completely sorted
            Set<Position> actual2 = controller.getMessagesToReplayNow(6, item -> true);
            Set<Position> expected2 = new TreeSet<>();
            expected2.add(PositionFactory.create(1, 1));
            expected2.add(PositionFactory.create(1, 2));
            expected2.add(PositionFactory.create(1, 3));
            expected2.add(PositionFactory.create(2, 1));
            expected2.add(PositionFactory.create(2, 2));
            expected2.add(PositionFactory.create(3, 1));
            assertEquals(actual2, expected2);
        }
    }

    // Owners "A", "B" and "C" by hash: hashes 1-99 belong to A, 100-199 to B, 200-299 to C, others to no owner
    private static Object ownerOf(int hash) {
        return hash < 100 ? "A" : hash < 200 ? "B" : hash < 300 ? "C" : null;
    }

    // Permits by owner, blocked hashes, and how many positions the selection checked
    private static final class Selection implements MessageRedeliveryController.ReplaySelection {
        final Map<Object, Integer> permits = new HashMap<>();
        final Set<Integer> blocked = new HashSet<>();
        final AtomicInteger checked = new AtomicInteger();

        @Override
        public int availablePermits(Object owner) {
            return permits.getOrDefault(owner, 0);
        }

        @Override
        public boolean isBlocked(Object owner, int stickyKeyHash) {
            checked.incrementAndGet();
            return blocked.contains(stickyKeyHash);
        }
    }

    private static MessageRedeliveryController indexed(boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = new MessageRedeliveryController(allowOutOfOrderDelivery);
        controller.setOwnerResolver(MessageRedeliveryControllerTest::ownerOf);
        return controller;
    }

    private static List<Position> positions(long... ledgerAndEntryIds) {
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < ledgerAndEntryIds.length; i += 2) {
            positions.add(PositionFactory.create(ledgerAndEntryIds[i], ledgerAndEntryIds[i + 1]));
        }
        return positions;
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testSelectionSkipsTheOwnersWithoutPermitsWithoutVisitingTheirPositions(
            boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = indexed(allowOutOfOrderDelivery);
        for (int entry = 0; entry < 20_000; entry++) {
            controller.add(1, entry, 1 + entry % 99);
        }
        for (int entry = 20_000; entry < 20_010; entry++) {
            controller.add(1, entry, 150);
        }
        Selection selection = new Selection();
        selection.permits.put("B", 100);

        assertEquals(controller.selectForReplay(500, selection), new TreeSet<>(positions(1, 20_000, 1, 20_001,
                1, 20_002, 1, 20_003, 1, 20_004, 1, 20_005, 1, 20_006, 1, 20_007, 1, 20_008, 1, 20_009)));
        // only B's positions were checked
        assertEquals(selection.checked.get(), 10);

        // A gets permits: its oldest positions come first, up to its permits
        selection.permits.put("A", 3);
        assertEquals(controller.selectForReplay(5, selection), new TreeSet<>(positions(1, 0, 1, 1, 1, 2, 1, 20_000,
                1, 20_001)));
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testSelectionTakesPositionsInOrderAcrossOwnersAndThoseWithoutAHash(boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = indexed(allowOutOfOrderDelivery);
        controller.add(2, 5, 150);
        controller.add(1, 7, 10);
        controller.add(1, 3);
        controller.add(3, 1, 250);
        controller.add(1, 9, 500);
        Selection selection = new Selection();
        selection.permits.put("A", 10);
        selection.permits.put("B", 10);
        selection.permits.put("C", 10);

        // the position without an owner, 1:9, is skipped
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(1, 3, 1, 7, 2, 5, 3, 1)));
        assertEquals(controller.selectForReplay(2, selection), new TreeSet<>(positions(1, 3, 1, 7)));
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testSelectionSkipsABlockedHash(boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = indexed(allowOutOfOrderDelivery);
        controller.add(1, 1, 10);
        controller.add(1, 2, 11);
        controller.add(1, 3, 10);
        controller.add(1, 4, 12);
        Selection selection = new Selection();
        selection.permits.put("A", 10);
        selection.blocked.add(10);

        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(1, 2, 1, 4)));
        // the blocked hash's later position isn't checked again: the check is the hash's and the owner's
        assertEquals(selection.checked.get(), 3);
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testSelectionFollowsChangedOwnersAndRemovals(boolean allowOutOfOrderDelivery) {
        Map<Integer, Object> owners = new HashMap<>(Map.of(10, "A", 20, "B"));
        MessageRedeliveryController controller = new MessageRedeliveryController(allowOutOfOrderDelivery);
        controller.setOwnerResolver(owners::get);
        controller.add(1, 1, 10);
        controller.add(1, 2, 20);
        Selection selection = new Selection();
        selection.permits.put("A", 10);
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(1, 1)));

        // B is removed and its hash moves to A
        owners.put(20, "A");
        controller.ownersChanged();
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(1, 1, 1, 2)));

        // positions added, re-added with another hash, removed and removed up to the mark-delete position
        controller.add(1, 3, 10);
        controller.add(2, 1, 10);
        controller.add(1, 1, 20);
        controller.remove(1, 2);
        controller.removeAllUpTo(1, 1);
        assertEquals(controller.size(), 2);
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(1, 3, 2, 1)));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(20));
            assertTrue(controller.containsStickyKeyHash(10));
        }
        controller.clear();
        assertTrue(controller.selectForReplay(10, selection).isEmpty());
        assertTrue(controller.getIndexedOwners().isEmpty());
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testRemoveAllUpToAcrossLedgersKeepsCountsAndHashes(boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = indexed(allowOutOfOrderDelivery);
        controller.add(1, 1, 10);
        controller.add(1, 5, 11);
        controller.add(2, 0, 10);
        controller.add(2, 4, 12);
        controller.add(2, 9, 12);
        controller.add(3, 2);

        controller.removeAllUpTo(2, 4);

        assertEquals(controller.size(), 2);
        assertEquals(controller.getFirstPositionInReplay().orElseThrow(), PositionFactory.create(2, 9));
        assertNull(controller.getHash(1, 5));
        assertEquals(controller.getHash(2, 9), Long.valueOf(12));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(10));
            assertFalse(controller.containsStickyKeyHash(11));
            assertTrue(controller.containsStickyKeyHash(12));
        }
        // the position without a hash, then A's
        Selection selection = new Selection();
        selection.permits.put("A", 10);
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(2, 9, 3, 2)));
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testIndexedEdgeCases(boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = indexed(allowOutOfOrderDelivery);
        Selection selection = new Selection();
        selection.permits.put("A", 10);
        selection.permits.put("B", 10);
        // the index exists before these additions
        assertTrue(controller.selectForReplay(10, selection).isEmpty());

        // unsigned entry IDs above Integer.MAX_VALUE stay in order
        long high = 0xFFFF_FFF0L;
        controller.add(1, high, 10);
        controller.add(1, 5, 10);
        assertEquals(controller.getFirstPositionInReplay().orElseThrow(), PositionFactory.create(1, 5));
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(1, 5, 1, high)));
        // a mark-delete position before the ledger's first entry removes nothing of it
        controller.removeAllUpTo(1, -1);
        assertEquals(controller.size(), 2);
        controller.removeAllUpTo(1, high);
        assertTrue(controller.isEmpty());

        // a position without a hash that gets one, and a position whose hash moves to another owner
        controller.add(2, 1);
        controller.add(2, 1, 150);
        controller.add(2, 2, 10);
        controller.add(2, 2, 150);
        selection.permits.put("B", 0);
        assertTrue(controller.selectForReplay(10, selection).isEmpty());
        selection.permits.put("B", 10);
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(2, 1, 2, 2)));
        if (!allowOutOfOrderDelivery) {
            assertFalse(controller.containsStickyKeyHash(10));
            assertTrue(controller.containsStickyKeyHash(150));
        }

        // reused after clear()
        controller.clear();
        controller.add(3, 1, 10);
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(3, 1)));
        assertEquals(controller.getIndexedOwners(), List.of("A"));
        // the index forgets the hashes with their last positions
        controller.remove(3, 1);
        controller.add(3, 2, 150);
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(3, 2)));
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testIndexedUpdatesWhileStaleAndAcrossLedgers(boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = indexed(allowOutOfOrderDelivery);
        Selection selection = new Selection();
        selection.permits.put("A", 10);
        selection.permits.put("B", 10);
        // A's positions in ledgers 1 and 3, B's in ledger 2
        controller.add(3, 1, 10);
        controller.add(1, 1, 11);
        controller.add(2, 1, 150);
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(1, 1, 2, 1, 3, 1)));

        // a hash blocked in ledger 1 is skipped in ledger 3 too
        controller.add(1, 2, 10);
        selection.blocked.add(10);
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(1, 1, 2, 1)));
        selection.blocked.clear();

        // a duplicate addition counts once; a re-addition with another hash while the index is stale moves it
        controller.add(1, 1, 11);
        controller.ownersChanged();
        controller.add(1, 1, 150);
        controller.remove(1, 1);
        assertFalse(controller.containsStickyKeyHash(11));
        // 2:1 keeps hash 150
        assertEquals(controller.containsStickyKeyHash(150), !allowOutOfOrderDelivery);
        selection.permits.put("A", 0);
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(2, 1)));
        assertEquals(controller.size(), 3);
    }

    @Test(timeOut = 10000)
    public void testAFailedReindexIsRetried() {
        AtomicInteger failures = new AtomicInteger(1);
        MessageRedeliveryController controller = new MessageRedeliveryController(false);
        controller.setOwnerResolver(hash -> {
            if (failures.getAndDecrement() > 0) {
                throw new IllegalStateException("selector failure");
            }
            return ownerOf(hash);
        });
        controller.add(1, 1, 10);
        Selection selection = new Selection();
        selection.permits.put("A", 10);
        try {
            controller.selectForReplay(10, selection);
        } catch (IllegalStateException expected) {
            // the next selection indexes the positions again
        }
        assertEquals(controller.selectForReplay(10, selection), new TreeSet<>(positions(1, 1)));
    }

    @Test(dataProvider = "allowOutOfOrderDelivery", timeOut = 10000)
    public void testBlockedHashesAreParkedUntilUnparked(boolean allowOutOfOrderDelivery) {
        MessageRedeliveryController controller = indexed(allowOutOfOrderDelivery);
        // A's hash 10 is blocked, as while another consumer drains it; its positions are many
        for (int entry = 0; entry < 1_000; entry++) {
            controller.add(1, entry, 10);
        }
        controller.add(1, 2_000, 11);
        Selection selection = new Selection();
        selection.permits.put("A", 100);
        selection.blocked.add(10);

        assertEquals(controller.selectForReplay(100, selection), new TreeSet<>(positions(1, 2_000)));
        assertEquals(controller.getParkedHashCount(), 1);
        assertTrue(controller.hasParkedHashes());

        // the next selections don't visit the parked positions, nor new ones of the hash
        controller.add(1, 3_000, 10);
        selection.checked.set(0);
        assertEquals(controller.selectForReplay(100, selection), new TreeSet<>(positions(1, 2_000)));
        assertEquals(selection.checked.get(), 1);
        assertEquals(controller.size(), 1_002);
        if (!allowOutOfOrderDelivery) {
            assertTrue(controller.containsStickyKeyHash(10));
        }

        // parked positions are removed as others are
        controller.remove(1, 3_000);
        controller.removeAllUpTo(1, 499);
        assertEquals(controller.size(), 501);

        // unparked, the hash's positions are selected in order
        selection.blocked.clear();
        controller.unpark(10);
        assertFalse(controller.hasParkedHashes());
        assertEquals(controller.selectForReplay(3, selection), new TreeSet<>(positions(1, 500, 1, 501, 1, 502)));

        // parking again, then unparking every hash
        selection.blocked.add(10);
        controller.selectForReplay(3, selection);
        assertEquals(controller.getParkedHashCount(), 1);
        controller.unpark(STICKY_KEY_HASH_NOT_SET);
        assertEquals(controller.getParkedHashCount(), 0);

        // removing the hash's last positions forgets it as parked
        controller.selectForReplay(3, selection);
        controller.removeAllUpTo(1, 999);
        assertEquals(controller.getParkedHashCount(), 0);
        assertFalse(controller.hasParkedHashes());
    }
}
