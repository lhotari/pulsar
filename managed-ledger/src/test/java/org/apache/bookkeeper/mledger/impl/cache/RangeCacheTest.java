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
package org.apache.bookkeeper.mledger.impl.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Lists;
import io.netty.buffer.Unpooled;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.CachedEntry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.CachedEntryImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class RangeCacheTest {

    @Test
    public void simple() {
        RangeCacheRemovalQueue removalQueue = new RangeCacheRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        putToCache(cache, 0, "0");
        putToCache(cache, 1, "1");

        assertEquals(cache.getSize(), 2);
        assertEquals(cache.getNumberOfEntries(), 2);

        CachedEntry s = cache.get(createPosition(0));
        assertEquals(s.getData(), "0".getBytes());
        assertEquals(s.refCnt(), 2);
        s.release();

        CachedEntry s1 = cache.get(createPosition(0));
        CachedEntry s2 = cache.get(createPosition(0));
        assertEquals(s1, s2);
        assertEquals(s1.refCnt(), 3);
        s1.release();
        s2.release();

        assertNull(cache.get(createPosition(2)));

        putToCache(cache, 2, "2");
        putToCache(cache, 8, "8");
        putToCache(cache, 11, "11");

        assertEquals(cache.getSize(), 5);
        assertEquals(cache.getNumberOfEntries(), 5);

        cache.removeRange(createPosition(1), createPosition(5),  true);
        assertEquals(cache.getSize(), 3);
        assertEquals(cache.getNumberOfEntries(), 3);

        cache.removeRange(createPosition(2), createPosition(8),  false);
        assertEquals(cache.getSize(), 3);
        assertEquals(cache.getNumberOfEntries(), 3);

        cache.removeRange(createPosition(0), createPosition(100),  false);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);

        cache.removeRange(createPosition(0), createPosition(100),  false);
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    private void putToCache(RangeCache cache, int i, String str) {
        Position position = createPosition(i);
        CachedEntry cachedEntry = createCachedEntry(position, str);
        cache.put(position, cachedEntry);
    }

    private static CachedEntry createCachedEntry(int i, String str) {
        return createCachedEntry(createPosition(i), str);
    }

    private static CachedEntry createCachedEntry(Position position, String str) {
        return CachedEntryImpl.create(position, Unpooled.wrappedBuffer(str.getBytes()));
    }

    private static Position createPosition(int i) {
        return PositionFactory.create(0, i);
    }

    @DataProvider
    public static Object[][] retainBeforeEviction() {
        return new Object[][]{ { true }, { false } };
    }


    @Test(dataProvider = "retainBeforeEviction")
    public void customTimeExtraction(boolean retain) {
        RangeCache cache = new RangeCache(removalQueue);

        putToCache(cache, 1, "1");
        putToCache(cache, 22, "22");
        putToCache(cache, 333, "333");
        long timestamp = System.nanoTime();
        putToCache(cache, 4444, "4444");

        assertEquals(cache.getSize(), 10);
        assertEquals(cache.getNumberOfEntries(), 4);
        final var retainedEntries = cache.getRange(1, 4444);
        for (final var entry : retainedEntries) {
            assertEquals(entry.refCnt(), 2);
            if (!retain) {
                entry.release();
            }
        }

        Pair<Integer, Long> evictedSize = removalQueue.evictLEntriesBeforeTimestamp(timestamp);
        assertEquals(evictedSize.getRight().longValue(), 6);
        assertEquals(evictedSize.getLeft().longValue(), 3);
        assertEquals(cache.getSize(), 4);
        assertEquals(cache.getNumberOfEntries(), 1);

        if (retain) {
            final var valueToRefCnt = retainedEntries.stream().collect(Collectors.toMap(RefString::getS,
                    AbstractReferenceCounted::refCnt));
            assertEquals(valueToRefCnt, Map.of("1", 1, "22", 1, "333", 1, "4444", 2));
            retainedEntries.forEach(AbstractReferenceCounted::release);
        } else {
            final var valueToRefCnt = retainedEntries.stream().filter(v -> v.refCnt() > 0).collect(Collectors.toMap(
                    RefString::getS, AbstractReferenceCounted::refCnt));
            assertEquals(valueToRefCnt, Map.of("4444", 1));
        }
    }

    @Test
    public void doubleInsert() {
        RangeCacheRemovalQueue removalQueue = new RangeCacheRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        CachedEntry s0 = new CachedEntry("zero", 0);
        assertEquals(s0.refCnt(), 1);
        assertTrue(cache.put(0, s0));
        assertEquals(s0.refCnt(), 1);

        cache.put(1, new CachedEntry("one", 1));

        assertEquals(cache.getSize(), 2);
        assertEquals(cache.getNumberOfEntries(), 2);
        CachedEntry s = cache.get(createPosition(1));
        assertEquals(s.s, "one");
        assertEquals(s.refCnt(), 2);

        CachedEntry s1 = new CachedEntry("uno", 1);
        assertEquals(s1.refCnt(), 1);
        assertFalse(cache.put(1, s1));
        assertEquals(s1.refCnt(), 1);
        s1.release();

        // Should not have been overridden in cache
        assertEquals(cache.getSize(), 2);
        assertEquals(cache.getNumberOfEntries(), 2);
        assertEquals(cache.get(createPosition(1)).s, "one");
    }

    @Test
    public void getRange() {
        RangeCacheRemovalQueue removalQueue = new RangeCacheRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        putToCache(cache, 0, "0");
        putToCache(cache, 1, "1");
        putToCache(cache, 3, "3");
        putToCache(cache, 5, "5");

        assertEquals(cache.getRange(createPosition(1), createPosition(8)),
                Lists.newArrayList(createCachedEntry("1"), createCachedEntry("3"), createCachedEntry("5")));

        putToCache(cache, 8, "8");
        assertEquals(cache.getRange(createPosition(1), createPosition(8)),
                Lists.newArrayList(createCachedEntry("1"), createCachedEntry("3"), createCachedEntry("5"), createCachedEntry("8")));

        cache.clear();
        assertEquals(cache.getSize(), 0);
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    public void eviction() {
        RangeCacheRemovalQueue removalQueue = new RangeCacheRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        cache.put(0, new CachedEntry("zero", 0));
        cache.put(1, new CachedEntry("one", 1));
        cache.put(2, new CachedEntry("two", 2));
        cache.put(3, new CachedEntry("three", 3));

        // This should remove the LRU entries: 0, 1 whose combined size is 7
        assertEquals(removalQueue.evictLeastAccessedEntries(5), Pair.of(2, (long) 7));

        assertEquals(cache.getNumberOfEntries(), 2);
        assertEquals(cache.getSize(), 8);
        assertNull(cache.get(createPosition(0)));
        assertNull(cache.get(createPosition(1)));
        assertEquals(cache.get(createPosition(2)).s, "two");
        assertEquals(cache.get(createPosition(3)).s, "three");

        assertEquals(removalQueue.evictLeastAccessedEntries(100), Pair.of(2, (long) 8));
        assertEquals(cache.getNumberOfEntries(), 0);
        assertEquals(cache.getSize(), 0);
        assertNull(cache.get(createPosition(0)));
        assertNull(cache.get(createPosition(1)));
        assertNull(cache.get(createPosition(2)));
        assertNull(cache.get(createPosition(3)));

        try {
            removalQueue.evictLeastAccessedEntries(0);
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // ok
        }

        try {
            removalQueue.evictLeastAccessedEntries(-1);
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // ok
        }
    }

    @Test
    public void evictions() {
        RangeCacheRemovalQueue removalQueue = new RangeCacheRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);

        for (int i = 0; i < 100; i++) {
            putToCache(cache, i, Integer.toString(i));
        }

        assertEquals(cache.getSize(), 100);
        Pair<Integer, Long> res = removalQueue.evictLeastAccessedEntries(1);
        assertEquals((int) res.getLeft(), 1);
        assertEquals((long) res.getRight(), 1);
        assertEquals(cache.getSize(), 99);

        res = removalQueue.evictLeastAccessedEntries(10);
        assertEquals((int) res.getLeft(), 10);
        assertEquals((long) res.getRight(), 10);
        assertEquals(cache.getSize(), 89);

        res = removalQueue.evictLeastAccessedEntries(100);
        assertEquals((int) res.getLeft(), 89);
        assertEquals((long) res.getRight(), 89);
        assertEquals(cache.getSize(), 0);

        for (int i = 0; i < 100; i++) {
            putToCache(cache, i, Integer.toString(i));
        }

        assertEquals(cache.getSize(), 100);

        res = cache.removeRange(createPosition(10), createPosition(20),  false);
        assertEquals((int) res.getLeft(), 10);
        assertEquals((long) res.getRight(), 10);
        assertEquals(cache.getSize(), 90);
    }

    @Test
    public void testPutWhileClearIsCalledConcurrently() {
        RangeCacheRemovalQueue removalQueue = new RangeCacheRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        int numberOfThreads = 8;
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            executor.scheduleWithFixedDelay(cache::clear, 0, 1, TimeUnit.MILLISECONDS);
        }
        for (int i = 0; i < 200000; i++) {
            putToCache(cache, i, Integer.toString(i));
        }
        executor.shutdown();
        // ensure that no clear operation got into endless loop
        Awaitility.await().untilAsserted(() -> assertTrue(executor.isTerminated()));
        // ensure that clear can be called and all entries are removed
        cache.clear();
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    public void testPutSameObj() {
        RangeCacheRemovalQueue removalQueue = new RangeCacheRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        CachedEntry s0 = new CachedEntry("zero", 0);
        assertEquals(s0.refCnt(), 1);
        assertTrue(cache.put(0, s0));
        assertFalse(cache.put(0, s0));
    }

    @Test
    public void testRemoveEntryWithInvalidRefCount() {
        RangeCacheRemovalQueue removalQueue = new RangeCacheRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        CachedEntry value = createCachedEntry("1");
        cache.put(1, value);
        // release the value to make the reference count invalid
        value.release();
        cache.clear();
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    public void testRemoveEntryWithInvalidMatchingKey() {
        RangeCacheRemovalQueue removalQueue = new RangeCacheRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        CachedEntry value = createCachedEntry("1");
        cache.put(1, value);
        // change the matching key to make it invalid
        value.setMatchingKey(123);
        cache.clear();
        assertEquals(cache.getNumberOfEntries(), 0);
    }

    @Test
    public void testGetKeyWithDifferentInstance() {
        RangeCacheRemovalQueue removalQueue = new RangeCacheRemovalQueue();
        RangeCache cache = new RangeCache(removalQueue);
        Integer key = 129;
        cache.put(key, new RefString("129"));
        // create a different instance of the key
        Integer key2 = Integer.valueOf(129);
        // key and key2 are different instances but they are equal
        assertNotSame(key, key2);
        assertEquals(key, key2);
        // get the value using key2
        RefString s = cache.get(key2);
        // the value should be found
        assertEquals(s.s, "129");
    }
}
