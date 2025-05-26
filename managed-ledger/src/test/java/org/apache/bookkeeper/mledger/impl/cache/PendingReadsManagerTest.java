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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertSame;
import io.opentelemetry.api.OpenTelemetry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class PendingReadsManagerTest  {

    static final Object CTX = "foo";
    static final Object CTX2 = "far";
    static final long ledgerId = 123414L;
    ExecutorService orderedExecutor;

    PendingReadsManagerTest() {
    }

    @BeforeClass(alwaysRun = true)
    void before() {
        orderedExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterClass(alwaysRun = true)
    void after() {
        if (orderedExecutor != null) {
            orderedExecutor.shutdown();
            orderedExecutor = null;
        }
    }


    RangeEntryCacheImpl rangeEntryCache;
    PendingReadsManager pendingReadsManager;
    InflightReadsLimiter inflighReadsLimiter;
    ReadHandle lh;
    ManagedLedgerImpl ml;

    @BeforeMethod(alwaysRun = true)
    void setupMocks() {
        rangeEntryCache = mock(RangeEntryCacheImpl.class);
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setReadEntryTimeoutSeconds(10000);
        when(rangeEntryCache.getName()).thenReturn("my-topic");
        when(rangeEntryCache.getManagedLedgerConfig()).thenReturn(config);
        inflighReadsLimiter = new InflightReadsLimiter(0, 0, 0,
                mock(ScheduledExecutorService.class), OpenTelemetry.noop());
        when(rangeEntryCache.getPendingReadsLimiter()).thenReturn(inflighReadsLimiter);
        pendingReadsManager = new PendingReadsManager(rangeEntryCache);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                log.info("rangeEntryCache asyncReadEntry0 {}", invocationOnMock);
                ReadHandle rh = invocationOnMock.getArgument(0);
                long startEntry = invocationOnMock.getArgument(1);
                long endEntry = invocationOnMock.getArgument(2);
                ToIntFunction<Entry> expectedReadCount = invocationOnMock.getArgument(3);
                AsyncCallbacks.ReadEntriesCallback callback = invocationOnMock.getArgument(4);
                Object ctx = invocationOnMock.getArgument(5);
                pendingReadsManager.readEntries(lh, startEntry, endEntry, expectedReadCount, callback, ctx);
                return null;
            }
        }).when(rangeEntryCache).asyncReadEntry0(any(), anyLong(), anyLong(),
                any(), any(), any(), anyBoolean());

        lh = mock(ReadHandle.class);
        ml = mock(ManagedLedgerImpl.class);
        when(ml.getExecutor()).thenReturn(orderedExecutor);
        when(rangeEntryCache.getManagedLedger()).thenReturn(ml);
    }


    @Data
    private static class CapturingReadEntriesCallback extends CompletableFuture<Void>
            implements AsyncCallbacks.ReadEntriesCallback  {
        List<Position> entries;
        Object ctx;
        Throwable error;

        @Override
        public synchronized void readEntriesComplete(List<Entry> entries, Object ctx) {
            this.entries = entries.stream().map(Entry::getPosition).collect(Collectors.toList());
            this.ctx = ctx;
            this.error = null;
            this.complete(null);
        }

        @Override
        public synchronized void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
            this.entries = null;
            this.ctx = ctx;
            this.error = exception;
            this.completeExceptionally(exception);
        }

    }

    private static List<EntryImpl> buildList(long start, long end) {
        List<EntryImpl> result = new ArrayList<>();
        for (long i = start; i <= end; i++) {
            long entryId = i;
            EntryImpl entry = EntryImpl.create(ledgerId, entryId, "data".getBytes(StandardCharsets.UTF_8));
            result.add(entry);
        }
        return result;
    }


    private void verifyRange(List<Position> entries, long firstEntry, long endEntry) {
        int pos = 0;
        log.info("verifyRange numEntries {}", entries.size());
        for (long entry = firstEntry; entry <= endEntry; entry++) {
            assertEquals(entries.get(pos++).getEntryId(), entry);
        }
    }

    private static class PreparedReadFromStorage extends CompletableFuture<List<EntryImpl>> {
        final long firstEntry;
        final long endEntry;
        final ToIntFunction<Entry> expectedReadCount;

        public PreparedReadFromStorage(long firstEntry, long endEntry, ToIntFunction<Entry> expectedReadCount) {
            this.firstEntry = firstEntry;
            this.endEntry = endEntry;
            this.expectedReadCount = expectedReadCount;
        }

        @Override
        public String toString() {
            return "PreparedReadFromStorage("+firstEntry+","+endEntry+","+expectedReadCount+")";
        }

        public void storageReadCompleted() {
            this.complete(buildList(firstEntry, endEntry));
        }
    }

    private PreparedReadFromStorage prepareReadFromStorage(ReadHandle lh, RangeEntryCacheImpl rangeEntryCache,
                                                           long firstEntry, long endEntry, ToIntFunction<Entry> expectedReadCount) {
        PreparedReadFromStorage read = new PreparedReadFromStorage(firstEntry, endEntry, expectedReadCount);
        log.info("prepareReadFromStorage from {} to {} expectedReadCount {}", firstEntry, endEntry, expectedReadCount);
        when(rangeEntryCache.readFromStorage(eq(lh), eq(firstEntry), eq(endEntry), eq(expectedReadCount))).thenAnswer(
                (invocationOnMock -> {
                    log.info("readFromStorage from {} to {} expectedReadCount {}", firstEntry, endEntry, expectedReadCount);
                    return read;
                })
        );
        return read;
    }

    @Test
    public void simpleRead() throws Exception {

        long firstEntry = 100;
        long endEntry = 199;
        ToIntFunction<Entry> expectedReadCount = entry -> 0;

        PreparedReadFromStorage read1
                = prepareReadFromStorage(lh, rangeEntryCache, firstEntry, endEntry, expectedReadCount);

        CapturingReadEntriesCallback callback = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntry, endEntry, expectedReadCount, callback, CTX);

        // complete the read
        read1.storageReadCompleted();

        // wait for the callback to complete
        callback.get();
        assertSame(callback.getCtx(), CTX);

        // verify
        verifyRange(callback.entries, firstEntry, endEntry);
    }


    @Test
    public void simpleConcurrentReadPerfectMatch() throws Exception {

        long firstEntry = 100;
        long endEntry = 199;
        ToIntFunction<Entry> expectedReadCount = entry -> 0;

        PreparedReadFromStorage read1 = prepareReadFromStorage(lh, rangeEntryCache, firstEntry, endEntry, expectedReadCount);

        PendingReadsManager pendingReadsManager = new PendingReadsManager(rangeEntryCache);
        CapturingReadEntriesCallback callback = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntry, endEntry, expectedReadCount, callback, CTX);

        CapturingReadEntriesCallback callback2 = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntry, endEntry, expectedReadCount, callback2, CTX2);

        // complete the read from BK
        // only one read completes 2 callbacks
        read1.storageReadCompleted();

        callback.get();
        callback2.get();

        assertSame(callback.getCtx(), CTX);
        assertSame(callback2.getCtx(), CTX2);

        verifyRange(callback.entries, firstEntry, endEntry);
        verifyRange(callback2.entries, firstEntry, endEntry);

        int pos = 0;
        for (long entry = firstEntry; entry <= endEntry; entry++) {;
            assertNotSame(callback.entries.get(pos), callback2.entries.get(pos));
            assertEquals(callback.entries.get(pos).getEntryId(), callback2.entries.get(pos).getEntryId());
            pos++;
        }

    }

    @Test
    public void simpleConcurrentReadIncluding() throws Exception {

        long firstEntry = 100;
        long endEntry = 199;

        long firstEntrySecondRead = firstEntry + 10;
        long endEntrySecondRead = endEntry - 10;

        ToIntFunction<Entry> expectedReadCount = entry -> 0;

        PreparedReadFromStorage read1 = prepareReadFromStorage(lh, rangeEntryCache, firstEntry, endEntry, expectedReadCount);

        PendingReadsManager pendingReadsManager = new PendingReadsManager(rangeEntryCache);
        CapturingReadEntriesCallback callback = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntry, endEntry, expectedReadCount, callback, CTX);


        CapturingReadEntriesCallback callback2 = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntrySecondRead, endEntrySecondRead, expectedReadCount, callback2, CTX2);

        // complete the read from BK
        // only one read completes 2 callbacks
        read1.storageReadCompleted();

        callback.get();
        callback2.get();

        assertSame(callback.getCtx(), CTX);
        assertSame(callback2.getCtx(), CTX2);

        verifyRange(callback.entries, firstEntry, endEntry);
        verifyRange(callback2.entries, firstEntrySecondRead, endEntrySecondRead);

        int pos = 0;
        for (long entry = firstEntry; entry <= endEntry; entry++) {;
            if (entry >= firstEntrySecondRead && entry <= endEntrySecondRead) {
                int posInSecondList = (int) (pos - (firstEntrySecondRead - firstEntry));
                assertNotSame(callback.entries.get(pos), callback2.entries.get(posInSecondList));
                assertEquals(callback.entries.get(pos).getEntryId(), callback2.entries.get(posInSecondList).getEntryId());
            }
            pos++;
        }

    }

    @Test
    public void simpleConcurrentReadMissingLeft() throws Exception {

        long firstEntry = 100;
        long endEntry = 199;

        long firstEntrySecondRead = firstEntry - 10;
        long endEntrySecondRead = endEntry;

        ToIntFunction<Entry> expectedReadCount = entry -> 0;

        PreparedReadFromStorage read1 =
                prepareReadFromStorage(lh, rangeEntryCache, firstEntry, endEntry, expectedReadCount);

        PreparedReadFromStorage readForLeft =
                prepareReadFromStorage(lh, rangeEntryCache, firstEntrySecondRead, firstEntry - 1, expectedReadCount);

        PendingReadsManager pendingReadsManager = new PendingReadsManager(rangeEntryCache);
        CapturingReadEntriesCallback callback = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntry, endEntry, expectedReadCount, callback, CTX);

        CapturingReadEntriesCallback callback2 = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntrySecondRead, endEntrySecondRead, expectedReadCount, callback2, CTX2);

        // complete the read from BK
        read1.storageReadCompleted();
        // the first read can move forward
        callback.get();

        readForLeft.storageReadCompleted();
        callback2.get();

        assertSame(callback.getCtx(), CTX);
        assertSame(callback2.getCtx(), CTX2);

        verifyRange(callback.entries, firstEntry, endEntry);
        verifyRange(callback2.entries, firstEntrySecondRead, endEntrySecondRead);

    }

    @Test
    public void simpleConcurrentReadMissingRight() throws Exception {

        long firstEntry = 100;
        long endEntry = 199;

        long firstEntrySecondRead = firstEntry;
        long endEntrySecondRead = endEntry + 10;

        ToIntFunction<Entry> expectedReadCount = entry -> 0;

        PreparedReadFromStorage read1 =
                prepareReadFromStorage(lh, rangeEntryCache, firstEntry, endEntry, expectedReadCount);

        PreparedReadFromStorage readForRight =
                prepareReadFromStorage(lh, rangeEntryCache, endEntry + 1, endEntrySecondRead, expectedReadCount);

        PendingReadsManager pendingReadsManager = new PendingReadsManager(rangeEntryCache);
        CapturingReadEntriesCallback callback = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntry, endEntry, expectedReadCount, callback, CTX);

        CapturingReadEntriesCallback callback2 = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntrySecondRead, endEntrySecondRead, expectedReadCount, callback2, CTX2);

        // complete the read from BK
        read1.storageReadCompleted();
        // the first read can move forward
        callback.get();

        readForRight.storageReadCompleted();
        callback2.get();

        assertSame(callback.getCtx(), CTX);
        assertSame(callback2.getCtx(), CTX2);

        verifyRange(callback.entries, firstEntry, endEntry);
        verifyRange(callback2.entries, firstEntrySecondRead, endEntrySecondRead);

    }

    @Test
    public void simpleConcurrentReadMissingBoth() throws Exception {

        long firstEntry = 100;
        long endEntry = 199;

        long firstEntrySecondRead = firstEntry - 10;
        long endEntrySecondRead = endEntry + 10;

        ToIntFunction<Entry> expectedReadCount = entry -> 0;

        PreparedReadFromStorage read1 =
                prepareReadFromStorage(lh, rangeEntryCache, firstEntry, endEntry, expectedReadCount);

        PreparedReadFromStorage readForLeft =
                prepareReadFromStorage(lh, rangeEntryCache, firstEntrySecondRead, firstEntry - 1, expectedReadCount);

        PreparedReadFromStorage readForRight =
                prepareReadFromStorage(lh, rangeEntryCache, endEntry + 1, endEntrySecondRead, expectedReadCount);

        PendingReadsManager pendingReadsManager = new PendingReadsManager(rangeEntryCache);
        CapturingReadEntriesCallback callback = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntry, endEntry, expectedReadCount, callback, CTX);

        CapturingReadEntriesCallback callback2 = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntrySecondRead, endEntrySecondRead, expectedReadCount, callback2, CTX2);

        // complete the read from BK
        read1.storageReadCompleted();
        // the first read can move forward
        callback.get();

        readForLeft.storageReadCompleted();
        readForRight.storageReadCompleted();
        callback2.get();

        assertSame(callback.getCtx(), CTX);
        assertSame(callback2.getCtx(), CTX2);

        verifyRange(callback.entries, firstEntry, endEntry);
        verifyRange(callback2.entries, firstEntrySecondRead, endEntrySecondRead);

    }


    @Test
    public void simpleConcurrentReadNoMatch() throws Exception {
        long firstEntry = 100;
        long endEntry = 199;

        long firstEntrySecondRead = 1000;
        long endEntrySecondRead = 1099;

        ToIntFunction<Entry> expectedReadCount = entry -> 0;

        PreparedReadFromStorage read1 =
                prepareReadFromStorage(lh, rangeEntryCache, firstEntry, endEntry, expectedReadCount);

        PreparedReadFromStorage read2 =
                prepareReadFromStorage(lh, rangeEntryCache, firstEntrySecondRead, endEntrySecondRead, expectedReadCount);

        PendingReadsManager pendingReadsManager = new PendingReadsManager(rangeEntryCache);
        CapturingReadEntriesCallback callback = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntry, endEntry, expectedReadCount, callback, CTX);

        CapturingReadEntriesCallback callback2 = new CapturingReadEntriesCallback();
        pendingReadsManager.readEntries(lh, firstEntrySecondRead, endEntrySecondRead, expectedReadCount, callback2, CTX2);

        read1.storageReadCompleted();
        callback.get();

        read2.storageReadCompleted();
        callback2.get();

        assertSame(callback.getCtx(), CTX);
        assertSame(callback2.getCtx(), CTX2);

        verifyRange(callback.entries, firstEntry, endEntry);
        verifyRange(callback2.entries, firstEntrySecondRead, endEntrySecondRead);

    }

}
