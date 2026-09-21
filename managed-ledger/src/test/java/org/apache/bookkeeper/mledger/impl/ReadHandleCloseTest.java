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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.util.Futures.PhysicalCloseFuture;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReadHandleCloseTest extends MockedBookKeeperTestCase {
    @DataProvider
    public Object[][] lateOpenCases() {
        return new Object[][] {{false, false, false}, {false, true, false},
                {true, false, false}, {true, true, false}, {false, false, true}, {false, true, true},
                {true, false, true}, {true, true, true}};
    }

    @Test(dataProvider = "lateOpenCases")
    public void testCloseJoinsLateReadOpenAndDisposal(boolean evicted, boolean fail, boolean offloaded)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            HeldRead read = fixture.read(offloaded);
            CompletableFuture<ReadHandle> opening = fixture.ledger.reopenReadHandle(read.handle().getId());
            read.started().get(5, TimeUnit.SECONDS);
            CompletableFuture<ReadHandle> shared = fixture.ledger.getLedgerHandle(read.handle().getId());
            assertThat(opening.cancel(false)).isTrue();
            if (evicted) {
                // Eviction must not remove the pending operation from the physical-close snapshot.
                synchronized (fixture.ledger) {
                    fixture.ledger.invalidateReadHandle(read.handle().getId());
                }
            }
            PhysicalCloseFuture first = new PhysicalCloseFuture();
            PhysicalCloseFuture repeated = new PhysicalCloseFuture();
            fixture.ledger.asyncClose(first, null);
            fixture.ledger.asyncClose(repeated, null);
            assertThat(first.cancel(false)).isTrue();
            assertPending(repeated);
            assertThat(fixture.ledger.getLedgerHandle(read.handle().getId() + 100)).isCompletedExceptionally();
            read.opened().complete(read.handle());
            read.closeStarted().get(5, TimeUnit.SECONDS);
            assertPending(repeated);
            assertThatThrownBy(() -> shared.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerAlreadyClosedException.class);
            release(read, fail);
            assertClosed(repeated, fail);
            PhysicalCloseFuture last = new PhysicalCloseFuture();
            fixture.ledger.asyncClose(last, null);
            assertClosed(last, fail);
            verify(read.handle(), times(1)).closeAsync();
            verify(fixture.bookKeeper, times(offloaded ? 0 : 1)).newOpenLedgerOp();
            assertThat(opening).isCancelled();
            Awaitility.await().untilAsserted(() -> assertThat(fixture.ledger.pendingReadHandleOperations()).isZero());
        }
    }

    @DataProvider
    public Object[][] offloadedReads() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "offloadedReads")
    public void testFailedReadOpenDoesNotPoisonPhysicalCleanup(boolean offloaded) throws Exception {
        try (Fixture fixture = new Fixture()) {
            HeldRead read = fixture.read(offloaded);
            CompletableFuture<ReadHandle> opening = fixture.ledger.getLedgerHandle(read.handle().getId());
            read.opened().completeExceptionally(BKException.create(BKException.Code.NoSuchLedgerExistsException));
            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);
            Awaitility.await().untilAsserted(() -> assertThat(fixture.ledger.pendingReadHandleOperations()).isZero());
            PhysicalCloseFuture closing = new PhysicalCloseFuture();
            fixture.ledger.asyncClose(closing, null);
            closing.get(5, TimeUnit.SECONDS);
            verify(read.handle(), never()).closeAsync();
        }
    }

    @Test
    public void testClosedLedgerNeverStartsReadOpen() throws Exception {
        try (Fixture fixture = new Fixture()) {
            HeldRead read = fixture.read(false);
            PhysicalCloseFuture closing = new PhysicalCloseFuture();
            fixture.ledger.asyncClose(closing, null);
            closing.get(5, TimeUnit.SECONDS);
            assertThat(fixture.ledger.getLedgerHandle(read.handle().getId())).isCompletedExceptionally();
            assertThat(fixture.ledger.reopenReadHandle(read.handle().getId())).isCompletedExceptionally();
            verify(fixture.bookKeeper, never()).newOpenLedgerOp();
        }
    }

    @Test
    public void testFailedEvictionCleanupRemainsInCloseBarrier() throws Exception {
        try (Fixture fixture = new Fixture()) {
            HeldRead read = fixture.read(false);
            CompletableFuture<ReadHandle> opening = fixture.ledger.getLedgerHandle(read.handle().getId());
            read.opened().complete(read.handle());
            assertThat(opening.get(5, TimeUnit.SECONDS)).isSameAs(read.handle());
            fixture.ledger.invalidateReadHandle(read.handle().getId());
            read.closeStarted().get(5, TimeUnit.SECONDS);
            release(read, true);
            Awaitility.await().untilAsserted(() -> assertThat(fixture.ledger.pendingReadHandleOperations()).isZero());
            PhysicalCloseFuture closing = new PhysicalCloseFuture();
            fixture.ledger.asyncClose(closing, null);
            assertClosed(closing, true);
        }
    }

    @Test
    public void testOldReadFailureCannotEvictReplacementGeneration() throws Exception {
        try (Fixture fixture = new Fixture()) {
            HeldRead old = fixture.read(false);
            CompletableFuture<ReadHandle> first = fixture.ledger.getLedgerHandle(old.handle().getId());
            old.opened().complete(old.handle());
            assertThat(first.get(5, TimeUnit.SECONDS)).isSameAs(old.handle());
            HeldRead next = fixture.read(false, old.handle().getId());
            CompletableFuture<ReadHandle> replacement = fixture.ledger.reopenReadHandle(old.handle().getId());
            old.closeStarted().get(5, TimeUnit.SECONDS);
            next.started().get(5, TimeUnit.SECONDS);
            fixture.ledger.invalidateLedgerHandle(old.handle());
            CompletableFuture<ReadHandle> joined = fixture.ledger.getLedgerHandle(old.handle().getId());
            next.opened().complete(next.handle());
            assertThat(replacement.get(5, TimeUnit.SECONDS)).isSameAs(next.handle());
            assertThat(joined.get(5, TimeUnit.SECONDS)).isSameAs(next.handle());
            verify(fixture.bookKeeper, times(2)).newOpenLedgerOp();
            PhysicalCloseFuture closing = new PhysicalCloseFuture();
            fixture.ledger.asyncClose(closing, null);
            next.closeStarted().get(5, TimeUnit.SECONDS);
            release(old, false);
            assertPending(closing);
            release(next, false);
            closing.get(5, TimeUnit.SECONDS);
            verify(old.handle(), times(1)).closeAsync();
            verify(next.handle(), times(1)).closeAsync();
        }
    }

    @Test
    public void testRejectedCallbackDispatchDisposesAcquiredReadHandle() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            HeldRead read = fixture.read(false);
            CompletableFuture<ReadHandle> opening = fixture.ledger.getLedgerHandle(read.handle().getId());
            read.started().get(5, TimeUnit.SECONDS);
            // Reject managed-ledger publication without shutting down the BookKeeper worker that must
            // still complete the unrelated current writer's physical close.
            doThrow(new RejectedExecutionException("Reject read publication"))
                    .when(fixture.ledger.getExecutor()).execute(any(Runnable.class));
            read.opened().complete(read.handle());
            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(RejectedExecutionException.class);
            assertThat(fixture.ledger.getStats().getPendingBookieOpsStats().dataLedgerOpenOp).isZero();
            read.closeStarted().get(5, TimeUnit.SECONDS);
            PhysicalCloseFuture closing = new PhysicalCloseFuture();
            fixture.ledger.asyncClose(closing, null);
            assertPending(closing);
            release(read, false);
            closing.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReadOpenStartedUnderMonitorRunsOutsideIt() throws Exception {
        try (Fixture fixture = new Fixture()) {
            HeldRead read = fixture.read(false);
            CompletableFuture<ReadHandle> opening;
            synchronized (fixture.ledger) {
                opening = fixture.ledger.getLedgerHandle(read.handle().getId());
            }
            read.started().get(5, TimeUnit.SECONDS);
            read.opened().complete(read.handle());
            assertThat(opening.get(5, TimeUnit.SECONDS)).isSameAs(read.handle());
        }
    }

    private final class Fixture implements AutoCloseable {
        private final BookKeeper bookKeeper = spy(bkc);
        private final ManagedLedgerFactoryImpl localFactory;
        private final ManagedLedgerImpl ledger;
        private final List<HeldRead> reads = new ArrayList<>();

        private Fixture() throws Exception {
            this(false);
        }

        private Fixture(boolean rejectablePublication) throws Exception {
            if (rejectablePublication) {
                OrderedExecutor workers = mock(OrderedExecutor.class, delegatesTo(bkc.getMainWorkerPool()));
                ThreadBoundExecutor callbackExecutor = mock(ThreadBoundExecutor.class,
                        delegatesTo(bkc.getMainWorkerPool().chooseThread("read-close")));
                doReturn(callbackExecutor).when(workers).chooseThread("read-close");
                doReturn(workers).when(bookKeeper).getMainWorkerPool();
            }
            localFactory = new ManagedLedgerFactoryImpl(metadataStore, bookKeeper);
            ledger = (ManagedLedgerImpl) localFactory.open("read-close", defaultConfig());
        }

        private HeldRead read(boolean offloaded) throws Exception {
            LedgerHandle writer = bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]);
            writer.close();
            return read(offloaded, writer.getId());
        }

        private HeldRead read(boolean offloaded, long id) throws Exception {
            LedgerHandle real = (LedgerHandle) bkc.newOpenLedgerOp().withLedgerId(id).withRecovery(false)
                    .withDigestType(BookKeeper.DigestType.CRC32C.toApiDigestType()).withPassword(new byte[0])
                    .execute().get(5, TimeUnit.SECONDS);
            LedgerHandle handle = spy(real);
            HeldRead read = new HeldRead(real, handle, new CompletableFuture<>(), new CompletableFuture<>(),
                    new CompletableFuture<>(), new CompletableFuture<>());
            reads.add(read);
            doAnswer(invocation -> {
                assertThat(Thread.holdsLock(ledger)).isFalse();
                read.closeStarted().complete(null);
                return read.closed();
            }).when(handle).closeAsync();
            if (offloaded) {
                LedgerInfo info = new LedgerInfo().setLedgerId(id);
                info.setOffloadContext().setComplete(true).setUidMsb(0).setUidLsb(1);
                ledger.ledgers.put(id, info);
                LedgerOffloader offloader = mock(LedgerOffloader.class);
                doReturn("held").when(offloader).getOffloadDriverName();
                ledger.getConfig().setLedgerOffloader(offloader);
                doAnswer(invocation -> {
                    assertThat(Thread.holdsLock(ledger)).isFalse();
                    read.started().complete(null);
                    return read.opened();
                }).when(offloader).readOffloaded(anyLong(), any(UUID.class), any());
            } else {
                OpenBuilder builder = mock(OpenBuilder.class, RETURNS_SELF);
                doAnswer(invocation -> {
                    assertThat(Thread.holdsLock(ledger)).isFalse();
                    read.started().complete(null);
                    return read.opened();
                }).when(builder).execute();
                doReturn(builder).when(bookKeeper).newOpenLedgerOp();
            }
            return read;
        }

        @Override
        public void close() throws Exception {
            for (HeldRead read : reads) {
                read.opened().complete(read.handle());
                read.closed().complete(null);
            }
            PhysicalCloseFuture closing = new PhysicalCloseFuture();
            ledger.asyncClose(closing, null);
            closing.exceptionally(error -> null).get(5, TimeUnit.SECONDS);
            for (HeldRead read : reads) {
                read.real().closeAsync().get(5, TimeUnit.SECONDS);
            }
            localFactory.shutdown();
        }
    }

    private record HeldRead(LedgerHandle real, LedgerHandle handle, CompletableFuture<ReadHandle> opened,
                            CompletableFuture<Void> started, CompletableFuture<Void> closed,
                            CompletableFuture<Void> closeStarted) { }

    private static void release(HeldRead read, boolean fail) {
        if (fail) {
            read.closed().completeExceptionally(BKException.create(BKException.Code.WriteException));
        } else {
            read.closed().complete(null);
        }
    }

    private static void assertPending(CompletableFuture<?> future) {
        assertThatThrownBy(() -> future.get(200, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }

    private static void assertClosed(PhysicalCloseFuture future, boolean failed) throws Exception {
        if (failed) {
            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(ManagedLedgerException.class);
        } else {
            future.get(5, TimeUnit.SECONDS);
        }
    }
}
