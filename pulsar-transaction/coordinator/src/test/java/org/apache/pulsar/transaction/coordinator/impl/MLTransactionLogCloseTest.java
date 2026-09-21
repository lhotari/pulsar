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
package org.apache.pulsar.transaction.coordinator.impl;

import static org.apache.pulsar.transaction.coordinator.impl.DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.proto.TransactionMetadataEntry;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MLTransactionLogCloseTest {
    @DataProvider
    public Object[][] closureCases() {
        return new Object[][] {{false, false, false}, {false, true, false}, {false, true, true},
                {true, false, false}, {true, true, false}, {true, true, true}};
    }

    @Test(dataProvider = "closureCases")
    public void testCloseJoinsInitializationWriterAndLedger(boolean cursorFailure, boolean batch,
                                                           boolean closeFailure) throws Exception {
        Fixture fixture = new Fixture(batch);
        CompletableFuture<Void> opening = fixture.log.initialize();
        CompletableFuture<Void> duplicateOpen = fixture.log.initialize();
        assertThat(opening.cancel(false)).isTrue();
        CompletableFuture<Void> firstClose = fixture.log.closeAsync();
        CompletableFuture<Void> repeated = fixture.log.closeAsync();
        assertThat(firstClose.cancel(false)).isTrue();
        assertThat(repeated).isNotDone();
        fixture.opened.get().openLedgerComplete(fixture.ledger, null);
        assertThat(repeated).isNotDone();
        verify(fixture.ledger, never()).asyncClose(any(), any());
        if (cursorFailure) {
            fixture.cursor.get().openCursorFailed(new ManagedLedgerException("cursor failed"), null);
        } else {
            fixture.cursor.get().openCursorComplete(mock(ManagedCursor.class), null);
        }
        if (batch) {
            verify(fixture.ledger, never()).asyncClose(any(), any());
            assertThat(repeated).isNotDone();
            assertThat(fixture.tasks).hasSize(1);
            fixture.tasks.remove().run();
        }
        AsyncCallbacks.CloseCallback callback = fixture.closed.get(5, TimeUnit.SECONDS);
        assertThat(repeated).isNotDone();
        fixture.finishClose(callback, closeFailure);
        assertCloseResult(repeated, closeFailure);
        assertCloseResult(fixture.log.closeAsync(), closeFailure);
        assertThatThrownBy(() -> duplicateOpen.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerException.class);
        assertThat(fixture.log.initialize()).isCompletedExceptionally();
        verify(fixture.factory, times(1)).asyncOpen(any(), any(), any(), any(), any());
        verify(fixture.ledger, times(1)).asyncClose(any(), any());
        if (batch) {
            verify(fixture.timeout).cancel();
        }
    }

    @Test
    public void testCursorFailureAutomaticallyJoinsCleanupBeforeInitializationFails() throws Exception {
        Fixture fixture = new Fixture(true);
        CompletableFuture<Void> opening = fixture.log.initialize();
        fixture.opened.get().openLedgerComplete(fixture.ledger, null);
        fixture.cursor.get().openCursorFailed(new ManagedLedgerException("cursor failed"), null);
        assertThat(opening).isNotDone();
        fixture.tasks.remove().run();
        assertThat(opening).isNotDone();
        fixture.finishClose(fixture.closed.get(5, TimeUnit.SECONDS), true);
        assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(ManagedLedgerException.class)
                .satisfies(error -> assertThat(error.getCause().getSuppressed()).hasSize(1));
        assertCloseResult(fixture.log.closeAsync(), true);
        verify(fixture.ledger, times(1)).asyncClose(any(), any());
    }

    @DataProvider
    public Object[][] cleanupFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "cleanupFailures")
    public void testFactoryFailureJoinsPhysicalCleanup(boolean failedCleanup) throws Exception {
        Fixture fixture = new Fixture(false);
        CompletableFuture<Void> opening = fixture.log.initialize();
        CompletableFuture<Void> cleanup = new CompletableFuture<>();
        fixture.opened.get().openLedgerFailed(new ManagedLedgerException("open failed"),
                cleanup.minimalCompletionStage(), null);
        CompletableFuture<Void> closing = fixture.log.closeAsync();
        assertThat(closing).isNotDone();
        assertThat(opening).isNotDone();
        if (failedCleanup) {
            cleanup.completeExceptionally(new ManagedLedgerException("cleanup failed"));
        } else {
            cleanup.complete(null);
        }
        assertCloseResult(closing, failedCleanup);
        assertCloseResult(fixture.log.closeAsync(), failedCleanup);
        assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(ManagedLedgerException.class);
        verify(fixture.ledger, never()).asyncClose(any(), any());
    }

    @Test
    public void testCloseBeforeInitializeSealsTheLog() throws Exception {
        Fixture fixture = new Fixture(false);
        fixture.log.closeAsync().get(5, TimeUnit.SECONDS);
        assertThat(fixture.log.initialize()).isCompletedExceptionally();
        verify(fixture.factory, never()).asyncOpen(any(), any(), any(), any(), any());
    }

    @Test
    public void testUntrackedFactoryFailureKeepsCloseFailed() throws Exception {
        Fixture fixture = new Fixture(false);
        CompletableFuture<Void> opening = fixture.log.initialize();
        fixture.opened.get().openLedgerFailed(new ManagedLedgerException("untracked open failure"), null);
        assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(ManagedLedgerException.class);
        assertCloseResult(fixture.log.closeAsync(), true);
        verify(fixture.ledger, never()).asyncClose(any(), any());
    }

    @Test
    public void testRejectedWriterCleanupStillJoinsLedgerClose() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.initialize();
        ThreadBoundExecutor executor = fixture.ledger.getExecutor();
        doThrow(new RejectedExecutionException("writer executor closed")).when(executor).execute(any());
        CompletableFuture<Void> closing = fixture.log.closeAsync();
        AsyncCallbacks.CloseCallback callback = fixture.closed.get(5, TimeUnit.SECONDS);
        assertThat(closing).isNotDone();
        fixture.finishClose(callback, false);
        assertThatThrownBy(() -> closing.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(RejectedExecutionException.class);
        assertThatThrownBy(() -> fixture.log.closeAsync().get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(RejectedExecutionException.class);
        verify(fixture.ledger, times(1)).asyncClose(any(), any());
    }

    @Test
    public void testCloseFailsBufferedAndLateAppendBeforeLedgerCompletion() throws Exception {
        Fixture fixture = new Fixture(true);
        fixture.initialize();
        CompletableFuture<Position> buffered = fixture.log.append(entry());
        fixture.tasks.remove().run();
        assertThat(buffered).isNotDone();
        CompletableFuture<Void> closing = fixture.log.closeAsync();
        CompletableFuture<Position> late = fixture.log.append(entry());
        assertThat(buffered).isNotDone();
        fixture.tasks.remove().run();
        assertThatThrownBy(() -> buffered.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
        fixture.tasks.remove().run();
        assertThatThrownBy(() -> late.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
        assertThat(closing).isNotDone();
        verify(fixture.ledger, never()).asyncAddEntry(any(ByteBuf.class), any(), any());
        fixture.finishClose(fixture.closed.get(5, TimeUnit.SECONDS), false);
        closing.get(5, TimeUnit.SECONDS);
        verify(fixture.ledger, times(1)).asyncClose(any(), any());
    }

    @Test
    public void testCloseSealsLateAppendWhileAnEarlierAppendIsInFlight() throws Exception {
        Fixture fixture = new Fixture(false);
        fixture.initialize();
        CompletableFuture<Runnable> failAppend = new CompletableFuture<>();
        CompletableFuture<ByteBuf> appendedBuffer = new CompletableFuture<>();
        doAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(0);
            AsyncCallbacks.AddEntryCallback callback = invocation.getArgument(1);
            Object context = invocation.getArgument(2);
            appendedBuffer.complete(buffer);
            failAppend.complete(() -> callback.addFailed(new ManagedLedgerException.ManagedLedgerFencedException(),
                    context));
            return null;
        }).when(fixture.ledger).asyncAddEntry(any(ByteBuf.class), any(), any());
        CompletableFuture<Position> writing = fixture.log.append(entry());
        CompletableFuture<Void> closing = fixture.log.closeAsync();
        try {
            CompletableFuture<Position> late = fixture.log.append(entry());
            assertThat(writing).isNotDone();
            assertThatThrownBy(() -> late.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
            failAppend.get(5, TimeUnit.SECONDS).run();
            assertThatThrownBy(() -> writing.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.ManagedLedgerFencedException.class);
            assertThat(appendedBuffer.get(5, TimeUnit.SECONDS).refCnt()).isZero();
            assertThat(closing).isNotDone();
            fixture.finishClose(fixture.closed.get(5, TimeUnit.SECONDS), false);
            closing.get(5, TimeUnit.SECONDS);
            verify(fixture.ledger, times(1)).asyncAddEntry(any(ByteBuf.class), any(), any());
            verify(fixture.ledger, never()).readyToCreateNewLedger();
        } finally {
            if (!writing.isDone()) {
                failAppend.get(5, TimeUnit.SECONDS).run();
            }
            if (!closing.isDone()) {
                fixture.finishClose(fixture.closed.get(5, TimeUnit.SECONDS), false);
            }
        }
    }

    private static TransactionMetadataEntry entry() {
        return new TransactionMetadataEntry().setTxnidMostBits(0).setTxnidLeastBits(1)
                .setMetadataOp(TransactionMetadataEntry.TransactionMetadataOp.NEW);
    }

    private static void assertCloseResult(CompletableFuture<Void> future, boolean failure) throws Exception {
        if (failure) {
            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(ManagedLedgerException.class);
        } else {
            future.get(5, TimeUnit.SECONDS);
        }
    }

    private static final class Fixture {
        private final ManagedLedgerFactory factory = mock(ManagedLedgerFactory.class);
        private final ManagedLedgerImpl ledger = mock(ManagedLedgerImpl.class);
        private final Timer timer = mock(Timer.class);
        private final Timeout timeout = mock(Timeout.class);
        private final Queue<Runnable> tasks = new ArrayDeque<>();
        private final CompletableFuture<AsyncCallbacks.OpenLedgerCallback> opened = new CompletableFuture<>();
        private final CompletableFuture<AsyncCallbacks.OpenCursorCallback> cursor = new CompletableFuture<>();
        private final CompletableFuture<AsyncCallbacks.CloseCallback> closed = new CompletableFuture<>();
        private final MLTransactionLogImpl log;

        private Fixture(boolean batch) {
            TxnLogBufferedWriterConfig config = new TxnLogBufferedWriterConfig();
            config.setBatchEnabled(batch);
            log = new MLTransactionLogImpl(TransactionCoordinatorID.get(0), factory, new ManagedLedgerConfig(),
                    config, timer, DISABLED_BUFFERED_WRITER_METRICS);
            ThreadBoundExecutor executor = mock(ThreadBoundExecutor.class);
            when(ledger.getExecutor()).thenReturn(executor);
            doAnswer(invocation -> {
                tasks.add(invocation.getArgument(0));
                return null;
            }).when(executor).execute(any());
            when(timer.newTimeout(any(), anyLong(), any())).thenReturn(timeout);
            doAnswer(invocation -> {
                assertThat(Thread.holdsLock(log)).isFalse();
                opened.complete(invocation.getArgument(2));
                return null;
            }).when(factory).asyncOpen(any(), any(), any(), any(), any());
            doAnswer(invocation -> {
                assertThat(Thread.holdsLock(log)).isFalse();
                cursor.complete(invocation.getArgument(2));
                return null;
            }).when(ledger).asyncOpenCursor(any(), any(), any(), any());
            doAnswer(invocation -> {
                assertThat(Thread.holdsLock(log)).isFalse();
                closed.complete(invocation.getArgument(0));
                return null;
            }).when(ledger).asyncClose(any(), any());
        }

        private void initialize() throws Exception {
            CompletableFuture<Void> opening = log.initialize();
            opened.get(5, TimeUnit.SECONDS).openLedgerComplete(ledger, null);
            cursor.get(5, TimeUnit.SECONDS).openCursorComplete(mock(ManagedCursor.class), null);
            opening.get(5, TimeUnit.SECONDS);
        }

        private void finishClose(AsyncCallbacks.CloseCallback callback, boolean failure) {
            if (failure) {
                callback.closeFailed(new ManagedLedgerException("cleanup failed"), null);
            } else {
                callback.closeComplete(null);
            }
        }
    }
}
