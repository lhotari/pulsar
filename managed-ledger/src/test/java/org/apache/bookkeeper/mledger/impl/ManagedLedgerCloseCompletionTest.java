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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.VoidCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.util.Futures.CloseFuture;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerCloseCompletionTest extends MockedBookKeeperTestCase {
    @DataProvider
    public Object[][] closeFailures() {
        return new Object[][] {{false}, {true}};
    }

    @DataProvider
    public Object[][] ledgerCloseFailures() {
        return new Object[][] {{false, false}, {true, false}, {false, true}};
    }

    @Test(dataProvider = "ledgerCloseFailures")
    public void testRepeatedLedgerCloseSharesPhysicalResult(boolean fail, boolean fenced) throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("ledger-close", defaultConfig());
        LedgerHandle handle = spy(ledger.currentLedger);
        ledger.currentLedger = handle;
        CompletableFuture<AsyncCallback.CloseCallback> physicalClose = new CompletableFuture<>();
        doAnswer(invocation -> {
            physicalClose.complete(invocation.getArgument(0));
            return null;
        }).when(handle).asyncClose(any(), any());
        if (fenced) {
            ledger.setFenced();
        }
        CloseFuture first = new CloseFuture();
        CloseFuture second = new CloseFuture();
        boolean released = false;
        try {
            ledger.asyncClose(first, "first");
            AsyncCallback.CloseCallback callback = physicalClose.get(5, TimeUnit.SECONDS);
            ledger.asyncClose(second, "second");
            assertThat(first).isNotDone();
            assertThat(second).as("a logical Closed state is not physical completion").isNotDone();
            released = true;
            callback.closeComplete(fail ? BKException.Code.WriteException : BKException.Code.OK, handle, null);
            assertResult(first, fail || fenced);
            assertResult(second, fail || fenced);
            CloseFuture third = new CloseFuture();
            ledger.asyncClose(third, "third");
            assertResult(third, fail || fenced);
            verify(handle, times(1)).asyncClose(any(), any());
        } finally {
            if (!released && physicalClose.isDone()) {
                physicalClose.getNow(null).closeComplete(BKException.Code.OK, handle, null);
            }
        }
    }

    @Test(dataProvider = "closeFailures")
    public void testRepeatedCursorCloseSharesPhysicalResult(boolean fail) throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cursor-close", defaultConfig());
        ManagedCursorImpl cursor = spy((ManagedCursorImpl) ledger.openCursor("cursor"));
        CompletableFuture<AsyncCallbacks.CloseCallback> physicalClose = new CompletableFuture<>();
        doAnswer(invocation -> {
            physicalClose.complete(invocation.getArgument(2));
            return null;
        }).when(cursor).persistPositionWhenClosing(any(), any(), any(), any());
        CloseFuture first = new CloseFuture();
        CloseFuture second = new CloseFuture();
        boolean released = false;
        try {
            cursor.asyncClose(first, "first");
            AsyncCallbacks.CloseCallback callback = physicalClose.get(5, TimeUnit.SECONDS);
            cursor.asyncClose(second, "second");
            assertThat(first).isNotDone();
            assertThat(second).as("a logical Closing state is not physical completion").isNotDone();
            released = true;
            if (fail) {
                callback.closeFailed(new ManagedLedgerException("held cursor persistence failed"), null);
            } else {
                callback.closeComplete(null);
            }
            assertResult(first, fail);
            assertResult(second, fail);
            CloseFuture third = new CloseFuture();
            cursor.asyncClose(third, "third");
            assertResult(third, fail);
            verify(cursor, times(1)).persistPositionWhenClosing(any(), any(), any(), any());
        } finally {
            if (!released && physicalClose.isDone()) {
                physicalClose.getNow(null).closeComplete(null);
            }
        }
    }

    @Test
    public void testLedgerCloseWaitsForEveryCursorAfterFailure() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cursor-fanout", defaultConfig());
        CompletableFuture<AsyncCallbacks.CloseCallback> firstClose = new CompletableFuture<>();
        CompletableFuture<AsyncCallbacks.CloseCallback> secondClose = new CompletableFuture<>();
        addHeldCursor(ledger, "first", firstClose);
        addHeldCursor(ledger, "second", secondClose);
        CloseFuture closing = new CloseFuture();
        boolean firstReleased = false;
        boolean secondReleased = false;
        try {
            ledger.asyncClose(closing, null);
            AsyncCallbacks.CloseCallback first = firstClose.get(5, TimeUnit.SECONDS);
            firstReleased = true;
            first.closeFailed(new ManagedLedgerException("first cursor failed"), null);
            AsyncCallbacks.CloseCallback remaining = secondClose.get(5, TimeUnit.SECONDS);
            assertThat(closing).as("failed sibling does not finish still-running physical work").isNotDone();
            secondReleased = true;
            remaining.closeComplete(null);
            assertResult(closing, true);
        } finally {
            if (!firstReleased && firstClose.isDone()) {
                firstClose.getNow(null).closeComplete(null);
            }
            if (!secondReleased && secondClose.isDone()) {
                secondClose.getNow(null).closeComplete(null);
            }
        }
    }

    @Test
    public void testClosingDeletedCursorDoesNotWriteMetadata() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("deleted-cursor", defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        ledger.deleteCursor("cursor");
        assertThat(cursor.isClosed()).isTrue();
        CloseFuture closing = new CloseFuture();
        cursor.asyncClose(closing, null);
        closing.get(5, TimeUnit.SECONDS);
    }

    @DataProvider
    public Object[][] lateCreates() {
        return new Object[][] {{false, false}, {true, false}, {false, true}, {true, true}};
    }

    @Test(dataProvider = "lateCreates")
    public void testCloseJoinsLateLedgerCreate(boolean timeout, boolean closeFailure) throws Exception {
        BookKeeper bookKeeper = spy(bkc);
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, bookKeeper);
        ManagedLedgerConfig config = defaultConfig().setMaxEntriesPerLedger(1)
                .setMetadataOperationsTimeoutSeconds(timeout ? 1 : 30);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) localFactory.open("late-create", config);
        LedgerHandle realLateHandle = bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]);
        LedgerHandle lateHandle = spy(realLateHandle);
        CompletableFuture<Void> lateClose = new CompletableFuture<>();
        CompletableFuture<Void> lateCloseStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            lateCloseStarted.complete(null);
            return lateClose;
        }).when(lateHandle).closeAsync();
        CreateBuilder builder = mock(CreateBuilder.class, RETURNS_SELF);
        CompletableFuture<WriteHandle> createResult = new CompletableFuture<>();
        CompletableFuture<Void> createStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            createStarted.complete(null);
            return createResult;
        }).when(builder).execute();
        doReturn(builder).when(bookKeeper).newCreateLedgerOp();
        try {
            ledger.addEntry(new byte[] {1});
            createStarted.get(5, TimeUnit.SECONDS);
            if (timeout) {
                Awaitility.await().atMost(5, TimeUnit.SECONDS)
                        .until(() -> ledger.getState() == ManagedLedgerImpl.State.ClosedLedger);
            }
            CompletableFuture<Void> currentClosed = observeCurrentLedgerClose(ledger);
            CloseFuture closing = new CloseFuture();
            ledger.asyncClose(closing, null);
            currentClosed.get(5, TimeUnit.SECONDS);
            assertThat(closing).as("a create timeout is not physical completion").isNotDone();
            createResult.complete(lateHandle);
            lateCloseStarted.get(5, TimeUnit.SECONDS);
            assertThat(closing).as("a late writer handle must finish closing before ownership can move").isNotDone();
            if (closeFailure) {
                lateClose.completeExceptionally(BKException.create(BKException.Code.WriteException));
            } else {
                lateClose.complete(null);
            }
            assertResult(closing, closeFailure);
            assertThat(ledger.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
            verify(lateHandle, times(1)).closeAsync();
        } finally {
            createResult.complete(lateHandle);
            lateClose.complete(null);
            realLateHandle.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "closeFailures")
    public void testCloseJoinsRolloverMetadata(boolean metadataFailure) throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                return new ManagedLedgerImpl(this, bk, spy(store), config, scheduledExecutor, name, ownershipChecker);
            }
        };
        ManagedLedgerConfig config = defaultConfig().setMaxEntriesPerLedger(1)
                .setRetentionTime(1, TimeUnit.HOURS);
        config.setMinimumRolloverTime(0, TimeUnit.SECONDS);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) localFactory.open("rollover-metadata", config);
        CompletableFuture<Runnable> metadataWrite = new CompletableFuture<>();
        doAnswer(invocation -> {
            String name = invocation.getArgument(0);
            ManagedLedgerInfo info = invocation.getArgument(1);
            Stat stat = invocation.getArgument(2);
            MetaStoreCallback<Void> callback = invocation.getArgument(3);
            metadataWrite.complete(() -> {
                if (metadataFailure) {
                    callback.operationFailed(new MetaStoreException("held rollover metadata failed"));
                } else {
                    localFactory.getMetaStore().asyncUpdateLedgerIds(name, info, stat, callback);
                }
            });
            return null;
        }).when(ledger.store).asyncUpdateLedgerIds(any(), any(), any(), any());
        Runnable release = null;
        try {
            ledger.addEntry(new byte[] {1});
            release = metadataWrite.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> currentClosed = observeCurrentLedgerClose(ledger);
            CloseFuture closing = new CloseFuture();
            ledger.asyncClose(closing, null);
            currentClosed.get(5, TimeUnit.SECONDS);
            assertThat(closing).as("the writer's metadata update still owns part of the close barrier").isNotDone();
            release.run();
            release = null;
            closing.get(5, TimeUnit.SECONDS);
            assertThat(ledger.getState()).as("late metadata failure cannot revive a closed ledger")
                    .isEqualTo(ManagedLedgerImpl.State.Closed);
        } finally {
            if (release != null) {
                release.run();
            }
        }
    }

    @Test(dataProvider = "closeFailures")
    public void testCursorCloseWaitsForSubmittedMarkDelete(boolean markDeleteFailure) throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("submitted-mark-delete", defaultConfig());
        ManagedCursorImpl original = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position first = ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        original.markDelete(first);
        ManagedCursorImpl cursor = spy(original);
        ledger.getCursors().removeCursor(cursor.getName());
        ledger.getCursors().add(cursor, cursor.getMarkDeletedPosition());
        CompletableFuture<VoidCallback> submitted = new CompletableFuture<>();
        CompletableFuture<Void> finalWriteStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            submitted.complete(invocation.getArgument(2));
            return null;
        }).when(cursor).persistPositionToLedger(any(), any(), any(), anyBoolean());
        doAnswer(invocation -> {
            finalWriteStarted.complete(null);
            return invocation.callRealMethod();
        }).when(cursor).persistPositionWhenClosing(any(), any(), any(), any());
        CompletableFuture<Void> acknowledged = markDelete(cursor, second);
        VoidCallback write = submitted.get(5, TimeUnit.SECONDS);
        boolean released = false;
        try {
            CloseFuture closing = new CloseFuture();
            cursor.asyncClose(closing, null);
            assertThat(finalWriteStarted).as("final metadata cannot overtake a submitted acknowledgment").isNotDone();
            assertThat(closing).isNotDone();
            released = true;
            if (markDeleteFailure) {
                write.operationFailed(new ManagedLedgerException("held mark-delete failed"));
                assertThatThrownBy(() -> acknowledged.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerException.class);
            } else {
                write.operationComplete();
                acknowledged.get(5, TimeUnit.SECONDS);
            }
            closing.get(5, TimeUnit.SECONDS);
            assertThat(finalWriteStarted).isCompleted();
        } finally {
            if (!released) {
                write.operationComplete();
            }
        }
    }

    @Test
    public void testCursorCloseRejectsQueuedMarkDelete() throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("queued-mark-delete", defaultConfig());
        ManagedCursorImpl cursor = spy((ManagedCursorImpl) ledger.openCursor("cursor"));
        ledger.getCursors().removeCursor(cursor.getName());
        ledger.getCursors().add(cursor, cursor.getMarkDeletedPosition());
        Position position = ledger.addEntry(new byte[] {1});
        // Pause before a cursor-ledger create is issued: the mark-delete is queued behind the switch.
        CompletableFuture<VoidCallback> switchStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            switchStarted.complete(invocation.getArgument(0));
            return null;
        }).when(cursor).createNewMetadataLedger(any());
        CompletableFuture<Void> acknowledged = markDelete(cursor, position);
        VoidCallback switching = switchStarted.get(5, TimeUnit.SECONDS);
        CloseFuture closing = new CloseFuture();
        cursor.asyncClose(closing, null);
        closing.get(5, TimeUnit.SECONDS);
        assertThat(acknowledged).as("a queued acknowledgment must not be abandoned at the close boundary")
                .isCompletedExceptionally();
        assertThatThrownBy(() -> acknowledged.get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerException.CursorAlreadyClosedException.class);
        switching.operationFailed(new ManagedLedgerException("switch cancelled before physical create"));
        assertThat(cursor.isClosed()).isTrue();
    }

    @Test(dataProvider = "lateCreates")
    public void testCursorCloseJoinsLateLedgerCreate(boolean timeout, boolean closeFailure) throws Exception {
        BookKeeper bookKeeper = spy(bkc);
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, bookKeeper);
        ManagedLedgerConfig config = defaultConfig().setMetadataOperationsTimeoutSeconds(timeout ? 1 : 30);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) localFactory.open("late-cursor-create", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position position = ledger.addEntry(new byte[] {1});
        LedgerHandle realLateHandle = bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]);
        LedgerHandle lateHandle = spy(realLateHandle);
        CompletableFuture<Void> lateClose = new CompletableFuture<>();
        CompletableFuture<Void> lateCloseStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            lateCloseStarted.complete(null);
            return lateClose;
        }).when(lateHandle).closeAsync();
        CreateBuilder builder = mock(CreateBuilder.class, RETURNS_SELF);
        CompletableFuture<WriteHandle> createResult = new CompletableFuture<>();
        CompletableFuture<Void> createStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            createStarted.complete(null);
            return createResult;
        }).when(builder).execute();
        doReturn(builder).when(bookKeeper).newCreateLedgerOp();
        try {
            markDelete(cursor, position);
            createStarted.get(5, TimeUnit.SECONDS);
            if (timeout) {
                Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> cursor.getState().equals("NoLedger"));
            }
            CloseFuture closing = new CloseFuture();
            cursor.asyncClose(closing, null);
            assertPending(closing);
            createResult.complete(lateHandle);
            lateCloseStarted.get(5, TimeUnit.SECONDS);
            assertPending(closing);
            if (closeFailure) {
                lateClose.completeExceptionally(BKException.create(BKException.Code.WriteException));
            } else {
                lateClose.complete(null);
            }
            assertResult(closing, closeFailure);
            verify(lateHandle, times(1)).closeAsync();
        } finally {
            createResult.complete(lateHandle);
            lateClose.complete(null);
            realLateHandle.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "closeFailures")
    public void testCursorMetadataCloseJoinsWriterHandle(boolean closeFailure) throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cursor-writer-close", defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        cursor.markDelete(ledger.addEntry(new byte[] {1}));
        LedgerHandle original = cursor.cursorLedger;
        LedgerHandle handle = spy(original);
        cursor.cursorLedger = handle;
        CompletableFuture<Void> handleClosed = new CompletableFuture<>();
        CompletableFuture<Void> handleCloseStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            handleCloseStarted.complete(null);
            return handleClosed;
        }).when(handle).closeAsync();
        try {
            CloseFuture closing = new CloseFuture();
            cursor.asyncClose(closing, null);
            assertPending(closing);
            handleCloseStarted.get(5, TimeUnit.SECONDS);
            if (closeFailure) {
                handleClosed.completeExceptionally(BKException.create(BKException.Code.WriteException));
            } else {
                handleClosed.complete(null);
            }
            assertResult(closing, closeFailure);
        } finally {
            handleClosed.complete(null);
            original.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @DataProvider
    public Object[][] cursorSwitches() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "cursorSwitches")
    public void testCursorCloseJoinsPublishedSwitch(boolean metadataFailure, boolean rangesInLedger) throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                return new ManagedLedgerImpl(this, bk, spy(store), config, scheduledExecutor, name, ownershipChecker);
            }
        };
        ManagedLedgerConfig config = defaultConfig();
        config.setMaxUnackedRangesToPersistInMetadataStore(rangesInLedger ? 0 : 100);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) localFactory.open("cursor-switch-close", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position first = ledger.addEntry(new byte[] {1});
        ledger.addEntry(new byte[] {2});
        Position third = ledger.addEntry(new byte[] {3});
        cursor.markDelete(first);
        cursor.delete(third);
        LedgerHandle original = cursor.cursorLedger;
        LedgerHandle oldHandle = spy(original);
        cursor.cursorLedger = oldHandle;
        CompletableFuture<Void> oldHandleClosed = new CompletableFuture<>();
        CompletableFuture<Void> oldHandleCloseStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            oldHandleCloseStarted.complete(null);
            return oldHandleClosed;
        }).when(oldHandle).closeAsync();
        CompletableFuture<Runnable> metadataWrite = new CompletableFuture<>();
        doAnswer(invocation -> {
            ManagedCursorInfo info = invocation.getArgument(2);
            if (info.getCursorsLedgerId() < 0 || info.getCursorsLedgerId() == oldHandle.getId()) {
                return invocation.callRealMethod();
            }
            String ledgerName = invocation.getArgument(0);
            String cursorName = invocation.getArgument(1);
            Stat stat = invocation.getArgument(3);
            MetaStoreCallback<Void> callback = invocation.getArgument(4);
            metadataWrite.complete(() -> {
                if (metadataFailure) {
                    callback.operationFailed(new MetaStoreException("held cursor switch failed"));
                } else {
                    localFactory.getMetaStore().asyncUpdateCursorInfo(ledgerName, cursorName, info, stat, callback);
                }
            });
            return null;
        }).when(ledger.store).asyncUpdateCursorInfo(any(), any(), any(), any(), any());
        Runnable release = null;
        try {
            cursor.startCreatingNewMetadataLedger();
            release = metadataWrite.get(5, TimeUnit.SECONDS);
            CloseFuture closing = new CloseFuture();
            cursor.asyncClose(closing, null);
            assertPending(closing);
            release.run();
            release = null;
            oldHandleCloseStarted.get(5, TimeUnit.SECONDS);
            assertPending(closing);
            oldHandleClosed.complete(null);
            closing.get(5, TimeUnit.SECONDS);
            if (metadataFailure) {
                assertThat(cursor.cursorLedger.getId()).isEqualTo(oldHandle.getId());
            } else {
                assertThat(cursor.cursorLedger.getId()).isNotEqualTo(oldHandle.getId());
            }
            ledger.close();
            ManagedLedgerImpl reopened = (ManagedLedgerImpl) localFactory.open("cursor-switch-close", config);
            ManagedCursor recovered = reopened.openCursor("cursor");
            assertThat(recovered.getNumberOfEntriesInBacklog(false))
                    .as("final persistence must use the handle actually referenced by cursor metadata").isEqualTo(1);
        } finally {
            if (release != null) {
                release.run();
            }
            oldHandleClosed.complete(null);
            original.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @DataProvider
    public Object[][] oldHandleFailures() {
        return new Object[][] {{false, false}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "oldHandleFailures")
    public void testCursorSwitchCanServeWhileOldHandleCloses(boolean closeFailure, boolean failBeforeClose)
            throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("old-cursor-handle", defaultConfig());
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position first = ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        cursor.markDelete(first);
        LedgerHandle original = cursor.cursorLedger;
        LedgerHandle oldHandle = spy(original);
        cursor.cursorLedger = oldHandle;
        CompletableFuture<Void> oldClosed = new CompletableFuture<>();
        CompletableFuture<Void> closeStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            closeStarted.complete(null);
            return oldClosed;
        }).when(oldHandle).closeAsync();
        try {
            cursor.startCreatingNewMetadataLedger();
            closeStarted.get(5, TimeUnit.SECONDS);
            markDelete(cursor, second).get(5, TimeUnit.SECONDS);
            assertThat(oldClosed).as("ordinary acknowledgments must not wait for old-handle cleanup").isNotDone();
            if (failBeforeClose) {
                oldClosed.completeExceptionally(BKException.create(BKException.Code.WriteException));
            }
            CloseFuture closing = new CloseFuture();
            cursor.asyncClose(closing, null);
            if (!failBeforeClose) {
                assertPending(closing);
            }
            if (closeFailure) {
                oldClosed.completeExceptionally(BKException.create(BKException.Code.WriteException));
            } else {
                oldClosed.complete(null);
            }
            assertResult(closing, closeFailure);
        } finally {
            oldClosed.complete(null);
            original.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @DataProvider
    public Object[][] propertyWrites() {
        return new Object[][] {{false, false}, {true, false}, {false, true}, {true, true}};
    }

    @Test(dataProvider = "propertyWrites")
    public void testCursorCloseJoinsPropertyWrite(boolean fail, boolean cancelResult) throws Exception {
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                return new ManagedLedgerImpl(this, bk, spy(store), config, scheduledExecutor, name, ownershipChecker);
            }
        };
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) localFactory.open("cursor-property-close", defaultConfig());
        ManagedCursor cursor = ledger.openCursor("cursor");
        CompletableFuture<Runnable> metadataWrite = new CompletableFuture<>();
        AtomicBoolean intercepted = new AtomicBoolean();
        doAnswer(invocation -> {
            if (!intercepted.compareAndSet(false, true)) {
                return invocation.callRealMethod();
            }
            String ledgerName = invocation.getArgument(0);
            String cursorName = invocation.getArgument(1);
            ManagedCursorInfo info = invocation.getArgument(2);
            Stat stat = invocation.getArgument(3);
            MetaStoreCallback<Void> callback = invocation.getArgument(4);
            metadataWrite.complete(() -> {
                if (fail) {
                    callback.operationFailed(new MetaStoreException("held property write failed"));
                } else {
                    localFactory.getMetaStore().asyncUpdateCursorInfo(ledgerName, cursorName, info, stat, callback);
                }
            });
            return null;
        }).when(ledger.store).asyncUpdateCursorInfo(any(), any(), any(), any(), any());
        CompletableFuture<Void> result = cursor.putCursorProperty("maintenance", "test");
        Runnable release = metadataWrite.get(5, TimeUnit.SECONDS);
        try {
            if (cancelResult) {
                result.cancel(false);
            }
            CloseFuture closing = new CloseFuture();
            cursor.asyncClose(closing, null);
            assertPending(closing);
            assertThatThrownBy(() -> cursor.putCursorProperty("late", "rejected").get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.CursorAlreadyClosedException.class);
            release.run();
            release = null;
            closing.get(5, TimeUnit.SECONDS);
            if (cancelResult) {
                assertThat(result).isCancelled();
            } else if (fail) {
                assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(MetaStoreException.class);
            } else {
                result.get(5, TimeUnit.SECONDS);
            }
            ledger.close();
            ManagedCursor recovered = localFactory.open("cursor-property-close", defaultConfig()).openCursor("cursor");
            if (fail) {
                assertThat(recovered.getCursorProperties()).doesNotContainKey("maintenance");
            } else {
                assertThat(recovered.getCursorProperties()).containsEntry("maintenance", "test");
            }
            assertThat(recovered.getCursorProperties()).doesNotContainKey("late");
        } finally {
            if (release != null) {
                release.run();
            }
        }
    }

    @Test(dataProvider = "closeFailures")
    public void testCursorCloseJoinsResetPersistence(boolean fail) throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("cursor-reset-close", defaultConfig());
        ManagedCursorImpl original = (ManagedCursorImpl) ledger.openCursor("cursor");
        Position first = ledger.addEntry(new byte[] {1});
        Position second = ledger.addEntry(new byte[] {2});
        ledger.addEntry(new byte[] {3});
        original.markDelete(second);
        ManagedCursorImpl cursor = spy(original);
        ledger.getCursors().removeCursor(cursor.getName());
        ledger.getCursors().add(cursor, cursor.getMarkDeletedPosition());
        CompletableFuture<VoidCallback> resetWrite = new CompletableFuture<>();
        doAnswer(invocation -> {
            resetWrite.complete(invocation.getArgument(2));
            return null;
        }).when(cursor).persistPositionToLedger(any(), any(), any(), anyBoolean());
        CompletableFuture<Void> reset = new CompletableFuture<>();
        cursor.asyncResetCursor(first, false, new AsyncCallbacks.ResetCursorCallback() {
            @Override
            public void resetComplete(Object ctx) {
                reset.complete(null);
            }

            @Override
            public void resetFailed(ManagedLedgerException exception, Object ctx) {
                reset.completeExceptionally(exception);
            }
        });
        VoidCallback write = resetWrite.get(5, TimeUnit.SECONDS);
        boolean released = false;
        try {
            CloseFuture closing = new CloseFuture();
            cursor.asyncClose(closing, null);
            assertPending(closing);
            released = true;
            if (fail) {
                write.operationFailed(new ManagedLedgerException("held reset write failed"));
                assertThatThrownBy(() -> reset.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerException.InvalidCursorPositionException.class);
            } else {
                write.operationComplete();
                reset.get(5, TimeUnit.SECONDS);
            }
            closing.get(5, TimeUnit.SECONDS);
            ledger.close();
            ManagedCursor recovered = factory.open("cursor-reset-close", defaultConfig()).openCursor("cursor");
            assertThat(recovered.getNumberOfEntriesInBacklog(false)).isEqualTo(fail ? 1 : 3);
        } finally {
            if (!released) {
                write.operationFailed(new ManagedLedgerException("test interrupted"));
            }
        }
    }

    private static void assertPending(CompletableFuture<?> future) {
        assertThatThrownBy(() -> future.get(200, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }

    private static CompletableFuture<Void> markDelete(ManagedCursor cursor, Position position) {
        CompletableFuture<Void> acknowledged = new CompletableFuture<>();
        cursor.asyncMarkDelete(position, new AsyncCallbacks.MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                acknowledged.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                acknowledged.completeExceptionally(exception);
            }
        }, null);
        return acknowledged;
    }

    private static CompletableFuture<Void> observeCurrentLedgerClose(ManagedLedgerImpl ledger) {
        LedgerHandle realHandle = ledger.currentLedger;
        LedgerHandle handle = spy(realHandle);
        ledger.currentLedger = handle;
        CompletableFuture<Void> closed = new CompletableFuture<>();
        doAnswer(invocation -> {
            AsyncCallback.CloseCallback callback = invocation.getArgument(0);
            Object context = invocation.getArgument(1);
            realHandle.asyncClose((rc, ignored, ctx) -> {
                callback.closeComplete(rc, handle, ctx);
                closed.complete(null);
            }, context);
            return null;
        }).when(handle).asyncClose(any(), any());
        return closed;
    }

    private static void addHeldCursor(ManagedLedgerImpl ledger, String name,
                                      CompletableFuture<AsyncCallbacks.CloseCallback> close) {
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn(name);
        doAnswer(invocation -> {
            close.complete(invocation.getArgument(0));
            return null;
        }).when(cursor).asyncClose(any(), any());
        ledger.getCursors().add(cursor, null);
    }

    private static void assertResult(CloseFuture future, boolean fail) throws Exception {
        if (fail) {
            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(ManagedLedgerException.class);
        } else {
            future.get(5, TimeUnit.SECONDS);
        }
    }
}
