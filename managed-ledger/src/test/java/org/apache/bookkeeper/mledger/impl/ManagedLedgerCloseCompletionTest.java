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
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
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
