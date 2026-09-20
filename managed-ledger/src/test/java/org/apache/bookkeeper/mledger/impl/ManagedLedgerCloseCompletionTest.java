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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.util.Futures.CloseFuture;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerCloseCompletionTest extends MockedBookKeeperTestCase {
    @DataProvider
    public Object[][] closeFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "closeFailures")
    public void testRepeatedLedgerCloseSharesPhysicalResult(boolean fail) throws Exception {
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("ledger-close", defaultConfig());
        LedgerHandle handle = spy(ledger.currentLedger);
        ledger.currentLedger = handle;
        CompletableFuture<AsyncCallback.CloseCallback> physicalClose = new CompletableFuture<>();
        doAnswer(invocation -> {
            physicalClose.complete(invocation.getArgument(0));
            return null;
        }).when(handle).asyncClose(any(), any());
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
            assertResult(first, fail);
            assertResult(second, fail);
            CloseFuture third = new CloseFuture();
            ledger.asyncClose(third, "third");
            assertResult(third, fail);
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
