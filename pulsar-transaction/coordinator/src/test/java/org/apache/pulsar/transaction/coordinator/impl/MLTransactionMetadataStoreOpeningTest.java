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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreOpening;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MLTransactionMetadataStoreOpeningTest {
    @DataProvider
    public Object[][] failureCases() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "failureCases")
    public void testFailedOpenRetainsCleanupAndOriginalError(boolean replayFailure, boolean cleanupFailure)
            throws Exception {
        Fixture fixture = new Fixture();
        Throwable error = replayFailure ? new IllegalStateException("replay failed")
                : new ManagedLedgerException("open failed");
        if (replayFailure) {
            doThrow(error).when(fixture.recovery).appendOpenTransactionToTimeoutTracker();
        }
        TransactionMetadataStoreOpening opening = fixture.open(new MLTransactionMetadataStoreProvider());
        opening.cancel(false);
        CompletableFuture<Void> physical = new CompletableFuture<>();
        if (replayFailure) {
            fixture.finishOpen();
            AsyncCallbacks.CloseCallback close = fixture.closed.get(5, TimeUnit.SECONDS);
            // A fenced logical status must not overwrite the actual physical outcome.
            close.closeFailed(new ManagedLedgerException.ManagedLedgerFencedException(),
                    physical.minimalCompletionStage(), null);
        } else {
            fixture.opened.get(5, TimeUnit.SECONDS).openLedgerFailed((ManagedLedgerException) error,
                    physical.minimalCompletionStage(), null);
        }
        assertThat(opening.store().toCompletableFuture()).isNotDone();
        assertThat(opening.failedOpenCleanup().toCompletableFuture()).isNotDone();
        verify(fixture.tracker, never()).close();
        if (cleanupFailure) {
            physical.completeExceptionally(new ManagedLedgerException("physical cleanup failed"));
        } else {
            physical.complete(null);
        }
        assertThatThrownBy(() -> opening.store().toCompletableFuture().get(5, TimeUnit.SECONDS)).hasCause(error);
        if (cleanupFailure) {
            assertThatThrownBy(() -> opening.failedOpenCleanup().toCompletableFuture().get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("physical cleanup failed");
        } else {
            opening.failedOpenCleanup().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        verify(fixture.tracker, times(1)).close();
        verify(fixture.tracker, never()).start();
    }

    @Test
    public void testCancelledObserverStillExposesLateSuccessfulStore() throws Exception {
        Fixture fixture = new Fixture();
        TransactionMetadataStoreOpening opening = fixture.open(new MLTransactionMetadataStoreProvider());
        opening.obtrudeException(new IllegalStateException("observer timeout"));
        assertThat(opening.failedOpenCleanup().toCompletableFuture()).isNotDone();
        fixture.finishOpen();
        TransactionMetadataStore store = opening.store().toCompletableFuture().get(5, TimeUnit.SECONDS);
        opening.failedOpenCleanup().toCompletableFuture().get(5, TimeUnit.SECONDS);
        verify(fixture.tracker, never()).close();
        CompletableFuture<Void> close = store.closeAsync();
        assertThat(close).isNotDone();
        fixture.closed.get(5, TimeUnit.SECONDS).closeComplete(null);
        close.get(5, TimeUnit.SECONDS);
        verify(fixture.tracker, times(1)).close();
    }

    @Test
    public void testTrackerFailureRemainsVisibleBeforeStoreConstruction() throws Exception {
        Fixture fixture = new Fixture();
        doThrow(new IllegalStateException("tracker close failed")).when(fixture.tracker).close();
        TransactionMetadataStoreOpening opening = fixture.open(new MLTransactionMetadataStoreProvider());
        ManagedLedgerException original = new ManagedLedgerException("open failed");
        fixture.opened.get(5, TimeUnit.SECONDS).openLedgerFailed(original,
                CompletableFuture.<Void>completedFuture(null).minimalCompletionStage(), null);
        assertThatThrownBy(() -> opening.store().toCompletableFuture().get(5, TimeUnit.SECONDS)).hasCause(original);
        assertThatThrownBy(() -> opening.failedOpenCleanup().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .hasRootCauseMessage("tracker close failed");
        verify(fixture.tracker, times(1)).close();
    }

    @Test
    public void testSynchronousSetupFailureIsReturnedAndClosesTracker() throws Exception {
        Fixture fixture = new Fixture();
        doThrow(new IllegalArgumentException("configuration failed")).when(fixture.config)
                .setManagedLedgerInterceptor(any());
        TransactionMetadataStoreOpening opening = fixture.open(new MLTransactionMetadataStoreProvider());
        assertThatThrownBy(() -> opening.store().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .hasRootCauseMessage("configuration failed");
        opening.failedOpenCleanup().toCompletableFuture().get(5, TimeUnit.SECONDS);
        verify(fixture.factory, never()).asyncOpen(any(), any(), any(), any(), any());
        verify(fixture.tracker, times(1)).close();
    }

    @Test
    public void testExistingSubclassOverrideRemainsTheEntryPoint() throws Exception {
        Fixture fixture = new Fixture();
        CompletableFuture<TransactionMetadataStore> subclassResult = new CompletableFuture<>();
        TransactionMetadataStoreProvider provider = new MLTransactionMetadataStoreProvider() {
            @Override
            public CompletableFuture<TransactionMetadataStore> openStore(TransactionCoordinatorID tcId,
                    ManagedLedgerFactory factory, ManagedLedgerConfig config, TransactionTimeoutTracker tracker,
                    TransactionRecoverTracker recovery, long maxActive, TxnLogBufferedWriterConfig writer,
                    Timer timer) {
                return subclassResult;
            }
        };
        TransactionMetadataStoreOpening opening = fixture.open(provider);
        TransactionMetadataStore store = mock(TransactionMetadataStore.class);
        subclassResult.complete(store);
        assertThat(opening.store().toCompletableFuture().get(5, TimeUnit.SECONDS)).isSameAs(store);
        verify(fixture.factory, never()).asyncOpen(any(), any(), any(), any(), any());
    }

    private static final class Fixture {
        private final ManagedLedgerFactory factory = mock(ManagedLedgerFactory.class);
        private final ManagedLedgerConfig config = spy(new ManagedLedgerConfig());
        private final ManagedLedgerImpl ledger = mock(ManagedLedgerImpl.class);
        private final TransactionTimeoutTracker tracker = mock(TransactionTimeoutTracker.class);
        private final TransactionRecoverTracker recovery = mock(TransactionRecoverTracker.class);
        private final CompletableFuture<AsyncCallbacks.OpenLedgerCallback> opened = new CompletableFuture<>();
        private final CompletableFuture<AsyncCallbacks.OpenCursorCallback> cursor = new CompletableFuture<>();
        private final CompletableFuture<AsyncCallbacks.CloseCallback> closed = new CompletableFuture<>();

        private Fixture() {
            ThreadBoundExecutor executor = mock(ThreadBoundExecutor.class);
            when(ledger.getExecutor()).thenReturn(executor);
            doAnswer(invocation -> {
                ((Runnable) invocation.getArgument(0)).run();
                return null;
            }).when(executor).execute(any());
            doAnswer(invocation -> {
                opened.complete(invocation.getArgument(2));
                return null;
            }).when(factory).asyncOpen(any(), any(), any(), any(), any());
            doAnswer(invocation -> {
                cursor.complete(invocation.getArgument(2));
                return null;
            }).when(ledger).asyncOpenCursor(any(), any(), any(), any());
            doAnswer(invocation -> {
                closed.complete(invocation.getArgument(0));
                return null;
            }).when(ledger).asyncClose(any(), any());
        }

        private TransactionMetadataStoreOpening open(TransactionMetadataStoreProvider provider) {
            TxnLogBufferedWriterConfig writer = new TxnLogBufferedWriterConfig();
            writer.setBatchEnabled(false);
            return TransactionMetadataStoreOpening.from(provider.openStore(TransactionCoordinatorID.get(0), factory,
                    config, tracker, recovery, 0, writer, mock(Timer.class)));
        }

        private void finishOpen() throws Exception {
            opened.get(5, TimeUnit.SECONDS).openLedgerComplete(ledger, null);
            cursor.get(5, TimeUnit.SECONDS).openCursorComplete(mock(ManagedCursor.class), null);
        }
    }
}
