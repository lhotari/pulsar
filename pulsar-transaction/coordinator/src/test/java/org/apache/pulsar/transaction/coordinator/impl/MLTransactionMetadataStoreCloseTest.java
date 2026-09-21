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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionLogReplayCallback;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreState.State;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MLTransactionMetadataStoreCloseTest {
    @DataProvider
    public Object[][] closureCases() {
        return new Object[][] {{0, false}, {0, true}, {1, false}, {1, true}, {2, false}, {2, true}};
    }

    @Test(dataProvider = "closureCases")
    public void testCloseJoinsLogAndQueuedWork(int initialState, boolean closeFailure) throws Exception {
        MLTransactionLogImpl log = mock(MLTransactionLogImpl.class);
        TransactionTimeoutTracker tracker = mock(TransactionTimeoutTracker.class);
        MLTransactionMetadataStore store = new MLTransactionMetadataStore(TransactionCoordinatorID.get(0), log,
                tracker, new MLTransactionSequenceIdGenerator(), 0);
        CompletableFuture<Void> logClosed = new CompletableFuture<>();
        when(log.closeAsync()).thenAnswer(invocation -> {
            assertThat(Thread.holdsLock(store)).isFalse();
            return logClosed;
        });
        CountDownLatch releaseReplay = new CountDownLatch(1);
        CompletableFuture<Void> replayStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            TransactionLogReplayCallback callback = invocation.getArgument(0);
            replayStarted.complete(null);
            if (initialState == 1) {
                assertThat(releaseReplay.await(5, TimeUnit.SECONDS)).isTrue();
            }
            callback.replayComplete();
            return null;
        }).when(log).replayAsync(any());
        try {
            CompletableFuture<TransactionMetadataStore> opening = null;
            if (initialState != 0) {
                opening = store.init(mock(TransactionRecoverTracker.class));
                replayStarted.get(5, TimeUnit.SECONDS);
                if (initialState == 2) {
                    opening.get(5, TimeUnit.SECONDS);
                } else {
                    assertThat(opening.cancel(false)).isTrue();
                }
            }
            CompletableFuture<Void> first = store.closeAsync();
            CompletableFuture<Void> repeated = store.closeAsync();
            assertThat(first.cancel(false)).isTrue();
            assertThat(repeated).isNotDone();
            verify(tracker, never()).close();
            if (closeFailure) {
                logClosed.completeExceptionally(new IllegalStateException("log close failed"));
            } else {
                logClosed.complete(null);
            }
            if (initialState == 1) {
                assertThat(repeated).isNotDone();
                verify(tracker, never()).close();
                releaseReplay.countDown();
                assertThat(opening).isCancelled();
            }
            if (closeFailure) {
                assertThatThrownBy(() -> repeated.get(5, TimeUnit.SECONDS)).hasRootCauseMessage("log close failed");
                assertThatThrownBy(() -> store.closeAsync().get(5, TimeUnit.SECONDS))
                        .hasRootCauseMessage("log close failed");
                assertThat(store.getState()).isEqualTo(State.Closing);
            } else {
                repeated.get(5, TimeUnit.SECONDS);
                store.closeAsync().get(5, TimeUnit.SECONDS);
                assertThat(store.getState()).isEqualTo(State.Close);
            }
            // These invocations must return failed futures, including when physical close failed
            // and the store remains Closing with its executor already shut down.
            TxnID txnID = new TxnID(0, 0);
            List<CompletableFuture<?>> rejected = List.of(store.newTransaction(1000, null),
                    store.addProducedPartitionToTxn(txnID, List.of("topic")),
                    store.addAckedPartitionToTxn(txnID, List.of()),
                    store.updateTxnStatus(txnID, TxnStatus.COMMITTING, TxnStatus.OPEN, false));
            for (CompletableFuture<?> operation : rejected) {
                assertThatThrownBy(() -> operation.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(RejectedExecutionException.class);
            }
            verify(log, times(1)).closeAsync();
            verify(tracker, times(1)).close();
        } finally {
            releaseReplay.countDown();
            logClosed.complete(null);
            store.closeAsync().exceptionally(error -> null).get(5, TimeUnit.SECONDS);
        }
    }

    @DataProvider
    public Object[][] cleanupFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "cleanupFailures")
    public void testFailedReplayJoinsCleanup(boolean closeFailure) throws Exception {
        MLTransactionLogImpl log = mock(MLTransactionLogImpl.class);
        TransactionTimeoutTracker tracker = mock(TransactionTimeoutTracker.class);
        MLTransactionMetadataStore store = new MLTransactionMetadataStore(TransactionCoordinatorID.get(0), log,
                tracker, new MLTransactionSequenceIdGenerator(), 0);
        CompletableFuture<Void> logClosed = new CompletableFuture<>();
        CompletableFuture<Void> closeStarted = new CompletableFuture<>();
        when(log.closeAsync()).thenAnswer(invocation -> {
            closeStarted.complete(null);
            return logClosed;
        });
        doThrow(new IllegalStateException("replay failed")).when(log).replayAsync(any());
        try {
            CompletableFuture<TransactionMetadataStore> opening = store.init(mock(TransactionRecoverTracker.class));
            closeStarted.get(5, TimeUnit.SECONDS);
            assertThat(opening).isNotDone();
            if (closeFailure) {
                logClosed.completeExceptionally(new IllegalStateException("cleanup failed"));
            } else {
                logClosed.complete(null);
            }
            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS)).hasRootCauseMessage("replay failed")
                    .satisfies(error -> assertThat(error.getCause().getSuppressed()).hasSize(closeFailure ? 1 : 0));
            verify(log, times(1)).closeAsync();
            verify(tracker).close();
        } finally {
            logClosed.complete(null);
            store.closeAsync().exceptionally(error -> null).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRepeatedInitializationDoesNotCloseReadyStore() throws Exception {
        MLTransactionLogImpl log = mock(MLTransactionLogImpl.class);
        TransactionTimeoutTracker tracker = mock(TransactionTimeoutTracker.class);
        MLTransactionMetadataStore store = new MLTransactionMetadataStore(TransactionCoordinatorID.get(0), log,
                tracker, new MLTransactionSequenceIdGenerator(), 0);
        doAnswer(invocation -> {
            TransactionLogReplayCallback callback = invocation.getArgument(0);
            callback.replayComplete();
            return null;
        }).when(log).replayAsync(any());
        when(log.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        try {
            store.init(mock(TransactionRecoverTracker.class)).get(5, TimeUnit.SECONDS);
            assertThat(store.init(mock(TransactionRecoverTracker.class))).isCompletedExceptionally();
            assertThat(store.getState()).isEqualTo(State.Ready);
            verify(log, never()).closeAsync();
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }
}
