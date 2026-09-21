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
package org.apache.pulsar.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.HashedWheelTimer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.broker.service.BrokerAdmission;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.broker.transaction.timeout.TransactionTimeoutTrackerImpl;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreOpening;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreOpening.UnreportedFailedOpenCleanupException;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TransactionMetadataStoreShutdownTest {
    private static final TransactionCoordinatorID TC = TransactionCoordinatorID.get(0);

    @Test
    public void testPendingOpenIsClosedWithoutPublishingAfterBundleSeal() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Opening opening = fixture.next();
            CompletableFuture<Void> request = fixture.service.handleTcClientConnect(TC);
            request.cancel(false);
            assertThat(fixture.service.hasStoresInBundle(fixture.bundle)).isTrue();
            fixture.admission.close();
            CompletableFuture<Integer> close = fixture.service.closeStoresForBundle(fixture.bundle);
            assertThat(close).isNotDone();
            CompletableFuture<Void> cancelled = fixture.service.removeTransactionMetadataStore(TC);
            cancelled.cancel(false);
            opening.result.complete(opening.store);
            assertThat(close).isNotDone();
            assertThat(fixture.service.getStores()).isEmpty();
            verify(opening.store).closeAsync();
            opening.closed.complete(null);
            assertThat(close.get(5, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(fixture.service.closeStoresForBundle(fixture.bundle).get(5, TimeUnit.SECONDS)).isZero();
            assertThat(fixture.service.hasStoresInBundle(fixture.bundle)).isFalse();
            assertThat(fixture.service.handleTcClientConnect(TC)).isCompletedExceptionally();
            verify(fixture.provider, times(1)).openStore(any(), any(), any(), any(), any(), anyLong(), any(), any());
        }
    }

    @Test
    public void testOwnershipCheckCannotStartProviderAfterSeal() throws Exception {
        try (Fixture fixture = new Fixture()) {
            CompletableFuture<Void> ownership = new CompletableFuture<>();
            when(fixture.broker.checkTopicNsOwnership(any())).thenReturn(ownership);
            fixture.service.handleTcClientConnect(TC);
            CompletableFuture<Integer> close = fixture.service.closeStoresForBundle(fixture.bundle);
            assertThat(close).isNotDone();
            ownership.complete(null);
            close.get(5, TimeUnit.SECONDS);
            verify(fixture.provider, never()).openStore(any(), any(), any(), any(), any(), anyLong(), any(), any());
        }
    }

    @Test
    public void testReconnectWaitsForPhysicalCloseAndRechecksOwnership() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Opening first = fixture.next();
            first.result.complete(first.store);
            fixture.service.handleTcClientConnect(TC).get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> close = fixture.service.removeTransactionMetadataStore(TC);
            Opening second = fixture.next();
            second.result.complete(second.store);
            CompletableFuture<Void> reconnect = fixture.service.handleTcClientConnect(TC);
            assertThat(reconnect).isNotDone();
            assertThat(close).isNotDone();
            verify(fixture.provider, times(1)).openStore(any(), any(), any(), any(), any(), anyLong(), any(), any());
            first.closed.complete(null);
            reconnect.get(5, TimeUnit.SECONDS);
            assertThat(fixture.service.getStores().get(TC)).isSameAs(second.store);
            verify(fixture.broker, times(2)).checkTopicNsOwnership(any());
            verify(first.store, times(1)).closeAsync();
        }
    }

    @DataProvider
    public Object[][] failureCases() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "failureCases")
    public void testFailedOpenKeepsIndependentCleanupBarrier(boolean cleanupFailure) throws Exception {
        try (Fixture fixture = new Fixture()) {
            Opening opening = fixture.next();
            CompletableFuture<Void> ready = fixture.service.handleTcClientConnect(TC);
            IllegalStateException original = new IllegalStateException("initialization failed");
            opening.result.completeExceptionally(original);
            assertThatThrownBy(() -> ready.get(5, TimeUnit.SECONDS)).hasCause(original);
            CompletableFuture<Integer> closing = fixture.service.closeStoresForBundle(fixture.bundle);
            assertThat(closing).isNotDone();
            assertThat(fixture.service.hasStoresInBundle(fixture.bundle)).isTrue();
            if (cleanupFailure) {
                opening.cleanup.completeExceptionally(new IllegalStateException("physical cleanup failed"));
                assertThatThrownBy(() -> closing.get(5, TimeUnit.SECONDS))
                        .hasRootCauseMessage("physical cleanup failed");
                assertThatThrownBy(() -> fixture.service.handleTcClientConnect(TC).get(5, TimeUnit.SECONDS))
                        .hasRootCauseMessage("physical cleanup failed");
                assertThat(fixture.service.hasStoresInBundle(fixture.bundle)).isTrue();
            } else {
                opening.cleanup.complete(null);
                closing.get(5, TimeUnit.SECONDS);
                assertThat(fixture.service.hasStoresInBundle(fixture.bundle)).isFalse();
                Opening retry = fixture.next();
                retry.result.complete(retry.store);
                fixture.service.handleTcClientConnect(TC).get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void testFailedStoreClosePreventsReuseAndRemainsInFinalClose() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Opening opening = fixture.next();
            opening.result.complete(opening.store);
            fixture.service.handleTcClientConnect(TC).get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> removal = fixture.service.removeTransactionMetadataStore(TC);
            opening.closed.completeExceptionally(new IllegalStateException("store close failed"));
            assertThatThrownBy(() -> removal.get(5, TimeUnit.SECONDS)).hasRootCauseMessage("store close failed");
            assertThatThrownBy(() -> fixture.service.handleTcClientConnect(TC).get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("store close failed");
            assertThatThrownBy(() -> fixture.service.closeAsync().get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("store close failed");
            verify(opening.store, times(1)).closeAsync();
        }
    }

    @Test
    public void testFinalCloseJoinsPendingOpenAndIgnoresCancelledObserver() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Opening opening = fixture.next();
            fixture.service.handleTcClientConnect(TC);
            CompletableFuture<Void> cancelled = fixture.service.closeAsync();
            cancelled.cancel(false);
            CompletableFuture<Void> closing = fixture.service.closeAsync();
            assertThat(closing).isNotDone();
            opening.result.complete(opening.store);
            assertThat(closing).isNotDone();
            opening.closed.complete(null);
            closing.get(5, TimeUnit.SECONDS);
            assertThat(fixture.service.handleTcClientConnect(TC)).isCompletedExceptionally();
            assertThat(fixture.service.getStores()).isEmpty();
            verify(opening.store, times(1)).closeAsync();
        }
    }

    @Test
    public void testBundleSnapshotDoesNotCloseAnotherCoordinator() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Opening first = fixture.next();
            Opening other = fixture.next();
            fixture.service.handleTcClientConnect(TC);
            fixture.service.handleTcClientConnect(TransactionCoordinatorID.get(1));
            CompletableFuture<Integer> closing = fixture.service.closeStoresForBundle(fixture.bundle);
            first.result.complete(first.store);
            first.closed.complete(null);
            closing.get(5, TimeUnit.SECONDS);
            verify(other.store, never()).closeAsync();
            assertThat(other.result).isNotDone();
        }
    }

    @Test
    public void testRemovalJoinsActivationAdmittedBeforeSeal() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (Fixture fixture = new Fixture()) {
            Opening opening = fixture.next();
            opening.activateRecovery = true;
            when(opening.store.getTxnMeta(any())).thenAnswer(invocation -> {
                entered.countDown();
                assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
                return CompletableFuture.failedFuture(new IllegalStateException("injected recovery failure"));
            });
            CompletableFuture<Void> ready = fixture.service.handleTcClientConnect(TC);
            opening.result.complete(opening.store);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                CompletableFuture<Void> closing = fixture.service.removeTransactionMetadataStore(TC);
                assertThat(ready).isCompletedExceptionally();
                assertThat(closing).isNotDone();
                verify(opening.store, never()).closeAsync();
                release.countDown();
                opening.closed.complete(null);
                closing.get(5, TimeUnit.SECONDS);
                verify(opening.store, times(1)).closeAsync();
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    public void testConfigurationFailureIsCleanAndRetryable() throws Exception {
        try (Fixture fixture = new Fixture()) {
            doThrow(new IllegalArgumentException("configuration failed")).when(fixture.broker)
                    .getManagedLedgerConfig(any());
            assertThatThrownBy(() -> fixture.service.handleTcClientConnect(TC).get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("configuration failed");
            fixture.service.closeStoresForBundle(fixture.bundle).get(5, TimeUnit.SECONDS);
            assertThat(fixture.service.hasStoresInBundle(fixture.bundle)).isFalse();
            verify(fixture.provider, never()).openStore(any(), any(), any(), any(), any(), anyLong(), any(), any());
            doReturn(CompletableFuture.completedFuture(new ManagedLedgerConfig())).when(fixture.broker)
                    .getManagedLedgerConfig(any());
            Opening retry = fixture.next();
            retry.result.complete(retry.store);
            fixture.service.handleTcClientConnect(TC).get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testRejectedActivationStillClosesStoreAndSettlesRequests() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Opening opening = fixture.next();
            CompletableFuture<Void> request = fixture.service.handleTcClientConnect(TC);
            fixture.service.activationExecutor().shutdown();
            opening.result.complete(opening.store);
            assertThatThrownBy(() -> request.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(RejectedExecutionException.class);
            CompletableFuture<Void> closing = fixture.service.closeAsync();
            assertThat(closing).isNotDone();
            opening.closed.complete(null);
            closing.get(5, TimeUnit.SECONDS);
            assertThat(fixture.service.getStores()).isEmpty();
            verify(opening.store, times(1)).closeAsync();
        }
    }

    @Test
    public void testThrowingLegacyProviderIsUntracked() throws Exception {
        try (Fixture fixture = new Fixture()) {
            doThrow(new IllegalArgumentException("legacy provider failed")).when(fixture.provider)
                    .openStore(any(), any(), any(), any(), any(), anyLong(), any(), any());
            assertThatThrownBy(() -> fixture.service.handleTcClientConnect(TC).get(5, TimeUnit.SECONDS))
                    .hasRootCauseMessage("legacy provider failed");
            assertThatThrownBy(() -> fixture.service.closeStoresForBundle(fixture.bundle).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(UnreportedFailedOpenCleanupException.class)
                    .hasRootCauseMessage("legacy provider failed");
            TransactionMetadataStore retry = mock(TransactionMetadataStore.class);
            when(retry.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            doReturn(CompletableFuture.completedFuture(retry)).when(fixture.provider)
                    .openStore(any(), any(), any(), any(), any(), anyLong(), any(), any());
            fixture.service.handleTcClientConnect(TC).get(5, TimeUnit.SECONDS);
            verify(fixture.broker, times(2)).checkTopicNsOwnership(any());
            assertThatThrownBy(() -> fixture.service.removeTransactionMetadataStore(TC).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(UnreportedFailedOpenCleanupException.class);
        }
    }

    @DataProvider
    public Object[][] legacyFailures() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "legacyFailures")
    public void testLegacyFailureAllowsRetryWithoutLosingHistoricalBarrier(boolean transformed,
                                                                          boolean waitingReconnect) throws Exception {
        try (Fixture fixture = new Fixture()) {
            Opening failed = fixture.next();
            failed.legacy = !transformed;
            failed.transformed = transformed;
            CompletableFuture<Void> ready = fixture.service.handleTcClientConnect(TC);
            IllegalArgumentException original = new IllegalArgumentException("legacy open failed");
            CompletableFuture<Void> oldRemoval = waitingReconnect
                    ? fixture.service.removeTransactionMetadataStore(TC) : null;
            CompletableFuture<Integer> oldSnapshot = waitingReconnect
                    ? fixture.service.closeStoresForBundle(fixture.bundle) : null;
            Opening retry = fixture.next();
            retry.result.complete(retry.store);
            CompletableFuture<Void> reconnect = waitingReconnect ? fixture.service.handleTcClientConnect(TC) : null;
            if (waitingReconnect) {
                assertThat(reconnect).isNotDone();
            }
            failed.result.completeExceptionally(original);
            if (waitingReconnect) {
                assertThatThrownBy(() -> oldRemoval.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(UnreportedFailedOpenCleanupException.class).hasRootCause(original);
                assertThatThrownBy(() -> oldSnapshot.get(5, TimeUnit.SECONDS)).hasRootCause(original);
            } else {
                assertThatThrownBy(() -> ready.get(5, TimeUnit.SECONDS)).hasCause(original);
                reconnect = fixture.service.handleTcClientConnect(TC);
            }
            reconnect.get(5, TimeUnit.SECONDS);
            assertThat(original.getSuppressed()).isEmpty();
            assertThat(fixture.service.getStores().get(TC)).isSameAs(retry.store);
            verify(fixture.broker, times(2)).checkTopicNsOwnership(any());
            assertThat(fixture.service.hasStoresInBundle(fixture.bundle)).isTrue();
            CompletableFuture<Void> removal = fixture.service.removeTransactionMetadataStore(TC);
            assertThat(removal).isNotDone();
            retry.closed.complete(null);
            assertThatThrownBy(() -> removal.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(UnreportedFailedOpenCleanupException.class).hasRootCause(original);
            assertThat(fixture.service.getStores()).isEmpty();
            assertThat(fixture.service.hasStoresInBundle(fixture.bundle)).isTrue();
            assertThatThrownBy(() -> fixture.service.closeStoresForBundle(fixture.bundle).get(5, TimeUnit.SECONDS))
                    .hasRootCause(original);
            assertThatThrownBy(() -> fixture.service.removeTransactionMetadataStore(TC).get(5, TimeUnit.SECONDS))
                    .hasRootCause(original);
            assertThatThrownBy(() -> fixture.service.closeAsync().get(5, TimeUnit.SECONDS)).hasRootCause(original);
        }
    }

    @Test
    public void testTrackerFailureDoesNotRetireLegacyFailedGeneration() throws Exception {
        IllegalStateException trackerFailure = new IllegalStateException("tracker close failed");
        try (var trackers = mockConstruction(TransactionTimeoutTrackerImpl.class,
                (tracker, context) -> doThrow(trackerFailure).when(tracker).close());
             Fixture fixture = new Fixture()) {
            Opening failed = fixture.next();
            failed.legacy = true;
            CompletableFuture<Void> ready = fixture.service.handleTcClientConnect(TC);
            IllegalArgumentException original = new IllegalArgumentException("legacy failed");
            failed.result.completeExceptionally(original);
            assertThatThrownBy(() -> ready.get(5, TimeUnit.SECONDS)).hasCause(original);
            assertThatThrownBy(() -> fixture.service.handleTcClientConnect(TC).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(UnreportedFailedOpenCleanupException.class)
                    .satisfies(error -> assertThat(error.getCause().getSuppressed()).containsExactly(trackerFailure));
            verify(fixture.broker, times(1)).checkTopicNsOwnership(any());
            assertThat(trackers.constructed()).hasSize(1);
            assertThat(fixture.service.hasStoresInBundle(fixture.bundle)).isTrue();
        }
    }

    private static final class Opening {
        private final TransactionMetadataStore store = mock(TransactionMetadataStore.class);
        private final CompletableFuture<TransactionMetadataStore> result = new CompletableFuture<>();
        private final CompletableFuture<Void> cleanup = new CompletableFuture<>();
        private final CompletableFuture<Void> closed = new CompletableFuture<>();
        private boolean activateRecovery;
        private boolean legacy;
        private boolean transformed;

        private Opening() {
            when(store.closeAsync()).thenReturn(closed);
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final PulsarService pulsar = mock(PulsarService.class);
        private final BrokerService broker = mock(BrokerService.class);
        private final BrokerAdmission admission = new BrokerAdmission();
        private final TransactionMetadataStoreProvider provider = mock(TransactionMetadataStoreProvider.class);
        private final NamespaceBundle bundle = mock(NamespaceBundle.class);
        private final Queue<Opening> pending = new ConcurrentLinkedQueue<>();
        private final List<Opening> all = new ArrayList<>();
        private final TransactionMetadataStoreService service;

        private Fixture() throws Exception {
            when(pulsar.getBrokerService()).thenReturn(broker);
            when(pulsar.getBrokerAdmission()).thenReturn(admission);
            when(pulsar.getConfiguration()).thenReturn(new ServiceConfiguration());
            when(broker.checkTopicNsOwnership(any())).thenReturn(CompletableFuture.completedFuture(null));
            when(broker.getManagedLedgerConfig(any())).thenReturn(
                    CompletableFuture.completedFuture(new ManagedLedgerConfig()));
            ManagedLedgerStorage storage = mock(ManagedLedgerStorage.class);
            ManagedLedgerStorageClass storageClass = mock(ManagedLedgerStorageClass.class);
            ManagedLedgerFactory factory = mock(ManagedLedgerFactory.class);
            when(pulsar.getManagedLedgerStorage()).thenReturn(storage);
            when(storage.getManagedLedgerStorageClass(any())).thenReturn(Optional.of(storageClass));
            when(storageClass.getManagedLedgerFactory()).thenReturn(factory);
            when(bundle.includes(any(TopicName.class))).thenAnswer(invocation ->
                    SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.getPartition(0).equals(invocation.getArgument(0)));
            when(provider.openStore(any(), any(), any(), any(), any(), anyLong(), any(), any()))
                    .thenAnswer(invocation -> {
                Opening opening = pending.remove();
                if (opening.activateRecovery) {
                    TransactionRecoverTracker recovery = invocation.getArgument(4);
                    recovery.updateTransactionStatus(1, TxnStatus.COMMITTING);
                }
                if (opening.legacy) {
                    return opening.result;
                }
                TransactionMetadataStoreOpening retained =
                        new TransactionMetadataStoreOpening(opening.result, opening.cleanup);
                return opening.transformed ? retained.thenApply(store -> store) : retained;
            });
            service = new TransactionMetadataStoreService(provider, pulsar, mock(TransactionBufferClient.class),
                    mock(HashedWheelTimer.class));
        }

        private Opening next() {
            Opening opening = new Opening();
            pending.add(opening);
            all.add(opening);
            return opening;
        }

        @Override
        public void close() throws Exception {
            CompletableFuture<Void> closing = service.closeAsync();
            for (Opening opening : all) {
                opening.result.complete(opening.store);
                opening.cleanup.complete(null);
                opening.closed.complete(null);
            }
            closing.exceptionally(error -> null).get(5, TimeUnit.SECONDS);
        }
    }
}
