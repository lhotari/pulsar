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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentReplicator;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ShutdownTopicCloseTest extends SharedPulsarBaseTest {
    @DataProvider
    public Object[][] outcomes() {
        return new Object[][] {
                {true, 0}, {false, 0}, {true, 1}, {false, 1}, {true, 2}, {false, 2},
                {true, 3}, {false, 3}, {true, 4}, {false, 4}
        };
    }

    @Test(dataProvider = "outcomes")
    public void ordinaryCloseCannotTakeShutdownNotificationOwnership(boolean persistent, int outcome) throws Exception {
        try (Harness harness = new Harness(persistent)) {
            CompletableFuture<Void> storage = harness.topic.closeForShutdownTransfer();
            assertThat(harness.closeRequests).hasValue(1);
            if (outcome == 4) {
                harness.remaining.set(0);
            }
            CompletableFuture<Void> forced = harness.topic.close(true);
            CompletableFuture<Void> ordinary = harness.topic.close(false);
            CompletableFuture<Void> canceled = harness.topic.close(true);
            if (outcome != 4) {
                assertThat(canceled.cancel(false)).isTrue();
                assertThat(forced).isNotDone();
                assertThat(ordinary).isNotDone();
            }
            assertThat(storage).isNotDone();
            harness.assertNoNotificationOrDisposal();
            if (outcome == 1) {
                harness.storage.completeExceptionally(new IllegalStateException("Storage failed"));
                assertFailed(storage);
                assertFailed(forced);
                assertFailed(ordinary);
                harness.assertNoNotificationOrDisposal();
                return;
            }
            harness.storage.complete(null);
            storage.get(5, TimeUnit.SECONDS);
            if (outcome == 3) {
                // Embedded shutdown can be unbounded and local-only: no transfer disposal will arrive.
                harness.shutdown.complete(null);
                assertFailed(forced);
                assertFailed(ordinary);
                harness.assertNoNotificationOrDisposal();
                return;
            }
            if (outcome == 4) {
                assertFailed(forced);
                assertFailed(ordinary);
                harness.assertNoNotificationOrDisposal();
            } else {
                assertThat(forced).isNotDone();
                assertThat(ordinary).isNotDone();
                harness.assertNoNotificationOrDisposal();
            }
            // The shutdown owner has now released ownership and removed its captured entities.
            harness.topic.getProducers().clear();
            harness.topic.getSubscriptions().clear();
            CompletableFuture<Void> canceledDisposal = harness.topic.disposeAfterTransfer();
            assertThat(canceledDisposal.cancel(false)).isTrue();
            CompletableFuture<Void> disposal = harness.topic.disposeAfterTransfer();
            harness.removalStarted.get(5, TimeUnit.SECONDS);
            assertThat(disposal).isNotDone();
            if (outcome == 2) {
                harness.removalAllowed.completeExceptionally(new IllegalStateException("Disposal failed"));
                assertFailed(disposal);
                assertFailed(forced);
                assertFailed(ordinary);
            } else {
                harness.removalAllowed.complete(null);
                disposal.get(5, TimeUnit.SECONDS);
                if (outcome != 4) {
                    forced.get(5, TimeUnit.SECONDS);
                    ordinary.get(5, TimeUnit.SECONDS);
                }
                assertThat(getPulsar().getBrokerService().getTopics()).doesNotContainKey(harness.topic.getName());
            }
            verify(harness.producer, never()).disconnect(any());
            verify(harness.subscription, never()).disconnect(any());
        }
    }

    @DataProvider
    public Object[][] topicKinds() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "topicKinds")
    public void existingOrdinaryTransferKeepsItsCloseGeneration(boolean persistent) throws Exception {
        try (Harness harness = new Harness(persistent)) {
            when(harness.producer.disconnect(Optional.empty())).thenReturn(CompletableFuture.completedFuture(null));
            when(harness.subscription.disconnect(Optional.empty())).thenReturn(CompletableFuture.completedFuture(null));
            CompletableFuture<Void> storage = harness.topic.close(false, false);
            CompletableFuture<Void> shutdownObserver = harness.topic.closeForShutdownTransfer();
            CompletableFuture<Void> ordinary = harness.topic.close(false);
            harness.assertNoNotificationOrDisposal();
            harness.removalAllowed.complete(null);
            harness.storage.complete(null);
            storage.get(5, TimeUnit.SECONDS);
            shutdownObserver.get(5, TimeUnit.SECONDS);
            ordinary.get(5, TimeUnit.SECONDS);
            verify(harness.producer, times(1)).disconnect(Optional.empty());
            verify(harness.subscription, times(1)).disconnect(Optional.empty());
        }
    }

    private static void assertFailed(CompletableFuture<?> future) {
        assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(Exception.class);
    }

    private final class Harness implements AutoCloseable {
        private final CompletableFuture<Void> storage = new CompletableFuture<>();
        private final CompletableFuture<Void> shutdown = new CompletableFuture<>();
        private final CompletableFuture<Void> removalAllowed = new CompletableFuture<>();
        private final CompletableFuture<Void> removalStarted = new CompletableFuture<>();
        private final AtomicLong remaining = new AtomicLong(Long.MAX_VALUE);
        private final AtomicInteger closeRequests = new AtomicInteger();
        private final AbstractTopic topic;
        private final Producer producer = mock(Producer.class);
        private final Subscription subscription;

        private Harness(boolean persistent) throws Exception {
            PulsarService service = mock(PulsarService.class, delegatesTo(getPulsar()));
            BrokerService actualBroker = getPulsar().getBrokerService();
            BrokerService broker = mock(BrokerService.class, delegatesTo(actualBroker));
            doReturn(service).when(broker).pulsar();
            doReturn(service).when(broker).getPulsar();
            doReturn(shutdown).when(service).getShutdownFuture();
            doAnswer(ignored -> remaining.get()).when(service).getRemainingShutdownDrainNanos();
            ManagedLedger ledger = mock(ManagedLedger.class);
            when(ledger.getConfig()).thenReturn(new ManagedLedgerConfig());
            when(ledger.getProperties()).thenReturn(Map.of());
            when(ledger.getLastConfirmedEntry()).thenReturn(PositionFactory.EARLIEST);
            doAnswer(invocation -> {
                CloseCallback callback = invocation.getArgument(0);
                callback.closeComplete(invocation.getArgument(1));
                return null;
            }).when(ledger).asyncClose(any(), any());
            String name = newTopicName();
            if (persistent) {
                PersistentTopic persistentTopic = new PersistentTopic(name, ledger, broker) {
                    @Override
                    public CompletableFuture<Void> close(boolean disconnectClients, boolean force) {
                        closeRequests.incrementAndGet();
                        return super.close(disconnectClients, force);
                    }
                };
                PersistentSubscription sub = mock(PersistentSubscription.class);
                persistentTopic.getSubscriptions().put("held", sub);
                subscription = sub;
                Replicator replicator = mock(Replicator.class);
                when(replicator.terminate()).thenReturn(storage);
                persistentTopic.getReplicators().put("remote", replicator);
                topic = persistentTopic;
            } else {
                NonPersistentTopic nonPersistentTopic = new NonPersistentTopic(
                        name.replace("persistent://", "non-persistent://"), broker) {
                    @Override
                    public CompletableFuture<Void> close(boolean disconnectClients, boolean force) {
                        closeRequests.incrementAndGet();
                        return super.close(disconnectClients, force);
                    }
                };
                NonPersistentSubscription sub = mock(NonPersistentSubscription.class);
                nonPersistentTopic.getSubscriptions().put("held", sub);
                subscription = sub;
                NonPersistentReplicator replicator = mock(NonPersistentReplicator.class);
                when(replicator.terminate()).thenReturn(storage);
                nonPersistentTopic.getReplicators().put("remote", replicator);
                topic = nonPersistentTopic;
            }
            when(subscription.close(false, Optional.empty())).thenReturn(CompletableFuture.completedFuture(null));
            topic.getProducers().put("held", producer);
            CompletableFuture<Optional<Topic>> cached = CompletableFuture.completedFuture(Optional.of(topic));
            topic.setCreateFuture(cached);
            actualBroker.getTopics().put(topic.getName(), cached);
            doAnswer(ignored -> {
                removalStarted.complete(null);
                return removalAllowed.thenCompose(unused -> actualBroker.removeTopicFromCache(topic));
            }).when(broker).removeTopicFromCache(topic);
        }

        private void assertNoNotificationOrDisposal() {
            verify(producer, never()).disconnect(any());
            verify(subscription, never()).disconnect(any());
            assertThat(removalStarted).isNotDone();
            assertThat(getPulsar().getBrokerService().getTopics()).containsKey(topic.getName());
        }

        @Override
        public void close() throws Exception {
            storage.complete(null);
            removalAllowed.complete(null);
            shutdown.complete(null);
            topic.getProducers().clear();
            topic.getSubscriptions().clear();
            topic.getReplicators().clear();
            getPulsar().getBrokerService().removeTopicFromCache(topic).get(5, TimeUnit.SECONDS);
        }
    }
}
