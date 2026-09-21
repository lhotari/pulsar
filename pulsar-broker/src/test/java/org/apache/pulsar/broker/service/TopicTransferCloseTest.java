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
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentReplicator;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicTransferCloseTest extends SharedPulsarBaseTest {
    @DataProvider
    public Object[][] transferCases() {
        return new Object[][] {
                {true, 0, false}, {true, 1, false}, {true, 2, false}, {true, 3, false},
                {false, 0, false}, {false, 2, false}, {false, 3, false},
                {true, 0, true}, {true, 1, true}, {true, 2, true}, {true, 3, true},
                {false, 0, true}, {false, 2, true}, {false, 3, true},
                {true, 4, false}, {true, 5, false}, {true, 4, true}, {true, 5, true}
        };
    }

    @Test(dataProvider = "transferCases")
    public void testStorageFailureAndConcurrentDisconnect(boolean persistent, int failure, boolean cancelRequests)
            throws Exception {
        String name = newTopicName();
        if (!persistent) {
            name = name.replace("persistent://", "non-persistent://");
        }
        String topicName = name;
        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.getConfig()).thenReturn(new ManagedLedgerConfig());
        when(ledger.getProperties()).thenReturn(Map.of());
        when(ledger.getLastConfirmedEntry()).thenReturn(PositionFactory.EARLIEST);
        CompletableFuture<CloseCallback> ledgerCloseStarted = new CompletableFuture<>();
        CompletableFuture<Void> ledgerClosed = new CompletableFuture<>();
        ManagedLedgerException expected = new ManagedLedgerException("held storage close failed");
        doAnswer(invocation -> {
            CloseCallback callback = invocation.getArgument(0);
            Object context = invocation.getArgument(1);
            ledgerCloseStarted.complete(callback);
            if (failure >= 4) {
                callback.closeFailed(new ManagedLedgerException.ManagedLedgerFencedException(),
                        ledgerClosed.minimalCompletionStage(), context);
                return null;
            }
            ledgerClosed.whenComplete((ignored, error) -> {
                if (error == null) {
                    callback.closeComplete(context);
                } else {
                    callback.closeFailed(expected, context);
                }
            });
            return null;
        }).when(ledger).asyncClose(any(), any());
        AbstractTopic topic = persistent
                ? new PersistentTopic(topicName, ledger, getPulsar().getBrokerService())
                : new NonPersistentTopic(topicName, getPulsar().getBrokerService());
        Subscription subscription;
        CompletableFuture<Void> subscriptionClosed = new CompletableFuture<>();
        if (persistent) {
            PersistentSubscription sub = mock(PersistentSubscription.class);
            ((PersistentTopic) topic).getSubscriptions().put("held", sub);
            subscription = sub;
        } else {
            NonPersistentSubscription sub = mock(NonPersistentSubscription.class);
            ((NonPersistentTopic) topic).getSubscriptions().put("held", sub);
            subscription = sub;
        }
        when(subscription.close(false, Optional.empty())).thenReturn(subscriptionClosed);
        CompletableFuture<Void> consumersRemoved = new CompletableFuture<>();
        when(subscription.disconnect(Optional.empty())).thenReturn(consumersRemoved);
        Producer producer = mock(Producer.class);
        CompletableFuture<Void> producerRemoved = new CompletableFuture<>();
        when(producer.disconnect(Optional.empty())).thenReturn(producerRemoved);
        topic.getProducers().put("held", producer);
        Replicator replicator;
        if (persistent) {
            replicator = mock(Replicator.class);
            ((PersistentTopic) topic).getReplicators().put("remote", replicator);
        } else {
            NonPersistentReplicator nonPersistentReplicator = mock(NonPersistentReplicator.class);
            ((NonPersistentTopic) topic).getReplicators().put("remote", nonPersistentReplicator);
            replicator = nonPersistentReplicator;
        }
        if (failure == 3) {
            when(replicator.terminate()).thenThrow(new IllegalStateException("replicator close failed"));
        } else {
            when(replicator.terminate()).thenReturn(CompletableFuture.completedFuture(null));
        }
        try (MockedStatic<ExtensibleLoadManagerImpl> lookup = Mockito.mockStatic(ExtensibleLoadManagerImpl.class)) {
            lookup.when(() -> ExtensibleLoadManagerImpl.getAssignedBrokerLookupData(getPulsar(), topicName))
                    .thenReturn(CompletableFuture.completedFuture(Optional.empty()));
            // Force is not permission to skip storage closure when clients are retained.
            CompletableFuture<Void> storage = topic.close(false, true);
            if (cancelRequests) {
                assertThat(storage.cancel(false)).isTrue();
                storage = topic.close(false, false);
                CompletableFuture<Void> canceledJoin = topic.close(false, false);
                assertThat(canceledJoin.cancel(false)).isTrue();
            }
            CompletableFuture<Void> repeatedStorage = topic.close(false, false);
            CompletableFuture<Void> disconnect = topic.close(true, false);
            if (cancelRequests) {
                assertThat(disconnect.cancel(false)).isTrue();
                disconnect = topic.close(true, false);
            }
            CompletableFuture<Void> repeatedDisconnect = topic.close(true, false);
            assertPending(storage);
            assertPending(repeatedStorage);
            assertPending(disconnect);
            verify(producer, never()).disconnect(any());
            verify(subscription, never()).disconnect(any());
            verify(subscription, times(1)).close(false, Optional.empty());
            if (failure == 2) {
                subscriptionClosed.completeExceptionally(expected);
            } else {
                subscriptionClosed.complete(null);
            }
            if (persistent) {
                ledgerCloseStarted.get(10, TimeUnit.SECONDS);
                assertPending(storage);
                assertPending(disconnect);
                if (failure == 1 || failure == 5) {
                    ledgerClosed.completeExceptionally(expected);
                } else {
                    ledgerClosed.complete(null);
                }
            }
            if (failure != 0 && failure != 4) {
                assertFailed(storage);
                assertFailed(repeatedStorage);
                assertFailed(disconnect);
                assertFailed(repeatedDisconnect);
                assertFailed(topic.close(false, false));
                assertThat(topic.isTransferring()).isTrue();
                verify(producer, never()).disconnect(any());
                verify(subscription, never()).disconnect(any());
            } else {
                storage.get(10, TimeUnit.SECONDS);
                repeatedStorage.get(10, TimeUnit.SECONDS);
                Mockito.verify(producer, Mockito.timeout(10000)).disconnect(Optional.empty());
                Mockito.verify(subscription, Mockito.timeout(10000)).disconnect(Optional.empty());
                assertPending(disconnect);
                producerRemoved.complete(null);
                assertPending(disconnect);
                consumersRemoved.complete(null);
                disconnect.get(10, TimeUnit.SECONDS);
                repeatedDisconnect.get(10, TimeUnit.SECONDS);
            }
            verify(replicator, times(1)).terminate();
            if (persistent) {
                verify(ledger, times(1)).asyncClose(any(), any());
            }
        } finally {
            subscriptionClosed.complete(null);
            ledgerClosed.complete(null);
            producerRemoved.complete(null);
            consumersRemoved.complete(null);
            topic.getProducers().clear();
            topic.getSubscriptions().clear();
            topic.getReplicators().clear();
        }
    }

    @Test
    public void testLedgerFailureAloneCannotCompleteTransferSuccessfully() throws Exception {
        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.getConfig()).thenReturn(new ManagedLedgerConfig());
        when(ledger.getProperties()).thenReturn(Map.of());
        when(ledger.getLastConfirmedEntry()).thenReturn(PositionFactory.EARLIEST);
        ManagedLedgerException failure = new ManagedLedgerException("ledger close failed");
        doAnswer(invocation -> {
            CloseCallback callback = invocation.getArgument(0);
            callback.closeFailed(failure, invocation.getArgument(1));
            return null;
        }).when(ledger).asyncClose(any(), any());
        PersistentTopic topic = new PersistentTopic(newTopicName(), ledger, getPulsar().getBrokerService());
        assertThatThrownBy(() -> topic.close(false, false).get(10, TimeUnit.SECONDS))
                .hasRootCauseMessage("ledger close failed");
        assertThatThrownBy(() -> topic.close(false, false).get(10, TimeUnit.SECONDS))
                .hasRootCauseMessage("ledger close failed");
        verify(ledger, times(1)).asyncClose(any(), any());
    }

    @DataProvider
    public Object[][] dispatcherFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "dispatcherFailures")
    public void testDisposeAfterTransferWaitsForStorageWithoutRepeatingIt(boolean persistent) throws Exception {
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
        AbstractTopic topic = persistent
                ? new PersistentTopic(name, ledger, getPulsar().getBrokerService())
                : new NonPersistentTopic(name.replace("persistent://", "non-persistent://"),
                        getPulsar().getBrokerService());
        topic = Mockito.spy(topic);
        doReturn(true).when(topic).isCloseWhileInactive();
        topic.getHierarchyTopicPolicies().getInactiveTopicPolicies().updateTopicValue(new InactiveTopicPolicies(
                InactiveTopicDeleteMode.delete_when_no_subscriptions, 0, false));
        assertThat(topic.disposeAfterTransfer()).isCompletedExceptionally();
        CompletableFuture<Void> replicationClosed = new CompletableFuture<>();
        Replicator replicator;
        if (persistent) {
            replicator = mock(Replicator.class);
            ((PersistentTopic) topic).getReplicators().put("remote", replicator);
        } else {
            NonPersistentReplicator nonPersistentReplicator = mock(NonPersistentReplicator.class);
            ((NonPersistentTopic) topic).getReplicators().put("remote", nonPersistentReplicator);
            replicator = nonPersistentReplicator;
        }
        when(replicator.terminate()).thenReturn(replicationClosed);
        try {
            CompletableFuture<Void> storage = topic.close(false, false);
            topic.checkGC();
            verify(topic, never()).close(true, false);
            verify(replicator, times(1)).terminate();
            CompletableFuture<Void> canceled = topic.disposeAfterTransfer();
            assertThat(canceled.cancel(false)).isTrue();
            CompletableFuture<Void> disposed = topic.disposeAfterTransfer();
            assertPending(disposed);
            replicationClosed.complete(null);
            storage.get(10, TimeUnit.SECONDS);
            disposed.get(10, TimeUnit.SECONDS);
            topic.close(true, false).get(10, TimeUnit.SECONDS);
            verify(replicator, times(1)).terminate();
            if (persistent) {
                verify(ledger, times(1)).asyncClose(any(), any());
            }
        } finally {
            replicationClosed.complete(null);
            topic.getReplicators().clear();
        }
    }

    @Test(dataProvider = "dispatcherFailures")
    public void testSubscriptionTransferRetainsDispatcherFailure(boolean synchronous) throws Exception {
        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.getConfig()).thenReturn(new ManagedLedgerConfig());
        when(ledger.getProperties()).thenReturn(Map.of());
        when(ledger.getLastConfirmedEntry()).thenReturn(PositionFactory.EARLIEST);
        PersistentTopic topic = new PersistentTopic(newTopicName(), ledger, getPulsar().getBrokerService());
        Dispatcher dispatcher = mock(Dispatcher.class);
        PersistentSubscription subscription = new SubscriptionWithDispatcher(topic, dispatcher);
        CompletableFuture<Void> dispatcherClosed = new CompletableFuture<>();
        IllegalStateException failure = new IllegalStateException("dispatcher failed");
        when(dispatcher.close(false, Optional.empty())).thenAnswer(invocation -> {
            assertThat(Thread.holdsLock(subscription)).isFalse();
            if (synchronous) {
                throw failure;
            }
            return dispatcherClosed;
        });
        CompletableFuture<Void> first = subscription.close(false, Optional.empty());
        if (!synchronous) {
            assertThat(first.cancel(false)).isTrue();
        }
        CompletableFuture<Void> second = subscription.close(false, Optional.empty());
        if (!synchronous) {
            assertPending(second);
            dispatcherClosed.completeExceptionally(failure);
        }
        assertThatThrownBy(() -> second.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("dispatcher failed");
        assertThatThrownBy(() -> subscription.close(false, Optional.empty()).get(10, TimeUnit.SECONDS))
                .hasRootCauseMessage("dispatcher failed");
        verify(dispatcher, times(1)).close(false, Optional.empty());
        verify(dispatcher, never()).reset();
    }

    private static final class SubscriptionWithDispatcher extends PersistentSubscription {
        SubscriptionWithDispatcher(PersistentTopic topic, Dispatcher dispatcher) {
            super(topic, "held", newCursor(), null);
            this.dispatcher = dispatcher;
        }

        private static ManagedCursor newCursor() {
            ManagedCursor cursor = mock(ManagedCursor.class);
            when(cursor.getName()).thenReturn("held");
            return cursor;
        }
    }

    private static void assertPending(CompletableFuture<?> future) {
        assertThatThrownBy(() -> future.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }

    private static void assertFailed(CompletableFuture<?> future) {
        assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(Exception.class);
    }
}
