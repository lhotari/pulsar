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
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.service.AbstractDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.PendingAcksMap;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentStickyKeyDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MultipleConsumerDispatcherCloseTest extends SharedPulsarBaseTest {
    @DataProvider
    public Object[][] dispatchers() {
        return new Object[][] {{"persistent"}, {"classic"}, {"non-persistent"},
                {"persistent-key-shared"}, {"classic-key-shared"}, {"non-persistent-key-shared"}};
    }

    @Test(dataProvider = "dispatchers")
    public void testConcurrentClientRemovalCanFinishDuringDisconnect(String kind) throws Exception {
        AbstractDispatcherMultipleConsumers dispatcher = newDispatcher(kind);
        Consumer consumer = addConsumer(dispatcher, "consumer");
        ExecutorService clientClose = Executors.newSingleThreadExecutor();
        try {
            doAnswer(invocation -> {
                assertFalse(Thread.holdsLock(dispatcher));
                CompletableFuture.runAsync(() -> removeConsumer(dispatcher, consumer), clientClose)
                        .get(5, TimeUnit.SECONDS);
                return null;
            }).when(consumer).disconnect(eq(false), any());

            dispatcher.disconnectAllConsumers(false, Optional.empty()).get(5, TimeUnit.SECONDS);
            assertTrue(dispatcher.getConsumers().isEmpty());
        } finally {
            clientClose.shutdownNow();
            assertTrue(clientClose.awaitTermination(5, TimeUnit.SECONDS));
            if (dispatcher.getConsumers().contains(consumer)) {
                dispatcher.removeConsumer(consumer);
            }
            dispatcher.close(false, Optional.empty()).get(5, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "dispatchers")
    public void testOverlappingDisconnectsRetainMembershipCompletion(String kind) throws Exception {
        AbstractDispatcherMultipleConsumers dispatcher = newDispatcher(kind);
        Consumer first = addConsumer(dispatcher, "first");
        Consumer second = addConsumer(dispatcher, "second");
        AtomicBoolean disconnecting = new AtomicBoolean();
        AtomicReference<CompletableFuture<Void>> overlapping = new AtomicReference<>();
        try {
            doAnswer(invocation -> {
                if (disconnecting.compareAndSet(false, true)) {
                    overlapping.set(dispatcher.disconnectActiveConsumers(false));
                    dispatcher.resetCloseFuture();
                    dispatcher.removeConsumer(first);
                }
                return null;
            }).when(first).disconnect(eq(false), any());

            CompletableFuture<Void> closing = dispatcher.disconnectAllConsumers(false, Optional.empty());
            assertFalse(closing.isDone());
            assertFalse(overlapping.get().isDone());
            dispatcher.removeConsumer(second);
            closing.get(5, TimeUnit.SECONDS);
            overlapping.get().get(5, TimeUnit.SECONDS);
        } finally {
            if (dispatcher.getConsumers().contains(first)) {
                dispatcher.removeConsumer(first);
            }
            if (dispatcher.getConsumers().contains(second)) {
                dispatcher.removeConsumer(second);
            }
            dispatcher.close(false, Optional.empty()).get(5, TimeUnit.SECONDS);
        }
    }

    private AbstractDispatcherMultipleConsumers newDispatcher(String kind) throws Exception {
        String name = newTopicName();
        if (kind.startsWith("non-persistent")) {
            name = name.replace("persistent://", "non-persistent://");
        }
        admin.lookups().lookupTopic(name);
        Topic topic = getTopic(name, true).get(5, TimeUnit.SECONDS).orElseThrow();
        Subscription subscription = mock(Subscription.class);
        when(subscription.getTopic()).thenReturn(topic);
        when(subscription.getName()).thenReturn("sub");
        ManagedCursor cursor = mock(ManagedCursor.class);
        when(cursor.getName()).thenReturn("sub");
        KeySharedMeta keyShared = new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT);
        return switch (kind) {
            case "persistent" -> new PersistentDispatcherMultipleConsumers((PersistentTopic) topic, cursor,
                    subscription);
            case "classic" -> new PersistentDispatcherMultipleConsumersClassic((PersistentTopic) topic, cursor,
                    subscription);
            case "non-persistent" -> new NonPersistentDispatcherMultipleConsumers((NonPersistentTopic) topic,
                    subscription);
            case "persistent-key-shared" -> new PersistentStickyKeyDispatcherMultipleConsumers((PersistentTopic) topic,
                    cursor, subscription, getConfig(), keyShared);
            case "classic-key-shared" -> new PersistentStickyKeyDispatcherMultipleConsumersClassic(
                    (PersistentTopic) topic, cursor, subscription, getConfig(), keyShared);
            case "non-persistent-key-shared" -> new NonPersistentStickyKeyDispatcherMultipleConsumers(
                    (NonPersistentTopic) topic, subscription, keyShared);
            default -> throw new IllegalArgumentException(kind);
        };
    }

    private static Consumer addConsumer(AbstractDispatcherMultipleConsumers dispatcher, String name) throws Exception {
        Consumer consumer = mock(Consumer.class);
        when(consumer.consumerName()).thenReturn(name);
        when(consumer.getPendingAcks()).thenReturn(mock(PendingAcksMap.class));
        dispatcher.addConsumer(consumer).get(5, TimeUnit.SECONDS);
        return consumer;
    }

    private static void removeConsumer(AbstractDispatcherMultipleConsumers dispatcher, Consumer consumer) {
        try {
            dispatcher.removeConsumer(consumer);
        } catch (BrokerServiceException error) {
            throw new CompletionException(error);
        }
    }
}
