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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.mockito.stubbing.Answer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SingleActiveDispatcherCloseTest {
    @Test
    public void testOverlappingAllConsumerDisconnectsBothComplete() throws Exception {
        AbstractDispatcherSingleActiveConsumer dispatcher = newDispatcher();
        Consumer consumer = addConsumer(dispatcher, "consumer");
        AtomicBoolean first = new AtomicBoolean(true);
        AtomicReference<CompletableFuture<Void>> overlapping = new AtomicReference<>();
        doAnswer(invocation -> {
            if (first.getAndSet(false)) {
                overlapping.set(dispatcher.disconnectAllConsumers(false, Optional.empty()));
                dispatcher.removeConsumer(consumer);
            }
            return null;
        }).when(consumer).disconnect(eq(false), any());

        CompletableFuture<Void> closing = dispatcher.disconnectAllConsumers(false, Optional.empty());

        assertTrue(closing.isDone());
        assertTrue(overlapping.get().isDone());
        closing.get(5, TimeUnit.SECONDS);
        overlapping.get().get(5, TimeUnit.SECONDS);
    }

    @Test(dataProvider = "activeOnly")
    public void testActiveDisconnectDoesNotReplaceAllConsumerCompletion(boolean activeStartsFirst) throws Exception {
        AbstractDispatcherSingleActiveConsumer dispatcher = newDispatcher();
        Consumer active = addConsumer(dispatcher, "active");
        Consumer standby = addConsumer(dispatcher, "standby");
        AtomicReference<CompletableFuture<Void>> all = new AtomicReference<>();
        doAnswer(invocation -> {
            if (activeStartsFirst) {
                all.set(dispatcher.disconnectAllConsumers(false, Optional.empty()));
            }
            dispatcher.removeConsumer(active);
            return null;
        }).when(active).disconnect(false);
        if (!activeStartsFirst) {
            all.set(dispatcher.disconnectAllConsumers(false, Optional.empty()));
        }

        dispatcher.disconnectActiveConsumers(false).get(5, TimeUnit.SECONDS);
        // Cursor reset clears its active-only disconnect marker after the active consumer leaves.
        dispatcher.resetCloseFuture();

        assertFalse(all.get().isDone());
        dispatcher.removeConsumer(standby);
        assertTrue(all.get().isDone());
        all.get().get(5, TimeUnit.SECONDS);

        Consumer nextActive = addConsumer(dispatcher, "next-active");
        Consumer nextStandby = addConsumer(dispatcher, "next-standby");
        dispatcher.removeConsumer(nextActive);
        assertSame(dispatcher.getActiveConsumer(), nextStandby);
    }

    private static AbstractDispatcherSingleActiveConsumer newDispatcher() {
        return mock(AbstractDispatcherSingleActiveConsumer.class,
                withSettings().useConstructor(SubType.Failover, 0, "persistent://public/default/close",
                        null, new ServiceConfiguration(), null).defaultAnswer(CALLS_REAL_METHODS));
    }

    private static Consumer addConsumer(AbstractDispatcherSingleActiveConsumer dispatcher, String name)
            throws Exception {
        Consumer consumer = mock(Consumer.class);
        when(consumer.consumerName()).thenReturn(name);
        dispatcher.addConsumer(consumer).get(5, TimeUnit.SECONDS);
        return consumer;
    }

    @DataProvider
    public Object[][] activeOnly() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "activeOnly")
    public void testConcurrentClientRemovalCanFinishDuringDisconnect(boolean activeOnly) throws Exception {
        AbstractDispatcherSingleActiveConsumer dispatcher = mock(AbstractDispatcherSingleActiveConsumer.class,
                withSettings().useConstructor(SubType.Exclusive, 0, "persistent://public/default/close",
                        null, new ServiceConfiguration(), null).defaultAnswer(CALLS_REAL_METHODS));
        Consumer consumer = mock(Consumer.class);
        when(consumer.consumerName()).thenReturn("consumer");
        dispatcher.addConsumer(consumer).get(5, TimeUnit.SECONDS);
        ExecutorService clientClose = Executors.newSingleThreadExecutor();
        try {
            Answer<Void> disconnect = invocation -> {
                // A client close holds its subscription monitor before it removes from the dispatcher.
                // It must be able to finish while this disconnect waits to acquire that subscription.
                assertFalse(Thread.holdsLock(dispatcher));
                CompletableFuture.runAsync(() -> {
                    try {
                        dispatcher.removeConsumer(consumer);
                    } catch (BrokerServiceException error) {
                        throw new CompletionException(error);
                    }
                }, clientClose).get(5, TimeUnit.SECONDS);
                return null;
            };
            doAnswer(disconnect).when(consumer).disconnect(false);
            doAnswer(disconnect).when(consumer).disconnect(eq(false), any());

            CompletableFuture<Void> closing = activeOnly ? dispatcher.disconnectActiveConsumers(false)
                    : dispatcher.disconnectAllConsumers(false, Optional.empty());
            closing.get(5, TimeUnit.SECONDS);
            assertTrue(dispatcher.getConsumers().isEmpty());
        } finally {
            clientClose.shutdownNow();
            assertTrue(clientClose.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
