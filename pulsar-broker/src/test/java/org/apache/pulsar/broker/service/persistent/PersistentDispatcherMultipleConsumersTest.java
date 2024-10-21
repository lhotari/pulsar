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

import com.carrotsearch.hppc.ObjectSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class PersistentDispatcherMultipleConsumersTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setManagedLedgerMaxReadsInFlightSizeInMB(10);
        conf.setManagedLedgerCacheSizeMB(1);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30 * 1000)
    public void testTopicDeleteIfConsumerSetMismatchConsumerList() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        // Make an error that "consumerSet" is mismatch with "consumerList".
        Dispatcher dispatcher = pulsar.getBrokerService()
                .getTopic(topicName, false).join().get()
                .getSubscription(subscription).getDispatcher();
        ObjectSet<org.apache.pulsar.broker.service.Consumer> consumerSet =
                WhiteboxImpl.getInternalState(dispatcher, "consumerSet");
        List<org.apache.pulsar.broker.service.Consumer> consumerList =
                WhiteboxImpl.getInternalState(dispatcher, "consumerList");

        org.apache.pulsar.broker.service.Consumer serviceConsumer = consumerList.get(0);
        consumerSet.add(serviceConsumer);
        consumerList.add(serviceConsumer);

        // Verify: the topic can be deleted successfully.
        consumer.close();
        admin.topics().delete(topicName, false);
    }

    @Test(timeOut = 30 * 1000)
    public void testTopicDeleteIfConsumerSetMismatchConsumerList2() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subscription, MessageId.earliest);

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared).subscribe();
        // Make an error that "consumerSet" is mismatch with "consumerList".
        Dispatcher dispatcher = pulsar.getBrokerService()
                .getTopic(topicName, false).join().get()
                .getSubscription(subscription).getDispatcher();
        ObjectSet<org.apache.pulsar.broker.service.Consumer> consumerSet =
                WhiteboxImpl.getInternalState(dispatcher, "consumerSet");
        consumerSet.clear();

        // Verify: the topic can be deleted successfully.
        consumer.close();
        admin.topics().delete(topicName, false);
    }

    @Test
    public void testSkipReadEntriesFromCloseCursor() throws Exception {
        final String topicName =
                BrokerTestUtil.newUniqueName("persistent://public/default/testSkipReadEntriesFromCloseCursor");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(topicName);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        for (int i = 0; i < 10; i++) {
            producer.send("message-" + i);
        }
        producer.close();

        // Get the dispatcher of the topic.
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopic(topicName, false).join().get();

        ManagedCursor cursor = Mockito.mock(ManagedCursorImpl.class);
        Mockito.doReturn(subscription).when(cursor).getName();
        Subscription sub = Mockito.mock(PersistentSubscription.class);
        Mockito.doReturn(topic).when(sub).getTopic();
        // Mock the dispatcher.
        PersistentDispatcherMultipleConsumers dispatcher =
                Mockito.spy(new PersistentDispatcherMultipleConsumers(topic, cursor, sub));
        // Return 10 permits to make the dispatcher can read more entries.
        Mockito.doReturn(10).when(dispatcher).getFirstAvailableConsumerPermits();

        // Make the count + 1 when call the scheduleReadEntriesWithDelay(...).
        AtomicInteger callScheduleReadEntriesWithDelayCnt = new AtomicInteger(0);
        Mockito.doAnswer(inv -> {
            callScheduleReadEntriesWithDelayCnt.getAndIncrement();
            return inv.callRealMethod();
        }).when(dispatcher).scheduleReadEntriesWithDelay(Mockito.any(), Mockito.any(), Mockito.anyLong());

        // Make the count + 1 when call the readEntriesFailed(...).
        AtomicInteger callReadEntriesFailed = new AtomicInteger(0);
        Mockito.doAnswer(inv -> {
            callReadEntriesFailed.getAndIncrement();
            return inv.callRealMethod();
        }).when(dispatcher).readEntriesFailed(Mockito.any(), Mockito.any());

        Mockito.doReturn(false).when(cursor).isClosed();

        // Mock the readEntriesOrWait(...) to simulate the cursor is closed.
        Mockito.doAnswer(inv -> {
            AbstractPersistentDispatcherMultipleConsumers dispatcher1 = inv.getArgument(2);
            dispatcher1.readEntriesFailed(new ManagedLedgerException.CursorAlreadyClosedException("cursor closed"),
                    null);
            return null;
        }).when(cursor).asyncReadEntriesWithSkipOrWait(Mockito.anyInt(), Mockito.anyLong(), Mockito.eq(dispatcher),
                Mockito.any(), Mockito.any(), Mockito.any());

        dispatcher.readMoreEntries();

        // Verify: the readEntriesFailed should be called once and the scheduleReadEntriesWithDelay should not be called.
        Assert.assertTrue(callReadEntriesFailed.get() == 1 && callScheduleReadEntriesWithDelayCnt.get() == 0);

        // Verify: the topic can be deleted successfully.
        admin.topics().delete(topicName, false);
    }

    @Test(timeOut = 30 * 1000)
    public void testManagedLedgerMaxReadsInFlightSizeInMBForRedeliveries() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName(
                "persistent://public/default/testManagedLedgerMaxReadsInFlightSizeInMBForRedeliveries");
        final String subscription = "sub";

        // Create two consumers on a shared subscription
        @Cleanup
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(10000)
                .subscribe();

        @Cleanup
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subscription)
                .subscriptionType(SubscriptionType.Shared)
                .startPaused(true)
                .receiverQueueSize(10000)
                .subscribe();

        // Produce about 20MB of messages
        @Cleanup
        Producer<byte[]> producer =
                pulsarClient.newProducer().enableBatching(false).topic(topicName).create();
        int numberOfMessages = 200;
        byte[] payload = RandomUtils.nextBytes(1025 * 1024); // 1025kB
        for (int i = 0; i < numberOfMessages; i++) {
            producer.send(payload);
        }

        // Consume messages with consumer1 but don't ack
        for (int i = 0; i < numberOfMessages; i++) {
            consumer1.receive();
        }

        // Close consumer1 and resume consumer2
        consumer1.close();

        Executor executor = CompletableFuture.delayedExecutor(4, TimeUnit.SECONDS);
        pulsarTestContext.getMockBookKeeper().setReadHandleInterceptor((ledgerId, firstEntry, lastEntry, entries) -> {
            return CompletableFuture.supplyAsync(() -> entries, executor);
        });

        consumer2.resume();

        // Verify that consumer2 can receive the messages
        for (int i = 0; i < 100; i++) {
            Message<byte[]> msg = consumer2.receive(5, TimeUnit.SECONDS);
            Assert.assertNotNull(msg, "Consumer2 should receive the message");
            consumer2.acknowledge(msg);
        }
    }
}
