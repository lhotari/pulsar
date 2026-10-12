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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The broker copies dispatched messages up to {@link PulsarCommandSenderImpl#MAX_COPIED_MESSAGE_BYTES} into shared
 * buffers and writes larger ones with a reference to the entry: a consumer receives both, mixed, in order and intact.
 */
@Test(groups = "broker-api")
public class DispatchSmallMessagesCoalescingTest extends ProducerConsumerBase {

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60_000)
    public void testMixedSmallAndLargeMessagesArriveInOrder() throws Exception {
        verifyDelivery(false);
    }

    @Test(timeOut = 60_000)
    public void testMixedSmallAndLargeBatchesArriveInOrder() throws Exception {
        verifyDelivery(true);
    }

    private void verifyDelivery(boolean batching) throws Exception {
        String topicName = newTopicName();
        // sizes around the copy limit and beyond the coalesced buffer's size, so that a dispatch has copied and
        // referenced messages and fills more than one buffer
        int[] sizes = {1, 100, PulsarCommandSenderImpl.MAX_COPIED_MESSAGE_BYTES - 200,
                PulsarCommandSenderImpl.MAX_COPIED_MESSAGE_BYTES + 1, 10, 50_000, 300, 1500};
        int count = 600;
        List<byte[]> sent = new ArrayList<>(count);
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                     .topic(topicName)
                     .subscriptionName("sub")
                     .subscriptionType(SubscriptionType.Shared)
                     .receiverQueueSize(1000)
                     .subscribe();
             Producer<byte[]> producer = pulsarClient.newProducer()
                     .topic(topicName)
                     .enableBatching(batching)
                     .batchingMaxMessages(5)
                     .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                     .create()) {
            for (int i = 0; i < count; i++) {
                byte[] payload = new byte[sizes[i % sizes.length]];
                Arrays.fill(payload, (byte) i);
                payload[0] = (byte) (i >> 8);
                sent.add(payload);
                producer.sendAsync(payload);
            }
            producer.flush();
            for (int i = 0; i < count; i++) {
                Message<byte[]> message = consumer.receive(10, TimeUnit.SECONDS);
                assertThat(message).as("message %d", i).isNotNull();
                assertThat(message.getData()).as("message %d", i).isEqualTo(sent.get(i));
                consumer.acknowledge(message);
            }
        }
    }
}
