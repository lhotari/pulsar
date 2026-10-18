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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.broker.BrokerTestUtil.newUniqueName;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class TopicCloseAckDrainTest extends ProducerConsumerBase {
    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        conf.setTopicCloseAckDrainTimeoutMillis(10_000);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider
    public Object[][] unloads() {
        // a topic's unload, and its bundle's unload, which closes the topic without waiting for its clients, as a
        // rolling restart of the brokers does
        return new Object[][] {{false}, {true}};
    }

    @Test(timeOut = 30_000, dataProvider = "unloads")
    public void testUnloadWaitsForTheConsumersAcknowledgments(boolean bundleUnload) throws Exception {
        String topic = newUniqueName("persistent://public/default/ack-drain");
        @lombok.Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .subscriptionName("sub").subscriptionType(SubscriptionType.Key_Shared).subscribe();
        @lombok.Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic)
                .enableBatching(false).create();
        for (int i = 0; i < 10; i++) {
            producer.newMessage().key("key-" + i).value("message-" + i).send();
        }
        List<Message<String>> received = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            received.add(consumer.receive(5, TimeUnit.SECONDS));
        }

        // the consumer has received the messages but not acknowledged them
        CompletableFuture<Void> unload = bundleUnload ? admin.namespaces().unloadAsync("public/default")
                : admin.topics().unloadAsync(topic);
        Thread.sleep(500);
        assertThat(unload).isNotDone();

        for (Message<String> message : received) {
            consumer.acknowledge(message);
        }
        unload.get(10, TimeUnit.SECONDS);

        // the topic, loaded again, has nothing to redeliver
        assertThat(consumer.receive(2, TimeUnit.SECONDS)).isNull();
    }
}
