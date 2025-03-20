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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class AbsoluteFqdnAdvertisedAddressTest extends ProducerConsumerBase {
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setAdvertisedAddress(null);
        conf.setAdvertisedAddressAutoResolvedHostNameAsAbsoluteDnsName(true);
    }

    @Test
    public void testLookupUrlHostIsAbsoluteFqdn() throws PulsarClientException {
        log.info("Lookup URL: {}", lookupUrl.toString());
        // absolute FQDN should end with a dot
        assertThat(lookupUrl.getHost()).endsWith(".");
    }

    @Test
    public void testBrokerIdDoesntContainTheDot() throws PulsarClientException {
        log.info("Broker ID: {}", pulsar.getBrokerId());
        assertThat(pulsar.getBrokerId()).doesNotContain(".:");
    }

    @Test
    public void testPublishConsumeWithAbsoluteFqdn() throws PulsarClientException {
        @Cleanup
        PulsarClient newPulsarClient = PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .build();

        final String topic = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/test-topic");

        @Cleanup
        Consumer<byte[]> consumer = newPulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("my-sub")
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = newPulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .create();

        final int numMessages = 5;
        for (int i = 0; i < numMessages; i++) {
            producer.newMessage()
                    .value(("value-" + i).getBytes(UTF_8))
                    .sendAsync();
        }
        producer.flush();

        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> msg = consumer.receive(1, TimeUnit.SECONDS);
            if (msg == null) {
                throw new RuntimeException("Did not receive message in time");
            }
            log.info("Received message '{}'.", new String(msg.getValue(), UTF_8));
            consumer.acknowledge(msg);
        }
    }
}
