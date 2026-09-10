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
package org.apache.pulsar.client.impl.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.channel.Channel;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.impl.MemoryLimitController;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class V5ProducerMemoryAccountingTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] batchingModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "batchingModes", timeOut = 30_000)
    public void v5UsesTransportControllerAndReleasesItsReservation(boolean batching) throws Exception {
        try (PulsarClientImpl transportClient = newClient()) {
            PulsarClientV5 client = new PulsarClientV5(transportClient, "test", Duration.ofSeconds(30));
            MemoryLimitController memory = transportClient.getMemoryLimitController();
            assertThat(client.sendMemory()).isSameAs(memory);
            try (var producer = client.newProducer(SchemaAdapter.toV5(Schema.BYTES))
                    .topic(newTopicName())
                    .blockIfQueueFull(false)
                    .batchingPolicy(BatchingPolicy.builder().enabled(batching)
                            .maxPublishDelay(Duration.ofSeconds(30)).build())
                    .create()) {
                // The V5 reservation exceeds the limit. A second debit by v4 would reject this
                // send; an extra v4 credit would leave the shared controller with negative usage.
                for (int i = 0; i < 3; i++) {
                    assertThat(producer.newMessage().value(new byte[2048]).send()).isNotNull();
                    Awaitility.await().untilAsserted(() -> assertThat(memory.currentUsage()).isZero());
                }
            }
        }
    }

    @DataProvider
    public Object[][] transportModes() {
        return new Object[][] {{"plain"}, {"batch"}, {"chunk"}, {"oversized"}, {"timeout"}, {"close"}};
    }

    @Test(dataProvider = "transportModes", timeOut = 30_000)
    public void externalAccountingSkipsTransportDebitsAndCredits(String mode) throws Exception {
        try (PulsarClientImpl client = newClient()) {
            ProducerConfigurationData conf = new ProducerConfigurationData();
            conf.setTopicName(newTopicName());
            conf.setBlockIfQueueFull(false);
            conf.setBatchingEnabled(mode.equals("batch") || mode.equals("timeout") || mode.equals("close"));
            conf.setBatchingMaxPublishDelayMicros(TimeUnit.MINUTES.toMicros(1));
            conf.setSendTimeoutMs(mode.equals("timeout") ? 200 : 0);
            conf.setChunkingEnabled(mode.equals("chunk"));
            conf.setChunkMaxMessageSize(1024);
            try (Producer<byte[]> producer = client.createSegmentProducerAsync(conf, Schema.BYTES)
                    .get(5, TimeUnit.SECONDS);
                 Producer<byte[]> ordinary = client.newProducer().topic(newTopicName())
                         .enableBatching(false).blockIfQueueFull(false).create()) {
                MemoryLimitController memory = client.getMemoryLimitController();
                byte[] payload = new byte[mode.equals("oversized") ? getConfig().getMaxMessageSize() + 1 : 4096];
                var builder = new TypedMessageBuilderImpl<byte[]>(null, Schema.BYTES);
                builder.value(payload);
                var prepared = builder.prepare(conf.getTopicName());
                long reservation = 32 * 1024L + prepared.estimatedMemorySize();
                // Exercise the internal API's ownership contract: its caller makes and owns this
                // reservation, just as V5 does before dispatching to a segment producer.
                memory.reserveMemory(reservation);
                Channel channel = ((ProducerImpl<?>) producer).getClientCnx().ctx().channel();
                try {
                    if (mode.equals("timeout")) {
                        // Hold back broker receipts so the real send-timeout path releases the op.
                        channel.eventLoop().submit(() -> channel.config().setAutoRead(false))
                                .get(5, TimeUnit.SECONDS);
                    }
                    var send = prepared.sendAsync((ProducerBase<?>) producer, !mode.equals("close"));
                    assertThat(memory.currentUsage()).isEqualTo(reservation);
                    // Ordinary v4 traffic shares the same budget and cannot bypass admission.
                    assertThatThrownBy(() -> ordinary.sendAsync(new byte[128]).get(5, TimeUnit.SECONDS))
                            .hasCauseInstanceOf(PulsarClientException.MemoryBufferIsFullError.class);
                    if (mode.equals("oversized")) {
                        assertThatThrownBy(() -> send.get(5, TimeUnit.SECONDS))
                                .hasCauseInstanceOf(PulsarClientException.InvalidMessageException.class);
                    } else if (mode.equals("timeout")) {
                        assertThatThrownBy(() -> send.get(5, TimeUnit.SECONDS))
                                .hasCauseInstanceOf(PulsarClientException.TimeoutException.class);
                    } else if (mode.equals("close")) {
                        producer.closeAsync().get(5, TimeUnit.SECONDS);
                        send.handle((id, error) -> null).get(5, TimeUnit.SECONDS);
                    } else {
                        assertThat(send.get(5, TimeUnit.SECONDS)).isNotNull();
                    }
                    assertThat(memory.currentUsage()).isEqualTo(reservation);
                } finally {
                    channel.eventLoop().submit(() -> channel.config().setAutoRead(true))
                            .get(5, TimeUnit.SECONDS);
                    memory.releaseMemory(reservation);
                }
                assertThat(memory.currentUsage()).isZero();
                assertThat(ordinary.sendAsync(new byte[128]).get(5, TimeUnit.SECONDS)).isNotNull();
                Awaitility.await().untilAsserted(() -> assertThat(memory.currentUsage()).isZero());
            }
        }
    }

    private PulsarClientImpl newClient() throws Exception {
        return (PulsarClientImpl) PulsarClient.builder().serviceUrl(getBrokerServiceUrl())
                .memoryLimit(1024, SizeUnit.BYTES).build();
    }
}
