/**
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

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Test(groups = "broker-impl")
public class ReaderTestCPULimited extends MockedPulsarServiceBaseTest {

    private static final String subscription = "reader-sub";
    private volatile CompletableFuture<Void> outstandingReadToEnd = null;
    private final AtomicInteger count = new AtomicInteger(0);
    private final String topic = "persistent://my-property/my-ns/my-reader-topic";
    private final String topic2 = "persistent://my-property/my-ns/my-reader-topic2";
    private final int numKeys = 10000;

    Producer<byte[]> producer;

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.ioThreads(2);
        clientBuilder.listenerThreads(2);
        //clientBuilder.memoryLimit(1024, SizeUnit.BYTES);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns", Sets.newHashSet("test"));

        ProducerBuilder<byte[]> builder = pulsarClient.newProducer();
        builder.messageRoutingMode(MessageRoutingMode.SinglePartition);
        builder.maxPendingMessages(numKeys);
        // disable periodical flushing
        builder.batchingMaxPublishDelay(1, TimeUnit.DAYS);
        builder.topic(topic);
        builder.enableBatching(false);

        producer = builder.create();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (producer != null) {
            producer.close();
        }
        super.internalCleanup();
    }

    private Set<String> publishMessages(String topic, int count) throws Exception {
        Set<String> keys = new HashSet<>();
        Future<?> lastFuture = null;
        for (int i = 0; i < count; i++) {
            String key = "key"+i;
            byte[] data = ("my-message-" + i).getBytes();
            lastFuture = producer.newMessage().key(key).value(data).sendAsync();
            keys.add(key);
            producer.flush();
        }
        lastFuture.get();
        return keys;
    }

    @Test
    public void testReadMessage() throws Exception {
        Set<String> keys = publishMessages(topic, numKeys);
        log.info("done publishing");

        super.restartBroker();

        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .readerName(subscription)
                .create();

        while (reader.hasMessageAvailable()) {
            Message<byte[]> message = reader.readNext();
            log.info("read message {}", message.getKey());
            Assert.assertTrue(keys.remove(message.getKey()));
        }
        Assert.assertTrue(keys.isEmpty());

        Reader<byte[]> readLatest = pulsarClient.newReader().topic(topic).startMessageId(MessageId.latest)
                                                .readerName(subscription + "latest").create();
        Assert.assertFalse(readLatest.hasMessageAvailable());
    }
/*
    @Test
    public void testReadMessageAsync() throws Exception {
        String topic = "persistent://my-property/my-ns/my-reader-topic";
        int numKeys = 10000;
        publishMessages(topic, numKeys);
        Reader<byte[]> reader = pulsarClient.newReader()
                .topic(topic)
                .startMessageId(MessageId.earliest)
                .readerName(subscription)
                .create();

        count.set(0);
        CompletableFuture<Void> endFuture = new CompletableFuture<>();
        readToEnd(endFuture, reader);
        endFuture.join();

        Assert.assertEquals(count.get(), numKeys);
    }
*/
    void readToEnd(CompletableFuture<Void> future, Reader reader) {
        synchronized (this) {
            if (outstandingReadToEnd != null) {
                outstandingReadToEnd.whenComplete((result, cause) -> {
                    if (null != cause) {
                        future.completeExceptionally(cause);
                    } else {
                        future.complete(result);
                    }
                });
                // return if the outstanding read has been issued
                return;
            } else {
                outstandingReadToEnd = future;
                future.whenComplete((result, cause) -> {
                    synchronized (ReaderTestCPULimited.this) {
                        outstandingReadToEnd = null;
                    }
                });
            }
        }

        checkAndReadNext(future, reader);
    }

    private void checkAndReadNext(CompletableFuture<Void> endFuture, Reader reader) {
        reader.hasMessageAvailableAsync().whenComplete((hasMessageAvailable, cause) -> {
            if (null != cause) {
                endFuture.completeExceptionally((Throwable) cause);
            } else {
                if ((Boolean) hasMessageAvailable) {
                    readNext(endFuture, reader);
                } else {
                    endFuture.complete(null);
                }
            }
        });
    }

    private void readNext(CompletableFuture<Void> endFuture, Reader reader) {
        reader.readNextAsync().whenComplete((message, cause) -> {
            if (null != cause) {
                endFuture.completeExceptionally((Throwable) cause);
            } else {
                log.info("read message {}", ((Message) message).getKey());
                count.incrementAndGet();
                checkAndReadNext(endFuture, reader);
            }
        });
    }

}
