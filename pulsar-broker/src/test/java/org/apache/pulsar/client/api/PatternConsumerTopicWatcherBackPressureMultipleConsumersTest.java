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
package org.apache.pulsar.client.api;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class PatternConsumerTopicWatcherBackPressureMultipleConsumersTest extends MockedPulsarServiceBaseTest {

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        isTcpLookup = useTcpLookup();
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setSubscriptionPatternMaxLength(100);
    }

    protected boolean useTcpLookup() {
        return true;
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60 * 1000)
    public void testPatternConsumerWithLargeAmountOfConcurrentClientConnections()
            throws PulsarAdminException, InterruptedException, IOException, ExecutionException, TimeoutException {
        // create a new namespace for this test
        String namespace = BrokerTestUtil.newUniqueName("public/ns");
        admin.namespaces().createNamespace(namespace);

        // use multiple clients so that each client has a separate connection to the broker
        final int numberOfClients = 100;

        // create a long topic name to consume more memory per topic
        final String topicNamePrefix = "persistent://" + namespace + "/" + StringUtils.repeat('a', 512) + "-";
        // number of topics to create
        final int topicCount = 1000;

        List<CompletableFuture<Void>> createTopicFutures =
                IntStream.range(0, topicCount)
                        .mapToObj(i -> admin.topics().createNonPartitionedTopicAsync(topicNamePrefix + i)).toList();
        // wait for all topics to be created
        FutureUtil.waitForAll(createTopicFutures).get(30, TimeUnit.SECONDS);

        @Cleanup
        PulsarClientSharedResources sharedResources = PulsarClientSharedResources.builder()
                // limit number of threads so that the test behaves somewhat similarly in CI
                .configureEventLoop(eventLoopGroupConfig -> eventLoopGroupConfig.numberOfThreads(2))
                .configureThreadPool(PulsarClientSharedResources.SharedResource.InternalExecutor,
                        threadPoolConfig -> threadPoolConfig.numberOfThreads(2))
                        .build();
        List<PulsarClientImpl> clients = new ArrayList<>(numberOfClients);
        @Cleanup
        Closeable closeClients = () -> {
            for (PulsarClient client : clients) {
                try {
                    client.close();
                } catch (PulsarClientException e) {
                    log.error("Failed to close client {}", client, e);
                }
            }
        };
        for (int i = 0; i < numberOfClients; i++) {
            PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder()
                    .serviceUrl(getClientServiceUrl())
                    .statsInterval(0, TimeUnit.SECONDS)
                    .sharedResources(sharedResources)
                    .build();
            clients.add(client);
        }

        List<CompletableFuture<Consumer<String>>> consumerFutures = new ArrayList<>(numberOfClients);
        for (int i = 0; i < topicCount; i++) {
            consumerFutures.add(clients.get(i % numberOfClients).newConsumer(Schema.STRING)
                    .topicsPattern(namespace + "/.*-" + i + "$").subscriptionName("sub" + i)
                    .subscribeAsync());
        }

        FutureUtil.waitForAll(consumerFutures).get(30, TimeUnit.SECONDS);

        List<Consumer<String>> consumers = consumerFutures.stream().map(CompletableFuture::join).toList();

        PulsarClientImpl client = clients.get(0);
        for (int i = 0; i < topicCount; i++) {
            // send message to every topic partition
            Producer<String> producer =
                    client.newProducer(Schema.STRING).topic(topicNamePrefix + i).create();
            producer.send("test" + i);
            producer.close();
        }

        // validate that every consumer receives a single message
        for (int i = 0; i < consumers.size(); i++) {
            Consumer<String> consumer = consumers.get(i);
            int finalI = i;
            assertThat(consumer.receive(5, TimeUnit.SECONDS)).isNotNull()
                    .satisfies(message -> assertThat(message.getValue()).isEqualTo("test" + finalI));
            // validate that no more messages are received
            assertThat(consumer.receive(1, TimeUnit.MICROSECONDS)).isNull();
        }
    }

    protected String getClientServiceUrl() {
        return lookupUrl.toString();
    }
}
