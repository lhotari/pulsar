/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.proxy.server;

import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.SocatContainer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doReturn;

public class ProxyStuckConnectionTest extends MockedPulsarServiceBaseTest {

    private static final Logger log = LoggerFactory.getLogger(ProxyStuckConnectionTest.class);

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig;
    private SocatContainer socatContainer;

    private static String brokerServiceUriSocat;
    private static volatile boolean useBrokerSocatProxy = true;

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setKeepAliveIntervalSeconds(3);
    }

    @Override
    @BeforeMethod
    protected void setup() throws Exception {
        useBrokerSocatProxy = true;
        internalSetup();

        int brokerPort = pulsar.getBrokerService().getListenPort().get();
        Testcontainers.exposeHostPorts(brokerPort);

        socatContainer = new SocatContainer();
        socatContainer.withTarget(brokerPort, "host.testcontainers.internal", brokerPort);
        socatContainer.start();
        brokerServiceUriSocat = "pulsar://" + socatContainer.getHost() + ":" + socatContainer.getMappedPort(brokerPort);

        proxyConfig = new ProxyConfiguration();
        proxyConfig.setServicePort(Optional.ofNullable(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        proxyConfig.setLookupHandler(TestLookupProxyHandler.class.getName());

        startProxyService();
        // use the same port for subsequent restarts
        proxyConfig.setServicePort(proxyService.getListenPort());
    }

    private void startProxyService() throws Exception {
        proxyService = Mockito.spy(new ProxyService(proxyConfig, new AuthenticationService(
                PulsarConfigurationLoader.convertFrom(proxyConfig))));
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(proxyService).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeperGlobal)).when(proxyService).createConfigurationMetadataStore();
        proxyService.start();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        if (proxyService != null) {
            proxyService.close();
        }
        if (socatContainer != null) {
            socatContainer.close();
        }
    }

    private static final class TestLookupProxyHandler extends DefaultLookupProxyHandler {
        @Override
        protected CompletableFuture<String> performLookup(long clientRequestId, String topic, String brokerServiceUrl, boolean authoritative, int numberOfRetries) {
            return super.performLookup(clientRequestId, topic, brokerServiceUrl, authoritative, numberOfRetries)
                    .thenApply(url -> useBrokerSocatProxy ? brokerServiceUriSocat : url);
        }
    }

    @Test
    public void testKeySharedStickyWithStuckConnection() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .keepAliveInterval(2, TimeUnit.SECONDS)
                .operationTimeout(5, TimeUnit.SECONDS)
                .build();
        String topicName = BrokerTestUtil.newUniqueName("persistent://sample/test/local/test-topic");

        @Cleanup
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName("test-subscription")
                .subscriptionType(SubscriptionType.Key_Shared)
                .keySharedPolicy(KeySharedPolicy.stickyHashRange()
                        .ranges(Range.of(0, 65535)))
                .receiverQueueSize(2)
                .subscribe();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .enableBatching(false)
                .create();
        for (int i = 0; i < 10; i++) {
            producer.newMessage().value(("test" + i).getBytes())
                    .key("A")
                    .send();
        }

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive();
            log.info("Received message {}", new String(msg.getData()));
            consumer.acknowledge(msg);
            if (i == 2) {
                log.info("Pausing connection between proxy and broker and making further connections from proxy directly to broker");
                useBrokerSocatProxy = false;
                socatContainer.getDockerClient().pauseContainerCmd(socatContainer.getContainerId()).exec();
            }
        }
    }


    @Test
    public void testExclusiveProducerWithStuckConnection() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .keepAliveInterval(2, TimeUnit.SECONDS)
                .operationTimeout(5, TimeUnit.SECONDS)
                .build();

        @Cleanup
        Producer<byte[]> producer = null;

        for (int i = 0; i < 10; i++) {
            boolean sentOk = false;
            for (int j = 0; j < 10; j++) {
                try {
                    if (producer == null) {
                        producer = client.newProducer()
                                .topic("persistent://sample/test/local/producer-topic")
                                .accessMode(ProducerAccessMode.Exclusive)
                                .producerName("test-producer")
                                .enableBatching(false)
                                .create();
                    }
                    producer.send(("test" + i).getBytes());
                    sentOk = true;
                    break;
                } catch (PulsarClientException.ProducerFencedException e) {
                    log.info("Producer fenced, retrying");
                    if (producer != null) {
                        producer.close();
                        producer = null;
                    }
                    Thread.sleep(1000);
                }
            }
            if (!sentOk) {
                Assert.fail("Failed to send message (i=" + i + ")");
            }
            if (i == 5) {
                log.info("Pausing connection between proxy and broker and making further connections from proxy directly to broker");
                useBrokerSocatProxy = false;
                socatContainer.getDockerClient().pauseContainerCmd(socatContainer.getContainerId()).exec();
            }
        }
    }
}
