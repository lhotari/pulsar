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
package org.apache.pulsar.proxy.server;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.doReturn;

public class ProxyRestartTest extends MockedPulsarServiceBaseTest {

    private static final Logger log = LoggerFactory.getLogger(ProxyRestartTest.class);

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0");

    private static String brokerServiceUriToxiproxy;
    private static volatile boolean useBrokerToxiproxy = true;
    private Proxy brokerproxyControl;
    private boolean isCurrentlyCut;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();

        int brokerPort = pulsar.getBrokerService().getListenPort().get();
        Testcontainers.exposeHostPorts(brokerPort);

        toxiproxy.start();
        final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        int brokerproxyPort = 8666;
        brokerServiceUriToxiproxy = "pulsar://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(brokerproxyPort);

        brokerproxyControl = toxiproxyClient.createProxy("brokerproxy", "0.0.0.0:" + brokerproxyPort, "host.testcontainers.internal:" + brokerPort);

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
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();

        proxyService.close();
        toxiproxy.close();
    }

    private static final class TestLookupProxyHandler extends DefaultLookupProxyHandler {
        @Override
        protected CompletableFuture<String> performLookup(long clientRequestId, String topic, String brokerServiceUrl, boolean authoritative, int numberOfRetries) {
            return super.performLookup(clientRequestId, topic, brokerServiceUrl, authoritative, numberOfRetries)
                    .thenApply(url -> useBrokerToxiproxy ? brokerServiceUriToxiproxy : url);
        }
    }

    private void setConnectionCut(boolean shouldCutConnection) {
        try {
            if (shouldCutConnection) {
                brokerproxyControl.toxics().resetPeer("CUT_CONNECTION_DOWNSTREAM", ToxicDirection.DOWNSTREAM, 0L);
                brokerproxyControl.toxics().timeout("CUT_CONNECTION_UPSTREAM", ToxicDirection.UPSTREAM, 0L);
                isCurrentlyCut = true;
            } else if (isCurrentlyCut) {
                brokerproxyControl.toxics().get("CUT_CONNECTION_DOWNSTREAM").remove();
                brokerproxyControl.toxics().get("CUT_CONNECTION_UPSTREAM").remove();
                isCurrentlyCut = false;
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not control proxy", e);
        }
    }
    @Test
    public void testProducer() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(proxyService.getServiceUrl())
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic("persistent://sample/test/local/producer-topic")
                .accessMode(ProducerAccessMode.Exclusive)
                .producerName("test-producer")
                .create();

        for (int i = 0; i < 10; i++) {
            producer.send(("test" + i).getBytes());
            if (i == 5) {
                log.info("Cutting connection");
                setConnectionCut(true);
                log.info("Connecting directly to broker");
                useBrokerToxiproxy=false;
            }
        }
    }
}
