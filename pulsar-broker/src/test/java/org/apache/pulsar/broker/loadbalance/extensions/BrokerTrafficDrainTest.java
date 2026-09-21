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
package org.apache.pulsar.broker.loadbalance.extensions;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateTableViewImpl;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException.BrokerDrainingException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ConsumerImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/** Verifies admission and existing traffic over a shared real binary connection. */
@Test(groups = "broker")
public class BrokerTrafficDrainTest extends ExtensibleLoadManagerImplBaseTest {
    private final CompletableFuture<Void> drainEntered = new CompletableFuture<>();
    private final CompletableFuture<Void> resumeDrain = new CompletableFuture<>();
    private volatile BrokerService heldBroker;

    private final boolean tls;

    @DataProvider
    public static Object[][] transportAndTableView() {
        return new Object[][]{
                {ServiceUnitStateTableViewImpl.class.getName(), false},
                {ServiceUnitStateTableViewImpl.class.getName(), true},
                {ServiceUnitStateMetadataStoreTableViewImpl.class.getName(), false},
                {ServiceUnitStateMetadataStoreTableViewImpl.class.getName(), true}
        };
    }

    @Factory(dataProvider = "transportAndTableView")
    public BrokerTrafficDrainTest(String tableViewClassName, boolean tls) {
        super("public/shutdown-traffic", tableViewClassName);
        this.tls = tls;
    }

    @Override
    protected ServiceConfiguration updateConfig(ServiceConfiguration configuration) {
        super.updateConfig(configuration);
        configuration.setBrokerClientTlsEnabled(tls);
        configuration.setBrokerClientTrustCertsFilePath(caCertPath);
        return configuration;
    }

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        updateConfig(conf);
        super.setup();
    }

    @Override
    protected BrokerService customizeNewBrokerService(BrokerService service) {
        BrokerService broker = mockingDetails(service).isMock() ? service : spy(service);
        doAnswer(invocation -> {
            if (heldBroker == broker) {
                drainEntered.complete(null);
                resumeDrain.get(30, TimeUnit.SECONDS);
            }
            return invocation.callRealMethod();
        }).when(broker).unloadNamespaceBundlesGracefully(anyInt(), anyBoolean());
        return broker;
    }

    @Test(timeOut = 60000)
    public void existingTrafficSurvivesAdmissionCutoff() throws Exception {
        String firstTopic = "persistent://" + defaultTestNamespace + "/first";
        String secondTopic = "persistent://" + defaultTestNamespace + "/second";
        assertTrue(!pulsar1.getNamespaceService().getBundle(TopicName.get(firstTopic)).equals(
                pulsar1.getNamespaceService().getBundle(TopicName.get(secondTopic))));
        doReturn(CompletableFuture.completedFuture(Optional.of(pulsar1.getBrokerId())))
                .when(primaryLoadManager).selectAsync(any(), any(), any());
        doReturn(CompletableFuture.completedFuture(Optional.of(pulsar1.getBrokerId())))
                .when(secondaryLoadManager).selectAsync(any(), any(), any());
        CompletableFuture<Void> shutdown = null;
        try (PulsarClient client = PulsarClient.builder().serviceUrl(tls
                ? pulsar1.getBrokerServiceUrlTls() : pulsar1.getBrokerServiceUrl()).tlsTrustCertsFilePath(caCertPath)
                .connectionsPerBroker(1).operationTimeout(10, TimeUnit.SECONDS).build();
             var producer1 = client.newProducer().topic(firstTopic).enableBatching(false).create();
             var producer2 = client.newProducer().topic(secondTopic).enableBatching(false).create();
             var consumer1 = client.newConsumer().topic(firstTopic).subscriptionName("drain").receiverQueueSize(1)
                     .subscribe();
             var consumer2 = client.newConsumer().topic(secondTopic).subscriptionName("drain").receiverQueueSize(1)
                     .subscribe()) {
            var connection = ((ConsumerImpl<?>) consumer1).getClientCnx();
            assertSame(connection, ((ConsumerImpl<?>) consumer2).getClientCnx());
            assertSame(connection, ((ProducerImpl<?>) producer1).getClientCnx());
            assertSame(connection, ((ProducerImpl<?>) producer2).getClientCnx());
            doCallRealMethod().when(primaryLoadManager).selectAsync(any(), any(), any());
            doCallRealMethod().when(secondaryLoadManager).selectAsync(any(), any(), any());
            heldBroker = pulsar1.getBrokerService();
            pulsar1.getConfig().setBrokerShutdownTimeoutMs(30000);
            shutdown = pulsar1.closeAsync();
            drainEntered.get(10, TimeUnit.SECONDS);
            assertEquals(pulsar1.getShutdownLookupServiceUrl().orElseThrow(),
                    tls ? pulsar2.getBrokerServiceUrlTls() : pulsar2.getBrokerServiceUrl());
            URI endpoint = URI.create(tls ? pulsar1.getBrokerServiceUrlTls() : pulsar1.getBrokerServiceUrl());
            Awaitility.await().untilAsserted(() -> {
                try (Socket socket = new Socket()) {
                    expectThrows(IOException.class, () -> socket.connect(
                            new InetSocketAddress(endpoint.getHost(), endpoint.getPort()), 1000));
                }
            });
            var producerError = expectThrows(ExecutionException.class, () -> connection.sendScalableSessionRequest(
                    Commands.newProducer(firstTopic, 9999, 9999, "rejected", Collections.emptyMap(), false), 9999)
                    .get(5, TimeUnit.SECONDS));
            assertTrue(producerError.getCause() instanceof PulsarClientException.ServiceNotReadyException,
                    producerError.toString());
            var consumerError = expectThrows(ExecutionException.class, () -> connection.sendScalableSessionRequest(
                    Commands.newSubscribe(firstTopic, "rejected", 9999, 10000,
                            CommandSubscribe.SubType.Shared, 0, "rejected", 0), 10000).get(5, TimeUnit.SECONDS));
            assertEquals(consumerError.getCause().getClass(), producerError.getCause().getClass());
            assertNotNull(connection.newLookup(Commands.newLookup(firstTopic, false, 10001), 10001)
                    .get(5, TimeUnit.SECONDS));
            assertNotNull(connection.newLookup(Commands.newPartitionMetadataRequest(firstTopic, 10002, false), 10002)
                    .get(5, TimeUnit.SECONDS));
            // Empty ownership and unexpected failures can occur while assignments are moving. Their
            // lookup errors must preserve this same connection, including its unaffected bundle.
            var namespace = pulsar1.getNamespaceService();
            TopicName requested = TopicName.get(firstTopic);
            try {
                for (int failure = 0; failure < 3; failure++) {
                    CompletableFuture<?> lookup = failure == 0
                            ? CompletableFuture.completedFuture(Optional.empty())
                            : CompletableFuture.failedFuture(failure == 1 ? new RuntimeException("Lookup failed")
                                    : new BrokerDrainingException());
                    doReturn(lookup).when(namespace).getBrokerServiceUrlAsync(eq(requested), any(LookupOptions.class));
                    long requestId = 20000L + failure;
                    var lookupError = expectThrows(ExecutionException.class, () -> connection.newLookup(
                            Commands.newLookup(firstTopic, false, requestId), requestId).get(5, TimeUnit.SECONDS));
                    assertTrue(lookupError.getCause() instanceof PulsarClientException.BrokerMetadataException,
                            lookupError.toString());
                    assertTrue(connection.ctx().channel().isActive());
                    assertSame(connection, ((ProducerImpl<?>) producer2).getClientCnx());
                }
            } finally {
                doCallRealMethod().when(namespace).getBrokerServiceUrlAsync(eq(requested), any(LookupOptions.class));
            }
            for (int i = 0; i < 3; i++) {
                producer1.send(new byte[]{(byte) i});
                producer2.send(new byte[]{(byte) i});
                var message1 = consumer1.receive(5, TimeUnit.SECONDS);
                var message2 = consumer2.receive(5, TimeUnit.SECONDS);
                assertNotNull(message1);
                assertNotNull(message2);
                consumer1.acknowledge(message1);
                consumer2.acknowledge(message2);
                assertTrue(consumer1.isConnected());
                assertSame(connection, ((ConsumerImpl<?>) consumer1).getClientCnx());
            }
            resumeDrain.complete(null);
            shutdown.get(35, TimeUnit.SECONDS);
            // The close commands include the successor, so the existing client can relocate after drain.
            producer1.send(new byte[]{42});
            var relocated = consumer1.receive(10, TimeUnit.SECONDS);
            assertNotNull(relocated);
            consumer1.acknowledge(relocated);
        } finally {
            resumeDrain.complete(null);
            if (shutdown != null) {
                shutdown.get(35, TimeUnit.SECONDS);
            }
        }
    }
}
