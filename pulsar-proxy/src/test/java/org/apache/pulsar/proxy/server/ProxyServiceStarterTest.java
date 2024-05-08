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
package org.apache.pulsar.proxy.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.websocket.data.ProducerMessage;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.util.BufferUtil;
import org.eclipse.jetty.websocket.api.Callback;
import org.eclipse.jetty.websocket.api.Frame;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketFrame;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ProxyServiceStarterTest extends MockedPulsarServiceBaseTest {

    public static final String[] ARGS = new String[]{"-c", "./src/test/resources/proxy.conf"};

    protected ProxyServiceStarter serviceStarter;
    protected String serviceUrl;

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        internalSetup();
        serviceStarter = new ProxyServiceStarter(ARGS, null, true);
        serviceStarter.getConfig().setBrokerServiceURL(pulsar.getBrokerServiceUrl());
        serviceStarter.getConfig().setBrokerWebServiceURL(pulsar.getWebServiceAddress());
        serviceStarter.getConfig().setWebServicePort(Optional.of(0));
        serviceStarter.getConfig().setServicePort(Optional.of(0));
        serviceStarter.getConfig().setWebSocketServiceEnabled(true);
        serviceStarter.getConfig().setBrokerProxyAllowedTargetPorts("*");
        serviceStarter.getConfig().setClusterName(configClusterName);
        serviceStarter.start();
        serviceUrl = serviceStarter.getProxyService().getServiceUrl();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        internalCleanup();
        serviceStarter.close();
    }

    private String computeWsBasePath() {
        return String.format("ws://localhost:%d/ws", serviceStarter.getServer().getListenPortHTTP().get());
    }


    @Test
    public void testProducer() throws Exception {
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl)
                .build();

        @Cleanup
        Producer<byte[]> producer = client.newProducer()
                .topic("persistent://sample/test/local/websocket-topic")
                .create();

        for (int i = 0; i < 10; i++) {
            producer.send("test".getBytes());
        }
    }

    @Test
    public void testProduceAndConsumeMessageWithWebsocket() throws Exception {
        @Cleanup("stop")
        HttpClient producerClient = new HttpClient();
        @Cleanup("stop")
        WebSocketClient producerWebSocketClient = new WebSocketClient(producerClient);
        producerWebSocketClient.start();
        MyWebSocket producerSocket = new MyWebSocket();
        String produceUri = computeWsBasePath() + "/producer/persistent/sample/test/local/websocket-topic";
        CompletableFuture<org.eclipse.jetty.websocket.api.Session>
                producerSession = producerWebSocketClient.connect(producerSocket, URI.create(produceUri));

        ProducerMessage produceRequest = new ProducerMessage();
        produceRequest.setContext("context");
        produceRequest.setPayload(Base64.getEncoder().encodeToString("my payload".getBytes()));

        @Cleanup("stop")
        HttpClient consumerClient = new HttpClient();
        @Cleanup("stop")
        WebSocketClient consumerWebSocketClient = new WebSocketClient(consumerClient);
        consumerWebSocketClient.start();
        MyWebSocket consumerSocket = new MyWebSocket();
        String consumeUri = computeWsBasePath() + "/consumer/persistent/sample/test/local/websocket-topic/my-sub";
        CompletableFuture<org.eclipse.jetty.websocket.api.Session>
                consumerSession = consumerWebSocketClient.connect(consumerSocket, URI.create(consumeUri));
        consumerSession.get().sendPing(ByteBuffer.wrap("ping".getBytes()), Callback.NOOP);
        producerSession.get()
                .sendText(ObjectMapperFactory.getMapper().writer().writeValueAsString(produceRequest), Callback.NOOP);
        assertTrue(consumerSocket.getResponse().contains("ping"));
        ProducerMessage message =
                ObjectMapperFactory.getMapper().reader().readValue(consumerSocket.getResponse(), ProducerMessage.class);
        assertEquals(new String(Base64.getDecoder().decode(message.getPayload())), "my payload");
    }

    @WebSocket
    public static class MyWebSocket  {
        ArrayBlockingQueue<String> incomingMessages = new ArrayBlockingQueue<>(10);

        @OnWebSocketMessage
        public void onWebSocketText(String message) {
            incomingMessages.add(message);
        }

        @OnWebSocketFrame
        public void onWebSocketFrame(Session session, Frame frame, Callback callback) {
            if (frame.getType() == Frame.Type.PONG) {
                ByteBuffer payload = frame.getPayload();
                payload.rewind();
                incomingMessages.add(BufferUtil.toDetailString(payload));
            }
            callback.succeed();
        }

        public String getResponse() throws InterruptedException {
            return incomingMessages.take();
        }
    }

}
