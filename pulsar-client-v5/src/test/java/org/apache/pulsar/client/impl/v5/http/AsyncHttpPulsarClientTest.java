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
package org.apache.pulsar.client.impl.v5.http;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.http.ClientHttpTlsPurpose;
import org.apache.pulsar.client.api.v5.http.HttpRequest;
import org.apache.pulsar.client.api.v5.http.HttpResponse;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClient;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientConfig;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactory;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactoryConfig;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientProvider;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for the default AsyncHttpClient-backed {@link PulsarHttpClientProvider} (PIP-478, Stage 5).
 */
public class AsyncHttpPulsarClientTest {

    private HttpServer server;
    private String baseUrl;

    @BeforeMethod
    public void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/echo-get", exchange -> {
            byte[] body = "hello-get".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("X-Test-Header", "abc");
            exchange.sendResponseHeaders(200, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/echo-post", exchange -> {
            byte[] received = readAll(exchange);
            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            byte[] body = ("posted:" + new String(received, StandardCharsets.UTF_8) + ":ct=" + contentType)
                    .getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(201, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/big", exchange -> {
            byte[] body = new byte[64 * 1024];
            exchange.sendResponseHeaders(200, body.length);
            try (var os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterMethod(alwaysRun = true)
    public void stopServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private static byte[] readAll(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody()) {
            return is.readAllBytes();
        }
    }

    @Test
    public void testProviderIsServiceLoaderDiscoverable() {
        AsyncHttpClientProvider provider = null;
        for (PulsarHttpClientProvider p : ServiceLoader.load(PulsarHttpClientProvider.class)) {
            if (p instanceof AsyncHttpClientProvider ahcProvider) {
                provider = ahcProvider;
            }
        }
        assertNotNull(provider, "AsyncHttpClientProvider should be discovered via ServiceLoader");
        assertEquals(provider.name(), "asynchttpclient");
        assertEquals(provider.priority(), 100);
    }

    @Test
    public void testGet() throws Exception {
        try (AsyncHttpClientFactory factory = newFactory()) {
            PulsarHttpClient client = factory.newHttpClient(genericConfig().build());
            HttpRequest request = HttpRequest.builder(
                    HttpRequest.Method.GET, URI.create(baseUrl + "/echo-get")).build();
            HttpResponse response = client.execute(request).get(30, TimeUnit.SECONDS);
            assertEquals(response.statusCode(), 200);
            assertEquals(response.bodyAsString(), "hello-get");
            // Header lookup is case-insensitive.
            assertEquals(response.header("x-test-header").orElse(null), "abc");
            client.close();
        }
    }

    @Test
    public void testPostBytes() throws Exception {
        try (AsyncHttpClientFactory factory = newFactory()) {
            PulsarHttpClient client = factory.newHttpClient(genericConfig().build());
            HttpRequest request = HttpRequest.builder(
                            HttpRequest.Method.POST, URI.create(baseUrl + "/echo-post"))
                    .body(new HttpRequest.Bytes("payload".getBytes(StandardCharsets.UTF_8),
                            "application/json"))
                    .build();
            HttpResponse response = client.execute(request).get(30, TimeUnit.SECONDS);
            assertEquals(response.statusCode(), 201);
            assertEquals(response.bodyAsString(), "posted:payload:ct=application/json");
            client.close();
        }
    }

    @Test
    public void testMaxResponseBodyBytesOverflowFails() throws Exception {
        try (AsyncHttpClientFactory factory = newFactory()) {
            PulsarHttpClient client = factory.newHttpClient(genericConfig()
                    .maxResponseBodyBytes(1024)
                    .build());
            HttpRequest request = HttpRequest.builder(
                    HttpRequest.Method.GET, URI.create(baseUrl + "/big")).build();
            ExecutionException ex = org.testng.Assert.expectThrows(ExecutionException.class,
                    () -> client.execute(request).get(30, TimeUnit.SECONDS));
            assertTrue(ex.getCause() instanceof IOException,
                    "Expected IOException cause, got " + ex.getCause());
            assertTrue(ex.getCause().getMessage().contains("exceeds the configured maximum"));
            client.close();
        }
    }

    private static PulsarHttpClientConfig.Builder genericConfig() {
        return PulsarHttpClientConfig.builder(ClientHttpTlsPurpose.GENERIC)
                .connectTimeout(Duration.ofSeconds(10))
                .readTimeout(Duration.ofSeconds(30))
                .requestTimeout(Duration.ofSeconds(30));
    }

    private static AsyncHttpClientFactory newFactory() {
        AsyncHttpClientProvider provider = new AsyncHttpClientProvider();
        PulsarHttpClientFactory factory = provider.newFactory(new PulsarHttpClientFactoryConfig() {
            @Override
            public String clientInstanceId() {
                return "test-client";
            }

            @Override
            public OpenTelemetry openTelemetry() {
                return OpenTelemetry.noop();
            }
        });
        return (AsyncHttpClientFactory) factory;
    }
}
