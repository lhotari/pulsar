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
package org.apache.pulsar.client.impl.auth.v5;

import io.opentelemetry.api.OpenTelemetry;
import java.net.URI;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactory;

/**
 * Minimal {@link AuthenticationInitContext} / {@link AuthenticationCallContext} /
 * {@link HttpAuthCallContext} implementations shared by the built-in v4 auth plugins, which are thin
 * shims over v5-native bodies (PIP-478). These are lightweight value holders; the per-call contexts
 * carry a {@link ConcurrentHashMap}-backed state slot keyed by class.
 */
public final class V5AuthContexts {

    private V5AuthContexts() {
    }

    /**
     * @param httpClientFactory the HTTP client factory (may be {@code null})
     * @param scheduler         the off-loop scheduler
     * @param clock             the clock (defaults to {@link Clock#systemUTC()} if {@code null})
     * @param clientInstanceId  a stable id for logging correlation
     * @param params            the configured params
     * @return a new init context
     */
    public static AuthenticationInitContext initContext(PulsarHttpClientFactory httpClientFactory,
            ScheduledExecutorService scheduler, Clock clock, String clientInstanceId, Map<String, String> params) {
        return new InitContext(httpClientFactory, scheduler,
                clock != null ? clock : Clock.systemUTC(), clientInstanceId,
                params == null ? Map.of() : Map.copyOf(params));
    }

    /**
     * @param brokerHost the broker host
     * @param brokerPort the broker port
     * @return a new binary-protocol call context
     */
    public static AuthenticationCallContext binaryCallContext(String brokerHost, int brokerPort) {
        return new BinaryCallContext(brokerHost, brokerPort);
    }

    /**
     * @param requestUri             the request URI
     * @param serverChallengeHeaders the server's prior challenge headers, if any
     * @return a new HTTP call context
     */
    public static HttpAuthCallContext httpCallContext(URI requestUri, HttpAuthHeaders serverChallengeHeaders) {
        return new HttpCallContext(requestUri, serverChallengeHeaders);
    }

    private static final class InitContext implements AuthenticationInitContext {
        private final PulsarHttpClientFactory httpClientFactory;
        private final ScheduledExecutorService scheduler;
        private final Clock clock;
        private final String clientInstanceId;
        private final Map<String, String> params;

        InitContext(PulsarHttpClientFactory httpClientFactory, ScheduledExecutorService scheduler, Clock clock,
                String clientInstanceId, Map<String, String> params) {
            this.httpClientFactory = httpClientFactory;
            this.scheduler = scheduler;
            this.clock = clock;
            this.clientInstanceId = clientInstanceId;
            this.params = params;
        }

        @Override
        public PulsarHttpClientFactory httpClientFactory() {
            return httpClientFactory;
        }

        @Override
        public ScheduledExecutorService scheduler() {
            return scheduler;
        }

        @Override
        public Clock clock() {
            return clock;
        }

        @Override
        public OpenTelemetry openTelemetry() {
            return OpenTelemetry.noop();
        }

        @Override
        public String clientInstanceId() {
            return clientInstanceId;
        }

        @Override
        public Map<String, String> params() {
            return params;
        }
    }

    private abstract static class StateSlotContext {
        private final ConcurrentHashMap<Class<?>, Object> stateSlot = new ConcurrentHashMap<>();

        public <T> Optional<T> getStateObject(Class<T> clazz) {
            return Optional.ofNullable(clazz.cast(stateSlot.get(clazz)));
        }

        public <T> void setStateObject(Class<T> clazz, T value) {
            stateSlot.put(clazz, value);
        }
    }

    private static final class BinaryCallContext extends StateSlotContext implements AuthenticationCallContext {
        private final String brokerHost;
        private final int brokerPort;

        BinaryCallContext(String brokerHost, int brokerPort) {
            this.brokerHost = brokerHost;
            this.brokerPort = brokerPort;
        }

        @Override
        public String brokerHost() {
            return brokerHost;
        }

        @Override
        public int brokerPort() {
            return brokerPort;
        }
    }

    private static final class HttpCallContext extends StateSlotContext implements HttpAuthCallContext {
        private final URI requestUri;
        private final HttpAuthHeaders serverChallengeHeaders;

        HttpCallContext(URI requestUri, HttpAuthHeaders serverChallengeHeaders) {
            this.requestUri = requestUri;
            this.serverChallengeHeaders = serverChallengeHeaders;
        }

        @Override
        public URI requestUri() {
            return requestUri;
        }

        @Override
        public Optional<HttpAuthHeaders> serverChallengeHeaders() {
            return Optional.ofNullable(serverChallengeHeaders);
        }
    }
}
