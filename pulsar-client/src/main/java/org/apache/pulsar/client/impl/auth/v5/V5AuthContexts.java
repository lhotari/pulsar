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
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactory;

/**
 * Minimal {@link AuthenticationInitContext} / {@link AuthenticationCallContext} implementations shared
 * by the built-in v4 auth plugins, which drive their v5-native bodies over the binary transport
 * (PIP-478 stage 3a). These are lightweight value holders; the per-call context carries a
 * {@link ConcurrentHashMap}-backed state slot keyed by class.
 *
 * <p>Stage 3a wires no framework services into the init context (the HTTP client factory, scheduler and
 * blocking executor are all {@code null}) — the credential-fetching bodies here perform no
 * initialization I/O and their {@code getAuthDataAsync(...)} is already-completed. Real client-service
 * wiring (shared executors, HTTP client factory) is stage 3b.
 */
public final class V5AuthContexts {

    private V5AuthContexts() {
    }

    /**
     * A minimal init context with no framework services wired (stage 3a). Suitable for bodies whose
     * {@code initializeAsync(...)} is a no-op.
     *
     * @param clientInstanceId a stable id for logging correlation
     * @param params           the configured params (possibly empty)
     * @return a new init context
     */
    public static AuthenticationInitContext initContext(String clientInstanceId, Map<String, String> params) {
        return new InitContext(clientInstanceId, params == null ? Map.of() : Map.copyOf(params));
    }

    /**
     * @param brokerHost the broker host
     * @param brokerPort the broker port (0 when unknown)
     * @return a new binary-protocol call context with a fresh state slot
     */
    public static AuthenticationCallContext binaryCallContext(String brokerHost, int brokerPort) {
        return new BinaryCallContext(brokerHost, brokerPort);
    }

    private static final class InitContext implements AuthenticationInitContext {
        private final String clientInstanceId;
        private final Map<String, String> params;

        InitContext(String clientInstanceId, Map<String, String> params) {
            this.clientInstanceId = clientInstanceId;
            this.params = params;
        }

        @Override
        public PulsarHttpClientFactory httpClientFactory() {
            return null;
        }

        @Override
        public ScheduledExecutorService scheduler() {
            return null;
        }

        @Override
        public Executor blockingExecutor() {
            return null;
        }

        @Override
        public Clock clock() {
            return Clock.systemUTC();
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

    private static final class BinaryCallContext implements AuthenticationCallContext {
        private final String brokerHost;
        private final int brokerPort;
        private final ConcurrentHashMap<Class<?>, Object> stateSlot = new ConcurrentHashMap<>();

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

        @Override
        public <T> Optional<T> getStateObject(Class<T> clazz) {
            return Optional.ofNullable(clazz.cast(stateSlot.get(clazz)));
        }

        @Override
        public <T> void setStateObject(Class<T> clazz, T value) {
            if (value == null) {
                stateSlot.remove(clazz);
            } else {
                stateSlot.put(clazz, value);
            }
        }
    }
}
