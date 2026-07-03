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
package org.apache.pulsar.client.impl.v5.auth;

import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.time.Clock;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver;
import org.apache.pulsar.client.api.v5.auth.AuthChallenge;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactory;
import org.apache.pulsar.common.api.AuthData;

/**
 * Exposes a new-SPI v5 {@link Authentication} plugin through the legacy v4
 * {@link org.apache.pulsar.client.api.Authentication} interface that {@code ClientCnx} drives
 * (PIP-478).
 *
 * <p>It also implements {@link AsyncAuthenticationDriver} so that a future {@code ClientCnx} carve-out
 * (PIP-478 stage 3) can authenticate via the asynchronous (non event-loop-blocking) path. The
 * synchronous v4 {@link #getAuthData(String)} path is preserved for callers that do not use the async
 * driver, by blocking on the async methods.
 *
 * <p>v5 {@link org.apache.pulsar.client.api.v5.PulsarClientException} subtypes are translated back to
 * their v4 counterparts.
 */
// Implements the deprecated v4 Authentication SPI by design (configure(Map)).
@SuppressWarnings("deprecation")
public class V5ToV4AuthenticationAdapter
        implements org.apache.pulsar.client.api.Authentication, AsyncAuthenticationDriver {

    private static final long serialVersionUID = 1L;

    private final transient Authentication v5;
    private final transient ScheduledExecutorService scheduler;
    private final transient Executor blockingExecutor;
    private final transient OpenTelemetry openTelemetry;
    private final transient String clientInstanceId;
    private final transient Map<String, String> params;
    private final transient Clock clock;
    private final transient PulsarHttpClientFactory httpClientFactory;

    /**
     * Create an adapter that exposes a v5 authentication plugin through the v4 interface.
     *
     * @param v5 the v5 authentication plugin to expose
     * @param scheduler the scheduler passed to the plugin's init context (may be {@code null} in stage 1)
     * @param blockingExecutor the blocking executor passed to the plugin's init context (may be
     *        {@code null} in stage 1)
     * @param httpClientFactory the HTTP client factory (may be {@code null} in stage 1)
     * @param clientInstanceId a stable identifier for the owning client instance
     * @param params the authentication parameters
     */
    public V5ToV4AuthenticationAdapter(Authentication v5, ScheduledExecutorService scheduler,
            Executor blockingExecutor, PulsarHttpClientFactory httpClientFactory, String clientInstanceId,
            Map<String, String> params) {
        this.v5 = v5;
        this.scheduler = scheduler;
        this.blockingExecutor = blockingExecutor;
        this.httpClientFactory = httpClientFactory;
        this.clientInstanceId = clientInstanceId;
        this.params = params == null ? Map.of() : Map.copyOf(params);
        this.clock = Clock.systemUTC();
        this.openTelemetry = OpenTelemetry.noop();
    }

    @Override
    public String getAuthMethodName() {
        return v5.capability(BinaryAuthDataProvider.class)
                .map(BinaryAuthDataProvider::authMethodName)
                .orElse("none");
    }

    @Override
    public void configure(Map<String, String> authParams) {
        v5.configure(authParams);
    }

    @Override
    public void start() throws PulsarClientException {
        AuthenticationInitContext initContext = new SimpleAuthInitContext(httpClientFactory, scheduler,
                blockingExecutor, clock, openTelemetry, clientInstanceId, params);
        try {
            v5.initializeAsync(initContext).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        } catch (Exception e) {
            throw toV4Exception(unwrap(e));
        }
    }

    @Override
    public AuthenticationDataProvider getAuthData(String brokerHostName) throws PulsarClientException {
        try {
            AuthData initial = getAuthDataAsync(brokerHostName).get();
            byte[] commandBytes = initial == null ? null : initial.getBytes();
            return new SynthesizedV4DataProvider(commandBytes);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        } catch (Exception e) {
            throw toV4Exception(unwrap(e));
        }
    }

    @Override
    public CompletableFuture<AuthData> getAuthDataAsync(String brokerHostName) {
        Optional<BinaryAuthDataProvider> provider =
                v5.capability(BinaryAuthDataProvider.class);
        if (provider.isEmpty()) {
            return CompletableFuture.completedFuture(AuthData.of(new byte[0]));
        }
        AuthenticationCallContext callContext = new SimpleAuthCallContext(brokerHostName, 0);
        return provider.get().getAuthDataAsync(callContext)
                .thenApply(d -> AuthData.of(d == null ? new byte[0] : d.authData()))
                .exceptionally(t -> {
                    throw new CompletionException(toV4Exception(unwrap(t)));
                });
    }

    @Override
    public CompletableFuture<AuthData> authenticateAsync(AuthData challenge, String brokerHostName) {
        Optional<BinaryAuthChallengeHandler> handler = v5.capability(BinaryAuthChallengeHandler.class);
        if (handler.isEmpty() || isSentinel(challenge)) {
            return getAuthDataAsync(brokerHostName);
        }
        AuthenticationCallContext callContext = new SimpleAuthCallContext(brokerHostName, 0);
        return handler.get()
                .respondToChallengeAsync(callContext, new AuthChallenge(challenge.getBytes()))
                .thenApply(r -> AuthData.of(r == null ? new byte[0] : r.responseBytes()))
                .exceptionally(t -> {
                    throw new CompletionException(toV4Exception(unwrap(t)));
                });
    }

    @Override
    public void close() throws IOException {
        try {
            v5.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * v5 {@link Authentication} plugins are not serializable. Serialization of this adapter is therefore
     * unsupported. Configure authentication on a remote/forked context via the
     * {@code authPluginClassName} + {@code authParams} string form instead.
     *
     * @param out the object output stream
     * @throws NotSerializableException always
     */
    private void writeObject(ObjectOutputStream out) throws NotSerializableException {
        throw new NotSerializableException("V5 authentication plugins are not serializable. "
                + "Configure authentication using authPluginClassName + authParams instead of a "
                + "pre-built Authentication instance.");
    }

    private static boolean isSentinel(AuthData challenge) {
        if (challenge == null) {
            return true;
        }
        byte[] bytes = challenge.getBytes();
        return Arrays.equals(bytes, AuthData.INIT_AUTH_DATA_BYTES)
                || Arrays.equals(bytes, AuthData.REFRESH_AUTH_DATA_BYTES);
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            return t.getCause() != null ? t.getCause() : t;
        }
        return t;
    }

    /**
     * Translate a v5-side throwable into the matching v4 {@link PulsarClientException} subtype.
     *
     * @param t the throwable raised by a v5 call
     * @return the translated v4 exception
     */
    private static PulsarClientException toV4Exception(Throwable t) {
        if (t instanceof org.apache.pulsar.client.api.v5.PulsarClientException.AuthenticationException) {
            // The v4 AuthenticationException only exposes a (String) constructor, so preserve the
            // original v5 throwable as the cause explicitly.
            PulsarClientException.AuthenticationException e =
                    new PulsarClientException.AuthenticationException(t.getMessage());
            e.initCause(t);
            return e;
        }
        if (t instanceof org.apache.pulsar.client.api.v5.PulsarClientException.GettingAuthenticationDataException) {
            return new PulsarClientException.GettingAuthenticationDataException(t);
        }
        if (t instanceof PulsarClientException pce) {
            return pce;
        }
        return new PulsarClientException(t);
    }

    /**
     * A v4 {@link AuthenticationDataProvider} synthesized from the v5 async result, so that the
     * synchronous v4 code path keeps working.
     *
     * <p>This bridge only carries the binary-protocol command credential. HTTP auth headers are served
     * through the v5 {@link org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider} capability via
     * the dedicated HTTP auth path, not through this binary-protocol bridge.
     */
    private static final class SynthesizedV4DataProvider implements AuthenticationDataProvider {

        private static final long serialVersionUID = 1L;

        private final transient byte[] commandData;

        SynthesizedV4DataProvider(byte[] commandData) {
            this.commandData = commandData;
        }

        @Override
        public boolean hasDataFromCommand() {
            return commandData != null;
        }

        @Override
        public String getCommandData() {
            return commandData == null ? null
                    : new String(commandData, java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public boolean hasDataForHttp() {
            return false;
        }

        @Override
        public Set<Map.Entry<String, String>> getHttpHeaders() {
            return null;
        }
    }
}
