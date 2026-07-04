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

import static java.nio.charset.StandardCharsets.UTF_8;
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
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver;
import org.apache.pulsar.client.api.v5.auth.AuthChallenge;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServicesAware;
import org.apache.pulsar.common.api.AuthData;

/**
 * Exposes a new-SPI v5 {@link Authentication} plugin through the legacy v4
 * {@link org.apache.pulsar.client.api.Authentication} interface that {@code ClientCnx} drives
 * (PIP-478).
 *
 * <p>It also implements {@link AsyncAuthenticationDriver} so that a future {@code ClientCnx} carve-out
 * (PIP-478 stage 3) can authenticate via the asynchronous (non event-loop-blocking) path. Both the
 * async driver and the synchronous v4 {@link #getAuthData(String)} path are exchange-scoped: one
 * {@link AuthenticationExchange} — and one {@link AuthenticationCallContext} with its state slot — backs
 * a single connection attempt, so a plugin's challenge/response conversation state survives across all
 * of that connection's rounds (initial data, refresh, and every {@code CommandAuthChallenge}).
 *
 * <p>Binary transport requires the {@link BinaryAuthDataProvider} capability (PIP-478 binary routing
 * rule 1). A wrapped v5 plugin that does not expose it fails loudly — {@link #start()} throws a
 * v4-mapped {@link PulsarClientException.UnsupportedAuthenticationException} — rather than synthesizing
 * an empty {@code "none"} credential.
 *
 * <p>v5 {@link org.apache.pulsar.client.api.v5.PulsarClientException} subtypes are translated back to
 * their v4 counterparts.
 */
// Implements the deprecated v4 Authentication SPI by design (configure(Map)).
@SuppressWarnings("deprecation")
public class V5ToV4AuthenticationAdapter
        implements org.apache.pulsar.client.api.Authentication, AsyncAuthenticationDriver,
        ClientAuthenticationServicesAware {

    private static final long serialVersionUID = 1L;

    private final transient Authentication v5;
    private final transient Map<String, String> params;
    // Late-bound by the client via bindClientAuthenticationServices(...) before start() (PIP-478 stage 3b);
    // null until then (e.g. when the adapter is exercised outside a client), yielding an init context with
    // no framework services.
    private transient volatile ClientAuthenticationServices services;

    /**
     * Create an adapter that exposes a v5 authentication plugin through the v4 interface. The framework
     * services the plugin's init context reports are late-bound by the client through
     * {@link #bindClientAuthenticationServices} before {@link #start()}.
     *
     * @param v5 the v5 authentication plugin to expose
     * @param params the authentication parameters
     */
    public V5ToV4AuthenticationAdapter(Authentication v5, Map<String, String> params) {
        this.v5 = v5;
        this.params = params == null ? Map.of() : Map.copyOf(params);
    }

    @Override
    public void bindClientAuthenticationServices(ClientAuthenticationServices services) {
        this.services = services;
    }

    @Override
    public String getAuthMethodName() {
        return requireBinaryProvider().authMethodName();
    }

    @Override
    public void configure(Map<String, String> authParams) {
        v5.configure(authParams);
    }

    @Override
    public void start() throws PulsarClientException {
        ClientAuthenticationServices bound = this.services;
        AuthenticationInitContext initContext = bound == null
                ? new SimpleAuthInitContext(null, null, null, Clock.systemUTC(), OpenTelemetry.noop(), null, params)
                : new SimpleAuthInitContext(bound.httpClientFactory(), bound.scheduler(), bound.blockingExecutor(),
                        bound.clock() == null ? Clock.systemUTC() : bound.clock(),
                        bound.openTelemetry() == null ? OpenTelemetry.noop() : bound.openTelemetry(),
                        bound.clientInstanceId(), params);
        try {
            v5.initializeAsync(initContext).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        } catch (Exception e) {
            throw toV4Exception(unwrap(e));
        }
        // Binary transport requires the BinaryAuthDataProvider capability (PIP-478 binary routing
        // rule 1). Fail loudly here rather than synthesizing a "none"/empty credential.
        if (v5.capability(BinaryAuthDataProvider.class).isEmpty()) {
            throw new PulsarClientException.UnsupportedAuthenticationException(
                    "v5 authentication plugin " + v5.getClass().getName() + " does not expose "
                            + "BinaryAuthDataProvider; the Pulsar binary transport requires it (PIP-478)");
        }
    }

    @Override
    public AuthenticationDataProvider getAuthData(String brokerHostName) throws PulsarClientException {
        AuthenticationExchange exchange = newAuthenticationExchange(brokerHostName);
        try {
            AuthData initial = exchange.getAuthDataAsync().get();
            return new SynthesizedV4DataProvider(exchange, initial);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException(e);
        } catch (Exception e) {
            throw toV4Exception(unwrap(e));
        }
    }

    @Override
    public AuthenticationExchange newAuthenticationExchange(String brokerHostName) {
        return new V4Exchange(brokerHostName);
    }

    /**
     * The v5 {@link BinaryAuthDataProvider} the wrapped plugin exposes for the binary transport.
     *
     * @return the capability
     * @throws IllegalStateException if the plugin does not support the binary transport; normally
     *         {@link #start()} has already failed loudly before this can be reached
     */
    private BinaryAuthDataProvider requireBinaryProvider() {
        return v5.capability(BinaryAuthDataProvider.class)
                .orElseThrow(() -> new IllegalStateException(
                        "v5 authentication plugin " + v5.getClass().getName() + " does not expose "
                                + "BinaryAuthDataProvider; it cannot authenticate a Pulsar binary-protocol "
                                + "connection (PIP-478)"));
    }

    /**
     * A single binary-transport authentication exchange: one call context (and its state slot) reused
     * for the initial credential, every {@code CommandAuthChallenge}, and the refresh sentinel of one
     * connection attempt.
     */
    private final class V4Exchange implements AuthenticationExchange {

        private final AuthenticationCallContext callContext;

        V4Exchange(String brokerHostName) {
            this.callContext = new SimpleAuthCallContext(brokerHostName, 0);
        }

        @Override
        public CompletableFuture<AuthData> getAuthDataAsync() {
            Optional<BinaryAuthDataProvider> provider = v5.capability(BinaryAuthDataProvider.class);
            if (provider.isEmpty()) {
                return CompletableFuture.failedFuture(new PulsarClientException.UnsupportedAuthenticationException(
                        "v5 authentication plugin " + v5.getClass().getName() + " does not expose "
                                + "BinaryAuthDataProvider; the Pulsar binary transport requires it (PIP-478)"));
            }
            return provider.get().getAuthDataAsync(callContext)
                    .thenApply(d -> AuthData.of(d == null ? new byte[0] : d.authData()))
                    .exceptionally(t -> {
                        throw new CompletionException(toV4Exception(unwrap(t)));
                    });
        }

        @Override
        public CompletableFuture<AuthData> authenticateAsync(AuthData challengeOrRefresh) {
            // PIP-478 binary routing rule 2: the refresh sentinel (and the init sentinel) re-produce the
            // current credential through BinaryAuthDataProvider — they never reach the challenge handler.
            if (isSentinel(challengeOrRefresh)) {
                return getAuthDataAsync();
            }
            // Rule 3: any other CommandAuthChallenge goes to the challenge handler; absent capability is a
            // hard failure (matching v4, where a plugin without authenticate(AuthData) cannot answer).
            Optional<BinaryAuthChallengeHandler> handler = v5.capability(BinaryAuthChallengeHandler.class);
            if (handler.isEmpty()) {
                return CompletableFuture.failedFuture(new PulsarClientException.AuthenticationException(
                        "v5 authentication plugin " + v5.getClass().getName() + " received a binary auth "
                                + "challenge but does not expose BinaryAuthChallengeHandler (PIP-478)"));
            }
            return handler.get()
                    .respondToChallengeAsync(callContext, new AuthChallenge(challengeOrRefresh.getBytes()))
                    .thenApply(r -> AuthData.of(r == null ? new byte[0] : r.responseBytes()))
                    .exceptionally(t -> {
                        throw new CompletionException(toV4Exception(unwrap(t)));
                    });
        }
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

    private static boolean isInitSentinel(AuthData challenge) {
        return challenge != null && Arrays.equals(challenge.getBytes(), AuthData.INIT_AUTH_DATA_BYTES);
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
        if (t instanceof org.apache.pulsar.client.api.v5.PulsarClientException.UnsupportedAuthenticationException) {
            PulsarClientException.UnsupportedAuthenticationException e =
                    new PulsarClientException.UnsupportedAuthenticationException(t.getMessage());
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
     * A v4 {@link AuthenticationDataProvider} synthesized from a single {@link AuthenticationExchange},
     * so that the synchronous v4 {@code ClientCnx} code path keeps working — including multi-round
     * challenge/response, which rides {@link #authenticate(AuthData)} through the same exchange (and thus
     * the same {@link AuthenticationCallContext} state slot).
     *
     * <p>This bridge only carries the binary-protocol command credential. HTTP auth headers are served
     * through the v5 {@link org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider} capability via
     * the dedicated HTTP auth path, not through this binary-protocol bridge.
     */
    private static final class SynthesizedV4DataProvider implements AuthenticationDataProvider {

        private static final long serialVersionUID = 1L;

        private final transient AuthenticationExchange exchange;
        private final transient byte[] initialData;

        SynthesizedV4DataProvider(AuthenticationExchange exchange, AuthData initial) {
            this.exchange = exchange;
            this.initialData = initial == null || initial.getBytes() == null ? null : initial.getBytes();
        }

        @Override
        public boolean hasDataFromCommand() {
            return initialData != null;
        }

        @Override
        public String getCommandData() {
            return initialData == null ? null : new String(initialData, UTF_8);
        }

        @Override
        public boolean hasDataForHttp() {
            return false;
        }

        @Override
        public Set<Map.Entry<String, String>> getHttpHeaders() {
            return null;
        }

        /**
         * Route a challenge (or the connect/refresh sentinel) through the bound exchange so that v5
         * challenge handlers work through the plain synchronous v4 path, sharing the exchange's state
         * slot across rounds. The init sentinel reuses the already-computed initial credential to avoid
         * re-running the initial round (which, for challenge/response plugins, would re-seed the
         * conversation).
         *
         * @param data the challenge payload, or the {@code INIT}/{@code REFRESH} sentinel
         * @return the response auth data
         * @throws javax.naming.AuthenticationException if the exchange fails
         */
        @Override
        public AuthData authenticate(AuthData data) throws javax.naming.AuthenticationException {
            if (isInitSentinel(data)) {
                return AuthData.of(initialData == null ? new byte[0] : initialData);
            }
            try {
                AuthData result = exchange.authenticateAsync(data).get();
                return AuthData.of(result == null ? new byte[0] : result.getBytes());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw namingAuthException(e);
            } catch (ExecutionException e) {
                throw namingAuthException(unwrap(e));
            }
        }

        private static javax.naming.AuthenticationException namingAuthException(Throwable cause) {
            javax.naming.AuthenticationException e = new javax.naming.AuthenticationException(
                    cause == null ? null : cause.getMessage());
            e.initCause(cause);
            return e;
        }
    }
}
