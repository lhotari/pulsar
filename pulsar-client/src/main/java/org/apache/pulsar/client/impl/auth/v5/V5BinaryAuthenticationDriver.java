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

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver;
import org.apache.pulsar.client.api.v5.auth.AuthChallenge;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthChallengeHandler;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.api.v5.internal.ClientAuthenticationServices;
import org.apache.pulsar.common.api.AuthData;

/**
 * Drives a v5-native {@link Authentication} body over the Pulsar binary transport, exposing it as the
 * legacy {@link AsyncAuthenticationDriver} that {@code ClientCnx} consumes (PIP-478 stage 3a). This is
 * the single exchange pattern shared by the built-in v4 auth shims (Token, Basic, OAuth2, Athenz, SASL);
 * each shim wraps its v5 body in this driver rather than re-implementing the exchange.
 *
 * <p>Routing follows the PIP's normative binary rules: initial connect and the REFRESH sentinel re-produce
 * the credential through {@link BinaryAuthDataProvider#getAuthDataAsync}; any other challenge is answered
 * by {@link BinaryAuthChallengeHandler}. One {@link AuthenticationExchange} owns one
 * {@link AuthenticationCallContext} (and its state slot) for the lifetime of a single connection attempt,
 * so multi-round conversation state survives across challenge rounds.
 *
 * <p>v5 {@link org.apache.pulsar.client.api.v5.PulsarClientException} subtypes are translated back to the
 * matching v4 {@link PulsarClientException} subtypes, since v4 exception types are part of the public API.
 */
public final class V5BinaryAuthenticationDriver implements AsyncAuthenticationDriver {

    private final Authentication v5;
    private final String clientInstanceId;
    private final Map<String, String> params;
    private final ClientAuthenticationServices services;
    private volatile boolean initialized;

    /**
     * @param v5 the v5-native authentication body to drive over the binary transport
     */
    public V5BinaryAuthenticationDriver(Authentication v5) {
        this(v5, null, null, Map.of());
    }

    /**
     * @param v5       the v5-native authentication body to drive over the binary transport
     * @param services the client's late-bound framework services (may be {@code null} before binding),
     *                 which flow to the body's {@code initializeAsync(...)} so a credential-fetching body
     *                 can off-load its blocking fetch onto the bounded blocking executor
     */
    public V5BinaryAuthenticationDriver(Authentication v5, ClientAuthenticationServices services) {
        this(v5, services, services == null ? null : services.clientInstanceId(), Map.of());
    }

    /**
     * @param v5               the v5-native authentication body to drive over the binary transport
     * @param services         the client's late-bound framework services (may be {@code null})
     * @param clientInstanceId a stable id for logging correlation (may be {@code null})
     * @param params           the configured auth params (possibly empty)
     */
    public V5BinaryAuthenticationDriver(Authentication v5, ClientAuthenticationServices services,
            String clientInstanceId, Map<String, String> params) {
        this.v5 = v5;
        this.services = services;
        this.clientInstanceId = clientInstanceId;
        this.params = params == null ? Map.of() : params;
    }

    @Override
    public AuthenticationExchange newAuthenticationExchange(String brokerHostName) {
        ensureInitialized();
        return new V5Exchange(V5AuthContexts.binaryCallContext(brokerHostName, 0));
    }

    private void ensureInitialized() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    // The built-in bodies (Token/Basic/OAuth2/Athenz/SASL binary) complete initializeAsync
                    // immediately: they only stash the bound blocking executor for later off-loading, so
                    // join() does not block. Failures are surfaced as v4 exceptions via the exchange futures.
                    v5.initializeAsync(V5AuthContexts.initContext(services, clientInstanceId, params)).join();
                    initialized = true;
                }
            }
        }
    }

    private BinaryAuthDataProvider requireProvider() {
        return v5.capability(BinaryAuthDataProvider.class)
                .orElseThrow(() -> new IllegalStateException(
                        "v5 authentication body " + v5.getClass().getName() + " does not expose "
                                + "BinaryAuthDataProvider; it cannot authenticate a Pulsar binary-protocol "
                                + "connection (PIP-478)"));
    }

    /**
     * A single binary-transport authentication exchange: one call context (and state slot) reused for the
     * initial credential, every {@code CommandAuthChallenge}, and the refresh sentinel of one connect.
     */
    private final class V5Exchange implements AuthenticationExchange {

        private final AuthenticationCallContext callContext;

        V5Exchange(AuthenticationCallContext callContext) {
            this.callContext = callContext;
        }

        @Override
        public CompletableFuture<AuthData> getAuthDataAsync() {
            // Never throw synchronously (PIP-478 error model): a body that throws while producing its
            // future is caught here and reported as a failed future, mapped to the v4 exception type.
            try {
                Optional<BinaryAuthDataProvider> provider = v5.capability(BinaryAuthDataProvider.class);
                if (provider.isEmpty()) {
                    return CompletableFuture.failedFuture(new PulsarClientException.UnsupportedAuthenticationException(
                            "v5 authentication body " + v5.getClass().getName() + " does not expose "
                                    + "BinaryAuthDataProvider; the Pulsar binary transport requires it (PIP-478)"));
                }
                return provider.get().getAuthDataAsync(callContext)
                        .thenApply(d -> AuthData.of(d == null ? new byte[0] : d.authData()))
                        .exceptionally(t -> {
                            throw new CompletionException(toV4Exception(unwrap(t)));
                        });
            } catch (Throwable t) {
                return CompletableFuture.failedFuture(toV4Exception(unwrap(t)));
            }
        }

        @Override
        public CompletableFuture<AuthData> authenticateAsync(AuthData challengeOrRefresh) {
            // Binary routing rule 2: the refresh (and init) sentinel re-produces the current credential
            // through BinaryAuthDataProvider — it never reaches the challenge handler.
            if (isSentinel(challengeOrRefresh)) {
                return getAuthDataAsync();
            }
            try {
                // Rule 3: any other CommandAuthChallenge goes to the challenge handler; absent capability is
                // a hard failure (matching v4, where a plugin without authenticate(AuthData) cannot answer).
                Optional<BinaryAuthChallengeHandler> handler = v5.capability(BinaryAuthChallengeHandler.class);
                if (handler.isEmpty()) {
                    return CompletableFuture.failedFuture(new PulsarClientException.AuthenticationException(
                            "v5 authentication body " + v5.getClass().getName() + " received a binary auth "
                                    + "challenge but does not expose BinaryAuthChallengeHandler (PIP-478)"));
                }
                return handler.get()
                        .respondToChallengeAsync(callContext, new AuthChallenge(challengeOrRefresh.getBytes()))
                        .thenApply(r -> AuthData.of(r == null ? new byte[0] : r.responseBytes()))
                        .exceptionally(t -> {
                            throw new CompletionException(toV4Exception(unwrap(t)));
                        });
            } catch (Throwable t) {
                return CompletableFuture.failedFuture(toV4Exception(unwrap(t)));
            }
        }
    }

    /**
     * @return the auth method name, from the required binary capability
     */
    public String authMethodName() {
        return requireProvider().authMethodName();
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
        if (t instanceof CompletionException && t.getCause() != null) {
            return t.getCause();
        }
        return t;
    }

    /**
     * Translate a v5-side throwable into the matching v4 {@link PulsarClientException} subtype.
     *
     * @param t the throwable raised by a v5 call
     * @return the translated v4 exception
     */
    static PulsarClientException toV4Exception(Throwable t) {
        if (t instanceof org.apache.pulsar.client.api.v5.PulsarClientException.AuthenticationException) {
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
}
