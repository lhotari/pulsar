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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.AuthChallenge;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.ChallengeResponse;
import org.apache.pulsar.client.api.v5.auth.ChallengeResponseHandler;
import org.apache.pulsar.client.api.v5.auth.DefaultBinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider;
import org.apache.pulsar.common.api.AuthData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridges an arbitrary v4 {@link org.apache.pulsar.client.api.Authentication} plugin so it can be
 * used through the new asynchronous v5 {@link Authentication} SPI.
 *
 * <p>The right v5 capability set is selected by inspecting the v4 plugin via {@link #wrap}, which
 * returns one of the package-private subclasses:
 * <ul>
 *     <li>{@link LegacyV4TlsAdapter} — for plugins that carry TLS material
 *         ({@code hasDataForTls()}), e.g. {@code AuthenticationTls}/{@code AuthenticationKeyStoreTls}.</li>
 *     <li>{@link LegacyV4ChallengeResponseAdapter} — for plugins that drive a multi-stage
 *         challenge/response flow (e.g. SASL).</li>
 *     <li>{@link LegacyV4CredentialAdapter} — for simple credential plugins that provide command
 *         and/or HTTP auth data (e.g. {@code AuthenticationToken}).</li>
 * </ul>
 *
 * <p>All blocking v4 calls are offloaded to the scheduler provided by the
 * {@link AuthenticationInitContext} captured during {@link #initializeAsync}. Any v4
 * {@link org.apache.pulsar.client.api.PulsarClientException} is translated into the matching v5
 * {@link PulsarClientException} subtype, completing the returned future exceptionally.
 */
// Bridges to the deprecated v4 Authentication SPI by design (getAuthData()/configure(Map)).
@SuppressWarnings("deprecation")
public abstract class LegacyV4AuthenticationAdapter implements Authentication {

    private static final Logger LOG = LoggerFactory.getLogger(LegacyV4AuthenticationAdapter.class);

    /**
     * The wrapped v4 authentication plugin.
     */
    protected final org.apache.pulsar.client.api.Authentication v4;

    private volatile AuthenticationInitContext initContext;

    LegacyV4AuthenticationAdapter(org.apache.pulsar.client.api.Authentication v4) {
        this.v4 = v4;
    }

    /**
     * Wrap a v4 authentication plugin in the appropriate v5 adapter subtype.
     *
     * <p>The selection heuristic is: TLS if the plugin reports {@code hasDataForTls()}; otherwise a
     * challenge/response adapter if the method name is {@code "sasl"} (case-insensitive); otherwise a
     * plain credential adapter.
     *
     * @param v4 the v4 authentication plugin to wrap
     * @return a v5 {@link Authentication} that delegates to the v4 plugin
     */
    public static Authentication wrap(org.apache.pulsar.client.api.Authentication v4) {
        if (v4 == null) {
            throw new IllegalArgumentException("v4 authentication must not be null");
        }
        boolean tls = false;
        try {
            AuthenticationDataProvider probe = v4.getAuthData(null);
            tls = probe != null && probe.hasDataForTls();
        } catch (Throwable t) {
            // The probe is best-effort; some plugins require start() before getAuthData() works.
            // Fall back to the method-name heuristic below.
            LOG.debug("Could not probe v4 auth plugin {} for TLS capability; using method-name heuristic",
                    v4.getClass().getName(), t);
        }
        if (tls) {
            return new LegacyV4TlsAdapter(v4);
        }
        if ("sasl".equalsIgnoreCase(v4.getAuthMethodName())) {
            return new LegacyV4ChallengeResponseAdapter(v4);
        }
        return new LegacyV4CredentialAdapter(v4);
    }

    @Override
    public void configure(Map<String, String> authParams) {
        v4.configure(authParams);
    }

    @Override
    public CompletableFuture<Void> initializeAsync(AuthenticationInitContext initContext) {
        this.initContext = initContext;
        return supplyOffloaded(() -> {
            v4.start();
            return null;
        });
    }

    @Override
    public void close() throws Exception {
        v4.close();
    }

    /**
     * Return the {@link AuthenticationInitContext} captured during {@link #initializeAsync}.
     *
     * @return the init context, or {@code null} if not yet initialized
     */
    protected AuthenticationInitContext initContext() {
        return initContext;
    }

    /**
     * Offload a (potentially blocking) v4 call to the scheduler captured at initialization,
     * translating v4 authentication exceptions into the matching v5 subtype.
     *
     * @param supplier the blocking work
     * @param <T> the result type
     * @return a future completing with the supplier's result, or exceptionally with a v5 exception
     */
    protected <T> CompletableFuture<T> supplyOffloaded(ThrowingSupplier<T> supplier) {
        Executor executor = initContext != null && initContext.scheduler() != null
                ? initContext.scheduler() : Runnable::run;
        CompletableFuture<T> future = new CompletableFuture<>();
        executor.execute(() -> {
            try {
                future.complete(supplier.get());
            } catch (Throwable t) {
                future.completeExceptionally(toV5Exception(t));
            }
        });
        return future;
    }

    /**
     * Translate a v4-side throwable into the matching v5 {@link PulsarClientException} subtype.
     *
     * @param t the throwable raised by a v4 call
     * @return the translated v5 exception
     */
    protected static Throwable toV5Exception(Throwable t) {
        if (t instanceof org.apache.pulsar.client.api.PulsarClientException.AuthenticationException) {
            return new PulsarClientException.AuthenticationException(t);
        }
        if (t instanceof org.apache.pulsar.client.api.PulsarClientException.GettingAuthenticationDataException) {
            return new PulsarClientException.GettingAuthenticationDataException(t);
        }
        if (t instanceof javax.naming.AuthenticationException) {
            return new PulsarClientException.AuthenticationException(t);
        }
        return new PulsarClientException.GettingAuthenticationDataException(t);
    }

    /**
     * A supplier that may throw a checked exception.
     *
     * @param <T> the result type
     */
    @FunctionalInterface
    protected interface ThrowingSupplier<T> {
        /**
         * Compute the result.
         *
         * @return the result
         * @throws Exception if the computation fails
         */
        T get() throws Exception;
    }

    /**
     * Adapter for simple credential plugins that supply command and/or HTTP auth data
     * (for example {@code AuthenticationToken}).
     */
    static final class LegacyV4CredentialAdapter extends LegacyV4AuthenticationAdapter
            implements BinaryProtocolAuthDataProvider, HttpAuthHeadersProvider {

        LegacyV4CredentialAdapter(org.apache.pulsar.client.api.Authentication v4) {
            super(v4);
        }

        @Override
        public String authMethodName() {
            return v4.getAuthMethodName();
        }

        @Override
        public CompletableFuture<BinaryProtocolAuthData> getAuthDataAsync(AuthenticationCallContext callContext) {
            return supplyOffloaded(() -> {
                AuthenticationDataProvider d = v4.getAuthData(callContext.brokerHost());
                byte[] bytes = d.hasDataFromCommand() && d.getCommandData() != null
                        ? d.getCommandData().getBytes(UTF_8) : new byte[0];
                return new DefaultBinaryProtocolAuthData(v4.getAuthMethodName(), bytes);
            });
        }

        @Override
        public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext callContext) {
            return supplyOffloaded(() -> {
                AuthenticationDataProvider d = v4.getAuthData();
                if (d.hasDataForHttp() && d.getHttpHeaders() != null) {
                    Map<String, String> headers = new HashMap<>();
                    for (Map.Entry<String, String> e : d.getHttpHeaders()) {
                        headers.put(e.getKey(), e.getValue());
                    }
                    return HttpAuthHeaders.of(headers);
                }
                return HttpAuthHeaders.empty();
            });
        }
    }

    /**
     * Adapter for plugins that drive a multi-stage challenge/response flow such as SASL, where the v4
     * {@link AuthenticationDataProvider#authenticate(AuthData)} method is used to exchange tokens.
     *
     * <p>The per-exchange v4 {@link AuthenticationDataProvider} is stored in the call-context state
     * slot so the same provider instance handles the whole challenge/response exchange.
     */
    static final class LegacyV4ChallengeResponseAdapter extends LegacyV4AuthenticationAdapter
            implements BinaryProtocolAuthDataProvider, ChallengeResponseHandler {

        LegacyV4ChallengeResponseAdapter(org.apache.pulsar.client.api.Authentication v4) {
            super(v4);
        }

        @Override
        public String authMethodName() {
            return v4.getAuthMethodName();
        }

        @Override
        public CompletableFuture<BinaryProtocolAuthData> getAuthDataAsync(AuthenticationCallContext callContext) {
            return supplyOffloaded(() -> {
                AuthenticationDataProvider d = v4.getAuthData(callContext.brokerHost());
                callContext.setStateObject(AuthenticationDataProvider.class, d);
                AuthData result = d.authenticate(AuthData.INIT_AUTH_DATA);
                byte[] bytes = result == null ? new byte[0] : result.getBytes();
                return new DefaultBinaryProtocolAuthData(v4.getAuthMethodName(), bytes);
            });
        }

        @Override
        public CompletableFuture<ChallengeResponse> respondToChallengeAsync(AuthenticationCallContext callContext,
                AuthChallenge challenge) {
            return supplyOffloaded(() -> {
                AuthenticationDataProvider d = callContext.getStateObject(AuthenticationDataProvider.class)
                        .orElse(null);
                if (d == null) {
                    d = v4.getAuthData(callContext.brokerHost());
                    callContext.setStateObject(AuthenticationDataProvider.class, d);
                }
                AuthData response = d.authenticate(AuthData.of(challenge.challenge()));
                return new ChallengeResponse(response == null ? null : response.getBytes());
            });
        }
    }

    /**
     * Adapter for v4 plugins that supply TLS material (for example {@code AuthenticationTls} or
     * {@code AuthenticationKeyStoreTls}). The binary-protocol handshake carries no payload — the TLS
     * material is applied at the transport layer — so this reuses {@link TlsAuthentication}.
     */
    static final class LegacyV4TlsAdapter extends TlsAuthentication {

        private static final Logger LOG = LoggerFactory.getLogger(LegacyV4TlsAdapter.class);

        private final org.apache.pulsar.client.api.Authentication v4;

        LegacyV4TlsAdapter(org.apache.pulsar.client.api.Authentication v4) {
            super(TlsAuthentication.DEFAULT_AUTH_METHOD_NAME);
            this.v4 = v4;
        }

        @Override
        public void configure(Map<String, String> authParams) {
            v4.configure(authParams);
        }

        /**
         * Extract the v4 {@link AuthenticationDataProvider} that carries the TLS certificate and
         * private-key material, so the client builder can register it with a
         * {@code FileBasedTlsMaterialProvider} when wiring the transport.
         *
         * <p>TODO PIP-478 follow-up: the returned provider (or the underlying cert/key file paths) must
         * be registered with the TLS material provider so the transport uses the client certificate.
         * This hook intentionally only exposes the material; the actual registration is wired in a
         * later stage and is not required for this module to compile.
         *
         * @return the v4 TLS data provider, or {@code null} if it cannot be obtained
         */
        @SuppressWarnings("deprecation")
        org.apache.pulsar.client.api.AuthenticationDataProvider extractTlsMaterial() {
            try {
                return v4.getAuthData();
            } catch (org.apache.pulsar.client.api.PulsarClientException e) {
                LOG.debug("Could not extract TLS material from v4 plugin {}", v4.getClass().getName(), e);
                return null;
            }
        }
    }
}
