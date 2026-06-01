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

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.auth.AuthChallenge;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.ChallengeResponse;
import org.apache.pulsar.client.api.v5.auth.ChallengeResponseHandler;
import org.apache.pulsar.client.api.v5.auth.DefaultBinaryProtocolAuthData;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.SaslConstants;

/**
 * v5-native SASL authentication for the Pulsar binary protocol (PIP-478). SASL is a multi-round
 * challenge/response handshake, so this body implements {@link BinaryProtocolAuthDataProvider} for the
 * initial {@code CommandConnect} credential and {@link ChallengeResponseHandler} for each subsequent
 * {@code CommandAuthChallenge}. The per-exchange SASL conversation state (an
 * {@link AuthenticationDataProvider} wrapping a {@code PulsarSaslClient}) is created on the initial
 * call and kept in the call-context state slot so each broker handshake stays isolated.
 *
 * <p>The built-in v4 {@code AuthenticationSasl} is a thin shim over this body for the binary path; the
 * SASL-over-HTTP loop (the {@code authenticationStage} / {@code newRequestHeader} methods) remains on
 * the v4 shim. A fresh per-exchange SASL provider is obtained through the serializable
 * {@link SaslProviderFactory} supplied by the shim (it reads the shim's JAAS subject and server type).
 *
 * <p>Although the v5 {@code Authentication} SPI deliberately does not extend {@link Serializable},
 * this concrete built-in body is serializable so the v4 {@code AuthenticationSasl} shim (whose
 * interface requires {@code Serializable}, for Functions/connector frameworks) round-trips. The
 * provider factory is itself serializable.
 */
public class SaslAuthenticationV5 implements Authentication, BinaryProtocolAuthDataProvider,
        ChallengeResponseHandler, Serializable {

    private static final long serialVersionUID = 1L;

    /** The stable auth-method name. */
    public static final String AUTH_METHOD_NAME = SaslConstants.AUTH_METHOD_NAME;

    private final SaslProviderFactory providerFactory;

    /**
     * @param providerFactory creates a fresh per-exchange SASL data provider for a broker host
     */
    public SaslAuthenticationV5(SaslProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public String authMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BinaryProtocolAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
        try {
            AuthenticationDataProvider provider = providerFactory.create(ctx.brokerHost());
            ctx.setStateObject(AuthenticationDataProvider.class, provider);
            AuthData initData = provider.authenticate(AuthData.INIT_AUTH_DATA);
            return CompletableFuture.completedFuture(
                    new DefaultBinaryProtocolAuthData(AUTH_METHOD_NAME, initData.getBytes()));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    @Override
    public CompletableFuture<ChallengeResponse> respondToChallengeAsync(AuthenticationCallContext ctx,
                                                                        AuthChallenge authChallenge) {
        try {
            AuthenticationDataProvider provider = ctx.getStateObject(AuthenticationDataProvider.class).orElse(null);
            if (provider == null) {
                // No prior state (e.g. a broker-pushed challenge without a preceding connect call on
                // this context); start a fresh SASL exchange.
                provider = providerFactory.create(ctx.brokerHost());
                ctx.setStateObject(AuthenticationDataProvider.class, provider);
            }
            AuthData response = provider.authenticate(AuthData.of(authChallenge.challenge()));
            return CompletableFuture.completedFuture(new ChallengeResponse(response.getBytes()));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    @Override
    public void close() {
    }

    /**
     * Creates a fresh per-exchange SASL {@link AuthenticationDataProvider} (wrapping a new
     * {@code PulsarSaslClient}) for a given broker host. Implementations must be serializable.
     */
    public interface SaslProviderFactory extends Serializable {
        /**
         * @param brokerHost the broker host the SASL client authenticates against
         * @return a fresh SASL data provider for this exchange
         * @throws Exception if the SASL client could not be created
         */
        AuthenticationDataProvider create(String brokerHost) throws Exception;
    }
}
