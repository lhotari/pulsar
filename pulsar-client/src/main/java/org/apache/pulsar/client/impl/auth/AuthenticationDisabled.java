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
package org.apache.pulsar.client.impl.auth;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver;
import org.apache.pulsar.client.impl.auth.v5.DisabledAuthenticationV5;
import org.apache.pulsar.client.impl.auth.v5.V5AuthContexts;
import org.apache.pulsar.common.api.AuthData;

/**
 * Authentication provider for the {@code none} auth method (no credential).
 *
 * <p>PIP-478: this v4 plugin is a thin shim over the v5-native {@link DisabledAuthenticationV5}. The
 * v4 surface (including the {@link #INSTANCE} singleton and {@link #getAuthData()} returning
 * {@link AuthenticationDataNull}) is preserved for source compatibility; {@link AsyncAuthenticationDriver}
 * lets {@code ClientCnx} drive it on the non-blocking async path.
 */
public class AuthenticationDisabled implements Authentication, EncodedAuthenticationParameterSupport,
        AsyncAuthenticationDriver {

    protected final AuthenticationDataProvider nullData = new AuthenticationDataNull();
    public static final AuthenticationDisabled INSTANCE = new AuthenticationDisabled();
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final DisabledAuthenticationV5 delegate = new DisabledAuthenticationV5();

    public AuthenticationDisabled() {
    }

    @Override
    public String getAuthMethodName() {
        return DisabledAuthenticationV5.AUTH_METHOD_NAME;
    }

    @SuppressWarnings("deprecation")
    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        return nullData;
    }

    @Override
    public void configure(String encodedAuthParamString) {
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
    }

    @Override
    public void start() throws PulsarClientException {
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }

    // --- AsyncAuthenticationDriver: route ClientCnx through the v5-native async path ---

    @Override
    public CompletableFuture<AuthData> getAuthDataAsync(String brokerHostName) {
        return delegate.getAuthDataAsync(V5AuthContexts.binaryCallContext(brokerHostName, 0))
                .thenApply(d -> AuthData.of(d.authData()));
    }

    @Override
    public CompletableFuture<AuthData> authenticateAsync(AuthData challenge, String brokerHostName) {
        // The "none" method carries no credential; any challenge is answered with empty auth data.
        return getAuthDataAsync(brokerHostName);
    }
}
