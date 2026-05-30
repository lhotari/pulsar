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
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.v5.TokenAuthenticationV5;

/**
 * Token based authentication provider.
 *
 * <p>PIP-478: this v4 plugin is a thin shim over the v5-native {@link TokenAuthenticationV5}. The v4
 * surface (including {@link #getAuthData()} and the {@link AuthenticationDataToken} data provider) is
 * preserved for source compatibility, and the plugin keeps the verbatim v4 synchronous connect/refresh
 * path when driven by {@code ClientCnx}. The async v5 SPI is exercised via the v5-native delegate and
 * the {@code V5ToV4AuthenticationAdapter}, not by this v4 plugin directly.
 */
public class AuthenticationToken implements Authentication, EncodedAuthenticationParameterSupport {
    static final String AUTH_METHOD_NAME = "token";

    private static final long serialVersionUID = 1L;

    private final TokenAuthenticationV5 delegate;

    public AuthenticationToken() {
        this.delegate = new TokenAuthenticationV5();
    }

    public AuthenticationToken(String token) {
        this(new SerializableTokenSupplier(token));
    }

    public AuthenticationToken(Supplier<String> tokenSupplier) {
        this.delegate = new TokenAuthenticationV5(tokenSupplier);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public String getAuthMethodName() {
        return AUTH_METHOD_NAME;
    }

    @SuppressWarnings("deprecation")
    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        return new AuthenticationDataToken(delegate.tokenSupplier());
    }

    @Override
    public void configure(String encodedAuthParamString) {
        delegate.configureEncoded(encodedAuthParamString);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void configure(Map<String, String> authParams) {
        // noop
    }

    @Override
    public void start() throws PulsarClientException {
        // noop — the v5-native delegate has no initialization I/O for token auth
    }

    private static class SerializableTokenSupplier implements Supplier<String>, java.io.Serializable {
        private static final long serialVersionUID = 5095234161799506913L;
        private final String token;

        SerializableTokenSupplier(String token) {
            this.token = token;
        }

        @Override
        public String get() {
            return token;
        }
    }
}
