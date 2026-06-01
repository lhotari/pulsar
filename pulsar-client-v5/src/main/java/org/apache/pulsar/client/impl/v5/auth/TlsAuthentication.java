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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.DefaultBinaryProtocolAuthData;

/**
 * Built-in mutual-TLS authentication plugin for the v5 client.
 *
 * <p>This plugin carries no TLS material itself: the client certificate and private key are
 * configured at the client-builder level and supplied to the transport (TLS handshake). The
 * binary-protocol handshake therefore carries an empty auth payload, and this plugin only declares
 * the {@code "tls"} authentication method name so the broker selects the TLS auth provider.
 */
public class TlsAuthentication implements Authentication, BinaryProtocolAuthDataProvider {

    /**
     * The default authentication method name for mutual TLS.
     */
    public static final String DEFAULT_AUTH_METHOD_NAME = "tls";

    private final String authMethodName;

    /**
     * Create a TLS authentication plugin with the default {@code "tls"} method name.
     */
    public TlsAuthentication() {
        this(DEFAULT_AUTH_METHOD_NAME);
    }

    /**
     * Create a TLS authentication plugin with a custom method name.
     *
     * @param authMethodName the authentication method name to advertise to the broker
     */
    public TlsAuthentication(String authMethodName) {
        this.authMethodName = authMethodName;
    }

    @Override
    public CompletableFuture<Void> initializeAsync(AuthenticationInitContext initContext) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String authMethodName() {
        return authMethodName;
    }

    @Override
    public CompletableFuture<BinaryProtocolAuthData> getAuthDataAsync(AuthenticationCallContext callContext) {
        return CompletableFuture.completedFuture(DefaultBinaryProtocolAuthData.empty(authMethodName));
    }
}
