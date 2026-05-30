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
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.DefaultBinaryProtocolAuthData;

/**
 * v5-native "disabled" authentication (PIP-478). Presents the well-known {@code none} auth method
 * with empty {@code auth_data} for the binary protocol and supplies no credential. The built-in v4
 * {@code AuthenticationDisabled} is a thin shim over this implementation.
 *
 * <p>Although the v5 {@code Authentication} SPI deliberately does not extend {@link Serializable},
 * this concrete built-in body is serializable so the v4 {@code AuthenticationDisabled} shim (whose
 * interface requires {@code Serializable}, for Functions/connector frameworks) round-trips.
 */
public class DisabledAuthenticationV5 implements Authentication, BinaryProtocolAuthDataProvider, Serializable {

    private static final long serialVersionUID = 1L;

    /** The stable auth-method name. */
    public static final String AUTH_METHOD_NAME = "none";

    /** No-arg constructor for reflective (string-based) construction. */
    public DisabledAuthenticationV5() {
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
        return CompletableFuture.completedFuture(DefaultBinaryProtocolAuthData.empty(AUTH_METHOD_NAME));
    }

    @Override
    public void close() {
    }
}
