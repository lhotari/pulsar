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

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.DefaultBinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.SinglePassAuthentication;

/**
 * v5-native token authentication (PIP-478). A single-pass credential served over both transports:
 * the binary protocol sends the raw token as {@code auth_data}, and HTTP sends
 * {@code Authorization: Bearer <token>}. The built-in v4 {@code AuthenticationToken} is a thin shim
 * over this implementation.
 *
 * <p>Although the v5 {@code Authentication} SPI deliberately does not extend {@link Serializable},
 * this concrete built-in body is serializable so the v4 {@code AuthenticationToken} shim (whose
 * interface requires {@code Serializable}, for Functions/connector frameworks) round-trips. The
 * token supplier is itself serializable.
 */
public class TokenAuthenticationV5 implements SinglePassAuthentication, Serializable {

    private static final long serialVersionUID = 1L;

    /** The stable auth-method name. */
    public static final String AUTH_METHOD_NAME = "token";
    /** The HTTP header used to carry the bearer token. */
    public static final String HTTP_HEADER_NAME = "Authorization";
    /** The Pulsar HTTP header that names the auth method. */
    public static final String PULSAR_AUTH_METHOD_NAME = "X-Pulsar-Auth-Method-Name";

    private Supplier<String> tokenSupplier;

    /** No-arg constructor for reflective (string-based) construction. */
    public TokenAuthenticationV5() {
    }

    /**
     * @param tokenSupplier supplies the current token on each call (enables refresh without rebuild)
     */
    public TokenAuthenticationV5(Supplier<String> tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
    }

    @Override
    public String authMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public void configure(Map<String, String> authParams) {
        // The map-based path is not used by the v4 token plugin; configuration arrives via the
        // encoded-string path handled in the v4 shim. Kept as a no-op for the string-based v5 path
        // (the shim forwards the raw token via the constructor / configureEncoded).
    }

    /**
     * Configure from the v4 encoded auth-param string (token:, file:, JSON, or raw token).
     *
     * @param encodedAuthParamString the encoded parameter string
     */
    public void configureEncoded(String encodedAuthParamString) {
        if (encodedAuthParamString.startsWith("token:")) {
            this.tokenSupplier = new SerializableTokenSupplier(
                    encodedAuthParamString.substring("token:".length()));
        } else if (encodedAuthParamString.startsWith("file:")) {
            URI filePath = URI.create(encodedAuthParamString);
            this.tokenSupplier = new SerializableURITokenSupplier(filePath);
        } else {
            try {
                JsonObject authParams = new Gson().fromJson(encodedAuthParamString, JsonObject.class);
                this.tokenSupplier = new SerializableTokenSupplier(authParams.get("token").getAsString());
            } catch (JsonSyntaxException e) {
                this.tokenSupplier = new SerializableTokenSupplier(encodedAuthParamString);
            }
        }
    }

    /**
     * @return the current token supplier (used by the v4 shim's synchronous data provider)
     */
    public Supplier<String> tokenSupplier() {
        return tokenSupplier;
    }

    @Override
    public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BinaryProtocolAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
        return CompletableFuture.completedFuture(
                new DefaultBinaryProtocolAuthData(AUTH_METHOD_NAME, token().getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(PULSAR_AUTH_METHOD_NAME, AUTH_METHOD_NAME);
        headers.put(HTTP_HEADER_NAME, "Bearer " + token());
        return CompletableFuture.completedFuture(HttpAuthHeaders.of(headers));
    }

    @Override
    public void close() {
    }

    private String token() {
        try {
            return tokenSupplier.get();
        } catch (Throwable t) {
            throw new RuntimeException("failed to get client token", t);
        }
    }

    private static final class SerializableURITokenSupplier implements Supplier<String>, Serializable {
        private static final long serialVersionUID = 3160666668166028760L;
        private final URI uri;

        SerializableURITokenSupplier(URI uri) {
            this.uri = uri;
        }

        @Override
        public String get() {
            try {
                return new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8).trim();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read token from file", e);
            }
        }
    }

    private static final class SerializableTokenSupplier implements Supplier<String>, Serializable {
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
