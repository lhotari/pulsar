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
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.DefaultBinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.HttpAuthCallContext;
import org.apache.pulsar.client.api.v5.auth.HttpAuthHeaders;
import org.apache.pulsar.client.api.v5.auth.SinglePassAuthentication;

/**
 * v5-native HTTP-basic authentication (PIP-478). A single-pass credential served over both
 * transports: the binary protocol sends {@code userId:password} as {@code auth_data}, and HTTP sends
 * {@code Authorization: Basic <base64(userId:password)>}. The built-in v4 {@code AuthenticationBasic}
 * is a thin shim over this implementation.
 *
 * <p>Although the v5 {@code Authentication} SPI deliberately does not extend {@link Serializable},
 * this concrete built-in body is serializable so the v4 {@code AuthenticationBasic} shim (whose
 * interface requires {@code Serializable}, for Functions/connector frameworks) round-trips.
 */
public class BasicAuthenticationV5 implements SinglePassAuthentication, Serializable {

    private static final long serialVersionUID = 1L;

    /** The stable auth-method name. */
    public static final String AUTH_METHOD_NAME = "basic";
    /** The HTTP header used to carry the basic credential. */
    public static final String HTTP_HEADER_NAME = "Authorization";
    /** The Pulsar HTTP header that names the auth method. */
    public static final String PULSAR_AUTH_METHOD_NAME = "X-Pulsar-Auth-Method-Name";

    private String userId;
    private String password;

    /** No-arg constructor for reflective (string-based) construction. */
    public BasicAuthenticationV5() {
    }

    /**
     * @param userId   the user id
     * @param password the password
     */
    public BasicAuthenticationV5(String userId, String password) {
        this.userId = userId;
        this.password = password;
    }

    @Override
    public String authMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public void configure(Map<String, String> authParams) {
        configureEncoded(new Gson().toJson(authParams));
    }

    /**
     * Configure from the v4 encoded auth-param string (a JSON object with {@code userId} and
     * {@code password} fields).
     *
     * @param encodedAuthParamString the encoded parameter string
     */
    public void configureEncoded(String encodedAuthParamString) {
        JsonObject params = new Gson().fromJson(encodedAuthParamString, JsonObject.class);
        this.userId = params.get("userId").getAsString();
        this.password = params.get("password").getAsString();
    }

    /**
     * @return the configured user id (used by the v4 shim's synchronous data provider)
     */
    public String userId() {
        return userId;
    }

    /**
     * @return the configured password (used by the v4 shim's synchronous data provider)
     */
    public String password() {
        return password;
    }

    @Override
    public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BinaryProtocolAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
        return CompletableFuture.completedFuture(
                new DefaultBinaryProtocolAuthData(AUTH_METHOD_NAME, userInfo().getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public CompletableFuture<HttpAuthHeaders> getHttpHeadersAsync(HttpAuthCallContext ctx) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(PULSAR_AUTH_METHOD_NAME, AUTH_METHOD_NAME);
        headers.put(HTTP_HEADER_NAME,
                "Basic " + Base64.getEncoder().encodeToString(userInfo().getBytes(StandardCharsets.UTF_8)));
        return CompletableFuture.completedFuture(HttpAuthHeaders.of(headers));
    }

    @Override
    public void close() {
    }

    private String userInfo() {
        return userId + ":" + password;
    }
}
