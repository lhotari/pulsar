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

import com.google.common.base.Strings;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.DefaultBinaryProtocolAuthData;
import org.apache.pulsar.client.impl.AuthenticationUtil;

/**
 * v5-native keystore-based mTLS authentication (PIP-478). Like {@link TlsAuthenticationV5} this
 * authenticates via the TLS handshake (the keystore supplies the TLS material), so the
 * binary-protocol credential carries the {@code tls} auth method with empty {@code auth_data}. This
 * body holds the configured keystore parameters so the v4 shim can keep supplying its
 * {@code AuthenticationDataKeyStoreTls} for the TLS material override path. The built-in v4
 * {@code AuthenticationKeyStoreTls} is a thin shim over this implementation.
 *
 * <p>Although the v5 {@code Authentication} SPI deliberately does not extend {@link Serializable},
 * this concrete built-in body is serializable so the v4 {@code AuthenticationKeyStoreTls} shim (whose
 * interface requires {@code Serializable}, for Functions/connector frameworks) round-trips. The
 * keystore parameters are kept as plain serializable strings.
 */
public class KeyStoreTlsAuthenticationV5 implements Authentication, BinaryProtocolAuthDataProvider, Serializable {

    private static final long serialVersionUID = 1L;

    /** The stable auth-method name. */
    public static final String AUTH_METHOD_NAME = "tls";

    /** Parameter name for the keystore type. */
    public static final String KEYSTORE_TYPE = "keyStoreType";
    /** Parameter name for the keystore path. */
    public static final String KEYSTORE_PATH = "keyStorePath";
    /** Parameter name for the keystore password. */
    public static final String KEYSTORE_PW = "keyStorePassword";

    private static final String DEFAULT_KEYSTORE_TYPE = "JKS";

    private String keyStoreType;
    private String keyStorePath;
    private String keyStorePassword;

    /** No-arg constructor for reflective (string-based) construction. */
    public KeyStoreTlsAuthenticationV5() {
    }

    /**
     * @param keyStoreType     the keystore type (for example {@code JKS})
     * @param keyStorePath     the keystore file path
     * @param keyStorePassword the keystore password
     */
    public KeyStoreTlsAuthenticationV5(String keyStoreType, String keyStorePath, String keyStorePassword) {
        this.keyStoreType = keyStoreType;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
    }

    @Override
    public String authMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public void configure(Map<String, String> params) {
        String type = params.get(KEYSTORE_TYPE);
        String path = params.get(KEYSTORE_PATH);
        String password = params.get(KEYSTORE_PW);

        if (Strings.isNullOrEmpty(path) || Strings.isNullOrEmpty(password)) {
            throw new IllegalArgumentException("Passed in parameter empty. "
                    + KEYSTORE_PATH + ": " + path + " " + KEYSTORE_PW + ": " + password);
        }

        if (Strings.isNullOrEmpty(type)) {
            type = DEFAULT_KEYSTORE_TYPE;
        }

        this.keyStoreType = type;
        this.keyStorePath = path;
        this.keyStorePassword = password;
    }

    /**
     * Configure from the v4 encoded auth-param string (a JSON object or a Pulsar-1 style param
     * string carrying {@code keyStoreType} / {@code keyStorePath} / {@code keyStorePassword}).
     *
     * @param paramsString the encoded parameter string
     */
    public void configureEncoded(String paramsString) {
        Map<String, String> params = null;
        try {
            params = AuthenticationUtil.configureFromJsonString(paramsString);
        } catch (Exception e) {
            // auth-param is not in json format
        }
        params = (params == null || params.isEmpty())
                ? AuthenticationUtil.configureFromPulsar1AuthParamString(paramsString)
                : params;
        configure(params);
    }

    /**
     * @return the configured keystore parameters (may be {@code null} if not yet configured)
     */
    public KeyStoreParams keyStoreParams() {
        if (keyStorePath == null && keyStorePassword == null && keyStoreType == null) {
            return null;
        }
        return KeyStoreParams.builder()
                .keyStoreType(keyStoreType)
                .keyStorePath(keyStorePath)
                .keyStorePassword(keyStorePassword)
                .build();
    }

    @Override
    public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<BinaryProtocolAuthData> getAuthDataAsync(AuthenticationCallContext ctx) {
        // mTLS authenticates via the TLS handshake; the connect command carries no auth_data.
        return CompletableFuture.completedFuture(DefaultBinaryProtocolAuthData.empty(AUTH_METHOD_NAME));
    }

    @Override
    public void close() {
    }
}
