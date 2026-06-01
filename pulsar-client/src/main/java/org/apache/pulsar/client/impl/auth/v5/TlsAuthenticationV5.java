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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthDataProvider;
import org.apache.pulsar.client.api.v5.auth.DefaultBinaryProtocolAuthData;
import org.apache.pulsar.client.impl.AuthenticationUtil;

/**
 * v5-native mTLS authentication (PIP-478). mTLS authenticates via the TLS handshake (the client
 * certificate and key are presented as the TLS material), so the binary-protocol credential carries
 * the {@code tls} auth method with empty {@code auth_data}. This body holds the configured TLS
 * material location (cert/key file paths, or {@link ByteArrayInputStream} stream providers) so the v4
 * shim can keep supplying its {@code AuthenticationDataTls} for the TLS material override path; the
 * material itself is not sent in the connect command.
 *
 * <p>Although the v5 {@code Authentication} SPI deliberately does not extend {@link Serializable},
 * this concrete built-in body is serializable so the v4 {@code AuthenticationTls} shim (whose
 * interface requires {@code Serializable}, for Functions/connector frameworks) round-trips, including
 * its serializable stream providers.
 */
public class TlsAuthenticationV5 implements Authentication, BinaryProtocolAuthDataProvider, Serializable {

    private static final long serialVersionUID = 1L;

    /** The stable auth-method name. */
    public static final String AUTH_METHOD_NAME = "tls";

    private String certFilePath;
    private String keyFilePath;
    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "Using custom serializer which Findbugs can't detect")
    private Supplier<ByteArrayInputStream> certStreamProvider;
    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "Using custom serializer which Findbugs can't detect")
    private Supplier<ByteArrayInputStream> keyStreamProvider;
    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "Using custom serializer which Findbugs can't detect")
    private Supplier<ByteArrayInputStream> trustStoreStreamProvider;

    /** No-arg constructor for reflective (string-based) construction. */
    public TlsAuthenticationV5() {
    }

    /**
     * @param certFilePath a file path for a client certificate
     * @param keyFilePath  a file path for a client private key
     */
    public TlsAuthenticationV5(String certFilePath, String keyFilePath) {
        this.certFilePath = certFilePath;
        this.keyFilePath = keyFilePath;
    }

    /**
     * @param certStreamProvider       supplies the client certificate PEM stream
     * @param keyStreamProvider        supplies the client private key PEM stream
     * @param trustStoreStreamProvider supplies the trust store stream (may be {@code null})
     */
    public TlsAuthenticationV5(Supplier<ByteArrayInputStream> certStreamProvider,
            Supplier<ByteArrayInputStream> keyStreamProvider,
            Supplier<ByteArrayInputStream> trustStoreStreamProvider) {
        this.certStreamProvider = certStreamProvider;
        this.keyStreamProvider = keyStreamProvider;
        this.trustStoreStreamProvider = trustStoreStreamProvider;
    }

    @Override
    public String authMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public void configure(Map<String, String> authParams) {
        this.certFilePath = authParams.get("tlsCertFile");
        this.keyFilePath = authParams.get("tlsKeyFile");
    }

    /**
     * Configure from the v4 encoded auth-param string (a JSON object or a Pulsar-1 style param
     * string carrying {@code tlsCertFile} / {@code tlsKeyFile}).
     *
     * @param encodedAuthParamString the encoded parameter string
     */
    public void configureEncoded(String encodedAuthParamString) {
        Map<String, String> authParamsMap = null;
        try {
            authParamsMap = AuthenticationUtil.configureFromJsonString(encodedAuthParamString);
        } catch (Exception e) {
            // auth-param is not in json format
        }
        authParamsMap = (authParamsMap == null || authParamsMap.isEmpty())
                ? AuthenticationUtil.configureFromPulsar1AuthParamString(encodedAuthParamString)
                : authParamsMap;
        configure(authParamsMap);
    }

    /**
     * @return the configured client certificate file path (may be {@code null})
     */
    public String certFilePath() {
        return certFilePath;
    }

    /**
     * @return the configured client private key file path (may be {@code null})
     */
    public String keyFilePath() {
        return keyFilePath;
    }

    /**
     * @return the configured certificate stream provider (may be {@code null})
     */
    public Supplier<ByteArrayInputStream> certStreamProvider() {
        return certStreamProvider;
    }

    /**
     * @return the configured private key stream provider (may be {@code null})
     */
    public Supplier<ByteArrayInputStream> keyStreamProvider() {
        return keyStreamProvider;
    }

    /**
     * @return the configured trust store stream provider (may be {@code null})
     */
    public Supplier<ByteArrayInputStream> trustStoreStreamProvider() {
        return trustStoreStreamProvider;
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
