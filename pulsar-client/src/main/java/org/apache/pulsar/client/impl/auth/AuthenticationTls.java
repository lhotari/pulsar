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

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.v5.TlsAuthenticationV5;

/**
 * This plugin requires these parameters.
 *
 * <p>tlsCertFile: A file path for a client certificate. tlsKeyFile: A file path for a client private key.
 *
 * <p>PIP-478: this v4 plugin is a thin shim over the v5-native {@link TlsAuthenticationV5}. mTLS
 * authenticates via the TLS handshake, so the binary-protocol credential carries the {@code tls} auth
 * method with empty {@code auth_data}; the v4 surface (including {@link #getAuthData()} returning
 * {@link AuthenticationDataTls}, which feeds the client's TLS material override path) is preserved for
 * source compatibility. The plugin keeps the verbatim v4 synchronous connect/refresh path when driven
 * by {@code ClientCnx}. The async v5 SPI is exercised via the v5-native delegate and the
 * {@code V5ToV4AuthenticationAdapter}, not by this v4 plugin directly.
 */
public class AuthenticationTls implements Authentication, EncodedAuthenticationParameterSupport {
    static final String AUTH_METHOD_NAME = "tls";
    private static final long serialVersionUID = 1L;

    private final TlsAuthenticationV5 delegate;

    public AuthenticationTls() {
        this.delegate = new TlsAuthenticationV5();
    }

    public AuthenticationTls(String certFilePath, String keyFilePath) {
        this.delegate = new TlsAuthenticationV5(certFilePath, keyFilePath);
    }

    public AuthenticationTls(Supplier<ByteArrayInputStream> certStreamProvider,
            Supplier<ByteArrayInputStream> keyStreamProvider) {
        this(certStreamProvider, keyStreamProvider, null);
    }

    public AuthenticationTls(Supplier<ByteArrayInputStream> certStreamProvider,
            Supplier<ByteArrayInputStream> keyStreamProvider, Supplier<ByteArrayInputStream> trustStoreStreamProvider) {
        this.delegate = new TlsAuthenticationV5(certStreamProvider, keyStreamProvider, trustStoreStreamProvider);
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
        try {
            if (delegate.certFilePath() != null && delegate.keyFilePath() != null) {
                return new AuthenticationDataTls(delegate.certFilePath(), delegate.keyFilePath());
            } else if (delegate.certStreamProvider() != null && delegate.keyStreamProvider() != null) {
                return new AuthenticationDataTls(delegate.certStreamProvider(), delegate.keyStreamProvider(),
                        delegate.trustStoreStreamProvider());
            }
        } catch (Exception e) {
            throw new PulsarClientException(e);
        }
        throw new IllegalArgumentException("cert/key file path or cert/key stream must be present");
    }

    @Override
    public void configure(String encodedAuthParamString) {
        delegate.configureEncoded(encodedAuthParamString);
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
        delegate.configure(authParams);
    }

    @Override
    public void start() throws PulsarClientException {
        // noop — the v5-native delegate has no initialization I/O for mTLS
    }

    @VisibleForTesting
    public String getCertFilePath() {
        return delegate.certFilePath();
    }

    @VisibleForTesting
    public String getKeyFilePath() {
        return delegate.keyFilePath();
    }

    @VisibleForTesting
    Supplier<ByteArrayInputStream> getCertStreamProvider() {
        return delegate.certStreamProvider();
    }

    @VisibleForTesting
    Supplier<ByteArrayInputStream> getKeyStreamProvider() {
        return delegate.keyStreamProvider();
    }

    @VisibleForTesting
    Supplier<ByteArrayInputStream> getTrustStoreStreamProvider() {
        return delegate.trustStoreStreamProvider();
    }
}
