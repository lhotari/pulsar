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

import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Map;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.v5.KeyStoreTlsAuthenticationV5;

/**
 * This plugin requires these parameters: keyStoreType, keyStorePath, and keyStorePassword.
 * This parameter will construct a AuthenticationDataProvider.
 *
 * <p>PIP-478: this v4 plugin is a thin shim over the v5-native {@link KeyStoreTlsAuthenticationV5}.
 * mTLS authenticates via the TLS handshake, so the binary-protocol credential carries the {@code tls}
 * auth method with empty {@code auth_data}; the v4 surface (including {@link #getAuthData()} returning
 * {@link AuthenticationDataKeyStoreTls}, which feeds the client's TLS material override path) is
 * preserved for source compatibility. The plugin keeps the verbatim v4 synchronous connect/refresh
 * path when driven by {@code ClientCnx}. The async v5 SPI is exercised via the v5-native delegate and
 * the {@code V5ToV4AuthenticationAdapter}, not by this v4 plugin directly.
 */
@CustomLog
public class AuthenticationKeyStoreTls implements Authentication, EncodedAuthenticationParameterSupport {
    private static final long serialVersionUID = 1L;

    private static final String AUTH_NAME = "tls";

    // parameter name
    public static final String KEYSTORE_TYPE = "keyStoreType";
    public static final String KEYSTORE_PATH = "keyStorePath";
    public static final String KEYSTORE_PW = "keyStorePassword";

    private final KeyStoreTlsAuthenticationV5 delegate;

    public AuthenticationKeyStoreTls() {
        this.delegate = new KeyStoreTlsAuthenticationV5();
    }

    public AuthenticationKeyStoreTls(String keyStoreType, String keyStorePath, String keyStorePassword) {
        this.delegate = new KeyStoreTlsAuthenticationV5(keyStoreType, keyStorePath, keyStorePassword);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public String getAuthMethodName() {
        return AUTH_NAME;
    }

    @SuppressWarnings("deprecation")
    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        try {
            return new AuthenticationDataKeyStoreTls(delegate.keyStoreParams());
        } catch (Exception e) {
            throw new PulsarClientException(e);
        }
    }

    // passed in KEYSTORE_TYPE/KEYSTORE_PATH/KEYSTORE_PW to construct parameters.
    // e.g. {"keyStoreType":"JKS","keyStorePath":"/path/to/keystorefile","keyStorePassword":"keystorepw"}
    //  or: "keyStoreType":"JKS","keyStorePath":"/path/to/keystorefile","keyStorePassword":"keystorepw"
    @Override
    public void configure(String paramsString) {
        delegate.configureEncoded(paramsString);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void configure(Map<String, String> params) {
        delegate.configure(params);
    }

    @Override
    public void start() throws PulsarClientException {
        // noop — the v5-native delegate has no initialization I/O for keystore mTLS
    }

    // return strings like : "key1":"value1", "key2":"value2", ...
    public static String mapToString(Map<String, String> map) {
        return Joiner.on(',').withKeyValueSeparator(':').join(map);
    }
}
