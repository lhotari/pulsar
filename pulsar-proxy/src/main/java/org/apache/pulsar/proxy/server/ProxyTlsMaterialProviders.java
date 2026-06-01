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
package org.apache.pulsar.proxy.server;

import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.common.tls.AuthenticationDataTlsMaterialSource;
import org.apache.pulsar.common.tls.ClientTlsPurposeContext;
import org.apache.pulsar.common.tls.ClientTlsPurposeContext.ClientPurpose;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialProvider;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialSource;
import org.apache.pulsar.common.tls.ServerTlsPurposeContext;
import org.apache.pulsar.common.tls.ServerTlsPurposeContext.ServerPurpose;

/**
 * Factory for the proxy's {@link FileBasedTlsMaterialProvider} (PIP-478), configured from a
 * {@link ProxyConfiguration}.
 *
 * <p>{@link ProxyConfiguration} is not a broker {@code ServiceConfiguration}, so its TLS settings are
 * mapped onto {@link FileBasedTlsMaterialSource}s here rather than via
 * {@code DefaultBrokerTlsMaterialProvider}. Two flavours are provided:
 * <ul>
 *   <li>{@link #forServer(ProxyConfiguration)} builds a provider from the proxy's <em>server</em> TLS
 *       configuration (the certificate/key/trust the proxy presents to inbound clients) and registers
 *       it for both the {@link ServerPurpose#PROXY} and {@link ServerPurpose#WEB_SERVICE} purposes so
 *       that the binary-protocol channel initializer and the Jetty web server both resolve it.</li>
 *   <li>{@link #forBrokerClient(ProxyConfiguration)} builds a provider from the proxy's
 *       <em>broker-client</em> (proxy-to-broker) TLS configuration and registers it for the
 *       {@link ClientPurpose#BINARY_CLIENT} and {@link ClientPurpose#ADMIN_HTTP} purposes.</li>
 * </ul>
 *
 * <p>The returned provider is <em>not</em> yet initialized: the caller is responsible for invoking
 * {@link FileBasedTlsMaterialProvider#initialize(org.apache.pulsar.common.tls.TlsMaterialProviderInitContext)}
 * to start file-rotation polling.
 */
final class ProxyTlsMaterialProviders {

    /**
     * Default file-rotation refresh interval, in seconds, used when the configuration requests
     * "check on every new connection" (a value of {@code 0} or less).
     */
    static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 300;

    private ProxyTlsMaterialProviders() {
    }

    /**
     * Build a server-side provider from the proxy's server TLS configuration. The same source is
     * registered for the {@link ServerPurpose#PROXY} and {@link ServerPurpose#WEB_SERVICE} purposes.
     *
     * @param config the proxy configuration
     * @return an uninitialized {@link FileBasedTlsMaterialProvider}
     */
    static FileBasedTlsMaterialProvider forServer(ProxyConfiguration config) {
        FileBasedTlsMaterialProvider provider =
                new FileBasedTlsMaterialProvider(refreshIntervalSeconds(config));
        FileBasedTlsMaterialSource source = buildServerSource(config);
        provider.registerSource(ServerTlsPurposeContext.of(ServerPurpose.PROXY), source);
        provider.registerSource(ServerTlsPurposeContext.of(ServerPurpose.WEB_SERVICE), source);
        return provider;
    }

    /**
     * Build the proxy's server-side {@link FileBasedTlsMaterialSource} (the proxy presents this
     * material to inbound clients).
     *
     * @param config the proxy configuration
     * @return the server-side TLS source
     */
    static FileBasedTlsMaterialSource buildServerSource(ProxyConfiguration config) {
        FileBasedTlsMaterialSource.Builder builder = FileBasedTlsMaterialSource.builder()
                .tlsProvider(config.getTlsProvider())
                .tlsProtocols(config.getTlsProtocols())
                .tlsCiphers(config.getTlsCiphers())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(config.isTlsRequireTrustedClientCertOnConnect());
        if (config.isTlsEnabledWithKeyStore()) {
            builder.keyStoreType(config.getTlsKeyStoreType())
                    .keyStorePath(config.getTlsKeyStore())
                    .keyStorePassword(config.getTlsKeyStorePassword())
                    .trustStoreType(config.getTlsTrustStoreType())
                    .trustStorePath(config.getTlsTrustStore())
                    .trustStorePassword(config.getTlsTrustStorePassword());
        } else {
            builder.certificateFilePath(config.getTlsCertificateFilePath())
                    .keyFilePath(config.getTlsKeyFilePath())
                    .trustCertsFilePath(config.getTlsTrustCertsFilePath());
        }
        return builder.build();
    }

    /**
     * Build a client-side provider from the proxy's broker-client (proxy-to-broker) TLS
     * configuration. The same source is registered for the {@link ClientPurpose#BINARY_CLIENT} and
     * {@link ClientPurpose#ADMIN_HTTP} purposes. When {@code authData} is non-{@code null} an
     * {@link AuthenticationDataTlsMaterialSource} is layered on top so the auth-plugin-supplied client
     * certificate (mTLS / keystore auth) takes precedence over the file-based source.
     *
     * @param config   the proxy configuration
     * @param authData the broker-client authentication data, or {@code null} when none applies
     * @return an uninitialized {@link FileBasedTlsMaterialProvider}
     */
    static FileBasedTlsMaterialProvider forBrokerClient(ProxyConfiguration config,
                                                        AuthenticationDataProvider authData) {
        FileBasedTlsMaterialProvider provider =
                new FileBasedTlsMaterialProvider(refreshIntervalSeconds(config));
        FileBasedTlsMaterialSource source = buildBrokerClientSource(config);
        registerBrokerClient(provider, source, authData);
        return provider;
    }

    /**
     * Register the broker-client source (and, when present, the auth-data override) for the binary and
     * admin client purposes.
     *
     * @param provider the provider to register on
     * @param source   the broker-client file source
     * @param authData the broker-client authentication data, or {@code null}
     */
    static void registerBrokerClient(FileBasedTlsMaterialProvider provider, FileBasedTlsMaterialSource source,
                                     AuthenticationDataProvider authData) {
        ClientTlsPurposeContext binary = ClientTlsPurposeContext.of(ClientPurpose.BINARY_CLIENT);
        ClientTlsPurposeContext admin = ClientTlsPurposeContext.of(ClientPurpose.ADMIN_HTTP);
        provider.registerSource(binary, source);
        provider.registerSource(admin, source);
        if (authData != null && hasDataForTls(authData)) {
            provider.registerSource(binary, AuthenticationDataTlsMaterialSource.of(authData, source));
            provider.registerSource(admin, AuthenticationDataTlsMaterialSource.of(authData, source));
        }
    }

    /**
     * Build the proxy's broker-client {@link FileBasedTlsMaterialSource} (the proxy uses this material
     * when connecting to brokers).
     *
     * @param config the proxy configuration
     * @return the broker-client TLS source
     */
    static FileBasedTlsMaterialSource buildBrokerClientSource(ProxyConfiguration config) {
        FileBasedTlsMaterialSource.Builder builder = FileBasedTlsMaterialSource.builder()
                .tlsProvider(config.getBrokerClientSslProvider())
                .tlsProtocols(config.getBrokerClientTlsProtocols())
                .tlsCiphers(config.getBrokerClientTlsCiphers())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .hostnameVerificationEnabled(config.isTlsHostnameVerificationEnabled());
        if (config.isBrokerClientTlsEnabledWithKeyStore()) {
            builder.keyStoreType(config.getBrokerClientTlsKeyStoreType())
                    .keyStorePath(config.getBrokerClientTlsKeyStore())
                    .keyStorePassword(config.getBrokerClientTlsKeyStorePassword())
                    .trustStoreType(config.getBrokerClientTlsTrustStoreType())
                    .trustStorePath(config.getBrokerClientTlsTrustStore())
                    .trustStorePassword(config.getBrokerClientTlsTrustStorePassword());
        } else {
            builder.certificateFilePath(config.getBrokerClientCertificateFilePath())
                    .keyFilePath(config.getBrokerClientKeyFilePath())
                    .trustCertsFilePath(config.getBrokerClientTrustCertsFilePath());
        }
        return builder.build();
    }

    private static boolean hasDataForTls(AuthenticationDataProvider authData) {
        try {
            return authData.hasDataForTls();
        } catch (Exception e) {
            return false;
        }
    }

    private static int refreshIntervalSeconds(ProxyConfiguration config) {
        long configured = config.getTlsCertRefreshCheckDurationSec();
        if (configured <= 0) {
            return DEFAULT_REFRESH_INTERVAL_SECONDS;
        }
        return (int) Math.min(configured, Integer.MAX_VALUE);
    }
}
