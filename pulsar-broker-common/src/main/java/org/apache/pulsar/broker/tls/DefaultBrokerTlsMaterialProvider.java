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
package org.apache.pulsar.broker.tls;

import java.util.Set;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.tls.ClientTlsPurposeContext;
import org.apache.pulsar.common.tls.ClientTlsPurposeContext.ClientPurpose;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialProvider;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialSource;
import org.apache.pulsar.common.tls.ServerTlsPurposeContext;
import org.apache.pulsar.common.tls.ServerTlsPurposeContext.ServerPurpose;

/**
 * Factory for the default broker/proxy/web {@link FileBasedTlsMaterialProvider} (PIP-478),
 * configured from a broker-side {@link ServiceConfiguration}.
 *
 * <p>Two flavours are provided:
 * <ul>
 *   <li>{@link #forServer(ServiceConfiguration)} builds a provider from the broker's
 *       <em>server</em> TLS configuration (the certificate/key/trust the broker presents and uses
 *       to authenticate inbound connections) and registers the same source for the
 *       {@link ServerPurpose#BROKER}, {@link ServerPurpose#PROXY} and
 *       {@link ServerPurpose#WEB_SERVICE} purposes so that any server consumer resolves it. Callers
 *       can later override an individual purpose with a more specific source.</li>
 *   <li>{@link #forBrokerClient(ServiceConfiguration)} builds a provider from the broker's
 *       <em>client</em> (outbound, broker-to-broker) TLS configuration and registers the source for
 *       the {@link ClientPurpose#BINARY_CLIENT}, {@link ClientPurpose#HTTP_LOOKUP} and
 *       {@link ClientPurpose#ADMIN_HTTP} purposes.</li>
 * </ul>
 *
 * <p>The returned provider is <em>not</em> yet initialized: the caller is responsible for invoking
 * {@link FileBasedTlsMaterialProvider#initialize(org.apache.pulsar.common.tls.TlsMaterialProviderInitContext)}
 * to start file-rotation polling.
 */
public final class DefaultBrokerTlsMaterialProvider {

    /**
     * Default file-rotation refresh interval, in seconds, used when the configuration requests
     * "check on every new connection" (a value of {@code 0}).
     */
    static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 300;

    private DefaultBrokerTlsMaterialProvider() {
    }

    /**
     * Build a server-side provider from the broker's server TLS configuration. The same source is
     * registered for the {@link ServerPurpose#BROKER}, {@link ServerPurpose#PROXY} and
     * {@link ServerPurpose#WEB_SERVICE} purposes.
     *
     * @param conf the broker service configuration
     * @return an uninitialized {@link FileBasedTlsMaterialProvider}
     */
    public static FileBasedTlsMaterialProvider forServer(ServiceConfiguration conf) {
        FileBasedTlsMaterialProvider provider =
                new FileBasedTlsMaterialProvider(refreshIntervalSeconds(conf));
        boolean keyStore = conf.isTlsEnabledWithKeyStore();
        FileBasedTlsMaterialSource source = buildSource(
                keyStore,
                conf.getTlsProvider(),
                // PEM material
                conf.getTlsCertificateFilePath(),
                conf.getTlsKeyFilePath(),
                conf.getTlsTrustCertsFilePath(),
                // Keystore (key material)
                conf.getTlsKeyStoreType(),
                conf.getTlsKeyStore(),
                conf.getTlsKeyStorePassword(),
                // Truststore (trust material)
                conf.getTlsTrustStoreType(),
                conf.getTlsTrustStore(),
                conf.getTlsTrustStorePassword(),
                // Handshake tuning + flags
                conf.getTlsProtocols(),
                conf.getTlsCiphers(),
                conf.isTlsAllowInsecureConnection(),
                conf.isTlsHostnameVerificationEnabled(),
                conf.isTlsRequireTrustedClientCertOnConnect());

        provider.registerSource(ServerTlsPurposeContext.of(ServerPurpose.BROKER), source);
        provider.registerSource(ServerTlsPurposeContext.of(ServerPurpose.PROXY), source);
        provider.registerSource(ServerTlsPurposeContext.of(ServerPurpose.WEB_SERVICE), source);
        return provider;
    }

    /**
     * Build a client-side provider from the broker's outbound (broker-to-broker) TLS configuration.
     * The same source is registered for the {@link ClientPurpose#BINARY_CLIENT},
     * {@link ClientPurpose#HTTP_LOOKUP} and {@link ClientPurpose#ADMIN_HTTP} purposes.
     *
     * @param conf the broker service configuration
     * @return an uninitialized {@link FileBasedTlsMaterialProvider}
     */
    public static FileBasedTlsMaterialProvider forBrokerClient(ServiceConfiguration conf) {
        FileBasedTlsMaterialProvider provider =
                new FileBasedTlsMaterialProvider(refreshIntervalSeconds(conf));
        boolean keyStore = conf.isBrokerClientTlsEnabledWithKeyStore();
        FileBasedTlsMaterialSource source = buildSource(
                keyStore,
                conf.getBrokerClientSslProvider(),
                // PEM material
                conf.getBrokerClientCertificateFilePath(),
                conf.getBrokerClientKeyFilePath(),
                conf.getBrokerClientTrustCertsFilePath(),
                // Keystore (key material)
                conf.getBrokerClientTlsKeyStoreType(),
                conf.getBrokerClientTlsKeyStore(),
                conf.getBrokerClientTlsKeyStorePassword(),
                // Truststore (trust material)
                conf.getBrokerClientTlsTrustStoreType(),
                conf.getBrokerClientTlsTrustStore(),
                conf.getBrokerClientTlsTrustStorePassword(),
                // Handshake tuning + flags. The insecure/hostname-verification settings for outbound
                // broker-client connections reuse the shared broker TLS flags (see BrokerService).
                conf.getBrokerClientTlsProtocols(),
                conf.getBrokerClientTlsCiphers(),
                conf.isTlsAllowInsecureConnection(),
                conf.isTlsHostnameVerificationEnabled(),
                // Client side: not applicable.
                false);

        provider.registerSource(ClientTlsPurposeContext.of(ClientPurpose.BINARY_CLIENT), source);
        provider.registerSource(ClientTlsPurposeContext.of(ClientPurpose.HTTP_LOOKUP), source);
        provider.registerSource(ClientTlsPurposeContext.of(ClientPurpose.ADMIN_HTTP), source);
        return provider;
    }

    /**
     * Map the supplied TLS configuration fields onto a {@link FileBasedTlsMaterialSource}. When
     * {@code keyStore} is {@code true} the keystore/truststore fields are used and the PEM fields are
     * ignored; otherwise the PEM fields are used and the keystore fields are ignored.
     *
     * @param keyStore                          whether keystore-based material is enabled
     * @param tlsProvider                       optional security provider name (may be {@code null})
     * @param certificateFilePath               PEM certificate chain file path (PEM mode)
     * @param keyFilePath                       PEM private key file path (PEM mode)
     * @param trustCertsFilePath                PEM trust certificates file path (PEM mode)
     * @param keyStoreType                      key store type (keystore mode)
     * @param keyStorePath                      key store path (keystore mode)
     * @param keyStorePassword                  key store password (keystore mode)
     * @param trustStoreType                    trust store type (keystore mode)
     * @param trustStorePath                    trust store path (keystore mode)
     * @param trustStorePassword                trust store password (keystore mode)
     * @param tlsProtocols                      enabled TLS protocols (may be {@code null}/empty)
     * @param tlsCiphers                        enabled TLS ciphers (may be {@code null}/empty)
     * @param allowInsecureConnection           whether insecure connections are allowed
     * @param hostnameVerificationEnabled       whether hostname verification is enabled (client side)
     * @param requireTrustedClientCertOnConnect whether mTLS is required (server side)
     * @return a built {@link FileBasedTlsMaterialSource}
     */
    static FileBasedTlsMaterialSource buildSource(
            boolean keyStore,
            String tlsProvider,
            String certificateFilePath,
            String keyFilePath,
            String trustCertsFilePath,
            String keyStoreType,
            String keyStorePath,
            String keyStorePassword,
            String trustStoreType,
            String trustStorePath,
            String trustStorePassword,
            Set<String> tlsProtocols,
            Set<String> tlsCiphers,
            boolean allowInsecureConnection,
            boolean hostnameVerificationEnabled,
            boolean requireTrustedClientCertOnConnect) {
        FileBasedTlsMaterialSource.Builder builder = FileBasedTlsMaterialSource.builder()
                .tlsProvider(tlsProvider)
                .tlsProtocols(tlsProtocols)
                .tlsCiphers(tlsCiphers)
                .allowInsecureConnection(allowInsecureConnection)
                .hostnameVerificationEnabled(hostnameVerificationEnabled)
                .requireTrustedClientCertOnConnect(requireTrustedClientCertOnConnect);
        if (keyStore) {
            builder.keyStoreType(keyStoreType)
                    .keyStorePath(keyStorePath)
                    .keyStorePassword(keyStorePassword)
                    .trustStoreType(trustStoreType)
                    .trustStorePath(trustStorePath)
                    .trustStorePassword(trustStorePassword);
        } else {
            builder.certificateFilePath(certificateFilePath)
                    .keyFilePath(keyFilePath)
                    .trustCertsFilePath(trustCertsFilePath);
        }
        return builder.build();
    }

    private static int refreshIntervalSeconds(ServiceConfiguration conf) {
        long configured = conf.getTlsCertRefreshCheckDurationSec();
        if (configured <= 0) {
            return DEFAULT_REFRESH_INTERVAL_SECONDS;
        }
        return (int) Math.min(configured, Integer.MAX_VALUE);
    }
}
