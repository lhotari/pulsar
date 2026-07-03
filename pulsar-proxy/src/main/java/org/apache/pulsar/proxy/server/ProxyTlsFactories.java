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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.tls.TlsFactorySupport;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;

/**
 * Composes the proxy's built-in {@link PulsarTlsFactory} instances from a {@link ProxyConfiguration}
 * (PIP-478 stage 2b), mirroring {@code DefaultBrokerTlsFactory} but reading the proxy config. Used when the
 * new PIP-478 TLS path is selected without a custom {@code tlsFactoryClassName}.
 */
final class ProxyTlsFactories {

    private ProxyTlsFactories() {
    }

    /**
     * A server-side factory serving a single server purpose (PROXY or WEB) from the proxy's server TLS
     * material with the supplied ciphers/protocols (the binary front-end and the web server use different
     * cipher/protocol lists).
     */
    static PulsarTlsFactory serverFactory(ProxyConfiguration config, TlsPurpose purpose,
                                          Set<String> ciphers, Set<String> protocols) {
        Map<TlsPurpose, TlsPolicy> policies = Map.of(purpose, serverPolicy(config, ciphers, protocols));
        FileBasedTlsFactorySettings settings = FileBasedTlsFactorySettings.builder()
                .requireTrustedClientCert(config.isTlsRequireTrustedClientCertOnConnect())
                .refreshIntervalSeconds(refreshIntervalSeconds(config))
                .engineProvider(TlsFactorySupport.engineProvider(config.getTlsProvider()))
                .build();
        return new FileBasedTlsFactory(policies, settings);
    }

    /**
     * A client-side factory serving {@link TlsPurpose#BROKER_CLIENT} from the proxy's {@code brokerClient*}
     * TLS material, for the proxy&rarr;broker binary path and the admin HTTP client.
     */
    static PulsarTlsFactory brokerClientFactory(ProxyConfiguration config) {
        Map<TlsPurpose, TlsPolicy> policies = Map.of(TlsPurpose.BROKER_CLIENT, brokerClientPolicy(config));
        FileBasedTlsFactorySettings settings = FileBasedTlsFactorySettings.builder()
                .requireTrustedClientCert(false)
                .refreshIntervalSeconds(refreshIntervalSeconds(config))
                .engineProvider(TlsFactorySupport.engineProvider(config.getBrokerClientSslProvider()))
                .build();
        return new FileBasedTlsFactory(policies, settings);
    }

    private static TlsPolicy serverPolicy(ProxyConfiguration config, Set<String> ciphers, Set<String> protocols) {
        TlsPolicy.Builder builder = TlsPolicy.builder()
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .enableHostnameVerification(config.isTlsHostnameVerificationEnabled())
                .protocols(toList(protocols))
                .ciphers(toList(ciphers));
        if (config.isTlsEnabledWithKeyStore()) {
            builder.format(TlsPolicy.Format.KEYSTORE)
                    .storeType(firstNonBlank(config.getTlsKeyStoreType(), config.getTlsTrustStoreType()))
                    .keyStorePath(config.getTlsKeyStore())
                    .keyStorePassword(config.getTlsKeyStorePassword())
                    .trustStorePath(config.getTlsTrustStore())
                    .trustStorePassword(config.getTlsTrustStorePassword());
        } else {
            builder.format(TlsPolicy.Format.PEM)
                    .trustCertsFilePath(config.getTlsTrustCertsFilePath())
                    .certificateFilePath(config.getTlsCertificateFilePath())
                    .keyFilePath(config.getTlsKeyFilePath());
        }
        return builder.build();
    }

    private static TlsPolicy brokerClientPolicy(ProxyConfiguration config) {
        TlsPolicy.Builder builder = TlsPolicy.builder()
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .enableHostnameVerification(config.isTlsHostnameVerificationEnabled())
                .protocols(toList(config.getBrokerClientTlsProtocols()))
                .ciphers(toList(config.getBrokerClientTlsCiphers()));
        if (config.isBrokerClientTlsEnabledWithKeyStore()) {
            builder.format(TlsPolicy.Format.KEYSTORE)
                    .storeType(firstNonBlank(config.getBrokerClientTlsKeyStoreType(),
                            config.getBrokerClientTlsTrustStoreType()))
                    .keyStorePath(config.getBrokerClientTlsKeyStore())
                    .keyStorePassword(config.getBrokerClientTlsKeyStorePassword())
                    .trustStorePath(config.getBrokerClientTlsTrustStore())
                    .trustStorePassword(config.getBrokerClientTlsTrustStorePassword());
        } else {
            builder.format(TlsPolicy.Format.PEM)
                    .trustCertsFilePath(config.getBrokerClientTrustCertsFilePath())
                    .certificateFilePath(config.getBrokerClientCertificateFilePath())
                    .keyFilePath(config.getBrokerClientKeyFilePath());
        }
        return builder.build();
    }

    private static int refreshIntervalSeconds(ProxyConfiguration config) {
        long configured = config.getTlsCertRefreshCheckDurationSec();
        if (configured <= 0) {
            return FileBasedTlsFactorySettings.DEFAULT_REFRESH_INTERVAL_SECONDS;
        }
        return (int) Math.min(configured, Integer.MAX_VALUE);
    }

    private static List<String> toList(Set<String> values) {
        return values == null ? List.of() : List.copyOf(values);
    }

    private static String firstNonBlank(String primary, String fallback) {
        return StringUtils.isNotBlank(primary) ? primary : fallback;
    }
}
