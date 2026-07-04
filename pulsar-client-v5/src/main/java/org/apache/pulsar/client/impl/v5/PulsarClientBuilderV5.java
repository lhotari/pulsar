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
package org.apache.pulsar.client.impl.v5;

import io.opentelemetry.api.OpenTelemetry;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.v5.auth.LegacyV4AuthenticationAdapter;
import org.apache.pulsar.client.impl.v5.auth.V5ToV4AuthenticationAdapter;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V5 implementation of PulsarClientBuilder.
 * Builds a v4 ClientConfigurationData internally and wraps the v4 PulsarClientImpl.
 */
final class PulsarClientBuilderV5 implements PulsarClientBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientBuilderV5.class);

    private final ClientConfigurationData conf = new ClientConfigurationData();
    private String description;
    private Duration transactionTimeout;

    PulsarClientBuilderV5() {
        conf.setStatsIntervalSeconds(0);
    }

    @Override
    public PulsarClient build() throws PulsarClientException {
        try {
            foldBridgedV4TlsMaterial();
            var v4Client = new PulsarClientImpl(conf);
            return new PulsarClientV5(v4Client, description, transactionTimeout);
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new PulsarClientException(e.getMessage(), e);
        }
    }

    @Override
    public PulsarClientBuilder serviceUrl(String serviceUrl) {
        validatePulsarServiceUrl(serviceUrl, "serviceUrl");
        conf.setServiceUrl(serviceUrl);
        return this;
    }

    @Override
    public PulsarClientBuilder authentication(Authentication authentication) {
        conf.setAuthentication(toV4Authentication(authentication));
        return this;
    }

    /**
     * Convert a v5 {@link Authentication} into the v4 {@code Authentication} the underlying
     * {@link PulsarClientImpl} consumes.
     *
     * <p>A plugin bridged from v4 (produced by {@code AuthenticationFactory.token/tls/create}) is routed
     * back onto the v4 client verbatim, preserving legacy behaviour — including
     * {@code AuthenticationTls}'s builder-level TLS material configuration — until the full client-side
     * migration lands. A genuinely v5-native plugin is exposed through
     * {@link V5ToV4AuthenticationAdapter}, whose framework services are late-bound by the
     * {@link PulsarClientImpl} once it exists (PIP-478 stage 3b): the adapter implements
     * {@code ClientAuthenticationServicesAware}, so the client binds real executors / HTTP client factory /
     * client instance id into it before {@code start()}.
     */
    private static org.apache.pulsar.client.api.Authentication toV4Authentication(Authentication v5) {
        return LegacyV4AuthenticationAdapter.unwrapV4(v5)
                .orElseGet(() -> new V5ToV4AuthenticationAdapter(v5, Map.of()));
    }

    @Override
    public PulsarClientBuilder authentication(String authPluginClassName, String authParamsString)
            throws PulsarClientException {
        conf.setAuthPluginClassName(authPluginClassName);
        conf.setAuthParams(authParamsString);
        return this;
    }

    @Override
    public PulsarClientBuilder operationTimeout(Duration timeout) {
        conf.setOperationTimeoutMs(timeout.toMillis());
        return this;
    }

    @Override
    public PulsarClientBuilder connectionPolicy(ConnectionPolicy policy) {
        conf.setConnectionTimeoutMs((int) policy.connectionTimeout().toMillis());
        conf.setConnectionsPerBroker(policy.connectionsPerBroker());
        conf.setUseTcpNoDelay(policy.enableTcpNoDelay());
        conf.setKeepAliveIntervalSeconds((int) policy.keepAliveInterval().toSeconds());
        conf.setConnectionMaxIdleSeconds((int) policy.connectionMaxIdleTime().toSeconds());
        conf.setNumIoThreads(policy.ioThreads());
        conf.setNumListenerThreads(policy.callbackThreads());
        if (policy.proxyServiceUrl() != null) {
            validatePulsarServiceUrl(policy.proxyServiceUrl(), "ConnectionPolicy.proxyServiceUrl");
            conf.setProxyServiceUrl(policy.proxyServiceUrl());
            if (policy.proxyProtocol() != null) {
                conf.setProxyProtocol(
                        org.apache.pulsar.client.api.ProxyProtocol.valueOf(policy.proxyProtocol().name()));
            }
        }
        // BackoffPolicy adaptation will be implemented when the v4 client exposes
        // a public way to override the reconnection backoff.
        return this;
    }

    @Override
    public PulsarClientBuilder transactionPolicy(TransactionPolicy policy) {
        conf.setEnableTransaction(true);
        this.transactionTimeout = policy.timeout();
        return this;
    }

    @Override
    public PulsarClientBuilder tlsPolicy(TlsPolicy policy) {
        return tlsPolicy(TlsPurpose.CLIENT_DEFAULT, policy);
    }

    @Override
    public PulsarClientBuilder tlsPolicy(TlsPurpose purpose, TlsPolicy policy) {
        if (purpose == null || policy == null) {
            throw new IllegalArgumentException("tlsPolicy purpose and policy must not be null");
        }
        conf.setUseTls(true);
        Map<TlsPurpose, TlsPolicy> map = conf.getTlsPolicyMap();
        if (map == null) {
            map = new LinkedHashMap<>();
            conf.setTlsPolicyMap(map);
        }
        map.put(purpose, policy);
        return this;
    }

    @Override
    public PulsarClientBuilder tlsFactory(PulsarTlsFactory factory) {
        if (factory == null) {
            throw new IllegalArgumentException("tlsFactory must not be null");
        }
        conf.setUseTls(true);
        conf.setTlsFactory(factory);
        return this;
    }

    /**
     * Fold a bridged v4 {@code AuthenticationTls} / {@code AuthenticationKeyStoreTls}'s mTLS material into
     * the {@link TlsPurpose#CLIENT_DEFAULT} policy when the new PIP-478 TLS path is active (PIP-478 stage
     * 3b). This lets a v5 user configure the trust store via {@link #tlsPolicy(TlsPolicy)} and the client
     * identity via {@code authentication(AuthenticationFactory.tls(cert, key))} — the transport reads its
     * material from the client TLS factory (not the auth plugin) on the new path, so the auth plugin's
     * certificate/key must be folded in here. Only applies when the v5 builder configured a
     * {@code tlsPolicy(...)}; a plain legacy client (no {@code tlsPolicy}) keeps the v4 behaviour where the
     * auth plugin supplies the transport material directly.
     *
     * <p>An arbitrary third-party v4 plugin that reports {@code hasDataForTls()} but is not one of the
     * built-in TLS classes is folded generically by {@link #foldGenericV4TlsMaterial}: {@code build()} runs
     * on the application thread (off the Netty event loop), so probing the plugin's {@code getAuthData()} for
     * its TLS material here is acceptable. This is scoped to the v5-builder-with-{@code tlsPolicy} path, so a
     * plain v4 client keeps its lazy behaviour (R6).
     */
    private void foldBridgedV4TlsMaterial() {
        if (conf.getTlsPolicyMap() == null) {
            return;
        }
        org.apache.pulsar.client.api.Authentication v4 = conf.getAuthentication();
        if (v4 instanceof AuthenticationTls tls
                && tls.getCertFilePath() != null && tls.getKeyFilePath() != null) {
            mergeClientDefault(base -> pemBuilder(base)
                    .certificateFilePath(tls.getCertFilePath())
                    .keyFilePath(tls.getKeyFilePath())
                    .build());
        } else if (v4 instanceof AuthenticationKeyStoreTls keyStoreTls
                && keyStoreTls.getKeyStoreParams() != null) {
            KeyStoreParams ks = keyStoreTls.getKeyStoreParams();
            mergeClientDefault(base -> keyStoreBuilder(base)
                    .keyStorePath(ks.getKeyStorePath())
                    .keyStorePassword(ks.getKeyStorePassword())
                    .keyStoreType(ks.getKeyStoreType())
                    .build());
        } else if (v4 != null) {
            foldGenericV4TlsMaterial(v4);
        }
    }

    /**
     * Fold a bridged third-party v4 plugin's TLS material into {@link TlsPurpose#CLIENT_DEFAULT} when it
     * reports {@code hasDataForTls()} (PIP-478 stage 3c). Probing {@code getAuthData()} is safe here because
     * {@code build()} runs on the application thread, not the event loop. Only <em>file-based</em> material
     * (PEM cert/key file paths or a keystore) can be represented in the file-path {@link TlsPolicy}; a plugin
     * that exposes only in-memory cert/key material is logged rather than silently dropped, since it cannot
     * be folded on this path.
     *
     * @param v4 the bridged v4 authentication plugin (not a built-in TLS class)
     */
    @SuppressWarnings("deprecation")
    private void foldGenericV4TlsMaterial(org.apache.pulsar.client.api.Authentication v4) {
        final AuthenticationDataProvider data;
        try {
            data = v4.getAuthData();
        } catch (Exception e) {
            // A plugin that fails to produce auth data at build time (e.g. a not-yet-reachable credential
            // endpoint) is not a fatal client-build error; it simply contributes no TLS material here.
            LOG.debug("Could not probe v4 authentication plugin {} for TLS material", v4.getClass().getName(), e);
            return;
        }
        if (data == null || !data.hasDataForTls()) {
            return;
        }
        String certPath = data.getTlsCertificateFilePath();
        String keyPath = data.getTlsPrivateKeyFilePath();
        KeyStoreParams ks = data.getTlsKeyStoreParams();
        if (isNotBlank(certPath) && isNotBlank(keyPath)) {
            mergeClientDefault(base -> pemBuilder(base).certificateFilePath(certPath).keyFilePath(keyPath).build());
        } else if (ks != null && isNotBlank(ks.getKeyStorePath())) {
            mergeClientDefault(base -> keyStoreBuilder(base)
                    .keyStorePath(ks.getKeyStorePath())
                    .keyStorePassword(ks.getKeyStorePassword())
                    .keyStoreType(ks.getKeyStoreType())
                    .build());
        } else {
            LOG.warn("Bridged v4 authentication plugin {} reports TLS material (hasDataForTls()) but exposes "
                    + "only in-memory cert/key, which cannot be represented in the file-path client TLS "
                    + "policy; its material will not be folded into the transport. Configure it via "
                    + "tlsPolicy(...) or a file-based plugin.", v4.getClass().getName());
        }
    }

    private static boolean isNotBlank(String s) {
        return s != null && !s.isBlank();
    }

    private void mergeClientDefault(java.util.function.Function<TlsPolicy, TlsPolicy> merge) {
        Map<TlsPurpose, TlsPolicy> map = conf.getTlsPolicyMap();
        TlsPolicy base = map.get(TlsPurpose.CLIENT_DEFAULT);
        map.put(TlsPurpose.CLIENT_DEFAULT, merge.apply(base));
    }

    /** Copy the trust material and flags of {@code base} (if any) into a PEM-format builder. */
    private static TlsPolicy.Builder pemBuilder(TlsPolicy base) {
        TlsPolicy.Builder b = copyFlags(base).format(TlsPolicy.Format.PEM);
        if (base != null && base.format() == TlsPolicy.Format.PEM) {
            b.trustCertsFilePath(base.trustCertsFilePath());
        }
        return b;
    }

    /** Copy the trust material and flags of {@code base} (if any) into a keystore-format builder. */
    private static TlsPolicy.Builder keyStoreBuilder(TlsPolicy base) {
        TlsPolicy.Builder b = copyFlags(base).format(TlsPolicy.Format.KEYSTORE);
        if (base != null && base.format() == TlsPolicy.Format.KEYSTORE) {
            // Preserve the base truststore (path, password, and TYPE): folding the auth plugin's keystore must
            // not clobber the truststore type configured via tlsPolicy(...) — the keystore and truststore may
            // use different types (e.g. a PKCS12 keystore with a JKS truststore).
            b.trustStorePath(base.trustStorePath())
                    .trustStorePassword(base.trustStorePassword())
                    .trustStoreType(base.trustStoreType());
        }
        return b;
    }

    private static TlsPolicy.Builder copyFlags(TlsPolicy base) {
        TlsPolicy.Builder b = TlsPolicy.builder();
        if (base != null) {
            b.allowInsecureConnection(base.allowInsecureConnection())
                    .enableHostnameVerification(base.enableHostnameVerification())
                    .protocols(base.protocols())
                    .ciphers(base.ciphers());
        }
        return b;
    }

    @Override
    public PulsarClientBuilder openTelemetry(OpenTelemetry openTelemetry) {
        conf.setOpenTelemetry(openTelemetry);
        return this;
    }

    @Override
    public PulsarClientBuilder memoryLimit(MemorySize size) {
        conf.setMemoryLimitBytes(size.bytes());
        return this;
    }

    @Override
    public PulsarClientBuilder listenerName(String name) {
        conf.setListenerName(name);
        return this;
    }

    @Override
    public PulsarClientBuilder description(String description) {
        this.description = description;
        conf.setDescription(description);
        return this;
    }

    /**
     * Reject anything that isn't the broker binary protocol. The most common
     * mistake is passing the admin/web service URL ({@code http://...}) where a
     * broker URL is expected — call that out specifically. The v4 client used to
     * silently fail far downstream with cryptic connection errors; here we fail
     * fast at configure time with a message the user can act on.
     */
    private static void validatePulsarServiceUrl(String url, String fieldName) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be null or blank");
        }
        if (url.startsWith("pulsar://") || url.startsWith("pulsar+ssl://")) {
            return;
        }
        if (url.startsWith("http://") || url.startsWith("https://")) {
            throw new IllegalArgumentException(fieldName + " must use the broker binary protocol "
                    + "(pulsar:// or pulsar+ssl://); got '" + url + "'. This looks like the admin/web "
                    + "service URL — pass the broker service URL instead (typically port 6650, or "
                    + "6651 for TLS).");
        }
        throw new IllegalArgumentException(fieldName + " must use the broker binary protocol "
                + "(pulsar:// or pulsar+ssl://); got '" + url + "'.");
    }
}
