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
package org.apache.pulsar.client.impl.tls;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsFactoryInitContext;
import org.apache.pulsar.common.tls.TlsHandle;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.common.util.DefaultPulsarSslFactory;

/**
 * Client-side counterpart of the server's {@code TlsFactorySupport} (which lives in
 * {@code pulsar-broker-common}, off the client classpath): the selection rule, the
 * {@code ClientConfigurationData}&rarr;{@link TlsPolicy} composition, the {@code sslProvider}&rarr;Netty
 * engine mapping, and the default {@link FileBasedTlsFactory} construction the v4 client's transport
 * uses on the new PIP-478 TLS path (PIP-478 stage 3b).
 *
 * <p><b>Path selection (opt-in, per decision D8).</b> The new PIP-478 path is active only when the v5
 * builder configured it — a {@code tlsPolicy(...)} map or an adopted {@code tlsFactory(...)} — or when
 * the flip experiment is switched on via the {@code pulsar.client.tls.useNewTlsFactory} system property.
 * A custom (non-default) {@code sslFactoryPlugin} always keeps the legacy PIP-337 path (unchanged; the
 * deprecation fail-loud is stage 4). Otherwise the default is legacy; the flip branch flips that default.
 *
 * <p><b>Fail-fast probing (per the R6 ruling).</b> Eager probing of the statically-needed client purpose
 * ({@link TlsPurpose#CLIENT_DEFAULT}) is a <em>v5-builder-path</em> behaviour only: it runs when the v5
 * builder configured TLS, so a bad path fails the client build with an actionable error. On the
 * flip-experiment path the v4 config stays lazily loaded, so unmodified v4 tests that lazily tolerated a
 * bad TLS field are not broken.
 */
public final class ClientTlsFactorySupport {

    /** System property that switches on the new PIP-478 TLS path for a plain v4 client (flip experiment). */
    public static final String USE_NEW_TLS_FACTORY_PROPERTY = "pulsar.client.tls.useNewTlsFactory";

    /** Reserved factory-class value selecting the built-in file-based factory (mirrors the server side). */
    private static final String DEFAULT_FACTORY = "default";

    private ClientTlsFactorySupport() {
    }

    /**
     * @param sslFactoryPlugin the configured {@code sslFactoryPlugin} class name
     * @return whether a custom (non-default) PIP-337 SSL factory plugin is configured
     */
    public static boolean isLegacyCustom(String sslFactoryPlugin) {
        return StringUtils.isNotBlank(sslFactoryPlugin)
                && !DefaultPulsarSslFactory.class.getName().equals(sslFactoryPlugin.trim());
    }

    /**
     * @param conf the client configuration
     * @return whether the new PIP-478 TLS path should be used (else the legacy PIP-337 path)
     */
    public static boolean useNewTlsPath(ClientConfigurationData conf) {
        // The v5 builder explicitly configured the new path.
        if (conf.getTlsFactory() != null || conf.getTlsPolicyMap() != null) {
            return true;
        }
        // A custom PIP-337 plugin keeps the legacy path (unchanged for stage 3).
        if (isLegacyCustom(conf.getSslFactoryPlugin())) {
            return false;
        }
        // Default: legacy. The flip experiment (and the eventual default flip) switches this.
        return Boolean.getBoolean(USE_NEW_TLS_FACTORY_PROPERTY);
    }

    /**
     * Map the v4 {@code sslProvider} string to the Netty engine provider. Only the literal OpenSSL
     * providers select {@code OPENSSL}; every other value (JCE provider names, {@code null}) is JDK.
     *
     * @param sslProvider the configured {@code sslProvider} string
     * @return the Netty {@link SslProvider}
     */
    public static SslProvider engineProvider(String sslProvider) {
        if (sslProvider != null) {
            String p = sslProvider.trim();
            if ("OPENSSL".equalsIgnoreCase(p) || "OPENSSL_REFCNT".equalsIgnoreCase(p)) {
                return SslProvider.OPENSSL;
            }
        }
        return SslProvider.JDK;
    }

    /**
     * Compose the {@link TlsPurpose#CLIENT_DEFAULT} {@link TlsPolicy} from the client's {@code tls*}
     * configuration fields. {@code sslProvider} is deliberately NOT part of the policy (it is a
     * factory-engine setting).
     *
     * @param conf the client configuration
     * @return the client-default TLS policy
     */
    public static TlsPolicy clientDefaultPolicy(ClientConfigurationData conf) {
        TlsPolicy.Builder b = TlsPolicy.builder()
                .allowInsecureConnection(conf.isTlsAllowInsecureConnection())
                .enableHostnameVerification(conf.isTlsHostnameVerificationEnable())
                .protocols(toList(conf.getTlsProtocols()))
                .ciphers(toList(conf.getTlsCiphers()));
        if (conf.isUseKeyStoreTls()) {
            // TlsPolicy carries a single store type; prefer the keystore type when a keystore is set,
            // otherwise fall back to the truststore type (they are usually identical).
            String storeType = conf.getTlsKeyStorePath() != null
                    ? conf.getTlsKeyStoreType() : conf.getTlsTrustStoreType();
            b.format(TlsPolicy.Format.KEYSTORE)
                    .trustStorePath(conf.getTlsTrustStorePath())
                    .trustStorePassword(conf.getTlsTrustStorePassword())
                    .keyStorePath(conf.getTlsKeyStorePath())
                    .keyStorePassword(conf.getTlsKeyStorePassword())
                    .storeType(storeType);
        } else {
            b.format(TlsPolicy.Format.PEM)
                    .trustCertsFilePath(conf.getTlsTrustCertsFilePath())
                    .certificateFilePath(conf.getTlsCertificateFilePath())
                    .keyFilePath(conf.getTlsKeyFilePath());
        }
        return b.build();
    }

    /**
     * Resolve the effective client TLS factory, initialized and (on the v5-builder path) probed. Returns
     * {@code null} on the legacy path, in which case the caller keeps the PIP-337 {@code PulsarSslFactory}.
     *
     * @param conf             the client configuration
     * @param scheduler        the framework scheduler (for material rotation polling)
     * @param blockingExecutor the framework executor for blocking material loading
     * @param openTelemetry    the telemetry root
     * @return the initialized factory, or {@code null} for the legacy path
     * @throws Exception if the factory cannot be built / initialized, or (v5-builder path) the fail-fast
     *         probe of {@link TlsPurpose#CLIENT_DEFAULT} fails
     */
    public static PulsarTlsFactory resolveClientTlsFactory(ClientConfigurationData conf,
            ScheduledExecutorService scheduler, Executor blockingExecutor, OpenTelemetry openTelemetry)
            throws Exception {
        boolean v5BuilderPath = conf.getTlsFactory() != null || conf.getTlsPolicyMap() != null;
        PulsarTlsFactory factory;
        if (conf.getTlsFactory() != null) {
            // The v5 builder adopted a custom factory; the client owns and closes it.
            factory = conf.getTlsFactory();
        } else {
            if (!useNewTlsPath(conf)) {
                return null;
            }
            factory = new FileBasedTlsFactory(composePolicies(conf), settings(conf));
        }
        initializeBlocking(factory, initContext(scheduler, blockingExecutor, openTelemetry));
        // Fail-fast probe only on the v5-builder path (R6 ruling).
        if (v5BuilderPath) {
            probe(factory, TlsPurpose.CLIENT_DEFAULT);
        }
        return factory;
    }

    /**
     * Build the (uninitialized) broker-client {@link PulsarTlsFactory} for a server component's own outbound
     * Pulsar client — geo-replication and cluster-internal lookup connections (the {@code BROKER_CLIENT}
     * purpose, PIP-478 stage 4a). The broker-client material is already mapped onto the config's
     * {@code tls*} fields (per-cluster {@code ClusterData.brokerClientTls*} first, else the broker's
     * {@code brokerClient*} {@code ServiceConfiguration}), so this composes {@link TlsPurpose#CLIENT_DEFAULT}
     * — the purpose the outbound transport requests — from those fields, additionally folding the
     * component's broker-client {@code Authentication} TLS material via the 3c
     * {@link FileBasedTlsFactory#authMaterialSupplier authMaterialSupplier} (auth-cert-wins) so an
     * {@code AuthenticationTls} broker-client identity reaches the transport on the new path.
     *
     * <p>The caller stores the result on {@code conf} via {@code setTlsFactory}; the owning
     * {@code PulsarClientImpl} adopts, initializes (with its shared scheduler / executor / OpenTelemetry) and
     * closes it — one factory per outbound client, so per-cluster material is served without minting
     * per-cluster purposes. A non-default {@code factoryClassName} names a custom {@link PulsarTlsFactory},
     * instantiated reflectively (it self-sources material; the config/auth composition below does not apply,
     * and {@code brokerClientTlsFactoryConfig} params are not yet plumbed to the client init context).
     *
     * @param conf            the outbound client configuration (its {@code tls*} fields hold the broker-client
     *                        material; {@code getAuthentication()} the broker-client authentication)
     * @param factoryClassName the {@code brokerClientTlsFactoryClassName} value ({@code default}/blank selects
     *                        the built-in file-based factory)
     * @return an uninitialized {@link PulsarTlsFactory}
     */
    public static PulsarTlsFactory brokerClientTlsFactory(ClientConfigurationData conf, String factoryClassName) {
        String className = factoryClassName == null ? "" : factoryClassName.trim();
        if (!className.isEmpty()
                && !DEFAULT_FACTORY.equalsIgnoreCase(className)
                && !FileBasedTlsFactory.class.getName().equals(className)) {
            try {
                return (PulsarTlsFactory) Class.forName(className).getConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException(
                        "Could not instantiate brokerClientTlsFactoryClassName '" + className + "'", e);
            }
        }
        TlsPolicy policy = clientDefaultPolicy(conf);
        Map<TlsPurpose, Supplier<AuthenticationDataProvider>> authSuppliers = Map.of();
        Authentication auth = conf.getAuthentication();
        if (auth != null && authHasTlsMaterial(auth)) {
            authSuppliers = Map.of(TlsPurpose.CLIENT_DEFAULT, FileBasedTlsFactory.authMaterialSupplier(auth));
        }
        return new FileBasedTlsFactory(Map.of(TlsPurpose.CLIENT_DEFAULT, policy), settings(conf), authSuppliers);
    }

    /**
     * Probe whether an authentication plugin carries TLS key material, so the broker-client fold registers
     * an {@code authMaterialSupplier} only for a genuine TLS-auth plugin (e.g. {@code AuthenticationTls}) and
     * not for a token/none plugin whose per-poll {@code getAuthData()} would be wasted work. Runs once at
     * factory-build time on the caller thread (broker startup / replication client creation), never on an
     * event loop.
     *
     * @param auth the broker-client authentication (never {@code null})
     * @return whether it exposes TLS material
     */
    @SuppressWarnings("deprecation")
    private static boolean authHasTlsMaterial(Authentication auth) {
        try {
            AuthenticationDataProvider data = auth.getAuthData();
            return data != null && data.hasDataForTls();
        } catch (Exception e) {
            return false;
        }
    }

    private static Map<TlsPurpose, TlsPolicy> composePolicies(ClientConfigurationData conf) {
        Map<TlsPurpose, TlsPolicy> policies = new LinkedHashMap<>();
        if (conf.getTlsPolicyMap() != null) {
            policies.putAll(conf.getTlsPolicyMap());
        }
        // Always ensure CLIENT_DEFAULT resolves — derive it from the conf fields if the v5 builder did
        // not supply one explicitly.
        policies.putIfAbsent(TlsPurpose.CLIENT_DEFAULT, clientDefaultPolicy(conf));
        return policies;
    }

    private static FileBasedTlsFactorySettings settings(ClientConfigurationData conf) {
        int refresh = conf.getAutoCertRefreshSeconds();
        return FileBasedTlsFactorySettings.builder()
                .engineProvider(engineProvider(conf.getSslProvider()))
                // Client purposes never build server contexts, so requireTrustedClientCert is irrelevant.
                .refreshIntervalSeconds(refresh)
                .build();
    }

    /**
     * Fail-fast probe: build one instance of the purpose and dispose it, surfacing a configuration error
     * (e.g. a missing cert file) as an actionable {@link IllegalArgumentException}.
     *
     * @param factory the initialized factory
     * @param purpose the purpose to probe
     */
    public static void probe(PulsarTlsFactory factory, TlsPurpose purpose) {
        try {
            Optional<TlsHandle<SslContext>> handle = factory.createInstance(purpose, SslContext.class).get();
            if (handle.isEmpty()) {
                throw new IllegalStateException("Client TLS factory " + factory.getClass().getName()
                        + " supplied no Netty SslContext for purpose " + purpose);
            }
            handle.get().dispose();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while probing the client TLS factory", e);
        } catch (ExecutionException | CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalArgumentException("Client TLS configuration is invalid for purpose " + purpose
                    + ": " + cause.getMessage(), cause);
        }
    }

    private static void initializeBlocking(PulsarTlsFactory factory, TlsFactoryInitContext context)
            throws Exception {
        try {
            factory.initialize(context).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw new RuntimeException(cause);
        }
    }

    private static TlsFactoryInitContext initContext(ScheduledExecutorService scheduler,
            Executor blockingExecutor, OpenTelemetry openTelemetry) {
        OpenTelemetry ot = openTelemetry == null ? OpenTelemetry.noop() : openTelemetry;
        return new TlsFactoryInitContext() {
            @Override
            public Map<String, String> params() {
                return Map.of();
            }

            @Override
            public ScheduledExecutorService scheduler() {
                return scheduler;
            }

            @Override
            public Executor blockingExecutor() {
                return blockingExecutor;
            }

            @Override
            public Clock clock() {
                return Clock.systemUTC();
            }

            @Override
            public OpenTelemetry openTelemetry() {
                return ot;
            }
        };
    }

    private static List<String> toList(Set<String> values) {
        return values == null ? List.of() : new ArrayList<>(values);
    }
}
