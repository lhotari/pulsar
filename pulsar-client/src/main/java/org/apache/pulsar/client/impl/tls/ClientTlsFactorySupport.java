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
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsFactoryInitContext;
import org.apache.pulsar.common.tls.TlsHandle;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.common.tls.impl.TlsContextAcquisition;
import org.apache.pulsar.common.tls.impl.TlsSynthesisSpec;

/**
 * Client-side counterpart of the server's {@code TlsFactorySupport} (which lives in
 * {@code pulsar-broker-common}, off the client classpath): the
 * {@code ClientConfigurationData}&rarr;{@link TlsPolicy} composition, the {@code sslProvider}&rarr;Netty
 * engine mapping, and the default {@link FileBasedTlsFactory} construction the v4 client's transport uses.
 * Since the PIP-337 {@code PulsarSslFactory} removal this is the only client TLS path: a TLS
 * client always builds a {@link PulsarTlsFactory} (an adopted v5 {@code tlsFactory(...)} or the built-in
 * file-based default composed from the {@code tls*} fields).
 *
 * <p><b>Fail-fast probing (per the R6 ruling).</b> Eager probing of the statically-needed client purpose
 * ({@link TlsPurpose#CLIENT_DEFAULT}) is a <em>v5-builder-path</em> behaviour only: it runs when the v5
 * builder configured TLS, so a bad path fails the client build with an actionable error. A plain v4 client's
 * config stays lazily loaded, so unmodified v4 tests that lazily tolerated a bad TLS field are not broken.
 */
public final class ClientTlsFactorySupport {

    /** Reserved factory-class value selecting the built-in file-based factory (mirrors the server side). */
    private static final String DEFAULT_FACTORY = "default";

    /**
     * FQCN of the removed PIP-337 default SSL factory. Matched as a string literal (the class itself is
     * removed in PIP-478) so a blank {@code sslFactoryPlugin} value OR this literal is treated as
     * "the default" — any other non-blank value names a custom PIP-337 plugin.
     */
    static final String REMOVED_DEFAULT_SSL_FACTORY_CLASS_NAME =
            "org.apache.pulsar.common.util.DefaultPulsarSslFactory";

    private ClientTlsFactorySupport() {
    }

    /**
     * Whether a {@code sslFactoryPlugin} value names a removed PIP-337 custom factory — a non-blank value
     * that is not the removed default's FQCN. The PIP-337 path no longer exists, so
     * {@code ClientBuilder.build()} fails loudly when this returns {@code true}.
     *
     * @param sslFactoryPlugin the configured {@code sslFactoryPlugin} class name
     * @return whether it names a (removed) custom PIP-337 SSL factory plugin
     */
    public static boolean isLegacyCustom(String sslFactoryPlugin) {
        return StringUtils.isNotBlank(sslFactoryPlugin)
                && !REMOVED_DEFAULT_SSL_FACTORY_CLASS_NAME.equals(sslFactoryPlugin.trim());
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
            // Map the keystore and truststore types independently (v4 parity): a mixed setup such as a PKCS12
            // keystore with a JKS truststore must load each store with its own type.
            b.format(TlsPolicy.Format.KEYSTORE)
                    .trustStorePath(conf.getTlsTrustStorePath())
                    .trustStorePassword(conf.getTlsTrustStorePassword())
                    .keyStorePath(conf.getTlsKeyStorePath())
                    .keyStorePassword(conf.getTlsKeyStorePassword())
                    .keyStoreType(conf.getTlsKeyStoreType())
                    .trustStoreType(conf.getTlsTrustStoreType());
        } else {
            b.format(TlsPolicy.Format.PEM)
                    .trustCertsFilePath(conf.getTlsTrustCertsFilePath())
                    .certificateFilePath(conf.getTlsCertificateFilePath())
                    .keyFilePath(conf.getTlsKeyFilePath());
        }
        return b.build();
    }

    /**
     * Resolve the effective client TLS factory, initialized and (on the v5-builder path) probed. Since the
     * PIP-337 removal this always returns a non-null factory: an adopted v5 {@code tlsFactory}, or
     * the built-in file-based factory composed from the {@code tls*} fields.
     *
     * @param conf             the client configuration
     * @param scheduler        the framework scheduler (for material rotation polling)
     * @param blockingExecutor the framework executor for blocking material loading
     * @param openTelemetry    the telemetry root
     * @return the initialized factory (never {@code null})
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
            // Fold the auth plugin's TLS identity (AuthenticationTls / AuthenticationKeyStoreTls) into
            // CLIENT_DEFAULT (auth-cert-wins), so a plugin-supplied client certificate reaches the transport
            // now that this is the only client TLS path (PIP-478). The supplier is registered
            // unconditionally and evaluated lazily at context-build time (per connection, off the event loop),
            // so client construction never eagerly calls getAuthData() — no eager OAuth2 token fetch.
            factory = new FileBasedTlsFactory(composePolicies(conf), settings(conf),
                    clientDefaultAuthMaterialSuppliers(conf));
        }
        initializeBlocking(factory, initContext(conf, scheduler, blockingExecutor, openTelemetry));
        // Fail-fast probe only on the v5-builder path (R6 ruling).
        if (v5BuilderPath) {
            probe(factory, TlsPurpose.CLIENT_DEFAULT,
                    TlsSynthesisSpec.client(conf.isTlsHostnameVerificationEnable()));
        }
        return factory;
    }

    /**
     * Build the (uninitialized) broker-client {@link PulsarTlsFactory} for a server component's own outbound
     * Pulsar client — geo-replication and cluster-internal lookup connections (the {@code BROKER_CLIENT}
     * purpose, PIP-478). The broker-client material is already mapped onto the config's
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
        // Broker-client (server startup / replication client creation): register the broker-client TLS fold
        // when the plugin carries TLS material.
        TlsPolicy policy = clientDefaultPolicy(conf);
        Map<TlsPurpose, Supplier<AuthenticationDataProvider>> authSuppliers = Map.of();
        Authentication auth = conf.getAuthentication();
        if (auth != null && shouldFoldBrokerClientAuthTls(auth)) {
            authSuppliers = Map.of(TlsPurpose.CLIENT_DEFAULT, FileBasedTlsFactory.authMaterialSupplier(auth));
        }
        return new FileBasedTlsFactory(Map.of(TlsPurpose.CLIENT_DEFAULT, policy), settings(conf), authSuppliers);
    }

    /**
     * Whether to register the broker-client {@code BROKER_CLIENT} TLS fold for this plugin (L3/S-F3). The
     * built-in TLS-auth plugins are matched by TYPE ({@code AuthenticationTls} / {@code AuthenticationKeyStoreTls})
     * so a transient read failure at factory build — the key file briefly missing while it is being rotated —
     * does not permanently skip the fold and disable rotation; {@code AuthProvidedMaterialSource} keeps the
     * last-good material and retries per poll. For an unknown custom plugin fall back to the material probe,
     * preserving support for third-party plugins that supply TLS material without extending the built-in types.
     */
    private static boolean shouldFoldBrokerClientAuthTls(Authentication auth) {
        return isTlsAuthPlugin(auth) || authHasTlsMaterial(auth);
    }

    /**
     * The auth-cert-wins {@code authMaterialSupplier} fold for the application/admin client's
     * {@link TlsPurpose#CLIENT_DEFAULT}: a plugin-supplied client identity (e.g. {@code AuthenticationTls} /
     * {@code AuthenticationKeyStoreTls}) reaches the transport on the new path. Unlike the broker-client
     * path there is no eager probe — the supplier is evaluated lazily at context build/reload, so client
     * construction never calls {@code getAuthData()}. The fold is registered when the identity must come from
     * the plugin (no {@code tls*} key material configured) or when a TLS-auth plugin is present alongside
     * {@code tls*} files (auth-cert-wins); a non-TLS plugin is not consulted when {@code tls*} files supply
     * the identity, so its {@code getAuthData()} stays untouched. {@code AuthProvidedMaterialSource} applies
     * auth-cert-wins only when the plugin actually carries TLS material.
     */
    private static Map<TlsPurpose, Supplier<AuthenticationDataProvider>> clientDefaultAuthMaterialSuppliers(
            ClientConfigurationData conf) {
        Authentication auth = conf.getAuthentication();
        if (auth == null) {
            return Map.of();
        }
        if (hasConfiguredClientKeyMaterial(conf) && !isTlsAuthPlugin(auth)) {
            // The client already has its own tls* cert/key (or keystore) identity, and the auth plugin is not
            // a TLS-auth plugin, so it is not consulted for TLS material — its getAuthData() is not called
            // (e.g. a non-TLS plugin, per TlsProducerConsumerTest.testTlsWithFakeAuthentication, which asserts
            // getAuthData() is never called). A genuine TLS-auth plugin, however, still overrides the tls*
            // files (auth-cert-wins, the removed PIP-337 behavior) — see below.
            return Map.of();
        }
        // Register the fold when the identity must come from the auth plugin (no tls* key material) OR when a
        // TLS-auth plugin (AuthenticationTls / AuthenticationKeyStoreTls) is present alongside tls* files: the
        // plugin certificate wins over the tls* files (auth-cert-wins), restoring the removed PIP-337
        // precedence so a broker/proxy whose broker-client identity comes from an AuthenticationTls plugin
        // presents that certificate rather than its configured brokerClient* cert files (issue: the internal
        // broker-client otherwise presented a server-only-EKU cert and was rejected for TLS client auth).
        // The supplier is evaluated lazily at CLIENT_DEFAULT build/reload (off the event loop), so client
        // construction never eagerly calls getAuthData().
        Supplier<AuthenticationDataProvider> supplier = FileBasedTlsFactory.authMaterialSupplier(auth);
        if (isTlsAuthPlugin(auth)) {
            // A TLS-auth plugin owns the client identity, so register the supplier directly (no swallow): a
            // transient read failure — e.g. the key file briefly missing while it is being rotated, per
            // AdminApiTlsAuthTest.testCertRefreshForPulsarAdmin — must propagate so AuthProvidedMaterialSource
            // keeps the last-good certificate. Swallowing it to null would instead fold nothing and downgrade
            // the identity to "no client certificate", which strands a pooled admin/HTTP connection that
            // completed a handshake presenting no cert (rejected 401 and, being non-5xx, kept alive and
            // reused) so the rotated certificate would never take effect.
            return Map.of(TlsPurpose.CLIENT_DEFAULT, supplier);
        }
        // A non-TLS plugin (token / OAuth2) has no client certificate to fold; when it is registered here it is
        // because no tls* key material is configured. Swallow a lookup failure to null so client build does not
        // fail (e.g. an OAuth2 plugin whose getAuthData() acquires a token and fails when the plugin is not
        // bound to a client); AuthProvidedMaterialSource then keeps the empty base material.
        return Map.of(TlsPurpose.CLIENT_DEFAULT, () -> {
            try {
                return supplier.get();
            } catch (RuntimeException e) {
                return null;
            }
        });
    }

    /**
     * Whether an authentication plugin is one of the built-in TLS-auth plugins that carry a client
     * certificate identity ({@link AuthenticationTls} / {@link AuthenticationKeyStoreTls}). Such a plugin's
     * certificate overrides the configured {@code tls*} key material (auth-cert-wins); any other plugin is
     * consulted for TLS material only when no {@code tls*} key material is configured, so its
     * {@code getAuthData()} is not called when the client already has its own certificate files.
     */
    private static boolean isTlsAuthPlugin(Authentication auth) {
        return auth instanceof AuthenticationTls || auth instanceof AuthenticationKeyStoreTls;
    }

    private static boolean hasConfiguredClientKeyMaterial(ClientConfigurationData conf) {
        if (conf.isUseKeyStoreTls()) {
            return StringUtils.isNotBlank(conf.getTlsKeyStorePath());
        }
        return StringUtils.isNotBlank(conf.getTlsCertificateFilePath())
                && StringUtils.isNotBlank(conf.getTlsKeyFilePath());
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
        // PIP-478: fold an OAuth2 plugin's own IdP TLS material (trustCerts / cert / key params,
        // issue #24944) into CLIENT_OAUTH2, so the framework HTTP client serves IdP mTLS / custom trust on
        // the new path instead of the deprecated private client. Only when the plugin carries IdP TLS
        // material; otherwise CLIENT_OAUTH2 keeps resolving to the system default (its empty fallback). The
        // plugin is reachable here because conf.getAuthentication() is set before this runs (v5 build /
        // client construction).
        foldOAuth2IdpPolicy(conf, policies);
        return policies;
    }

    private static void foldOAuth2IdpPolicy(ClientConfigurationData conf, Map<TlsPurpose, TlsPolicy> policies) {
        if (conf.getAuthentication() instanceof AuthenticationOAuth2 oauth2) {
            oauth2.idpTlsPolicy().ifPresent(policy -> policies.putIfAbsent(TlsPurpose.CLIENT_OAUTH2, policy));
        }
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
     * (e.g. a missing cert file) as an actionable {@link IllegalArgumentException}. Acquisition goes through
     * {@link TlsContextAcquisition}, so a custom factory that supplies only the JDK {@code SSLContext}
     * fallback probes successfully via the framework-synthesized Netty context.
     *
     * @param factory   the initialized factory
     * @param purpose   the purpose to probe
     * @param synthesis the settings baked into a synthesized Netty context on the fallback path
     */
    public static void probe(PulsarTlsFactory factory, TlsPurpose purpose, TlsSynthesisSpec synthesis) {
        try {
            Optional<TlsHandle<SslContext>> handle =
                    TlsContextAcquisition.acquireNettyContext(factory, purpose, synthesis).get();
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

    private static TlsFactoryInitContext initContext(ClientConfigurationData conf,
            ScheduledExecutorService scheduler, Executor blockingExecutor, OpenTelemetry openTelemetry) {
        OpenTelemetry ot = openTelemetry == null ? OpenTelemetry.noop() : openTelemetry;
        // PIP-478: deliver the broker-supplied factory params (from brokerClientTlsFactoryConfig) to
        // a custom factory; empty for the default file-based factory, which ignores them.
        Map<String, String> params = conf.getTlsFactoryParams() == null
                ? Map.of() : Map.copyOf(conf.getTlsFactoryParams());
        return new TlsFactoryInitContext() {
            @Override
            public Map<String, String> params() {
                return params;
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
