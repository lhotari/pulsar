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
package org.apache.pulsar.common.tls.impl;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.security.cert.Certificate;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import lombok.CustomLog;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.util.SecurityUtility;

/**
 * Builds the well-known TLS instance classes from loaded {@link TlsMaterial} and a {@link TlsPolicy}
 * for the default {@code FileBasedTlsFactory}, and synthesizes the richer objects from a JDK
 * {@code SSLContext} for the framework fallback (PIP-478).
 *
 * <p>Reuses {@link SecurityUtility} for the JDK {@code SSLContext} build. The Netty contexts are
 * assembled here (rather than through {@code SecurityUtility}'s file-path helpers) because two
 * per-context settings must be baked at build time from in-memory material:
 * <ul>
 *   <li><b>Client hostname verification</b> — set via
 *       {@link SslContextBuilder#endpointIdentificationAlgorithm(String)} to {@code "HTTPS"} when the
 *       policy enables it. It cannot be left to a per-{@code SSLEngine} override: Netty's OpenSSL
 *       backend fixes the endpoint-identification algorithm at build time.</li>
 *   <li><b>Server insecure trust (decision D3)</b> — {@code policy.allowInsecureConnection()} on a
 *       server context installs {@link InsecureTrustManagerFactory#INSTANCE} so an untrusted client
 *       certificate still completes the handshake and remains available for TLS authentication, while
 *       client-auth stays {@link ClientAuth#REQUIRE}/{@link ClientAuth#OPTIONAL} (never
 *       {@link ClientAuth#NONE}, per {@code tlsRequireTrustedClientCert} semantics).</li>
 * </ul>
 */
@CustomLog
public final class TlsContexts {

    // WARN-once dedup for the insecure (trust-all) mode, keyed by policy value so a rotation's context rebuilds
    // (and both the Netty and JDK builds of the same policy) log at most once (S-F1).
    private static final Set<TlsPolicy> INSECURE_WARNED = ConcurrentHashMap.newKeySet();

    private TlsContexts() {
    }

    /**
     * Log a one-time WARN when an insecure (trust-all) TLS policy is applied at context build (S-F1): peer
     * certificates are not validated. Deduplicated per policy value so rotation rebuilds do not spam the log.
     */
    private static void warnInsecureModeOnce(TlsPolicy policy) {
        if (policy.allowInsecureConnection() && INSECURE_WARNED.add(policy)) {
            log.warn().log("TLS insecure mode is enabled (allowInsecureConnection=true): peer certificates are "
                    + "NOT validated (trust-all). This disables authentication of the remote peer and must be "
                    + "used only for testing.");
        }
    }

    /**
     * Build a client Netty {@link SslContext}, baking hostname verification when the policy enables it.
     *
     * @param material the loaded client material
     * @param policy   the policy (flags, protocols, ciphers)
     * @param provider the Netty SSL provider (engine selection)
     * @return the built client context
     * @throws Exception if the context cannot be built
     */
    static SslContext buildNettyClientContext(TlsMaterial material, TlsPolicy policy, SslProvider provider)
            throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient().sslProvider(provider);
        applyClientTrust(builder, material, policy);
        if (material.hasKeyMaterial()) {
            builder.keyManager(material.privateKey(), material.keyCertChainArray());
        }
        applyCiphersAndProtocols(builder, policy);
        if (policy.enableHostnameVerification()) {
            builder.endpointIdentificationAlgorithm("HTTPS");
        }
        return builder.build();
    }

    /**
     * Build a server Netty {@link SslContext} honoring the D3 insecure rule.
     *
     * @param material                 the loaded server material
     * @param policy                   the policy (flags, protocols, ciphers)
     * @param provider                 the Netty SSL provider (engine selection)
     * @param requireTrustedClientCert whether to require (vs. merely request) a trusted client cert
     * @return the built server context
     * @throws Exception if the context cannot be built
     */
    static SslContext buildNettyServerContext(TlsMaterial material, TlsPolicy policy, SslProvider provider,
                                              boolean requireTrustedClientCert) throws Exception {
        SslContextBuilder builder = SslContextBuilder.forServer(material.privateKey(), material.keyCertChainArray())
                .sslProvider(provider);
        applyServerTrust(builder, material, policy);
        applyCiphersAndProtocols(builder, policy);
        // Never drop to ClientAuth.NONE, even when insecure: a captured client cert powers TLS auth.
        builder.clientAuth(requireTrustedClientCert ? ClientAuth.REQUIRE : ClientAuth.OPTIONAL);
        return builder.build();
    }

    /**
     * Build a JDK {@link SSLContext} for the material (the universal fallback and non-Netty consumers).
     *
     * @param material the loaded material
     * @param policy   the policy (only the insecure flag affects a JDK context)
     * @return the built JDK context
     * @throws Exception if the context cannot be built
     */
    static SSLContext buildJdkContext(TlsMaterial material, TlsPolicy policy) throws Exception {
        warnInsecureModeOnce(policy);
        Certificate[] keyCertChain = material.keyCertChainArray();
        return SecurityUtility.createSslContext(policy.allowInsecureConnection(), material.trustCertsArray(),
                keyCertChain, material.privateKey());
    }

    /**
     * Wrap a JDK {@link SSLContext} as a Netty {@link SslContext} via {@link JdkSslContext}. Used by the
     * framework when a custom factory returns {@code empty()} for the Netty class but supplies the JDK
     * {@code SSLContext} fallback.
     *
     * @param sslContext               the JDK context to wrap
     * @param isClient                 whether the wrapped context is for client-mode engines
     * @param requireTrustedClientCert server-side client-auth requirement (ignored when {@code isClient})
     * @return a Netty context backed by the JDK context
     */
    public static SslContext synthesizeNettyFromJdk(SSLContext sslContext, boolean isClient,
                                                    boolean requireTrustedClientCert) {
        ClientAuth clientAuth = isClient ? ClientAuth.NONE
                : (requireTrustedClientCert ? ClientAuth.REQUIRE : ClientAuth.OPTIONAL);
        return new JdkSslContext(sslContext, isClient, null, IdentityCipherSuiteFilter.INSTANCE,
                ApplicationProtocolConfig.DISABLED, clientAuth, null, false);
    }

    /**
     * Synthesize a <em>client</em> Netty {@link SslContext} from a JDK {@link SSLContext}, applying the
     * factory's engine-policy {@code SSLParameters} companion (when present) and the consumer's
     * hostname-verification flag to the produced engines. Used by the framework when a custom factory returns
     * {@code empty()} for the Netty class on a client purpose but supplies the JDK {@code SSLContext} fallback.
     *
     * <p>Engine-level policy cannot be encoded in the JDK {@code SSLContext} itself — the enabled protocols
     * and cipher suites, algorithm constraints, application protocols, and the endpoint-identification
     * algorithm are all {@link SSLParameters} settings — so the composed baseline is applied per engine by a
     * {@link SynthesizedEngineSslContext} wrapper. This mirrors the native path, where
     * {@link #buildNettyClientContext} bakes the same settings into the context via {@code SslContextBuilder};
     * consumers rely on the context to carry the policy and never re-apply it per connection.
     *
     * <p>Merge order (PIP-478): the factory {@code SSLParameters} form the baseline (non-null members only);
     * for {@code endpointIdentificationAlgorithm} the factory's value wins when set, otherwise
     * {@code enableHostnameVerification} applies {@code "HTTPS"}; SNI {@code serverNames} are never taken from
     * the factory (the per-connection SNI wins). When the factory supplied no companion and hostname
     * verification is disabled, the bare JDK-backed context is returned unwrapped (nothing to overlay).
     *
     * @param sslContext                 the JDK context to wrap
     * @param enableHostnameVerification whether the client policy enables hostname verification
     * @param factoryBaseline            the factory's engine-policy companion, or {@code null} if none supplied
     * @return a Netty client context backed by the JDK context, carrying the composed engine policy
     */
    public static SslContext synthesizeNettyClientFromJdk(SSLContext sslContext,
                                                          boolean enableHostnameVerification,
                                                          SSLParameters factoryBaseline) {
        SslContext clientContext = synthesizeNettyFromJdk(sslContext, true, false);
        SSLParameters overlay = composeClientOverlay(factoryBaseline, enableHostnameVerification);
        return overlay == null ? clientContext : new SynthesizedEngineSslContext(clientContext, overlay, false);
    }

    /**
     * Synthesize a <em>server</em> Netty {@link SslContext} from a JDK {@link SSLContext}, applying the
     * factory's engine-policy {@code SSLParameters} companion (when present) to the produced engines. Used by
     * the framework when a custom factory returns {@code empty()} for the Netty class on a server purpose but
     * supplies the JDK {@code SSLContext} fallback.
     *
     * <p>When the factory supplies no companion, the bare JDK-backed context is returned with the consumer's
     * {@code requireTrustedClientCert} mapped as usual ({@link ClientAuth#REQUIRE}/{@link ClientAuth#OPTIONAL}).
     * When it does, its non-null baseline members (protocols/ciphers/algorithm-constraints/application-
     * protocols) are overlaid per engine and — merge rule 4 — its {@code needClientAuth}/{@code wantClientAuth}
     * are authoritative for the client-auth mode.
     *
     * @param sslContext               the JDK context to wrap
     * @param requireTrustedClientCert the consumer's client-auth requirement (used when no companion supplied)
     * @param factoryBaseline          the factory's engine-policy companion, or {@code null} if none supplied
     * @return a Netty server context backed by the JDK context, carrying the composed engine policy
     */
    public static SslContext synthesizeNettyServerFromJdk(SSLContext sslContext,
                                                          boolean requireTrustedClientCert,
                                                          SSLParameters factoryBaseline) {
        SslContext serverContext = synthesizeNettyFromJdk(sslContext, false, requireTrustedClientCert);
        if (factoryBaseline == null) {
            // No factory engine policy: the base context already carries the consumer's client-auth mode.
            return serverContext;
        }
        return new SynthesizedEngineSslContext(serverContext, copyBaselineMembers(factoryBaseline), true);
    }

    /**
     * Compose the per-engine overlay for a client purpose, or {@code null} when there is nothing to apply
     * (no factory companion and hostname verification disabled — the base context is used verbatim).
     */
    private static SSLParameters composeClientOverlay(SSLParameters factoryBaseline,
                                                      boolean enableHostnameVerification) {
        if (factoryBaseline == null) {
            if (!enableHostnameVerification) {
                return null;
            }
            SSLParameters overlay = new SSLParameters();
            overlay.setEndpointIdentificationAlgorithm("HTTPS");
            return overlay;
        }
        SSLParameters overlay = copyBaselineMembers(factoryBaseline);
        // Merge rule 2: the factory's endpointIdentificationAlgorithm wins when set; otherwise the consumer's
        // hostname-verification flag applies "HTTPS".
        if (overlay.getEndpointIdentificationAlgorithm() == null && enableHostnameVerification) {
            overlay.setEndpointIdentificationAlgorithm("HTTPS");
        }
        return overlay;
    }

    /**
     * Take the framework's single defensive snapshot of a factory-supplied {@link SSLParameters} — a mutable
     * object — copying only the engine-baseline members the framework overlays per engine. SNI
     * {@code serverNames} are deliberately excluded (merge rule 3: the per-connection SNI always wins). The
     * returned object is owned by the framework and never mutated after composition, so a later mutation of
     * the factory's original object cannot affect an already-acquired context.
     */
    private static SSLParameters copyBaselineMembers(SSLParameters source) {
        SSLParameters overlay = new SSLParameters();
        if (source.getProtocols() != null) {
            overlay.setProtocols(source.getProtocols().clone());
        }
        if (source.getCipherSuites() != null) {
            overlay.setCipherSuites(source.getCipherSuites().clone());
        }
        if (source.getAlgorithmConstraints() != null) {
            overlay.setAlgorithmConstraints(source.getAlgorithmConstraints());
        }
        if (source.getApplicationProtocols() != null) {
            overlay.setApplicationProtocols(source.getApplicationProtocols().clone());
        }
        if (source.getEndpointIdentificationAlgorithm() != null) {
            overlay.setEndpointIdentificationAlgorithm(source.getEndpointIdentificationAlgorithm());
        }
        // Client-auth mode is carried on the overlay for server purposes (applied by
        // SynthesizedEngineSslContext when applyClientAuth is set); needClientAuth wins over wantClientAuth.
        if (source.getNeedClientAuth()) {
            overlay.setNeedClientAuth(true);
        } else if (source.getWantClientAuth()) {
            overlay.setWantClientAuth(true);
        }
        return overlay;
    }

    private static void applyClientTrust(SslContextBuilder builder, TlsMaterial material, TlsPolicy policy) {
        if (policy.allowInsecureConnection()) {
            warnInsecureModeOnce(policy);
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        } else if (material.trustCertsArray().length > 0) {
            builder.trustManager(material.trustCertsArray());
        }
        // else: leave the platform default trust manager (system trust store).
    }

    private static void applyServerTrust(SslContextBuilder builder, TlsMaterial material, TlsPolicy policy) {
        if (policy.allowInsecureConnection()) {
            warnInsecureModeOnce(policy);
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        } else if (material.trustCertsArray().length > 0) {
            builder.trustManager(material.trustCertsArray());
        }
        // else: leave the platform default trust manager (system trust store).
    }

    private static void applyCiphersAndProtocols(SslContextBuilder builder, TlsPolicy policy) {
        Set<String> ciphers = toSet(policy.ciphers());
        Set<String> protocols = toSet(policy.protocols());
        if (ciphers != null) {
            builder.ciphers(ciphers);
        }
        if (protocols != null) {
            builder.protocols(protocols.toArray(new String[0]));
        }
    }

    private static Set<String> toSet(List<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return new LinkedHashSet<>(values);
    }
}
