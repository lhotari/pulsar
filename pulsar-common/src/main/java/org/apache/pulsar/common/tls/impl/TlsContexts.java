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
import javax.net.ssl.SSLContext;
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
public final class TlsContexts {

    private TlsContexts() {
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

    private static void applyClientTrust(SslContextBuilder builder, TlsMaterial material, TlsPolicy policy) {
        if (policy.allowInsecureConnection()) {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        } else if (material.trustCertsArray().length > 0) {
            builder.trustManager(material.trustCertsArray());
        }
        // else: leave the platform default trust manager (system trust store).
    }

    private static void applyServerTrust(SslContextBuilder builder, TlsMaterial material, TlsPolicy policy) {
        if (policy.allowInsecureConnection()) {
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
