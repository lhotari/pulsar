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
package org.apache.pulsar.common.tls;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import lombok.CustomLog;
import org.apache.pulsar.common.util.SecurityUtility;

/**
 * The framework "engine" that turns {@link TlsMaterial} provided by a
 * {@link PulsarTlsMaterialProvider} into ready-to-use {@code SSLEngine} / {@code SSLContext}
 * objects (PIP-478, Stage 4).
 *
 * <p>Derived contexts are cached per {@link TlsPurposeContext}. The cache entry records the
 * {@link TlsMaterial} it was built from, and is rebuilt only when the material it was derived from no
 * longer {@link Object#equals(Object) equals} the current material returned by the provider — which
 * is exactly the contract the {@link TlsMaterial} implementations honour (same instance when nothing
 * changed, a new non-equal instance on rotation).
 *
 * <p>All public methods return a {@link CompletableFuture} because resolving material through the
 * provider is asynchronous; once the material is available the context construction itself is
 * synchronous.
 */
@CustomLog
public class PulsarTlsEngineProvider {

    /** A cached Netty {@link SslContext} together with the {@link TlsMaterial} it was built from. */
    private record CachedNettyContext(TlsMaterial material, SslContext context) {
    }

    /** A cached JDK {@link SSLContext} together with the {@link TlsMaterial} it was built from. */
    private record CachedJdkContext(TlsMaterial material, SSLContext context) {
    }

    private final PulsarTlsMaterialProvider materialProvider;

    private final ConcurrentHashMap<TlsPurposeContext, CachedNettyContext> nettyCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TlsPurposeContext, CachedJdkContext> jdkCache = new ConcurrentHashMap<>();

    /**
     * Create an engine provider backed by the supplied material provider.
     *
     * @param materialProvider the source of TLS material
     */
    public PulsarTlsEngineProvider(PulsarTlsMaterialProvider materialProvider) {
        this.materialProvider = Objects.requireNonNull(materialProvider, "materialProvider");
    }

    /**
     * Build a client-side {@link SSLEngine} for the given purpose, peer host and port. When the
     * resolved material requires hostname verification the engine's endpoint identification
     * algorithm is set to {@code HTTPS}.
     *
     * @param purpose  the TLS purpose
     * @param alloc    the byte buffer allocator the engine should use
     * @param peerHost the peer host the connection targets (may be {@code null})
     * @param peerPort the peer port the connection targets
     * @return a future of the configured client engine
     */
    public CompletableFuture<SSLEngine> createClientSslEngine(TlsPurposeContext purpose, ByteBufAllocator alloc,
                                                              String peerHost, int peerPort) {
        return getNettySslContext(purpose).thenCombine(materialProvider.getTlsMaterial(purpose),
                (sslContext, material) -> {
                    SSLEngine engine = peerHost != null
                            ? sslContext.newEngine(alloc, peerHost, peerPort)
                            : sslContext.newEngine(alloc);
                    engine.setUseClientMode(true);
                    if (material instanceof ClientTlsMaterial client && client.isHostnameVerificationRequired()) {
                        SSLParameters params = engine.getSSLParameters();
                        params.setEndpointIdentificationAlgorithm("HTTPS");
                        engine.setSSLParameters(params);
                    }
                    return engine;
                });
    }

    /**
     * Build a server-side {@link SSLEngine} for the given purpose. Client authentication (mTLS) is
     * configured by the underlying {@link SslContext} according to the material's
     * {@link ServerTlsMaterial#isTrustedClientCertRequired()} flag.
     *
     * @param purpose the TLS purpose
     * @param alloc   the byte buffer allocator the engine should use
     * @return a future of the configured server engine
     */
    public CompletableFuture<SSLEngine> createServerSslEngine(TlsPurposeContext purpose, ByteBufAllocator alloc) {
        return getNettySslContext(purpose).thenApply(sslContext -> {
            SSLEngine engine = sslContext.newEngine(alloc);
            engine.setUseClientMode(false);
            return engine;
        });
    }

    /**
     * Resolve a JDK {@link SSLContext} for the given purpose, suitable for Jetty / HTTPS servers and
     * for bridging into the AsyncHttpClient. The result is cached and rebuilt only when the material
     * changes.
     *
     * @param purpose the TLS purpose
     * @return a future of the JDK SSL context
     */
    public CompletableFuture<SSLContext> getJdkSslContext(TlsPurposeContext purpose) {
        return materialProvider.getTlsMaterial(purpose).thenApply(material -> {
            CachedJdkContext cached = jdkCache.compute(purpose, (key, existing) -> {
                if (existing != null && Objects.equals(existing.material(), material)) {
                    return existing;
                }
                try {
                    return new CachedJdkContext(material, buildJdkContext(material));
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to build JDK SSLContext for purpose " + purpose, e);
                }
            });
            return cached.context();
        });
    }

    /**
     * Resolve a Netty {@link SslContext} for the given purpose. The result is cached and rebuilt only
     * when the material changes.
     *
     * @param purpose the TLS purpose
     * @return a future of the Netty SSL context
     */
    public CompletableFuture<SslContext> getNettySslContext(TlsPurposeContext purpose) {
        return materialProvider.getTlsMaterial(purpose).thenApply(material -> {
            CachedNettyContext cached = nettyCache.compute(purpose, (key, existing) -> {
                if (existing != null && Objects.equals(existing.material(), material)) {
                    return existing;
                }
                try {
                    return new CachedNettyContext(material, buildNettyContext(purpose, material));
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to build Netty SslContext for purpose " + purpose, e);
                }
            });
            return cached.context();
        });
    }

    private SslContext buildNettyContext(TlsPurposeContext purpose, TlsMaterial material) throws Exception {
        SslProvider sslProvider = resolveSslProvider(material);
        Set<String> ciphers = toSet(material.getTlsCiphers());
        Set<String> protocols = toSet(material.getTlsProtocols());
        X509Certificate[] keyCertChain = toArray(material.getKeyCertChain());
        PrivateKey privateKey = material.getPrivateKey();
        X509Certificate[] trustCerts = toArray(material.getTrustCerts());

        if (purpose.isServer()) {
            boolean clientAuthRequired = material instanceof ServerTlsMaterial server
                    && server.isTrustedClientCertRequired();
            boolean trustAnyClientCert = material instanceof ServerTlsMaterial server
                    && server.isTrustAnyClientCert();
            // SecurityUtility has no server overload accepting already-loaded key/cert material, so the
            // server SslContext is assembled here mirroring SecurityUtility's PEM-file server builder
            // (trust certs via an in-memory PEM stream, optional/required client auth).
            SslContextBuilder builder = SslContextBuilder.forServer(privateKey, keyCertChain)
                    .sslProvider(sslProvider);
            if (ciphers != null) {
                builder.ciphers(ciphers);
            }
            if (protocols != null) {
                builder.protocols(protocols.toArray(new String[0]));
            }
            if (trustAnyClientCert) {
                // Insecure mode (legacy tlsAllowInsecureConnection): accept any client certificate so the
                // handshake completes and the cert is available for TLS authentication.
                builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else {
                try (InputStream trustStream = trustCertsStream(trustCerts)) {
                    if (trustStream != null) {
                        builder.trustManager(trustStream);
                    } else {
                        builder.trustManager((File) null);
                    }
                }
            }
            builder.clientAuth(clientAuthRequired ? ClientAuth.REQUIRE : ClientAuth.OPTIONAL);
            return builder.build();
        } else {
            boolean trustAnyCaCert = material instanceof ClientTlsMaterial client && client.isTrustAnyCaCert();
            SslContextBuilder builder = SslContextBuilder.forClient().sslProvider(sslProvider);
            if (trustAnyCaCert) {
                builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else {
                try (InputStream trustStream = trustCertsStream(trustCerts)) {
                    if (trustStream != null) {
                        builder.trustManager(trustStream);
                    } else {
                        builder.trustManager((File) null);
                    }
                }
            }
            if (keyCertChain != null && privateKey != null) {
                builder.keyManager(privateKey, keyCertChain);
            }
            if (ciphers != null) {
                builder.ciphers(ciphers);
            }
            if (protocols != null) {
                builder.protocols(protocols.toArray(new String[0]));
            }
            return builder.build();
        }
    }

    private SSLContext buildJdkContext(TlsMaterial material) throws Exception {
        // Insecure mode: on the client side trust any server cert; on the server side accept any client
        // cert (the legacy tlsAllowInsecureConnection behaviour, so an untrusted client cert still
        // completes the handshake and remains available for TLS authentication).
        boolean trustAny = (material instanceof ClientTlsMaterial client && client.isTrustAnyCaCert())
                || (material instanceof ServerTlsMaterial server && server.isTrustAnyClientCert());
        X509Certificate[] trustCerts = toArray(material.getTrustCerts());
        X509Certificate[] keyCertChain = toArray(material.getKeyCertChain());
        PrivateKey privateKey = material.getPrivateKey();
        return SecurityUtility.createSslContext(trustAny, trustCerts, keyCertChain, privateKey);
    }

    /**
     * Resolve the Netty {@link SslProvider} for a material. The {@link TlsMaterial} interface does
     * not currently expose a provider name, so this defaults to {@link SslProvider#JDK}; subclasses
     * may override to honour a provider hint (e.g. Conscrypt / OpenSSL).
     *
     * @param material the material to build a context for
     * @return the Netty SSL provider
     */
    protected SslProvider resolveSslProvider(TlsMaterial material) {
        return SslProvider.JDK;
    }

    private static InputStream trustCertsStream(X509Certificate[] trustCerts) throws Exception {
        if (trustCerts == null || trustCerts.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Base64.Encoder encoder = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII));
        for (X509Certificate cert : trustCerts) {
            out.write("-----BEGIN CERTIFICATE-----\n".getBytes(StandardCharsets.US_ASCII));
            out.write(encoder.encode(cert.getEncoded()));
            out.write("\n-----END CERTIFICATE-----\n".getBytes(StandardCharsets.US_ASCII));
        }
        return new ByteArrayInputStream(out.toByteArray());
    }

    private static Set<String> toSet(Iterable<String> values) {
        if (values == null) {
            return null;
        }
        Set<String> set = new LinkedHashSet<>();
        for (String value : values) {
            set.add(value);
        }
        return set.isEmpty() ? null : set;
    }

    private static X509Certificate[] toArray(Iterable<? extends X509Certificate> certs) {
        if (certs == null) {
            return null;
        }
        List<X509Certificate> list = new ArrayList<>();
        for (X509Certificate cert : certs) {
            list.add(cert);
        }
        return list.isEmpty() ? null : list.toArray(new X509Certificate[0]);
    }
}
