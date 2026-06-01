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

import java.io.InputStream;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.common.util.SecurityUtility;

/**
 * A {@link TlsMaterialSource} that bridges a legacy (v4) client
 * {@link AuthenticationDataProvider} to the PIP-478 TLS material SPI.
 *
 * <p>This is the PIP-478 "TLS override hook": it lets an authentication plugin (notably
 * {@code AuthenticationTls} / {@code AuthenticationKeyStoreTls}) supply the <em>client</em> TLS
 * key material (certificate chain + private key, or a keystore) directly, replacing the old
 * {@code PulsarSslConfiguration.authData} coupling. The remaining client-side TLS configuration —
 * the trust certificates, enabled ciphers/protocols, hostname-verification and allow-insecure flags
 * — comes from a {@link FileBasedTlsMaterialSource} {@code baseConfig} built from the client
 * builder. The trust material can also be supplied by the {@code authData} itself via
 * {@link AuthenticationDataProvider#getTlsTrustStoreStream()}.
 *
 * <p>The key material is resolved, in priority order, from:
 * <ol>
 *   <li>{@link AuthenticationDataProvider#getTlsCertificates()} +
 *       {@link AuthenticationDataProvider#getTlsPrivateKey()} (in-memory material);</li>
 *   <li>{@link AuthenticationDataProvider#getTlsCertificateFilePath()} +
 *       {@link AuthenticationDataProvider#getTlsPrivateKeyFilePath()} (PEM files);</li>
 *   <li>{@link AuthenticationDataProvider#getTlsKeyStoreParams()} (a keystore).</li>
 * </ol>
 * If none of these is available, the source produces material with no client certificate.
 *
 * <p><b>Rotation.</b> This source is queried repeatedly by the provider/engine cache, which detects
 * change through {@link DefaultClientTlsMaterial#equals(Object)}. To support rotation of
 * file/stream/in-memory-backed material, {@link #getTlsMaterial()} rebuilds the material on every
 * call and relies on value equality so a rotation is observed. Authentication providers that
 * hot-reload internally (for example via {@code FileModifiedTimeUpdater}) return refreshed arrays
 * from {@link AuthenticationDataProvider#getTlsCertificates()}, which equality then picks up. No
 * caching is performed here that would mask a rotation.
 */
public final class AuthenticationDataTlsMaterialSource implements TlsMaterialSource {

    private final Supplier<AuthenticationDataProvider> authDataSupplier;
    private final FileBasedTlsMaterialSource baseConfig;

    /**
     * Create a source bridging the supplied authentication data to the PIP-478 SPI.
     *
     * @param authData   the v4 authentication data provider supplying the client key material
     *                   (and possibly the trust stream / keystore params); must not be {@code null}
     * @param baseConfig the client-builder TLS configuration supplying the trust certificates path,
     *                   ciphers, protocols, hostname-verification and allow-insecure flags; may be
     *                   {@code null}, in which case the defaults (hostname verification on, insecure
     *                   off, platform-default trust store) apply
     */
    public AuthenticationDataTlsMaterialSource(AuthenticationDataProvider authData,
                                               FileBasedTlsMaterialSource baseConfig) {
        this(() -> authData, baseConfig);
        Objects.requireNonNull(authData, "authData");
    }

    /**
     * Create a source that resolves the authentication data lazily on each call. This is the form the
     * v4 {@code AuthenticationTls} bridge uses: a fresh {@link AuthenticationDataProvider} is obtained
     * from the plugin on every {@link #getTlsMaterial()} call, so credential rotation (cert/key/trust
     * refresh) is observed and a construction-time failure (e.g. a not-yet-valid cert) does not
     * permanently disable the source.
     *
     * @param authDataSupplier supplies the current {@link AuthenticationDataProvider}; must not be
     *                         {@code null} (but may itself return {@code null} or throw transiently)
     * @param baseConfig       the client-builder TLS configuration; may be {@code null}
     */
    public AuthenticationDataTlsMaterialSource(Supplier<AuthenticationDataProvider> authDataSupplier,
                                               FileBasedTlsMaterialSource baseConfig) {
        this.authDataSupplier = Objects.requireNonNull(authDataSupplier, "authDataSupplier");
        this.baseConfig = baseConfig;
    }

    /**
     * Static convenience factory.
     *
     * @param authData   the v4 authentication data provider; must not be {@code null}
     * @param baseConfig the client-builder TLS configuration; may be {@code null}
     * @return a new {@link AuthenticationDataTlsMaterialSource}
     */
    public static AuthenticationDataTlsMaterialSource of(AuthenticationDataProvider authData,
                                                         FileBasedTlsMaterialSource baseConfig) {
        return new AuthenticationDataTlsMaterialSource(authData, baseConfig);
    }

    /**
     * Static convenience factory for the lazy/supplier form.
     *
     * @param authDataSupplier supplies the current {@link AuthenticationDataProvider}; must not be {@code null}
     * @param baseConfig       the client-builder TLS configuration; may be {@code null}
     * @return a new {@link AuthenticationDataTlsMaterialSource}
     */
    public static AuthenticationDataTlsMaterialSource ofSupplier(
            Supplier<AuthenticationDataProvider> authDataSupplier, FileBasedTlsMaterialSource baseConfig) {
        return new AuthenticationDataTlsMaterialSource(authDataSupplier, baseConfig);
    }

    @Override
    public boolean isServer() {
        return false;
    }

    @Override
    public TlsMaterial getTlsMaterial() throws Exception {
        AuthenticationDataProvider authData = authDataSupplier.get();
        if (authData == null) {
            // No auth data available right now: contribute no client material (the file-based source,
            // if any, still applies). An empty material is value-equal across calls so it doesn't churn.
            return DefaultClientTlsMaterial.builder()
                    .hostnameVerificationRequired(baseConfig == null || baseConfig.isHostnameVerificationEnabled())
                    .trustAnyCaCert(baseConfig != null && baseConfig.isAllowInsecureConnection())
                    .build();
        }
        DefaultClientTlsMaterial.Builder builder = DefaultClientTlsMaterial.builder();

        // --- Client key material (mTLS): certificate chain + private key. ---
        List<X509Certificate> keyCertChain = resolveKeyCertChain(authData);
        PrivateKey privateKey = resolvePrivateKey(authData);
        if (keyCertChain != null || privateKey != null) {
            builder.keyCertChain(keyCertChain).privateKey(privateKey);
        }

        // --- Trust certificates. ---
        builder.trustCerts(resolveTrustCerts(authData));

        // --- Handshake tuning and behavioural flags from the base config. ---
        if (baseConfig != null) {
            builder.hostnameVerificationRequired(baseConfig.isHostnameVerificationEnabled());
            builder.trustAnyCaCert(baseConfig.isAllowInsecureConnection());
            if (!baseConfig.getTlsProtocols().isEmpty()) {
                builder.tlsProtocols(List.copyOf(baseConfig.getTlsProtocols()));
            }
            if (!baseConfig.getTlsCiphers().isEmpty()) {
                builder.tlsCiphers(List.copyOf(baseConfig.getTlsCiphers()));
            }
        }
        // Otherwise the DefaultClientTlsMaterial.Builder defaults apply:
        // hostnameVerificationRequired = true, trustAnyCaCert = false.

        return builder.build();
    }

    /**
     * Resolve the client certificate chain from the authentication data: prefer the in-memory
     * certificates, then a PEM certificate file, then a keystore. Returns {@code null} when no
     * client certificate is configured.
     */
    private List<X509Certificate> resolveKeyCertChain(AuthenticationDataProvider authData) throws Exception {
        Certificate[] certificates = authData.getTlsCertificates();
        if (certificates != null && certificates.length > 0) {
            return toX509List(certificates);
        }
        String certFilePath = authData.getTlsCertificateFilePath();
        if (StringUtils.isNotBlank(certFilePath)) {
            X509Certificate[] certs = SecurityUtility.loadCertificatesFromPemFile(certFilePath);
            return certs == null ? null : List.of(certs);
        }
        KeyStoreParams keyStoreParams = authData.getTlsKeyStoreParams();
        if (keyStoreParams != null && StringUtils.isNotBlank(keyStoreParams.getKeyStorePath())) {
            return FileBasedTlsMaterialLoader.loadCertificateChain(keyStoreConfig(keyStoreParams));
        }
        return null;
    }

    /**
     * Resolve the client private key from the authentication data: prefer the in-memory key, then a
     * PEM key file, then a keystore. Returns {@code null} when no client key is configured.
     */
    private PrivateKey resolvePrivateKey(AuthenticationDataProvider authData) throws Exception {
        PrivateKey privateKey = authData.getTlsPrivateKey();
        if (privateKey != null) {
            return privateKey;
        }
        String keyFilePath = authData.getTlsPrivateKeyFilePath();
        if (StringUtils.isNotBlank(keyFilePath)) {
            return SecurityUtility.loadPrivateKeyFromPemFile(keyFilePath);
        }
        KeyStoreParams keyStoreParams = authData.getTlsKeyStoreParams();
        if (keyStoreParams != null && StringUtils.isNotBlank(keyStoreParams.getKeyStorePath())) {
            return FileBasedTlsMaterialLoader.loadPrivateKey(keyStoreConfig(keyStoreParams));
        }
        return null;
    }

    /**
     * Resolve the trust certificates: prefer the {@code baseConfig} trust-certs file, then the
     * authentication-data trust-store stream; otherwise an empty list (the platform default trust
     * store applies).
     */
    private List<X509Certificate> resolveTrustCerts(AuthenticationDataProvider authData) throws Exception {
        // The trust material comes from the client-builder base config: either a PEM trust-certs file or
        // a JKS/PKCS12 trust store (keystore mode). FileBasedTlsMaterialLoader handles both.
        if (baseConfig != null && (StringUtils.isNotBlank(baseConfig.getTrustCertsFilePath())
                || StringUtils.isNotBlank(baseConfig.getTrustStorePath()))) {
            return FileBasedTlsMaterialLoader.loadTrustCerts(baseConfig);
        }
        try (InputStream trustStoreStream = authData.getTlsTrustStoreStream()) {
            if (trustStoreStream != null) {
                X509Certificate[] certs = SecurityUtility.loadCertificatesFromPemStream(trustStoreStream);
                return certs == null ? Collections.emptyList() : List.of(certs);
            }
        }
        return Collections.emptyList();
    }

    private static FileBasedTlsMaterialSource keyStoreConfig(KeyStoreParams params) {
        return FileBasedTlsMaterialSource.builder()
                .keyStoreType(params.getKeyStoreType())
                .keyStorePath(params.getKeyStorePath())
                .keyStorePassword(params.getKeyStorePassword())
                .build();
    }

    private static List<X509Certificate> toX509List(Certificate[] certificates) {
        List<X509Certificate> result = new ArrayList<>(certificates.length);
        for (Certificate certificate : certificates) {
            if (certificate instanceof X509Certificate) {
                result.add((X509Certificate) certificate);
            }
        }
        return result;
    }
}
