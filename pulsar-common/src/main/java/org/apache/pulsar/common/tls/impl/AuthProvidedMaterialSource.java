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

import java.io.InputStream;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.common.util.tls.PemReader;

/**
 * A {@link MaterialSource} that folds a server component's broker-client {@code Authentication} TLS
 * material over a file-based base source for the {@code BROKER_CLIENT} purpose (PIP-478). This is the
 * server-side mirror of the client TLS override hook, preserving the removed PIP-337 auth-data behavior:
 * when the broker-client authentication plugin (e.g.
 * {@code AuthenticationTls}) supplies key material, its in-memory certificate/key <em>override</em> the
 * configured {@code brokerClient*} file paths, so a proxy or broker presents the right identity to the
 * broker and forwarded-principal authorization works.
 *
 * <p>Precedence per material slot:
 * <ul>
 *   <li><b>key + certificate chain</b>: the authentication plugin's material wins when it supplies both;
 *       otherwise the base file material is used;</li>
 *   <li><b>trust certificates</b>: the base (file) trust material wins; only when the base has none is the
 *       authentication plugin's trust-store stream consulted.</li>
 * </ul>
 *
 * <p><b>Rotation.</b> {@link #refresh()} re-reads the authentication data on every call (the plugin may
 * hot-reload its own files) and rebuilds the combined material, signalling a change through
 * {@link TlsMaterial} value equality — so a rotated auth certificate is picked up independently of whether
 * the base files changed. A transient authentication failure keeps the last-good combined material.
 */
final class AuthProvidedMaterialSource implements MaterialSource {

    private final TlsMaterialSource base;
    private final Supplier<AuthenticationDataProvider> authDataSupplier;
    private TlsMaterial cached;

    AuthProvidedMaterialSource(TlsMaterialSource base, Supplier<AuthenticationDataProvider> authDataSupplier) {
        this.base = Objects.requireNonNull(base, "base");
        this.authDataSupplier = Objects.requireNonNull(authDataSupplier, "authDataSupplier");
    }

    @Override
    public RefreshOutcome refresh() throws Exception {
        TlsMaterial baseMaterial = base.refresh().material();
        AuthenticationDataProvider authData;
        try {
            authData = authDataSupplier.get();
        } catch (RuntimeException e) {
            // Transient auth failure: keep the last-good combined material and retry on the next poll.
            if (cached != null) {
                return new RefreshOutcome(cached, false);
            }
            throw e;
        }
        TlsMaterial combined = overlay(baseMaterial, authData);
        boolean changed = cached == null || !combined.equals(cached);
        if (changed) {
            cached = combined;
        }
        return new RefreshOutcome(cached, changed);
    }

    private TlsMaterial overlay(TlsMaterial base, AuthenticationDataProvider authData) throws Exception {
        List<X509Certificate> authChain = null;
        PrivateKey authKey = null;
        if (authData != null && authData.hasDataForTls()) {
            authChain = resolveKeyCertChain(authData);
            authKey = resolvePrivateKey(authData);
        }
        boolean useAuthKey = authKey != null && authChain != null && !authChain.isEmpty();
        PrivateKey privateKey = useAuthKey ? authKey : base.privateKey();
        List<X509Certificate> keyCertChain = useAuthKey ? authChain : base.keyCertChain();
        // The authentication plugin supplies a single identity; when it overrides, drop the base keystore's
        // multi-alias entries. Otherwise carry them through so a keystore base keeps JSSE alias selection.
        List<TlsMaterial.KeyEntry> keyEntries = useAuthKey ? List.of() : base.keyEntries();

        List<X509Certificate> trustCerts = base.trustCerts();
        if (trustCerts.isEmpty() && authData != null) {
            trustCerts = resolveTrustFromAuth(authData);
        }
        return new TlsMaterial(privateKey, keyCertChain, trustCerts, keyEntries);
    }

    private static List<X509Certificate> resolveKeyCertChain(AuthenticationDataProvider authData) throws Exception {
        Certificate[] certificates = authData.getTlsCertificates();
        if (certificates != null && certificates.length > 0) {
            return toX509(certificates);
        }
        String certFilePath = authData.getTlsCertificateFilePath();
        if (StringUtils.isNotBlank(certFilePath)) {
            X509Certificate[] certs = PemReader.loadCertificatesFromPemFile(certFilePath);
            return certs == null ? null : List.of(certs);
        }
        KeyStoreParams params = authData.getTlsKeyStoreParams();
        if (params != null && StringUtils.isNotBlank(params.getKeyStorePath())) {
            return TlsKeyStoreLoader.extractCertificateChain(loadKeyStore(params));
        }
        return null;
    }

    private static PrivateKey resolvePrivateKey(AuthenticationDataProvider authData) throws Exception {
        PrivateKey privateKey = authData.getTlsPrivateKey();
        if (privateKey != null) {
            return privateKey;
        }
        String keyFilePath = authData.getTlsPrivateKeyFilePath();
        if (StringUtils.isNotBlank(keyFilePath)) {
            return PemReader.loadPrivateKeyFromPemFile(keyFilePath);
        }
        KeyStoreParams params = authData.getTlsKeyStoreParams();
        if (params != null && StringUtils.isNotBlank(params.getKeyStorePath())) {
            return TlsKeyStoreLoader.extractPrivateKey(loadKeyStore(params), params.getKeyStorePassword());
        }
        return null;
    }

    private static List<X509Certificate> resolveTrustFromAuth(AuthenticationDataProvider authData) throws Exception {
        try (InputStream trustStoreStream = authData.getTlsTrustStoreStream()) {
            if (trustStoreStream != null) {
                X509Certificate[] certs = PemReader.loadCertificatesFromPemStream(trustStoreStream);
                return certs == null ? List.of() : List.of(certs);
            }
        }
        return List.of();
    }

    private static java.security.KeyStore loadKeyStore(KeyStoreParams params) throws Exception {
        return TlsKeyStoreLoader.loadKeyStore(params.getKeyStoreType(), params.getKeyStorePath(),
                params.getKeyStorePassword());
    }

    private static List<X509Certificate> toX509(Certificate[] certificates) {
        List<X509Certificate> result = new ArrayList<>(certificates.length);
        for (Certificate certificate : certificates) {
            if (certificate instanceof X509Certificate x509) {
                result.add(x509);
            }
        }
        return result;
    }
}
