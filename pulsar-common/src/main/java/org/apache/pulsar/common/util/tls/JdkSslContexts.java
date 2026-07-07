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
package org.apache.pulsar.common.util.tls;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.pulsar.common.util.KeyStoreHolder;

/**
 * Assembles JDK {@link SSLContext} instances from loaded certificate/key material, resolving the security
 * provider through {@link JcaProviders} and parsing PEM inputs through {@link PemReader}. This is the JDK
 * {@code SSLContext} assembly primitive extracted from the former {@code SecurityUtility} grab-bag (PIP-478);
 * the default file-based TLS factory ({@code TlsContexts}) builds its JDK fallback context here, and TLS
 * tests reuse it to construct JDK contexts from PEM material.
 */
public final class JdkSslContexts {

    private JdkSslContexts() {
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertificates,
                                              String providerName)
            throws GeneralSecurityException {
        return createSslContext(allowInsecureConnection, trustCertificates, null, null, providerName);
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, String trustCertsFilePath,
            String certFilePath, String keyFilePath, String providerName) throws GeneralSecurityException {
        X509Certificate[] trustCertificates = PemReader.loadCertificatesFromPemFile(trustCertsFilePath);
        X509Certificate[] certificates = PemReader.loadCertificatesFromPemFile(certFilePath);
        PrivateKey privateKey = PemReader.loadPrivateKeyFromPemFile(keyFilePath);
        return createSslContext(allowInsecureConnection, trustCertificates, certificates, privateKey, providerName);
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertficates,
                                              Certificate[] certificates, PrivateKey privateKey)
            throws GeneralSecurityException {
        return createSslContext(allowInsecureConnection, trustCertficates, certificates, privateKey,
                (String) null);
    }

    public static SSLContext createSslContext(boolean allowInsecureConnection, Certificate[] trustCertficates,
                                              Certificate[] certificates, PrivateKey privateKey, String providerName)
            throws GeneralSecurityException {
        return createSslContextWithProvider(allowInsecureConnection, trustCertficates, certificates, privateKey,
                JcaProviders.resolveProvider(providerName));
    }

    /**
     * Assemble a JDK {@link SSLContext} backed by an already-resolved {@link Provider} (or the platform default
     * when {@code provider} is {@code null}). This is the PIP-478 {@code jsseProvider} entry point: the caller
     * resolves the named JSSE (SSLContext) provider (fail-loud) via
     * {@link JcaProviders#resolveNamedProvider(String)} and passes it here, so the JDK-engine web/Jetty and
     * fallback paths honor a pinned FIPS JSSE provider (e.g. BCJSSE, backed by BCFIPS as its crypto provider).
     *
     * @param allowInsecureConnection whether to trust all certificates (insecure)
     * @param trustCertficates        the trusted CA certificates (may be null/empty)
     * @param certificates            the key-cert chain (may be null when no client/server cert)
     * @param privateKey              the private key (may be null)
     * @param provider                the resolved crypto provider, or {@code null} for the platform default
     * @return the assembled JDK {@link SSLContext}
     * @throws GeneralSecurityException if the context cannot be assembled
     */
    public static SSLContext createSslContextWithProvider(boolean allowInsecureConnection,
                                                          Certificate[] trustCertficates, Certificate[] certificates,
                                                          PrivateKey privateKey, Provider provider)
            throws GeneralSecurityException {
        KeyStoreHolder ksh = new KeyStoreHolder();

        TrustManager[] trustManagers = setupTrustCerts(ksh, allowInsecureConnection, trustCertficates, provider);
        KeyManager[] keyManagers = setupKeyManager(ksh, privateKey, certificates, provider);

        SSLContext sslCtx = provider != null ? SSLContext.getInstance("TLS", provider)
                : SSLContext.getInstance("TLS");
        sslCtx.init(keyManagers, trustManagers, new SecureRandom());
        return sslCtx;
    }

    private static KeyManager[] setupKeyManager(KeyStoreHolder ksh, PrivateKey privateKey, Certificate[] certificates,
                                                Provider provider)
            throws KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException {
        KeyManager[] keyManagers = null;
        if (certificates != null && privateKey != null) {
            ksh.setPrivateKey("private", privateKey, certificates);
            // Pin the KeyManagerFactory to the resolved provider (like the SSLContext and TrustManagerFactory
            // above), so a configured jsseProvider (a FIPS JSSE provider such as BCJSSE) also backs the
            // private-key side; the null-provider path is unchanged, so no regression when jsseProvider is unset.
            KeyManagerFactory kmf = provider != null
                    ? KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm(), provider)
                    : KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ksh.getKeyStore(), "".toCharArray());
            keyManagers = kmf.getKeyManagers();
        }
        return keyManagers;
    }

    private static TrustManager[] setupTrustCerts(KeyStoreHolder ksh, boolean allowInsecureConnection,
                                                  Certificate[] trustCertficates, Provider securityProvider)
            throws NoSuchAlgorithmException, KeyStoreException {
        TrustManager[] trustManagers;
        if (allowInsecureConnection) {
            trustManagers = InsecureTrustManagerFactory.INSTANCE.getTrustManagers();
        } else {
            TrustManagerFactory tmf = securityProvider != null
                    ? TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm(), securityProvider)
                    : TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

            if (trustCertficates == null || trustCertficates.length == 0) {
                tmf.init((KeyStore) null);
            } else {
                for (int i = 0; i < trustCertficates.length; i++) {
                    ksh.setCertificate("trust" + i, trustCertficates[i]);
                }
                tmf.init(ksh.getKeyStore());
            }

            trustManagers = JcaProviders.processConscryptTrustManagers(tmf.getTrustManagers());
        }
        return trustManagers;
    }
}
