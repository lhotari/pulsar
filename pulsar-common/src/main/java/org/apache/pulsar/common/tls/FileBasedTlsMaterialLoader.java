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

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.SecurityUtility;

/**
 * Internal helper that loads PEM and keystore material from the filesystem and converts it into the
 * in-memory representations ({@link PrivateKey}/{@link X509Certificate}) used by the
 * {@link TlsMaterial} implementations.
 *
 * <p>The actual cryptographic loading is delegated to
 * {@link org.apache.pulsar.common.util.SecurityUtility} (for PEM) and the standard
 * {@link java.security.KeyStore} API (for keystores) so that no crypto logic is duplicated here.
 */
final class FileBasedTlsMaterialLoader {

    private FileBasedTlsMaterialLoader() {
    }

    /**
     * Load the trust certificates either from a PEM trust-certs file or from a trust store, or an
     * empty list if neither is configured.
     */
    static List<X509Certificate> loadTrustCerts(FileBasedTlsMaterialSource config) throws Exception {
        if (StringUtils.isNotBlank(config.getTrustStorePath())) {
            KeyStore trustStore = loadKeyStore(config.getTrustStoreType(), config.getTrustStorePath(),
                    config.getTrustStorePassword());
            return extractTrustCerts(trustStore);
        }
        if (StringUtils.isNotBlank(config.getTrustCertsFilePath())) {
            X509Certificate[] certs = SecurityUtility.loadCertificatesFromPemFile(config.getTrustCertsFilePath());
            return certs == null ? Collections.emptyList() : List.of(certs);
        }
        return Collections.emptyList();
    }

    /**
     * Load the private key either from a PEM key file or from a key store, or {@code null} if neither
     * is configured.
     */
    static PrivateKey loadPrivateKey(FileBasedTlsMaterialSource config) throws Exception {
        if (StringUtils.isNotBlank(config.getKeyStorePath())) {
            KeyStore keyStore = loadKeyStore(config.getKeyStoreType(), config.getKeyStorePath(),
                    config.getKeyStorePassword());
            return extractPrivateKey(keyStore, config.getKeyStorePassword());
        }
        if (StringUtils.isNotBlank(config.getKeyFilePath())) {
            return SecurityUtility.loadPrivateKeyFromPemFile(config.getKeyFilePath());
        }
        return null;
    }

    /**
     * Load the certificate chain either from a PEM certificate file or from a key store, or
     * {@code null} if neither is configured.
     */
    static List<X509Certificate> loadCertificateChain(FileBasedTlsMaterialSource config) throws Exception {
        if (StringUtils.isNotBlank(config.getKeyStorePath())) {
            KeyStore keyStore = loadKeyStore(config.getKeyStoreType(), config.getKeyStorePath(),
                    config.getKeyStorePassword());
            return extractCertificateChain(keyStore);
        }
        if (StringUtils.isNotBlank(config.getCertificateFilePath())) {
            X509Certificate[] certs = SecurityUtility.loadCertificatesFromPemFile(config.getCertificateFilePath());
            return certs == null ? null : List.of(certs);
        }
        return null;
    }

    private static KeyStore loadKeyStore(String type, String path, String password) throws Exception {
        String storeType = StringUtils.isNotBlank(type) ? type : KeyStore.getDefaultType();
        KeyStore keyStore = KeyStore.getInstance(storeType);
        try (InputStream input = new FileInputStream(path)) {
            keyStore.load(input, password == null ? null : password.toCharArray());
        }
        return keyStore;
    }

    private static List<X509Certificate> extractTrustCerts(KeyStore trustStore) throws Exception {
        List<X509Certificate> certs = new ArrayList<>();
        Enumeration<String> aliases = trustStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            Certificate certificate = trustStore.getCertificate(alias);
            if (certificate instanceof X509Certificate) {
                certs.add((X509Certificate) certificate);
            }
        }
        return certs;
    }

    private static PrivateKey extractPrivateKey(KeyStore keyStore, String password) throws Exception {
        char[] pwd = password == null ? null : password.toCharArray();
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                Key key = keyStore.getKey(alias, pwd);
                if (key instanceof PrivateKey) {
                    return (PrivateKey) key;
                }
            }
        }
        return null;
    }

    private static List<X509Certificate> extractCertificateChain(KeyStore keyStore) throws Exception {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                Certificate[] chain = keyStore.getCertificateChain(alias);
                if (chain != null && chain.length > 0) {
                    List<X509Certificate> result = new ArrayList<>(chain.length);
                    for (Certificate certificate : chain) {
                        if (certificate instanceof X509Certificate) {
                            result.add((X509Certificate) certificate);
                        }
                    }
                    if (!result.isEmpty()) {
                        return result;
                    }
                }
            }
        }
        return null;
    }
}
