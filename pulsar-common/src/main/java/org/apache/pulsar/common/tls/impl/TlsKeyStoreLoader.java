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

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 * Loads TLS material (private key, certificate chain, trust certificates) from a PKCS12/JKS keystore for
 * the default {@code FileBasedTlsFactory} (PIP-478). Shared by the file-based {@link TlsMaterialSource}
 * and the {@code BROKER_CLIENT} authentication overlay ({@link AuthProvidedMaterialSource}) so a single
 * alias-walk implementation serves both.
 */
final class TlsKeyStoreLoader {

    private TlsKeyStoreLoader() {
    }

    static KeyStore loadKeyStore(String type, String path, String password) throws Exception {
        String storeType = StringUtils.isNotBlank(type) ? type : KeyStore.getDefaultType();
        KeyStore keyStore = KeyStore.getInstance(storeType);
        try (InputStream input = new FileInputStream(path)) {
            keyStore.load(input, password == null ? null : password.toCharArray());
        }
        return keyStore;
    }

    static List<X509Certificate> extractTrustCerts(KeyStore trustStore) throws Exception {
        List<X509Certificate> certs = new ArrayList<>();
        Enumeration<String> aliases = trustStore.aliases();
        while (aliases.hasMoreElements()) {
            Certificate certificate = trustStore.getCertificate(aliases.nextElement());
            if (certificate instanceof X509Certificate x509) {
                certs.add(x509);
            }
        }
        return certs;
    }

    static PrivateKey extractPrivateKey(KeyStore keyStore, String password) throws Exception {
        char[] pwd = password == null ? null : password.toCharArray();
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                Key key = keyStore.getKey(alias, pwd);
                if (key instanceof PrivateKey privateKey) {
                    return privateKey;
                }
            }
        }
        return null;
    }

    static List<X509Certificate> extractCertificateChain(KeyStore keyStore) throws Exception {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                Certificate[] chain = keyStore.getCertificateChain(alias);
                if (chain != null && chain.length > 0) {
                    List<X509Certificate> result = new ArrayList<>(chain.length);
                    for (Certificate certificate : chain) {
                        if (certificate instanceof X509Certificate x509) {
                            result.add(x509);
                        }
                    }
                    if (!result.isEmpty()) {
                        return result;
                    }
                }
            }
        }
        return List.of();
    }
}
