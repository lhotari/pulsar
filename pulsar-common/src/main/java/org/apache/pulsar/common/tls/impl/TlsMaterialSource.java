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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.security.Key;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.util.SecurityUtility;

/**
 * Loads, watches and caches ONE material set (the crypto for a single {@code TlsPurpose}) from a
 * {@link TlsPolicy} for the default {@code FileBasedTlsFactory} (PIP-478).
 *
 * <p>This collapses the old branch's {@code FileBasedClient/ServerTlsMaterialSource} split into a
 * single, role-neutral class: the difference was only the value builder and a few flags, while the
 * watch/cache machinery was duplicated verbatim.
 *
 * <p><b>Keystore-over-PEM per-field precedence.</b> For each of the three material slots (trust
 * certs, private key, key certificate chain) the keystore location wins when set, otherwise the PEM
 * location is used. PEM material is loaded via {@link SecurityUtility}; keystores (PKCS12/JKS) are read
 * with the raw {@link KeyStore} API and an alias walk.
 *
 * <p><b>Rotation detection (fixed mtime baseline).</b> Change detection snapshots the modification
 * times of every configured file, and — unlike the old {@code FileModifiedTimeUpdater}-based scheme,
 * which advanced the baseline <em>before</em> the load and so never retried a failed rotation until the
 * next mtime change — commits the new baseline <strong>only after a successful load</strong>. A load
 * that throws (a half-written or invalid rotated file, the canonical incident) leaves both the baseline
 * and the last-good material untouched, so the next poll observes the same change again and retries.
 * When mtimes advanced but the loaded material is byte-for-byte {@link TlsMaterial#equals(Object)
 * equal} to the cached one (a file touched without content change), the baseline is committed but no
 * change is signalled.
 *
 * <p>Not thread-safe on its own; the owning factory serialises access under its per-source monitor.
 */
final class TlsMaterialSource {

    /** Sentinel modification time recorded for a path that is currently missing or unreadable. */
    private static final FileTime MISSING = FileTime.fromMillis(Long.MIN_VALUE);

    private final TlsPolicy policy;
    private final List<String> watchedPaths;

    private Map<String, FileTime> baseline;
    private TlsMaterial cached;

    TlsMaterialSource(TlsPolicy policy) {
        this.policy = Objects.requireNonNull(policy, "policy must not be null");
        this.watchedPaths = watchedPathsFor(policy);
    }

    TlsPolicy policy() {
        return policy;
    }

    /**
     * Re-stat the configured files, reloading the material when they changed since the last successful
     * load. Returns the current (possibly rebuilt) material together with whether it changed in value.
     *
     * @return the refresh outcome
     * @throws Exception if the material could not be loaded (the last-good material and baseline are
     *                   left untouched so the next call retries)
     */
    RefreshOutcome refresh() throws Exception {
        Map<String, FileTime> snapshot = snapshotModificationTimes();
        if (cached != null && snapshot.equals(baseline)) {
            return new RefreshOutcome(cached, false);
        }
        TlsMaterial loaded = load();
        boolean changed = cached == null || !loaded.equals(cached);
        // Commit the baseline only after a successful load (keep-last-good + retry-on-next-change).
        baseline = snapshot;
        if (changed) {
            cached = loaded;
        }
        return new RefreshOutcome(cached, changed);
    }

    private TlsMaterial load() throws Exception {
        PrivateKey privateKey = loadPrivateKey();
        List<X509Certificate> keyCertChain = loadCertificateChain();
        List<X509Certificate> trustCerts = loadTrustCerts();
        return new TlsMaterial(privateKey, keyCertChain, trustCerts);
    }

    private List<X509Certificate> loadTrustCerts() throws Exception {
        if (StringUtils.isNotBlank(policy.trustStorePath())) {
            return extractTrustCerts(loadKeyStore(policy.storeType(), policy.trustStorePath(),
                    policy.trustStorePassword()));
        }
        if (StringUtils.isNotBlank(policy.trustCertsFilePath())) {
            X509Certificate[] certs = SecurityUtility.loadCertificatesFromPemFile(policy.trustCertsFilePath());
            return certs == null ? List.of() : List.of(certs);
        }
        return List.of();
    }

    private PrivateKey loadPrivateKey() throws Exception {
        if (StringUtils.isNotBlank(policy.keyStorePath())) {
            return extractPrivateKey(loadKeyStore(policy.storeType(), policy.keyStorePath(),
                    policy.keyStorePassword()), policy.keyStorePassword());
        }
        if (StringUtils.isNotBlank(policy.keyFilePath())) {
            return SecurityUtility.loadPrivateKeyFromPemFile(policy.keyFilePath());
        }
        return null;
    }

    private List<X509Certificate> loadCertificateChain() throws Exception {
        if (StringUtils.isNotBlank(policy.keyStorePath())) {
            return extractCertificateChain(loadKeyStore(policy.storeType(), policy.keyStorePath(),
                    policy.keyStorePassword()));
        }
        if (StringUtils.isNotBlank(policy.certificateFilePath())) {
            X509Certificate[] certs = SecurityUtility.loadCertificatesFromPemFile(policy.certificateFilePath());
            return certs == null ? List.of() : List.of(certs);
        }
        return List.of();
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
            Certificate certificate = trustStore.getCertificate(aliases.nextElement());
            if (certificate instanceof X509Certificate x509) {
                certs.add(x509);
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
                if (key instanceof PrivateKey privateKey) {
                    return privateKey;
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

    private Map<String, FileTime> snapshotModificationTimes() {
        Map<String, FileTime> snapshot = new LinkedHashMap<>();
        for (String path : watchedPaths) {
            FileTime mtime;
            try {
                mtime = Files.getLastModifiedTime(Paths.get(path));
            } catch (Exception e) {
                mtime = MISSING;
            }
            snapshot.put(path, mtime);
        }
        return snapshot;
    }

    private static List<String> watchedPathsFor(TlsPolicy policy) {
        List<String> paths = new ArrayList<>(5);
        addIfPresent(paths, policy.trustCertsFilePath());
        addIfPresent(paths, policy.certificateFilePath());
        addIfPresent(paths, policy.keyFilePath());
        addIfPresent(paths, policy.keyStorePath());
        addIfPresent(paths, policy.trustStorePath());
        return List.copyOf(paths);
    }

    private static void addIfPresent(List<String> paths, String path) {
        if (StringUtils.isNotBlank(path)) {
            paths.add(path);
        }
    }

    /**
     * The result of a {@link TlsMaterialSource#refresh()}: the current material and whether it changed
     * in value since the previous refresh.
     *
     * @param material the current (last-good) material
     * @param changed  {@code true} when the material's value changed (first load counts as a change)
     */
    record RefreshOutcome(TlsMaterial material, boolean changed) {
    }
}
