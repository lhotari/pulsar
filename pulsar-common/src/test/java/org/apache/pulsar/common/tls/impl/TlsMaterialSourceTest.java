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

import static org.apache.pulsar.common.tls.impl.TlsTestSupport.resource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import io.netty.util.ReferenceCountUtil;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.common.util.tls.PemReader;
import org.apache.pulsar.tls.TlsPolicy;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TlsMaterialSourceTest {

    private static final String CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String PROXY_CERT = resource("certificate-authority/server-keys/proxy.cert.pem");
    private static final String PROXY_KEY = resource("certificate-authority/server-keys/proxy.key-pk8.pem");
    // EC identity, so a keystore can hold two identities of different key types (RSA broker + EC client).
    private static final String EC_CERT = resource("certificate-authority/ec/client.cert.pem");
    private static final String EC_KEY = resource("certificate-authority/ec/client.key-pk8.pem");

    private static final char[] STORE_PW = "changeit".toCharArray();

    private Path dir;

    @BeforeMethod
    public void setUp() throws Exception {
        dir = Files.createTempDirectory("pip478-src-");
        Files.copy(Paths.get(CA), dir.resolve("ca.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(BROKER_CERT), dir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(BROKER_KEY), dir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (dir != null) {
            FileUtils.deleteDirectory(dir.toFile());
        }
    }

    private TlsMaterialSource source() {
        return new TlsMaterialSource(TlsPolicy.pem(dir.resolve("ca.pem").toString(),
                dir.resolve("cert.pem").toString(), dir.resolve("key.pem").toString()));
    }

    private void bumpMtime(String name) throws Exception {
        Files.setLastModifiedTime(dir.resolve(name), FileTime.fromMillis(System.currentTimeMillis() + 5000));
    }

    @Test
    public void firstLoadIsAChangeThenStable() throws Exception {
        TlsMaterialSource source = source();
        TlsMaterialSource.RefreshOutcome first = source.refresh();
        assertThat(first.changed()).isTrue();
        assertThat(first.material().hasKeyMaterial()).isTrue();
        assertThat(first.material().trustCerts()).isNotEmpty();

        assertThat(source.refresh().changed()).as("no file change -> stable").isFalse();
    }

    @Test
    public void touchWithSameContentDoesNotSignalChange() throws Exception {
        TlsMaterialSource source = source();
        TlsMaterial initial = source.refresh().material();

        bumpMtime("cert.pem");
        TlsMaterialSource.RefreshOutcome outcome = source.refresh();
        assertThat(outcome.changed()).as("mtime advanced but content identical -> suppressed").isFalse();
        assertThat(outcome.material()).isEqualTo(initial);
    }

    @Test
    public void differentContentSignalsChange() throws Exception {
        TlsMaterialSource source = source();
        TlsMaterial initial = source.refresh().material();

        Files.copy(Paths.get(PROXY_CERT), dir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(PROXY_KEY), dir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        bumpMtime("cert.pem");
        bumpMtime("key.pem");

        TlsMaterialSource.RefreshOutcome outcome = source.refresh();
        assertThat(outcome.changed()).isTrue();
        assertThat(outcome.material()).isNotEqualTo(initial);
    }

    @Test
    public void failedLoadKeepsBaselineSoNextPollRetries() throws Exception {
        TlsMaterialSource source = source();
        TlsMaterial good = source.refresh().material();

        // Corrupt the cert: the load throws and neither the baseline nor the cached material advance.
        Files.writeString(dir.resolve("cert.pem"), "-----BEGIN CERTIFICATE-----\ngarbage\n");
        bumpMtime("cert.pem");
        assertThatThrownBy(source::refresh).isInstanceOf(Exception.class);

        // Because the baseline was NOT advanced on failure, a retry without any further mtime change
        // still attempts the load again (the fix for the old advance-before-load sharp edge).
        assertThatThrownBy(source::refresh).isInstanceOf(Exception.class);

        // A subsequent good change recovers.
        Files.copy(Paths.get(PROXY_CERT), dir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(PROXY_KEY), dir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        bumpMtime("cert.pem");
        bumpMtime("key.pem");
        TlsMaterialSource.RefreshOutcome recovered = source.refresh();
        assertThat(recovered.changed()).isTrue();
        assertThat(recovered.material()).isNotEqualTo(good);
    }

    // ---- v4-parity: separate keystore/truststore types (PIP-478) ----

    /**
     * A PKCS12 keystore paired with a JKS truststore must load — each store parsed with its OWN configured
     * type. The pre-fix policy carried a single {@code storeType} applied to all three loads, which cannot
     * express this mixed setup (and breaks outright with FIPS/BCFKS mixes or when {@code keystore.type.compat}
     * is disabled).
     */
    @Test
    public void mixedStoreTypesLoadEachWithItsOwnType() throws Exception {
        Path pkcs12KeyStore = writePkcs12KeyStore();
        Path jksTrustStore = writeJksTrustStore();

        TlsMaterial material = new TlsMaterialSource(mixedPolicy(pkcs12KeyStore, "PKCS12", jksTrustStore, "JKS"))
                .refresh().material();

        assertThat(material.hasKeyMaterial()).as("PKCS12 keystore -> key + chain loaded").isTrue();
        assertThat(material.trustCerts()).as("JKS truststore -> trust certs loaded").isNotEmpty();
    }

    /**
     * The truststore must be loaded with {@code trustStoreType()}, not the keystore type. An invalid TRUST
     * type fails the load even though the KEY type is valid — before the fix (single shared type) the good
     * key type was used for the truststore and this would NOT have failed.
     */
    @Test
    public void trustStoreTypeIsConsultedForTheTruststore() throws Exception {
        Path pkcs12KeyStore = writePkcs12KeyStore();
        Path jksTrustStore = writeJksTrustStore();
        assertThatThrownBy(() ->
                new TlsMaterialSource(mixedPolicy(pkcs12KeyStore, "PKCS12", jksTrustStore, "NOSUCHTYPE")).refresh())
                .isInstanceOf(Exception.class);
    }

    /**
     * Symmetrically, the keystore must be loaded with {@code keyStoreType()}: an invalid KEY type fails even
     * though the TRUST type is valid.
     */
    @Test
    public void keyStoreTypeIsConsultedForTheKeystore() throws Exception {
        Path pkcs12KeyStore = writePkcs12KeyStore();
        Path jksTrustStore = writeJksTrustStore();
        assertThatThrownBy(() ->
                new TlsMaterialSource(mixedPolicy(pkcs12KeyStore, "NOSUCHTYPE", jksTrustStore, "JKS")).refresh())
                .isInstanceOf(Exception.class);
    }

    /**
     * A keystore with two identities (RSA + EC) loads every key entry into {@link TlsMaterial#keyEntries()}, so
     * the context builders can preserve JSSE alias selection. The pre-fix loader kept only the first alias — the
     * v4-parity regression this exercises — and {@link TlsMaterial#hasKeyMaterial()} still mirrors the first
     * entry. A byte-for-byte re-read stays {@link TlsMaterial#equals(Object) equal} (rotation suppression).
     */
    @Test
    public void keystoreWithTwoIdentitiesCarriesEveryEntry() throws Exception {
        Path twoIdentityKeyStore = writeTwoIdentityPkcs12();
        Path jksTrustStore = writeJksTrustStore();
        TlsMaterialSource source = new TlsMaterialSource(
                mixedPolicy(twoIdentityKeyStore, "PKCS12", jksTrustStore, "JKS"));

        TlsMaterial material = source.refresh().material();
        assertThat(material.keyEntries()).as("both keystore identities are carried").hasSize(2);
        assertThat(material.keyEntries()).extracting(TlsMaterial.KeyEntry::alias)
                .as("entries are ordered by alias").containsExactly("ec", "rsa");
        assertThat(material.hasKeyStoreEntries()).isTrue();
        assertThat(material.hasKeyMaterial()).as("first entry still mirrored for back-compat").isTrue();

        // Re-reading identical content stays equal, so a touched-but-unchanged keystore suppresses a rebuild.
        assertThat(new TlsMaterialSource(mixedPolicy(twoIdentityKeyStore, "PKCS12", jksTrustStore, "JKS"))
                .refresh().material()).isEqualTo(material);
    }

    /**
     * The full production build path (the same {@code TlsContexts} calls the factory makes) constructs the Netty
     * server/client and JDK contexts from two-identity keystore material.
     */
    @Test
    public void contextsBuildFromTwoIdentityKeystoreMaterial() throws Exception {
        Path twoIdentityKeyStore = writeTwoIdentityPkcs12();
        Path jksTrustStore = writeJksTrustStore();
        TlsPolicy policy = mixedPolicy(twoIdentityKeyStore, "PKCS12", jksTrustStore, "JKS");
        TlsMaterial material = new TlsMaterialSource(policy).refresh().material();

        assertThat(TlsContexts.buildJdkContext(material, policy)).isNotNull();
        SslContext server = TlsContexts.buildNettyServerContext(material, policy, SslProvider.JDK, true);
        SslContext client = TlsContexts.buildNettyClientContext(material, policy, SslProvider.JDK);
        assertThat(server).isNotNull();
        assertThat(client).isNotNull();
        ReferenceCountUtil.release(server);
        ReferenceCountUtil.release(client);
    }

    private Path writeTwoIdentityPkcs12() throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("rsa", PemReader.loadPrivateKeyFromPemFile(BROKER_KEY), STORE_PW,
                PemReader.loadCertificatesFromPemFile(BROKER_CERT));
        ks.setKeyEntry("ec", PemReader.loadPrivateKeyFromPemFile(EC_KEY), STORE_PW,
                PemReader.loadCertificatesFromPemFile(EC_CERT));
        Path path = dir.resolve("two-identity.p12");
        try (OutputStream out = Files.newOutputStream(path)) {
            ks.store(out, STORE_PW);
        }
        return path;
    }

    private static TlsPolicy mixedPolicy(Path keyStore, String keyStoreType, Path trustStore, String trustStoreType) {
        return TlsPolicy.builder().format(TlsPolicy.Format.KEYSTORE)
                .keyStorePath(keyStore.toString()).keyStorePassword(new String(STORE_PW)).keyStoreType(keyStoreType)
                .trustStorePath(trustStore.toString()).trustStorePassword(new String(STORE_PW))
                .trustStoreType(trustStoreType)
                .build();
    }

    private Path writePkcs12KeyStore() throws Exception {
        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(BROKER_KEY);
        X509Certificate[] chain = PemReader.loadCertificatesFromPemFile(BROKER_CERT);
        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        ks.setKeyEntry("key", key, STORE_PW, chain);
        Path path = dir.resolve("key.p12");
        try (OutputStream out = Files.newOutputStream(path)) {
            ks.store(out, STORE_PW);
        }
        return path;
    }

    private Path writeJksTrustStore() throws Exception {
        X509Certificate[] cas = PemReader.loadCertificatesFromPemFile(CA);
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, null);
        for (int i = 0; i < cas.length; i++) {
            ks.setCertificateEntry("ca" + i, cas[i]);
        }
        Path path = dir.resolve("trust.jks");
        try (OutputStream out = Files.newOutputStream(path)) {
            ks.store(out, STORE_PW);
        }
        return path;
    }
}
