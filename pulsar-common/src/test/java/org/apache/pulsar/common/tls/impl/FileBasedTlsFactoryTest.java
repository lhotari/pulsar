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

import static org.apache.pulsar.common.tls.impl.TlsTestSupport.handshake;
import static org.apache.pulsar.common.tls.impl.TlsTestSupport.initContext;
import static org.apache.pulsar.common.tls.impl.TlsTestSupport.resource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.common.tls.TlsHandle;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FileBasedTlsFactoryTest {

    private static final String RSA_CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String PROXY_CERT = resource("certificate-authority/server-keys/proxy.cert.pem");
    private static final String PROXY_KEY = resource("certificate-authority/server-keys/proxy.key-pk8.pem");
    // EC identity is signed by the EC CA — untrusted by the RSA CA the server above trusts.
    private static final String EC_CLIENT_CERT = resource("certificate-authority/ec/client.cert.pem");
    private static final String EC_CLIENT_KEY = resource("certificate-authority/ec/client.key-pk8.pem");

    private static final String KEYSTORE = resource("certificate-authority/jks/broker.keystore.jks");
    private static final String TRUSTSTORE = resource("certificate-authority/jks/broker.truststore.jks");
    private static final String STORE_PW = "111111";

    private ScheduledExecutorService scheduler;
    private final Executor directExecutor = Runnable::run;
    private Path tempDir;

    @BeforeMethod
    public void setUp() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        tempDir = Files.createTempDirectory("pip478-tls-");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        scheduler.shutdownNow();
        if (tempDir != null) {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    private FileBasedTlsFactory factory(Map<TlsPurpose, TlsPolicy> policies, FileBasedTlsFactorySettings settings) {
        FileBasedTlsFactory factory = new FileBasedTlsFactory(policies, settings);
        factory.initialize(initContext(scheduler, directExecutor)).join();
        return factory;
    }

    @Test
    public void buildsNettyAndJdkContextsFromPem() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        Optional<TlsHandle<SslContext>> netty = factory.createInstance(TlsPurpose.BROKER, SslContext.class).join();
        Optional<TlsHandle<SSLContext>> jdk = factory.createInstance(TlsPurpose.BROKER, SSLContext.class).join();

        assertThat(netty).isPresent();
        assertThat(netty.get().get()).isNotNull();
        assertThat(jdk).isPresent();
        assertThat(jdk.get().get()).isNotNull();
        netty.get().dispose();
        jdk.get().dispose();
        factory.close();
    }

    @Test
    public void buildsContextsFromKeystore() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.keyStore(TRUSTSTORE, STORE_PW, KEYSTORE, STORE_PW, "JKS")),
                FileBasedTlsFactorySettings.defaults());

        Optional<TlsHandle<SslContext>> netty = factory.createInstance(TlsPurpose.BROKER, SslContext.class).join();
        assertThat(netty).isPresent();
        assertThat(netty.get().get()).isNotNull();
        factory.close();
    }

    @Test
    public void resolutionFollowsSingleLevelFallbackChain() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.pem(RSA_CA, null, null)),
                FileBasedTlsFactorySettings.defaults());

        TlsPurpose minted = TlsPurpose.client("oauth2.myPlugin", TlsPurpose.CLIENT_DEFAULT);
        Optional<TlsHandle<SslContext>> resolved = factory.createInstance(minted, SslContext.class).join();
        assertThat(resolved).as("minted purpose resolves via fallback to CLIENT_DEFAULT").isPresent();
        factory.close();
    }

    @Test
    public void oauth2ResolvesToSystemDefaultNotEmptyNotError() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.pem(RSA_CA, null, null)),
                FileBasedTlsFactorySettings.defaults());

        Optional<TlsHandle<SslContext>> handle =
                factory.createInstance(TlsPurpose.CLIENT_OAUTH2, SslContext.class).join();
        assertThat(handle).as("CLIENT_OAUTH2 empty fallback resolves to the system default").isPresent();

        // The system default verifies hostnames (secure defaults) — proven by the baked HTTPS algorithm.
        SSLEngine engine = ((SslContext) handle.get().get()).newEngine(ByteBufAllocator.DEFAULT);
        assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
        factory.close();
    }

    @Test
    public void unconfiguredServerPurposeFailsExceptionally() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        // PROXY (server role, no fallback) is not configured: this is an error, not empty().
        assertThatThrownBy(() -> factory.createInstance(TlsPurpose.PROXY, SslContext.class).join())
                .hasCauseInstanceOf(FileBasedTlsFactory.TlsMaterialUnavailableException.class);
        factory.close();
    }

    @Test
    public void unsupportedClassReturnsEmpty() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        // A class the factory cannot build natively (stands in for Jetty's SslContextFactory.Server,
        // which pulsar-common cannot reference) yields empty(), which the framework synthesizes.
        Optional<TlsHandle<String>> handle = factory.createInstance(TlsPurpose.BROKER, String.class).join();
        assertThat(handle).isEmpty();
        factory.close();
    }

    @Test
    public void resolvedButUnbuildableFailsExceptionallyNeverEmpty() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA,
                        tempDir.resolve("missing-cert.pem").toString(),
                        tempDir.resolve("missing-key.pem").toString())),
                FileBasedTlsFactorySettings.defaults());

        assertThatThrownBy(() -> factory.createInstance(TlsPurpose.BROKER, SslContext.class).join())
                .isInstanceOf(Exception.class);
        factory.close();
    }

    @Test
    public void clientHostnameVerificationIsBakedIntoTheContext() throws Exception {
        FileBasedTlsFactory verifying = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).enableHostnameVerification(true).build()),
                FileBasedTlsFactorySettings.defaults());
        FileBasedTlsFactory notVerifying = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).enableHostnameVerification(false).build()),
                FileBasedTlsFactorySettings.defaults());

        SSLEngine verifyingEngine = ((SslContext) verifying.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class)
                .join().get().get()).newEngine(ByteBufAllocator.DEFAULT);
        SSLEngine plainEngine = ((SslContext) notVerifying.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class)
                .join().get().get()).newEngine(ByteBufAllocator.DEFAULT);

        assertThat(verifyingEngine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
        assertThat(plainEngine.getSSLParameters().getEndpointIdentificationAlgorithm()).isNullOrEmpty();
        verifying.close();
        notVerifying.close();
    }

    @Test
    public void insecureServerAcceptsUntrustedClientAndCapturesItsCert() throws Exception {
        // D3: insecure server installs InsecureTrustManagerFactory but keeps ClientAuth OPTIONAL,
        // so an untrusted (cross-CA) client certificate still completes the handshake and is captured.
        FileBasedTlsFactory server = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(BROKER_CERT).keyFilePath(BROKER_KEY)
                        .allowInsecureConnection(true).build()),
                FileBasedTlsFactorySettings.builder().requireTrustedClientCert(false).build());
        FileBasedTlsFactory client = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(EC_CLIENT_CERT).keyFilePath(EC_CLIENT_KEY)
                        .enableHostnameVerification(false).build()),
                FileBasedTlsFactorySettings.defaults());

        SSLEngine serverEngine = serverEngine(server);
        SSLEngine clientEngine = clientEngine(client);
        handshake(clientEngine, serverEngine);

        assertThat(serverEngine.getSession().getPeerCertificates())
                .as("insecure server captured the untrusted client certificate").isNotEmpty();
        server.close();
        client.close();
    }

    @Test
    public void secureServerDoesNotCaptureUntrustedClientCert() throws Exception {
        // The contrast to the D3 case: a SECURE server (real trust manager) advertises only its trusted
        // CA (RSA) in the CertificateRequest, so the client withholds its cross-CA (EC) certificate and
        // the handshake completes anonymously — the server captures no client certificate.
        FileBasedTlsFactory server = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(BROKER_CERT).keyFilePath(BROKER_KEY)
                        .allowInsecureConnection(false).build()),
                FileBasedTlsFactorySettings.defaults());
        FileBasedTlsFactory client = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(EC_CLIENT_CERT).keyFilePath(EC_CLIENT_KEY)
                        .enableHostnameVerification(false).build()),
                FileBasedTlsFactorySettings.defaults());

        SSLEngine serverEngine = serverEngine(server);
        SSLEngine clientEngine = clientEngine(client);
        handshake(clientEngine, serverEngine);
        assertThatThrownBy(() -> serverEngine.getSession().getPeerCertificates())
                .as("secure server did not capture the untrusted client certificate")
                .isInstanceOf(SSLPeerUnverifiedException.class);
        server.close();
        client.close();
    }

    @Test
    public void rotationDeliversRebuiltInstanceToSubscriber() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());

        List<SslContext> deliveries = new CopyOnWriteArrayList<>();
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, deliveries::add).join();
        assertThat(deliveries).as("initial delivery").hasSize(1);

        overwriteServerCerts(PROXY_CERT, PROXY_KEY);
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> deliveries.size() == 2);
        assertThat(deliveries.get(1)).as("rebuilt on rotation").isNotSameAs(deliveries.get(0));
        factory.close();
    }

    @Test
    public void touchWithoutContentChangeSuppressesReload() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());

        AtomicInteger deliveries = new AtomicInteger();
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, ctx -> deliveries.incrementAndGet()).join();
        assertThat(deliveries.get()).isEqualTo(1);

        // Touch the cert file (advance mtime) without changing its content.
        Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(System.currentTimeMillis() + 5000));
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(4))
                .until(() -> deliveries.get() == 1);
        factory.close();
    }

    @Test
    public void failedRotationKeepsLastGoodThenRecovers() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());

        List<SslContext> deliveries = new CopyOnWriteArrayList<>();
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, deliveries::add).join();
        assertThat(deliveries).hasSize(1);

        // Corrupt the cert file: the rebuild fails, the subscriber keeps the last-good instance.
        Files.writeString(tempDir.resolve("cert.pem"), "-----BEGIN CERTIFICATE-----\nnot a cert\n");
        Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(System.currentTimeMillis() + 5000));
        Awaitility.await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(4))
                .until(() -> deliveries.size() == 1);

        // A subsequent good change is picked up (retry-on-next-change).
        overwriteServerCerts(PROXY_CERT, PROXY_KEY);
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> deliveries.size() == 2);
        factory.close();
    }

    @Test
    public void subscriberCallbackExceptionDoesNotKillSubscription() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());

        AtomicInteger deliveries = new AtomicInteger();
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, ctx -> {
            deliveries.incrementAndGet();
            throw new RuntimeException("boom");
        }).join();
        assertThat(deliveries.get()).as("initial delivery still happened despite throwing callback").isEqualTo(1);

        overwriteServerCerts(PROXY_CERT, PROXY_KEY);
        Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> deliveries.get() == 2);
        factory.close();
    }

    @Test
    public void initialDeliveryHappensBeforeFutureCompletes() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        AtomicInteger deliveries = new AtomicInteger();
        // No await: by the time join() returns, the initial delivery must already have run.
        factory.createInstance(TlsPurpose.BROKER, SslContext.class, ctx -> deliveries.incrementAndGet()).join();
        assertThat(deliveries.get()).isEqualTo(1);
        factory.close();
    }

    @Test
    public void synthesizeNettyFromJdkWrapsTheJdkContext() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());
        SSLContext jdk = (SSLContext) factory.createInstance(TlsPurpose.BROKER, SSLContext.class)
                .join().get().get();

        SslContext synthesized = TlsContexts.synthesizeNettyFromJdk(jdk, false, true);
        assertThat(synthesized).isNotNull();
        assertThat(synthesized.isServer()).isTrue();
        assertThat(synthesized.newEngine(ByteBufAllocator.DEFAULT)).isNotNull();
        factory.close();
    }

    @Test
    public void probeRetainsInitialInstanceAndFailsFastOnBootError() {
        FileBasedTlsFactory ok = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());
        TlsHandle<SslContext> handle = TlsFactoryProbe.probe(ok, TlsPurpose.BROKER, SslContext.class);
        assertThat(handle.get()).isNotNull();
        ok.close();

        FileBasedTlsFactory broken = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA,
                        tempDir.resolve("nope-cert.pem").toString(), tempDir.resolve("nope-key.pem").toString())),
                FileBasedTlsFactorySettings.defaults());
        assertThatThrownBy(() -> TlsFactoryProbe.probe(broken, TlsPurpose.BROKER, SslContext.class))
                .isInstanceOf(IllegalStateException.class);
        broken.close();
    }

    // ---- PIP-478 stage 4c: ports the removed SslContextTest matrix (SslProvider x ciphers x keystore/PEM) ----
    // onto the new FileBasedTlsFactory. OpenSSL rejects the JDK-named TLS 1.2 ciphers used here (matching the
    // removed test's assertion); the JDK engine accepts them, and keystore-format material builds regardless.

    private static final List<String> MATRIX_CIPHERS = List.of(
            "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

    @DataProvider(name = "engineAndCiphers")
    public static Object[][] engineAndCiphers() {
        return new Object[][] {
                {SslProvider.JDK, MATRIX_CIPHERS},
                {SslProvider.JDK, null},
                {SslProvider.OPENSSL, MATRIX_CIPHERS},
                {SslProvider.OPENSSL, null},
        };
    }

    @Test(dataProvider = "engineAndCiphers")
    public void serverPemContextAcrossEngineAndCiphers(SslProvider provider, List<String> ciphers) {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).certificateFilePath(BROKER_CERT).keyFilePath(BROKER_KEY)
                        .ciphers(ciphers).build()),
                FileBasedTlsFactorySettings.builder().engineProvider(provider)
                        .requireTrustedClientCert(true).build());
        assertNettyContextBuildsUnlessOpenSslWithCiphers(factory, TlsPurpose.BROKER, provider, ciphers);
        factory.close();
    }

    @Test(dataProvider = "engineAndCiphers")
    public void clientPemContextAcrossEngineAndCiphers(SslProvider provider, List<String> ciphers) {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .trustCertsFilePath(RSA_CA).allowInsecureConnection(true)
                        .ciphers(ciphers).build()),
                FileBasedTlsFactorySettings.builder().engineProvider(provider).build());
        assertNettyContextBuildsUnlessOpenSslWithCiphers(factory, TlsPurpose.CLIENT_DEFAULT, provider, ciphers);
        factory.close();
    }

    @Test
    public void serverKeystoreContextBuildsWithCiphers() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.builder()
                        .format(TlsPolicy.Format.KEYSTORE).storeType("JKS")
                        .trustStorePath(TRUSTSTORE).trustStorePassword(STORE_PW)
                        .keyStorePath(KEYSTORE).keyStorePassword(STORE_PW)
                        .ciphers(MATRIX_CIPHERS).build()),
                FileBasedTlsFactorySettings.builder().requireTrustedClientCert(true).build());
        Optional<TlsHandle<SslContext>> handle = factory.createInstance(TlsPurpose.BROKER, SslContext.class).join();
        assertThat(handle).isPresent();
        assertThat(handle.get().get()).isNotNull();
        handle.get().dispose();
        factory.close();
    }

    @Test
    public void clientKeystoreContextBuildsWithCiphers() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.CLIENT_DEFAULT, TlsPolicy.builder()
                        .format(TlsPolicy.Format.KEYSTORE).storeType("JKS")
                        .trustStorePath(TRUSTSTORE).trustStorePassword(STORE_PW)
                        .ciphers(MATRIX_CIPHERS).build()),
                FileBasedTlsFactorySettings.defaults());
        Optional<TlsHandle<SslContext>> handle =
                factory.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class).join();
        assertThat(handle).isPresent();
        assertThat(handle.get().get()).isNotNull();
        handle.get().dispose();
        factory.close();
    }

    private static void assertNettyContextBuildsUnlessOpenSslWithCiphers(FileBasedTlsFactory factory,
            TlsPurpose purpose, SslProvider provider, List<String> ciphers) {
        if (ciphers != null && provider == SslProvider.OPENSSL) {
            // OpenSSL does not support these JDK-named TLS 1.2 ciphers (as the removed SslContextTest asserted).
            assertThatThrownBy(() -> factory.createInstance(purpose, SslContext.class).join())
                    .hasCauseInstanceOf(SSLException.class);
            return;
        }
        Optional<TlsHandle<SslContext>> handle = factory.createInstance(purpose, SslContext.class).join();
        assertThat(handle).isPresent();
        assertThat(handle.get().get()).isNotNull();
        handle.get().dispose();
    }

    private SSLEngine serverEngine(FileBasedTlsFactory factory) {
        SslContext ctx = (SslContext) factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().get().get();
        SSLEngine engine = ctx.newEngine(ByteBufAllocator.DEFAULT);
        engine.setUseClientMode(false);
        return engine;
    }

    private SSLEngine clientEngine(FileBasedTlsFactory factory) {
        SslContext ctx = (SslContext) factory.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class)
                .join().get().get();
        SSLEngine engine = ctx.newEngine(ByteBufAllocator.DEFAULT);
        engine.setUseClientMode(true);
        return engine;
    }

    private TlsPolicy copyServerCertsToTemp(String certSrc, String keySrc) throws Exception {
        Files.copy(Paths.get(RSA_CA), tempDir.resolve("ca.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(certSrc), tempDir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(keySrc), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        return TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString());
    }

    private void overwriteServerCerts(String certSrc, String keySrc) throws Exception {
        Files.copy(Paths.get(certSrc), tempDir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(keySrc), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        long later = System.currentTimeMillis() + 5000;
        Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(later));
        Files.setLastModifiedTime(tempDir.resolve("key.pem"), FileTime.fromMillis(later));
    }
}
