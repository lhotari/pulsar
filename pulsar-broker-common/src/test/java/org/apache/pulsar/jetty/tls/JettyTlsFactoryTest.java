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
package org.apache.pulsar.jetty.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.io.Resources;
import io.opentelemetry.api.OpenTelemetry;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsFactoryInitContext;
import org.apache.pulsar.common.tls.TlsHandle;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.common.util.SecurityUtility;
import org.awaitility.Awaitility;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JettyTlsFactoryTest {

    private static final String CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String PROXY_CERT = resource("certificate-authority/server-keys/proxy.cert.pem");
    private static final String PROXY_KEY = resource("certificate-authority/server-keys/proxy.key-pk8.pem");
    // Client certificates (clientAuth EKU) trusted by the shared CA above.
    private static final String TRUSTED_CLIENT_CERT = resource("certificate-authority/client-keys/admin.cert.pem");
    private static final String TRUSTED_CLIENT_KEY = resource("certificate-authority/client-keys/admin.key-pk8.pem");
    private static final String USER1_CLIENT_CERT = resource("certificate-authority/client-keys/user1.cert.pem");
    private static final String USER1_CLIENT_KEY = resource("certificate-authority/client-keys/user1.key-pk8.pem");
    // A self-signed client certificate NOT signed by the shared CA (from a separate my-ca root).
    private static final String UNTRUSTED_CLIENT_CERT = resource("ssl/my-ca/client-ca.pem");
    private static final String UNTRUSTED_CLIENT_KEY = resource("ssl/my-ca/client-key.pem");

    private ScheduledExecutorService scheduler;
    private Path tempDir;

    @BeforeMethod
    public void setUp() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        tempDir = Files.createTempDirectory("pip478-jetty-");
        Files.copy(Paths.get(CA), tempDir.resolve("ca.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(BROKER_CERT), tempDir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(BROKER_KEY), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        scheduler.shutdownNow();
        if (tempDir != null) {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    @Test
    public void reloadingServerFactoryServesRotatedCertificateToNewConnections() throws Exception {
        FileBasedTlsFactory factory = new FileBasedTlsFactory(
                Map.of(TlsPurpose.WEB, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString())),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());
        factory.initialize(initContext()).join();

        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, null, false, false, null, null);

        Server server = new Server();
        ServerConnector connector = new ServerConnector(server, reloadable.sslContextFactory());
        connector.setPort(0);
        server.setConnectors(new ServerConnector[] {connector});
        server.start();
        try {
            SSLContext clientContext = SecurityUtility.createSslContext(false,
                    SecurityUtility.loadCertificatesFromPemFile(CA), null, null);
            int port = connector.getLocalPort();

            BigInteger initialSerial = serverCertSerial(clientContext, port);
            assertThat(initialSerial).isEqualTo(certSerial(BROKER_CERT));

            // Rotate the server material to a different (proxy) certificate signed by the same CA.
            Files.copy(Paths.get(PROXY_CERT), tempDir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(Paths.get(PROXY_KEY), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
            long later = System.currentTimeMillis() + 5000;
            Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(later));
            Files.setLastModifiedTime(tempDir.resolve("key.pem"), FileTime.fromMillis(later));

            BigInteger proxySerial = certSerial(PROXY_CERT);
            Awaitility.await().atMost(Duration.ofSeconds(15))
                    .until(() -> serverCertSerial(clientContext, port).equals(proxySerial));
            assertThat(serverCertSerial(clientContext, port))
                    .as("new connections use the rotated certificate")
                    .isEqualTo(proxySerial).isNotEqualTo(initialSerial);
        } finally {
            reloadable.subscription().dispose();
            server.stop();
            factory.close();
        }
    }

    /**
     * PIP-478 (F3): a self-reloading {@link SslContextFactory.Client} presents rotated client-certificate
     * material to new connections while the factory stays alive (mirrors the stage-2a server rotation test).
     */
    @Test
    public void reloadingClientFactoryPresentsRotatedClientCertOnNewConnections() throws Exception {
        // Distinct filenames from the server rotation test's cert.pem/key.pem; these hold a client-auth
        // certificate (the broker/proxy server certs carry a serverAuth-only EKU and cannot be presented as
        // a client identity).
        Path clientCert = tempDir.resolve("clientcert.pem");
        Path clientKey = tempDir.resolve("clientkey.pem");
        Files.copy(Paths.get(TRUSTED_CLIENT_CERT), clientCert, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(TRUSTED_CLIENT_KEY), clientKey, StandardCopyOption.REPLACE_EXISTING);

        FileBasedTlsFactory factory = new FileBasedTlsFactory(
                Map.of(TlsPurpose.BROKER_CLIENT, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        clientCert.toString(), clientKey.toString())),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());
        factory.initialize(initContext()).join();

        JettyTlsFactory.ReloadableClientTls reloadable = JettyTlsFactory.createReloadingClientFactory(
                factory, TlsPurpose.BROKER_CLIENT, null);
        SslContextFactory.Client clientFactory = reloadable.sslContextFactory();
        clientFactory.start();
        try {
            assertThat(presentedClientCertSerial(clientFactory)).isEqualTo(certSerial(TRUSTED_CLIENT_CERT));

            // Rotate the client identity to a different client certificate signed by the same CA.
            Files.copy(Paths.get(USER1_CLIENT_CERT), clientCert, StandardCopyOption.REPLACE_EXISTING);
            Files.copy(Paths.get(USER1_CLIENT_KEY), clientKey, StandardCopyOption.REPLACE_EXISTING);
            long later = System.currentTimeMillis() + 5000;
            Files.setLastModifiedTime(clientCert, FileTime.fromMillis(later));
            Files.setLastModifiedTime(clientKey, FileTime.fromMillis(later));

            BigInteger rotatedSerial = certSerial(USER1_CLIENT_CERT);
            Awaitility.await().atMost(Duration.ofSeconds(15))
                    .until(() -> presentedClientCertSerial(clientFactory).equals(rotatedSerial));
            assertThat(presentedClientCertSerial(clientFactory))
                    .as("new connections present the rotated client certificate")
                    .isEqualTo(rotatedSerial).isNotEqualTo(certSerial(TRUSTED_CLIENT_CERT));
        } finally {
            clientFactory.stop();
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    // Handshake the reloading client factory's current context against a client-auth-requiring server and
    // return the serial of the client certificate the server observed.
    private BigInteger presentedClientCertSerial(SslContextFactory.Client clientFactory) throws Exception {
        SSLContext serverContext = SecurityUtility.createSslContext(false, CA, BROKER_CERT, BROKER_KEY, null);
        try (SSLServerSocket serverSocket =
                     (SSLServerSocket) serverContext.getServerSocketFactory().createServerSocket(0)) {
            serverSocket.setNeedClientAuth(true);
            // TLSv1.2 so client auth completes symmetrically within the handshake (avoids the TLS 1.3
            // post-handshake close race where the server closes before the client flushes its Finished).
            serverSocket.setEnabledProtocols(new String[] {"TLSv1.2"});
            serverSocket.setSoTimeout(15000);
            int port = serverSocket.getLocalPort();
            CompletableFuture<BigInteger> serverSaw = new CompletableFuture<>();
            Thread serverThread = new Thread(() -> {
                try (SSLSocket accepted = (SSLSocket) serverSocket.accept()) {
                    accepted.setSoTimeout(15000);
                    accepted.startHandshake();
                    X509Certificate peer = (X509Certificate) accepted.getSession().getPeerCertificates()[0];
                    serverSaw.complete(peer.getSerialNumber());
                } catch (Throwable t) {
                    serverSaw.completeExceptionally(t);
                }
            });
            serverThread.setDaemon(true);
            serverThread.start();

            SSLContext clientContext = clientFactory.getSslContext();
            try (SSLSocket clientSocket =
                         (SSLSocket) clientContext.getSocketFactory().createSocket("localhost", port)) {
                clientSocket.setEnabledProtocols(new String[] {"TLSv1.2"});
                clientSocket.setSoTimeout(15000);
                clientSocket.startHandshake();
            }
            return serverSaw.get(15, TimeUnit.SECONDS);
        }
    }

    /**
     * PIP-478 (F2): under optional client auth ({@code requireTrustedClientCert=false}), an <em>untrusted</em>
     * client certificate is rejected at the web listener's handshake when {@code allowInsecureConnection=false}
     * and accepted when it is true; a trusted client certificate is accepted in both cases.
     */
    @Test
    public void optionalClientAuthScopesTrustAllToInsecureFlag() throws Exception {
        // Secure (insecure=false): untrusted client cert is rejected, trusted client cert is accepted. The
        // server aborts the handshake, surfaced to the client either as an SSLHandshakeException or, once the
        // server has already sent its close/alert, as a broken-pipe SocketException — both are handshake
        // failures.
        try (JettyServer secure = startWebServer(false)) {
            assertThatThrownBy(() -> handshakeWithClientCert(secure.port, UNTRUSTED_CLIENT_CERT, UNTRUSTED_CLIENT_KEY))
                    .as("an untrusted client cert must be rejected when insecure=false")
                    .isInstanceOf(IOException.class);
            handshakeWithClientCert(secure.port, TRUSTED_CLIENT_CERT, TRUSTED_CLIENT_KEY);
        }
        // Insecure (insecure=true): both untrusted and trusted client certs are accepted (trust-all).
        try (JettyServer insecure = startWebServer(true)) {
            handshakeWithClientCert(insecure.port, UNTRUSTED_CLIENT_CERT, UNTRUSTED_CLIENT_KEY);
            handshakeWithClientCert(insecure.port, TRUSTED_CLIENT_CERT, TRUSTED_CLIENT_KEY);
        }
    }

    /**
     * PIP-478: a factory that supplies its {@code SSLContext} together with an {@code SSLParameters} companion
     * drives the synthesized server {@link SslContextFactory.Server} — its enabled protocols/ciphers and its
     * client-auth mode are mapped from the companion, the latter authoritatively (merge rule 4).
     */
    @Test
    public void serverFactoryMapsCompanionProtocolsAndClientAuth() throws Exception {
        SSLParameters companion = new SSLParameters();
        companion.setProtocols(new String[] {"TLSv1.2"});
        companion.setCipherSuites(new String[] {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"});
        companion.setNeedClientAuth(true);

        FileBasedTlsFactory delegate = new FileBasedTlsFactory(
                Map.of(TlsPurpose.WEB, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString())),
                FileBasedTlsFactorySettings.builder().build());
        CompanionFactory factory = new CompanionFactory(delegate, companion);
        factory.initialize(initContext()).join();

        // Consumer config asks for no protocol/cipher restriction and only optional client auth; the companion
        // overrides all three.
        JettyTlsFactory.ReloadableServerTls reloadable = JettyTlsFactory.createReloadingServerFactory(
                factory, TlsPurpose.WEB, null, false, false, null, null);
        SslContextFactory.Server sslContextFactory = reloadable.sslContextFactory();
        try {
            assertThat(sslContextFactory.getIncludeProtocols()).containsExactly("TLSv1.2");
            assertThat(sslContextFactory.getIncludeCipherSuites())
                    .containsExactly("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
            assertThat(sslContextFactory.getNeedClientAuth()).isTrue();
        } finally {
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    /**
     * PIP-478: the synthesized client {@link SslContextFactory.Client} maps the companion's enabled
     * protocols/ciphers (client-auth is a server concept and is not mapped on the client factory).
     */
    @Test
    public void clientFactoryMapsCompanionProtocols() throws Exception {
        Path clientCert = tempDir.resolve("clientcert.pem");
        Path clientKey = tempDir.resolve("clientkey.pem");
        Files.copy(Paths.get(TRUSTED_CLIENT_CERT), clientCert, StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(TRUSTED_CLIENT_KEY), clientKey, StandardCopyOption.REPLACE_EXISTING);

        SSLParameters companion = new SSLParameters();
        companion.setProtocols(new String[] {"TLSv1.2"});

        FileBasedTlsFactory delegate = new FileBasedTlsFactory(
                Map.of(TlsPurpose.BROKER_CLIENT, TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                        clientCert.toString(), clientKey.toString())),
                FileBasedTlsFactorySettings.builder().build());
        CompanionFactory factory = new CompanionFactory(delegate, companion);
        factory.initialize(initContext()).join();

        JettyTlsFactory.ReloadableClientTls reloadable = JettyTlsFactory.createReloadingClientFactory(
                factory, TlsPurpose.BROKER_CLIENT, null);
        try {
            assertThat(reloadable.sslContextFactory().getIncludeProtocols()).containsExactly("TLSv1.2");
        } finally {
            reloadable.subscription().dispose();
            factory.close();
        }
    }

    /**
     * A factory that delegates every request to a {@link FileBasedTlsFactory} except the {@code SSLParameters}
     * companion, which it supplies from a fixed instance (the file-based factory returns {@code empty()} for it).
     */
    private static final class CompanionFactory implements PulsarTlsFactory {
        private final FileBasedTlsFactory delegate;
        private final SSLParameters companion;

        CompanionFactory(FileBasedTlsFactory delegate, SSLParameters companion) {
            this.delegate = delegate;
            this.companion = companion;
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            return delegate.initialize(context);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                                                                            Class<T> instanceClass) {
            if (instanceClass == SSLParameters.class) {
                return CompletableFuture.completedFuture(Optional.of((TlsHandle<T>) companionHandle()));
            }
            return delegate.createInstance(purpose, instanceClass);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
                TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return delegate.createInstance(purpose, instanceClass, onLoadOrReload);
        }

        private TlsHandle<SSLParameters> companionHandle() {
            return new TlsHandle<>() {
                @Override
                public SSLParameters get() {
                    return companion;
                }

                @Override
                public void dispose() {
                }
            };
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /** A running Jetty HTTPS server (WEB purpose, optional client auth) and its resources. */
    private final class JettyServer implements AutoCloseable {
        private final Server server;
        private final FileBasedTlsFactory factory;
        private final JettyTlsFactory.ReloadableServerTls reloadable;
        private final int port;

        JettyServer(boolean allowInsecureConnection) throws Exception {
            // The trust gate lives in the WEB SSLContext's trust managers (built from the policy's insecure
            // flag), which the framework hands to Jetty via setSslContext — Jetty's own setTrustAll is inert
            // on that path. So the policy's allowInsecureConnection is what actually scopes client-cert trust.
            factory = new FileBasedTlsFactory(
                    Map.of(TlsPurpose.WEB, TlsPolicy.builder()
                            .format(TlsPolicy.Format.PEM)
                            .trustCertsFilePath(CA).certificateFilePath(BROKER_CERT).keyFilePath(BROKER_KEY)
                            .allowInsecureConnection(allowInsecureConnection)
                            .enableHostnameVerification(false)
                            .build()),
                    FileBasedTlsFactorySettings.builder().build());
            factory.initialize(initContext()).join();
            // Optional client auth (requireTrustedClientCert=false); TLSv1.2 so an untrusted-cert rejection
            // surfaces synchronously in the client handshake rather than as a post-handshake alert.
            reloadable = JettyTlsFactory.createReloadingServerFactory(factory, TlsPurpose.WEB, null,
                    false, allowInsecureConnection, null, Set.of("TLSv1.2"));
            server = new Server();
            ServerConnector connector = new ServerConnector(server, reloadable.sslContextFactory());
            connector.setPort(0);
            server.setConnectors(new ServerConnector[] {connector});
            server.start();
            port = connector.getLocalPort();
        }

        @Override
        public void close() throws Exception {
            reloadable.subscription().dispose();
            server.stop();
            factory.close();
        }
    }

    private JettyServer startWebServer(boolean allowInsecureConnection) throws Exception {
        return new JettyServer(allowInsecureConnection);
    }

    // Connect presenting a client certificate over TLSv1.2 and complete the handshake (throws on rejection).
    // The key manager is forced to always present the given certificate regardless of the server's advertised
    // acceptable-CA list, so an untrusted certificate is actually sent (and therefore rejected) rather than
    // silently withheld by the default JSSE key manager.
    private void handshakeWithClientCert(int port, String clientCert, String clientKey) throws Exception {
        SSLContext clientContext = forcingClientContext(clientCert, clientKey);
        SSLSocketFactory socketFactory = clientContext.getSocketFactory();
        try (SSLSocket socket = (SSLSocket) socketFactory.createSocket("localhost", port)) {
            socket.setEnabledProtocols(new String[] {"TLSv1.2"});
            socket.setSoTimeout(10000);
            socket.startHandshake();
        }
    }

    private static SSLContext forcingClientContext(String clientCert, String clientKey) throws Exception {
        X509Certificate[] chain = SecurityUtility.loadCertificatesFromPemFile(clientCert);
        PrivateKey key = SecurityUtility.loadPrivateKeyFromPemFile(clientKey);
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);
        X509Certificate[] ca = SecurityUtility.loadCertificatesFromPemFile(CA);
        for (int i = 0; i < ca.length; i++) {
            trustStore.setCertificateEntry("ca-" + i, ca[i]);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        tmf.init(trustStore);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(new KeyManager[] {new ForcingKeyManager(chain, key)}, tmf.getTrustManagers(), null);
        return context;
    }

    /** A key manager that always presents a fixed certificate chain, ignoring the server's CA hints. */
    private static final class ForcingKeyManager extends X509ExtendedKeyManager {
        private static final String ALIAS = "client";
        private final X509Certificate[] chain;
        private final PrivateKey key;

        ForcingKeyManager(X509Certificate[] chain, PrivateKey key) {
            this.chain = chain;
            this.key = key;
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return new String[] {ALIAS};
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
            return ALIAS;
        }

        @Override
        public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
            return ALIAS;
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            return null;
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            return null;
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            return chain;
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            return key;
        }
    }

    private static BigInteger serverCertSerial(SSLContext clientContext, int port) throws Exception {
        SSLSocketFactory socketFactory = clientContext.getSocketFactory();
        try (SSLSocket socket = (SSLSocket) socketFactory.createSocket("localhost", port)) {
            socket.setSoTimeout(10000);
            socket.startHandshake();
            X509Certificate leaf = (X509Certificate) socket.getSession().getPeerCertificates()[0];
            return leaf.getSerialNumber();
        }
    }

    private static BigInteger certSerial(String pemPath) throws Exception {
        return SecurityUtility.loadCertificatesFromPemFile(pemPath)[0].getSerialNumber();
    }

    private TlsFactoryInitContext initContext() {
        Executor direct = Runnable::run;
        return new TlsFactoryInitContext() {
            @Override
            public Map<String, String> params() {
                return Map.of();
            }

            @Override
            public ScheduledExecutorService scheduler() {
                return scheduler;
            }

            @Override
            public Executor blockingExecutor() {
                return direct;
            }

            @Override
            public Clock clock() {
                return Clock.systemUTC();
            }

            @Override
            public OpenTelemetry openTelemetry() {
                return OpenTelemetry.noop();
            }
        };
    }

    private static String resource(String name) {
        return new File(Resources.getResource(name).getPath()).getAbsolutePath();
    }
}
