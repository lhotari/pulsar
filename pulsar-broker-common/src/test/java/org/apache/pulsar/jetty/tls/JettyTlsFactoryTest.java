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
import com.google.common.io.Resources;
import io.opentelemetry.api.OpenTelemetry;
import java.io.File;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.common.tls.TlsFactoryInitContext;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.common.util.SecurityUtility;
import org.awaitility.Awaitility;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class JettyTlsFactoryTest {

    private static final String CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String PROXY_CERT = resource("certificate-authority/server-keys/proxy.cert.pem");
    private static final String PROXY_KEY = resource("certificate-authority/server-keys/proxy.key-pk8.pem");

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
                factory, TlsPurpose.WEB, null, false, null, null);

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
