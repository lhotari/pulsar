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
package org.apache.pulsar.broker.tls;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.io.Resources;
import io.netty.handler.ssl.SslContext;
import io.opentelemetry.api.OpenTelemetry;
import java.io.File;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.net.ssl.SSLContext;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.tls.TlsFactoryInitContext;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DefaultBrokerTlsFactoryTest {

    private static final String CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String ADMIN_CERT = resource("certificate-authority/client-keys/admin.cert.pem");
    private static final String ADMIN_KEY = resource("certificate-authority/client-keys/admin.key-pk8.pem");
    private static final String KEYSTORE = resource("certificate-authority/jks/broker.keystore.jks");
    private static final String TRUSTSTORE = resource("certificate-authority/jks/broker.truststore.jks");

    private ScheduledExecutorService scheduler;

    @BeforeMethod
    public void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    public void composesAllPurposesFromPemConfig() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setTlsTrustCertsFilePath(CA);
        conf.setTlsCertificateFilePath(BROKER_CERT);
        conf.setTlsKeyFilePath(BROKER_KEY);
        conf.setBrokerClientTrustCertsFilePath(CA);
        conf.setBrokerClientCertificateFilePath(ADMIN_CERT);
        conf.setBrokerClientKeyFilePath(ADMIN_KEY);

        DefaultBrokerTlsFactory factory = DefaultBrokerTlsFactory.fromServiceConfiguration(conf);
        factory.initialize(initContext()).join();

        // Server purposes build server contexts; the broker-client purpose builds a client context.
        for (TlsPurpose purpose : new TlsPurpose[] {TlsPurpose.BROKER, TlsPurpose.PROXY, TlsPurpose.WEB,
                TlsPurpose.BROKER_CLIENT}) {
            assertThat(factory.createInstance(purpose, SslContext.class).join())
                    .as("Netty context for " + purpose).isPresent();
            assertThat(factory.createInstance(purpose, SSLContext.class).join())
                    .as("JDK context for " + purpose).isPresent();
        }
        factory.close();
    }

    @Test
    public void composesServerPurposesFromKeystoreConfig() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setTlsEnabledWithKeyStore(true);
        conf.setTlsKeyStoreType("JKS");
        conf.setTlsKeyStore(KEYSTORE);
        conf.setTlsKeyStorePassword("111111");
        conf.setTlsTrustStoreType("JKS");
        conf.setTlsTrustStore(TRUSTSTORE);
        conf.setTlsTrustStorePassword("111111");

        DefaultBrokerTlsFactory factory = DefaultBrokerTlsFactory.fromServiceConfiguration(conf);
        factory.initialize(initContext()).join();

        assertThat(factory.createInstance(TlsPurpose.BROKER, SslContext.class).join()).isPresent();
        assertThat(factory.createInstance(TlsPurpose.WEB, SSLContext.class).join()).isPresent();
        factory.close();
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
