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

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.io.Resources;
import java.io.File;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import org.testng.annotations.Test;

/**
 * PIP-478 (FIX): {@link JdkSslContexts#createSslContextWithProvider} pins the {@code KeyManagerFactory} to the
 * resolved jsseProvider (like the {@code SSLContext} and {@code TrustManagerFactory}), while the null-provider
 * path is unchanged so an unset jsseProvider does not regress.
 */
public class JdkSslContextsTest {

    private static String resource(String name) {
        return new File(Resources.getResource(name).getPath()).getAbsolutePath();
    }

    private static final String RSA_CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");

    // With a client key present (cert + key non-null) setupKeyManager builds a KeyManagerFactory; FIX 5 pins it
    // to the resolved provider. SunJSSE supplies the default KMF algorithm, so the build succeeds and the whole
    // context (SSLContext + KMF + TMF) is backed by the pinned JSSE provider — the FIPS JSSE-provider (BCJSSE) intent.
    @Test
    public void pinsKeyManagerFactoryToTheProviderWithClientKey() throws Exception {
        X509Certificate[] trust = PemReader.loadCertificatesFromPemFile(RSA_CA);
        X509Certificate[] cert = PemReader.loadCertificatesFromPemFile(BROKER_CERT);
        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(BROKER_KEY);
        Provider sunjsse = Security.getProvider("SunJSSE");
        assertThat(sunjsse).as("SunJSSE is installed in every JVM").isNotNull();

        SSLContext ctx = JdkSslContexts.createSslContextWithProvider(false, trust, cert, key, sunjsse);
        assertThat(ctx).isNotNull();
        assertThat(ctx.getProvider().getName())
                .as("the SSLContext (and now the KeyManagerFactory) is backed by the pinned provider")
                .isEqualTo("SunJSSE");
    }

    // Regression guard: provider == null keeps the KeyManagerFactory on the default (no-provider) form, so an
    // unset jsseProvider is unaffected by FIX 5.
    @Test
    public void nullProviderPathUnchanged() throws Exception {
        X509Certificate[] trust = PemReader.loadCertificatesFromPemFile(RSA_CA);
        X509Certificate[] cert = PemReader.loadCertificatesFromPemFile(BROKER_CERT);
        PrivateKey key = PemReader.loadPrivateKeyFromPemFile(BROKER_KEY);

        SSLContext ctx = JdkSslContexts.createSslContextWithProvider(false, trust, cert, key, null);
        assertThat(ctx).isNotNull();
    }
}
