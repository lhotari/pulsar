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
package org.apache.pulsar.client.impl.v5.auth;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Verifies that {@link LegacyV4AuthenticationAdapter#wrap} bridges a legacy v4
 * {@link org.apache.pulsar.client.api.Authentication} plugin onto the new asynchronous v5
 * {@link Authentication} SPI: the wrapped plugin exposes the {@link BinaryAuthDataProvider} capability,
 * advertises the v4 method name, and serves the credential bytes through the async
 * {@code getAuthDataAsync} path.
 */
public class LegacyV4AuthenticationAdapterTest {

    private ScheduledExecutorService blockingExecutor;

    @BeforeClass
    public void setUp() {
        blockingExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "legacy-v4-auth-adapter-test");
            t.setDaemon(true);
            return t;
        });
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() {
        if (blockingExecutor != null) {
            blockingExecutor.shutdownNow();
        }
    }

    @Test
    public void tokenAuthIsWrappedAsBinaryDataProvider() throws Exception {
        Authentication v5Auth = LegacyV4AuthenticationAdapter.wrap(new AuthenticationToken("my-jwt"));
        v5Auth.initializeAsync(initContext()).get();

        Optional<BinaryAuthDataProvider> provider = v5Auth.capability(BinaryAuthDataProvider.class);
        assertThat(provider).isPresent();
        assertThat(provider.get().authMethodName()).isEqualTo("token");

        BinaryAuthData data = provider.get().getAuthDataAsync(callContext()).get();
        assertThat(new String(data.authData(), UTF_8)).isEqualTo("my-jwt");
    }

    @Test
    public void wrapSelectsCredentialAdapterForToken() {
        Authentication v5Auth = LegacyV4AuthenticationAdapter.wrap(new AuthenticationToken("any"));
        assertThat(v5Auth).isInstanceOf(LegacyV4AuthenticationAdapter.LegacyV4CredentialAdapter.class);
    }

    @Test
    public void wrapSelectsTlsAdapterForTlsAndUnwrapsBackToV4() {
        // A v4 plugin that reports hasDataForTls() is bridged onto the built-in TlsAuthentication plugin
        // (auth_method_name = "tls"); a fake avoids the eager cert/key file load of AuthenticationTls.
        org.apache.pulsar.client.api.Authentication v4 = new FakeTlsV4Auth();
        Authentication v5Auth = LegacyV4AuthenticationAdapter.wrap(v4);
        assertThat(v5Auth).isInstanceOf(TlsAuthentication.class);
        assertThat(v5Auth.capability(BinaryAuthDataProvider.class))
                .get()
                .extracting(BinaryAuthDataProvider::authMethodName)
                .isEqualTo(TlsAuthentication.DEFAULT_AUTH_METHOD_NAME);
        // The builder must be able to route the bridged plugin back onto the v4 client verbatim.
        assertThat(LegacyV4AuthenticationAdapter.unwrapV4(v5Auth)).containsSame(v4);
    }

    @Test
    public void unwrapV4ReturnsEmptyForNativePlugin() {
        assertThat(LegacyV4AuthenticationAdapter.unwrapV4(new TlsAuthentication())).isEmpty();
    }

    /** A minimal v4 plugin that reports TLS material without touching the filesystem. */
    // Implements the deprecated v4 configure(Map) by design.
    @SuppressWarnings("deprecation")
    private static final class FakeTlsV4Auth implements org.apache.pulsar.client.api.Authentication {
        @Override
        public String getAuthMethodName() {
            return "tls";
        }

        @Override
        public AuthenticationDataProvider getAuthData(String brokerHostName) {
            return new AuthenticationDataProvider() {
                @Override
                public boolean hasDataForTls() {
                    return true;
                }
            };
        }

        @Override
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() {
        }

        @Override
        public void close() {
        }
    }

    private SimpleAuthInitContext initContext() {
        return new SimpleAuthInitContext(null, blockingExecutor, blockingExecutor, Clock.systemUTC(),
                OpenTelemetry.noop(), "test-client", Map.of());
    }

    private AuthenticationCallContext callContext() {
        return new SimpleAuthCallContext("broker-1.example.com", 6650);
    }
}
