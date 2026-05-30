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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationCallContext;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthData;
import org.apache.pulsar.client.api.v5.auth.BinaryProtocolAuthDataProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Verifies that {@link LegacyV4AuthenticationAdapter#wrap} bridges a legacy v4
 * {@link org.apache.pulsar.client.api.Authentication} plugin onto the new asynchronous v5
 * {@link Authentication} SPI: the wrapped plugin must expose the
 * {@link BinaryProtocolAuthDataProvider} capability, advertise the v4 method name, and serve the
 * credential bytes through the async {@code getAuthDataAsync} path (the old v5 glue still called the
 * removed synchronous {@code authData()} surface and failed to compile).
 */
public class LegacyV4AuthenticationAdapterTest {

    private ScheduledExecutorService scheduler;

    @BeforeClass
    public void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "legacy-v4-auth-adapter-test");
            t.setDaemon(true);
            return t;
        });
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Test
    public void tokenAuthIsWrappedAsBinaryProtocolProvider() throws Exception {
        Authentication v5Auth = LegacyV4AuthenticationAdapter.wrap(new AuthenticationToken("my-jwt"));
        v5Auth.initializeAsync(initContext()).get();

        Optional<BinaryProtocolAuthDataProvider> provider =
                v5Auth.capability(BinaryProtocolAuthDataProvider.class);
        assertTrue(provider.isPresent(), "token auth must expose the binary-protocol capability");
        assertEquals(provider.get().authMethodName(), "token");

        BinaryProtocolAuthData data = provider.get().getAuthDataAsync(callContext()).get();
        assertEquals(data.authMethodName(), "token");
        assertEquals(new String(data.authData(), UTF_8), "my-jwt");
    }

    @Test
    public void wrapSelectsCredentialAdapterForToken() {
        Authentication v5Auth = LegacyV4AuthenticationAdapter.wrap(new AuthenticationToken("any"));
        assertTrue(v5Auth instanceof LegacyV4AuthenticationAdapter.LegacyV4CredentialAdapter,
                "a plain token plugin must be wrapped as a credential adapter, got "
                        + v5Auth.getClass().getName());
    }

    private SimpleAuthInitContext initContext() {
        return new SimpleAuthInitContext(null, scheduler, Clock.systemUTC(), OpenTelemetry.noop(),
                "test-client", Map.of());
    }

    private AuthenticationCallContext callContext() {
        return new SimpleAuthCallContext("broker-1.example.com", 6650);
    }
}
