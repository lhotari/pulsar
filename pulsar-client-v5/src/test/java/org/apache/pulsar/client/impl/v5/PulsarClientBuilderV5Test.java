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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Map;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.config.ConnectionPolicy;
import org.apache.pulsar.client.impl.v5.auth.LegacyV4AuthenticationAdapter;
import org.apache.pulsar.common.tls.TlsPolicy;
import org.testng.annotations.Test;

/**
 * Service-URL validation on the V5 client builder. The v5 client only speaks the
 * broker binary protocol, so {@code pulsar://} / {@code pulsar+ssl://} are the
 * only valid schemes — anything else (especially the admin/web service URL) gets
 * rejected at configure-time with a message that points to the right URL.
 */
public class PulsarClientBuilderV5Test {

    @Test
    public void testAcceptsPulsarScheme() {
        // Must not throw — these are the valid forms.
        PulsarClient.builder().serviceUrl("pulsar://localhost:6650");
        PulsarClient.builder().serviceUrl("pulsar+ssl://localhost:6651");
        PulsarClient.builder().serviceUrl("pulsar://h1:6650,h2:6650,h3:6650");
    }

    @Test
    public void testRejectsHttpWithGuidance() {
        IllegalArgumentException e = assertThrowsIAE(() ->
                PulsarClient.builder().serviceUrl("http://localhost:8080"));
        assertTrue(e.getMessage().contains("pulsar://"),
                "error must point at the correct scheme: " + e.getMessage());
        assertTrue(e.getMessage().toLowerCase().contains("admin")
                        || e.getMessage().toLowerCase().contains("web"),
                "error must call out the http→admin-URL confusion: " + e.getMessage());
        assertTrue(e.getMessage().contains("6650"),
                "error must hint at the broker port: " + e.getMessage());
    }

    @Test
    public void testRejectsHttpsWithGuidance() {
        IllegalArgumentException e = assertThrowsIAE(() ->
                PulsarClient.builder().serviceUrl("https://localhost:8443"));
        assertTrue(e.getMessage().contains("pulsar+ssl://"),
                "error must mention the TLS broker scheme: " + e.getMessage());
    }

    @Test
    public void testRejectsUnknownScheme() {
        IllegalArgumentException e = assertThrowsIAE(() ->
                PulsarClient.builder().serviceUrl("ws://localhost:6650"));
        assertTrue(e.getMessage().contains("pulsar://"),
                "error must point at the correct scheme: " + e.getMessage());
    }

    @Test
    public void testRejectsNullAndBlank() {
        assertThrows(IllegalArgumentException.class,
                () -> PulsarClient.builder().serviceUrl(null));
        assertThrows(IllegalArgumentException.class,
                () -> PulsarClient.builder().serviceUrl(""));
        assertThrows(IllegalArgumentException.class,
                () -> PulsarClient.builder().serviceUrl("   "));
    }

    @Test
    public void testProxyServiceUrlIsValidatedToo() {
        PulsarClientBuilder builder = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650");

        ConnectionPolicy badProxy = ConnectionPolicy.builder()
                .proxy("http://proxy:8080", null)
                .build();

        IllegalArgumentException e = assertThrowsIAE(() -> builder.connectionPolicy(badProxy));
        assertTrue(e.getMessage().contains("proxyServiceUrl"),
                "error must name the offending field: " + e.getMessage());
    }

    /**
     * PIP-478 stage 3b: on the v5-builder TLS path a bad {@link TlsPolicy} (here a non-existent trust
     * cert file) fails the client build fast — at {@code build()} time, before any connection is
     * attempted — with an actionable error, rather than surfacing later as an opaque handshake failure.
     */
    @Test
    public void testTlsPolicyBadPathFailsClientBuild() {
        String badPath = "/nonexistent/pip478/ca-does-not-exist.pem";
        PulsarClientException e = null;
        try {
            PulsarClient.builder()
                    .serviceUrl("pulsar+ssl://localhost:6651")
                    .tlsPolicy(TlsPolicy.pem(badPath, null, null))
                    .build();
            fail("expected the client build to fail fast on the bad TLS policy");
        } catch (PulsarClientException ex) {
            e = ex;
        }
        assertNotNull(e, "a bad TLS policy must fail the client build");
        assertTrue(allMessages(e).toLowerCase().contains("tls"),
                "the failure must be actionable and mention TLS: " + allMessages(e));
    }

    /**
     * PIP-478 stage 3c: a bridged third-party v4 plugin (not a built-in TLS class) that reports
     * {@code hasDataForTls()} with file-based cert/key must have that material folded into
     * {@link TlsPurpose#CLIENT_DEFAULT} at build time. Proven here by pointing the plugin at a non-existent
     * cert file: the fold makes the client build fail fast on the missing file; without the fold the
     * system-default CLIENT_DEFAULT policy would build cleanly.
     */
    @Test
    public void genericV4TlsFilePluginMaterialIsFoldedIntoClientDefault() {
        AuthenticationDataProvider data = new AuthenticationDataProvider() {
            @Override
            public boolean hasDataForTls() {
                return true;
            }

            @Override
            public String getTlsCertificateFilePath() {
                return "/nonexistent/pip478/generic-cert.pem";
            }

            @Override
            public String getTlsPrivateKeyFilePath() {
                return "/nonexistent/pip478/generic-key.pem";
            }
        };
        Authentication v5 = LegacyV4AuthenticationAdapter.wrap(new GenericTlsV4Auth("custom-tls", data));

        PulsarClientException e = null;
        try {
            PulsarClient.builder()
                    .serviceUrl("pulsar+ssl://localhost:6651")
                    .authentication(v5)
                    .tlsPolicy(TlsPolicy.pem(null, null, null))
                    .build();
            fail("expected the client build to fail fast on the folded generic TLS material");
        } catch (PulsarClientException ex) {
            e = ex;
        }
        assertNotNull(e, "the folded generic cert path must fail the client build");
        assertTrue(allMessages(e).toLowerCase().contains("tls"),
                "the failure must be actionable and mention TLS: " + allMessages(e));
    }

    /**
     * PIP-478 stage 3c: a generic v4 plugin exposing only in-memory TLS material cannot be represented in a
     * file-path {@link TlsPolicy}; it is logged rather than folded, and the client build still succeeds
     * (the material simply is not applied on this path).
     */
    @Test
    public void genericV4InMemoryOnlyTlsMaterialDoesNotFailBuild() throws Exception {
        AuthenticationDataProvider inMemoryOnly = new AuthenticationDataProvider() {
            @Override
            public boolean hasDataForTls() {
                return true;
            }
            // No file paths and no keystore params — only (notional) in-memory material.
        };
        Authentication v5 = LegacyV4AuthenticationAdapter.wrap(new GenericTlsV4Auth("custom-tls", inMemoryOnly));

        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://localhost:6651")
                .authentication(v5)
                .tlsPolicy(TlsPolicy.pem(null, null, null))
                .build()) {
            assertNotNull(client, "an in-memory-only generic TLS plugin must not fail the client build");
        }
    }

    /** A minimal generic (non-built-in) v4 TLS plugin for the fold tests. */
    @SuppressWarnings("deprecation")
    private static final class GenericTlsV4Auth implements org.apache.pulsar.client.api.Authentication {
        private final String methodName;
        private final AuthenticationDataProvider data;

        GenericTlsV4Auth(String methodName, AuthenticationDataProvider data) {
            this.methodName = methodName;
            this.data = data;
        }

        @Override
        public String getAuthMethodName() {
            return methodName;
        }

        @Override
        public AuthenticationDataProvider getAuthData() {
            return data;
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

    private static String allMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
            if (c.getMessage() != null) {
                sb.append(c.getMessage()).append(" | ");
            }
        }
        return sb.toString();
    }

    private static IllegalArgumentException assertThrowsIAE(Runnable r) {
        try {
            r.run();
            fail("expected IllegalArgumentException");
            return null; // unreachable
        } catch (IllegalArgumentException e) {
            return e;
        }
    }
}
