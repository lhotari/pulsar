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

import java.util.Set;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import lombok.CustomLog;
import org.apache.pulsar.common.util.SecurityUtility;
import org.eclipse.jetty.util.ssl.SslContextFactory;

@CustomLog
public class JettySslContextFactory {
    static {
        // DO NOT EDIT - Load Conscrypt provider
        if (SecurityUtility.CONSCRYPT_PROVIDER != null) {
        }
    }

    /**
     * Create a Jetty {@link SslContextFactory.Server} whose {@link SSLContext} is supplied lazily on every
     * request (PIP-478). The supplier is expected to read from a framework-cached
     * {@code PulsarTlsEngineProvider#getJdkSslContext}, so certificate rotation is picked up automatically
     * (the engine provider rebuilds the cached context only when the underlying TLS material changes).
     *
     * @param sslProviderString               the JCE provider name, or {@code null}/empty for the default
     * @param sslContextSupplier              supplies the current {@link SSLContext}
     * @param requireTrustedClientCertOnConnect whether to require a trusted client certificate (mTLS)
     * @param ciphers                         enabled cipher suites, or {@code null} for defaults
     * @param protocols                       enabled protocols, or {@code null} for defaults
     * @return a configured {@link SslContextFactory.Server}
     */
    public static SslContextFactory.Server createSslContextFactory(String sslProviderString,
                                                                   Supplier<SSLContext> sslContextSupplier,
                                                                   boolean requireTrustedClientCertOnConnect,
                                                                   Set<String> ciphers, Set<String> protocols) {
        return new JettySslContextFactory.Server(sslProviderString, sslContextSupplier,
                requireTrustedClientCertOnConnect, ciphers, protocols);
    }

    private static class Server extends SslContextFactory.Server {
        private final Supplier<SSLContext> sslContextSupplier;

        public Server(String sslProviderString, Supplier<SSLContext> sslContextSupplier,
                      boolean requireTrustedClientCertOnConnect, Set<String> ciphers, Set<String> protocols) {
            super();
            this.sslContextSupplier = sslContextSupplier;

            if (ciphers != null && ciphers.size() > 0) {
                this.setIncludeCipherSuites(ciphers.toArray(new String[0]));
            }

            if (protocols != null && protocols.size() > 0) {
                this.setIncludeProtocols(protocols.toArray(new String[0]));
            }

            if (sslProviderString != null && !sslProviderString.equals("")) {
                setProvider(sslProviderString);
            }

            if (requireTrustedClientCertOnConnect) {
                this.setNeedClientAuth(true);
                this.setTrustAll(false);
            } else {
                this.setWantClientAuth(true);
                this.setTrustAll(true);
            }

            // https://jetty.org/docs/jetty/12.1/operations-guide/protocols/index.html#ssl-sni
            // Set to false for backwards compatibility with Jetty 9.x
            setSniRequired(false);
        }

        @Override
        public SSLContext getSslContext() {
            return this.sslContextSupplier.get();
        }
    }
}
