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
package org.apache.pulsar.common.tls;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * The TLS material and configuration for a given {@link TlsPurposeContext} (PIP-478).
 *
 * <p>The SPI provides <em>material</em>; the framework turns it into an {@code SSLEngine} /
 * {@code SSLContext}, caches it, and rebuilds only when the material changes. Providers should
 * return the same instance when nothing changed and a new (non-equal) instance when it did, so the
 * framework can cache efficiently.
 *
 * <p>This single type covers both PEM (PKCS#8) and keystore (PKCS12/JKS) sources — implementations
 * expose the loaded {@link PrivateKey} and {@link X509Certificate}s regardless of origin.
 */
public interface TlsMaterial {

    /**
     * @return the private key (client key for mTLS, server key on the server side), or {@code null}
     */
    PrivateKey getPrivateKey();

    /**
     * @return the key certificate chain matching {@link #getPrivateKey()}, or empty if none
     */
    Iterable<? extends X509Certificate> getKeyCertChain();

    /**
     * @return the trusted CA certificates, or empty to use the platform default trust store
     */
    Iterable<? extends X509Certificate> getTrustCerts();

    /**
     * @return the enabled TLS protocols (e.g. {@code TLSv1.3}, {@code TLSv1.2}), or empty for defaults
     */
    Iterable<String> getTlsProtocols();

    /**
     * @return the enabled TLS cipher suites, or empty for defaults
     */
    Iterable<String> getTlsCiphers();
}
