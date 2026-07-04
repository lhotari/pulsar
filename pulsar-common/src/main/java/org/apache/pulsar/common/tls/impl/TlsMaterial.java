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

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * The loaded TLS material for one {@code TlsPurpose}: the private key, its certificate chain, and the
 * trust certificates (PIP-478, internal to the default {@code FileBasedTlsFactory}).
 *
 * <p>This is a value type with structural {@code equals}/{@code hashCode} (a record). The default
 * factory uses value equality to detect real rotations: a {@code TlsMaterialSource} that re-reads its
 * files but finds identical content produces an {@code equal} instance, which suppresses spurious
 * context rebuilds and reload callbacks (a file touched without content change fires nothing).
 *
 * <p>Role-neutral: the policy flags (insecure, hostname verification, client-auth requirement) and the
 * protocols/ciphers are fixed at construction and live on the owning {@code TlsPolicy}/factory
 * settings, not here — only the crypto material rotates.
 *
 * @param privateKey   the private key (client key for mTLS, server key on the server side), or
 *                     {@code null} when none is configured
 * @param keyCertChain the certificate chain matching {@link #privateKey()} (never {@code null})
 * @param trustCerts   the trusted CA certificates, or empty to use the platform default trust store
 */
record TlsMaterial(PrivateKey privateKey, List<X509Certificate> keyCertChain, List<X509Certificate> trustCerts) {

    /** The "system default" material: no key, no configured trust (resolves to the platform trust store). */
    static final TlsMaterial SYSTEM_DEFAULT = new TlsMaterial(null, List.of(), List.of());

    TlsMaterial {
        keyCertChain = keyCertChain == null ? List.of() : List.copyOf(keyCertChain);
        trustCerts = trustCerts == null ? List.of() : List.copyOf(trustCerts);
    }

    /** @return {@code true} when both a private key and a non-empty certificate chain are present */
    boolean hasKeyMaterial() {
        return privateKey != null && !keyCertChain.isEmpty();
    }

    /** @return the certificate chain as an array, or {@code null} when empty (Netty/JSSE "unset") */
    X509Certificate[] keyCertChainArray() {
        return keyCertChain.isEmpty() ? null : keyCertChain.toArray(new X509Certificate[0]);
    }

    /** @return the trust certificates as an array (never {@code null}; empty means platform default) */
    X509Certificate[] trustCertsArray() {
        return trustCerts.toArray(new X509Certificate[0]);
    }
}
