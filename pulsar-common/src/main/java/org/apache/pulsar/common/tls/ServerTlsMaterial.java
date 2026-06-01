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

/**
 * Server-side {@link TlsMaterial} (PIP-478).
 */
public interface ServerTlsMaterial extends TlsMaterial {

    /**
     * @return whether a trusted client certificate is required (mTLS / TLS authentication). Default
     *         {@code false}; must be enabled for mTLS or TLS authentication.
     */
    default boolean isTrustedClientCertRequired() {
        return false;
    }

    /**
     * @return whether any client certificate is accepted without trust-chain validation (the server-side
     *         counterpart of the legacy {@code tlsAllowInsecureConnection} mode). Default {@code false}.
     *         When {@code true}, the framework installs a permissive trust manager so that a client
     *         presenting an untrusted certificate still completes the TLS handshake (the certificate is
     *         then available for TLS authentication); this matches the pre-PIP-478 behaviour where a
     *         broker with {@code tlsAllowInsecureConnection=true} accepted untrusted client certificates.
     */
    default boolean isTrustAnyClientCert() {
        return false;
    }
}
