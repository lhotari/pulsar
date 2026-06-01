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

import java.util.Optional;

/**
 * Identifies <em>why</em> TLS is requested and in what role (PIP-478, the PIP-337 replacement).
 *
 * <p>Never instantiated directly: a context is always a {@link ClientTlsPurposeContext} or a
 * {@link ServerTlsPurposeContext}. Sealing keeps the type evolvable — accessors or members can be
 * added for the closed set of permitted implementations without breaking binary compatibility.
 */
public sealed interface TlsPurposeContext permits ClientTlsPurposeContext, ServerTlsPurposeContext {

    /**
     * @return {@code true} for server-side TLS, {@code false} for client-side
     */
    boolean isServer();

    /**
     * An optional finer-grained tag identifying a specific usage within a well-known purpose, so a
     * provider can serve distinct material for that one usage (resolution otherwise falls back to
     * the purpose). Example: the OAuth2 plugin's IdP HTTP client uses purpose {@code GENERIC} with
     * {@code new UsageIdentifier(AuthenticationOAuth2.class, "idpHttpClient")}.
     *
     * @return the usage identifier, if present
     */
    Optional<UsageIdentifier> usageIdentifier();

    /**
     * Identifies a specific TLS usage: the owning class and a stable name within it.
     *
     * @param owner      the owning class
     * @param identifier a stable name unique within the owner
     */
    record UsageIdentifier(Class<?> owner, String identifier) {
    }
}
