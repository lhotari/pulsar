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

import java.util.Objects;
import java.util.Optional;

/**
 * A client-side {@link TlsPurposeContext} (PIP-478). Listener concerns never appear here — the
 * client side stays simple.
 */
public final class ClientTlsPurposeContext implements TlsPurposeContext {

    /**
     * Well-known client purposes. Each declares the purpose it FALLS BACK to when no material is
     * configured for it; resolution walks the chain and fails if nothing is found. {@link #GENERIC}
     * has no fallback: it means "OS default trust store, no client certificate" (Netty's default
     * when no custom trust store is set) — used for OAuth2 / IdP calls that must not reuse
     * Pulsar-cluster TLS material.
     */
    public enum ClientPurpose {
        /** OS default trust store, no client certificate. No fallback. */
        GENERIC(null),
        /** Pulsar binary protocol. */
        BINARY_CLIENT(GENERIC),
        /** HTTP topic lookup. */
        HTTP_LOOKUP(BINARY_CLIENT),
        /** Admin HTTP client. */
        ADMIN_HTTP(GENERIC);

        private final ClientPurpose fallback;

        ClientPurpose(ClientPurpose fallback) {
            this.fallback = fallback;
        }

        /**
         * @return the purpose this one falls back to, or {@code null} for {@link #GENERIC}
         */
        public ClientPurpose fallback() {
            return fallback;
        }
    }

    private final ClientPurpose purpose;
    private final UsageIdentifier usageIdentifier;

    /**
     * @param purpose the well-known client purpose
     */
    public ClientTlsPurposeContext(ClientPurpose purpose) {
        this(purpose, null);
    }

    /**
     * @param purpose         the well-known client purpose
     * @param usageIdentifier an optional finer-grained usage identifier (may be {@code null})
     */
    public ClientTlsPurposeContext(ClientPurpose purpose, UsageIdentifier usageIdentifier) {
        this.purpose = Objects.requireNonNull(purpose, "purpose");
        this.usageIdentifier = usageIdentifier;
    }

    /**
     * Convenience factory for a purpose with no usage identifier.
     *
     * @param purpose the well-known client purpose
     * @return a new context
     */
    public static ClientTlsPurposeContext of(ClientPurpose purpose) {
        return new ClientTlsPurposeContext(purpose);
    }

    /**
     * @return the well-known client purpose
     */
    public ClientPurpose purpose() {
        return purpose;
    }

    @Override
    public boolean isServer() {
        return false;
    }

    @Override
    public Optional<UsageIdentifier> usageIdentifier() {
        return Optional.ofNullable(usageIdentifier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClientTlsPurposeContext that)) {
            return false;
        }
        return purpose == that.purpose && Objects.equals(usageIdentifier, that.usageIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(purpose, usageIdentifier);
    }

    @Override
    public String toString() {
        return "ClientTlsPurposeContext{" + purpose
                + (usageIdentifier != null ? ", " + usageIdentifier : "") + "}";
    }
}
