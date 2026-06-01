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
package org.apache.pulsar.client.api.v5.http;

/**
 * Identifies, in transport-neutral terms, which client TLS material an HTTP client should use
 * (PIP-478).
 *
 * <p>This is the Tier-0 counterpart of the framework's {@code ClientTlsPurposeContext} (which lives
 * in {@code org.apache.pulsar.common.tls} and cannot be referenced from this dependency-free API
 * module). The framework maps each value here one-to-one onto the corresponding
 * {@code ClientTlsPurposeContext.ClientPurpose} when it resolves TLS material from the configured
 * {@code PulsarTlsMaterialProvider}.
 *
 * <p>Each purpose declares the purpose it FALLS BACK to when no material is configured for it;
 * {@link #GENERIC} has no fallback and means "OS default trust store, no client certificate" — the
 * appropriate choice for OAuth2 / identity-provider calls that must not reuse Pulsar-cluster TLS
 * material.
 */
public enum ClientHttpTlsPurpose {
    /** OS default trust store, no client certificate. Has no fallback. */
    GENERIC(null),
    /** Pulsar binary protocol client material; falls back to {@link #GENERIC}. */
    BINARY_CLIENT(GENERIC),
    /** HTTP topic-lookup material; falls back to {@link #BINARY_CLIENT}. */
    HTTP_LOOKUP(BINARY_CLIENT),
    /** Admin HTTP client material; falls back to {@link #GENERIC}. */
    ADMIN_HTTP(GENERIC);

    private final ClientHttpTlsPurpose fallback;

    ClientHttpTlsPurpose(ClientHttpTlsPurpose fallback) {
        this.fallback = fallback;
    }

    /**
     * @return the purpose this one falls back to, or {@code null} for {@link #GENERIC}
     */
    public ClientHttpTlsPurpose fallback() {
        return fallback;
    }
}
