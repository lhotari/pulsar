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

import java.util.concurrent.CompletableFuture;

/**
 * The v5 TLS material SPI (PIP-478), replacing PIP-337's {@code PulsarSslFactory} /
 * {@code PulsarSslConfiguration}.
 *
 * <p>The SPI provides <em>material</em>; the framework turns {@link TlsMaterial} into an
 * {@code SSLEngine} / {@code SSLContext}, caches it, and rebuilds only when the material changes.
 * The framework calls {@link #getTlsMaterial} with a {@link TlsPurposeContext} identifying the TLS
 * usage, and the provider returns the matching {@link TlsMaterial}.
 *
 * <p>Resolution first tries material registered for the context's specific usage identifier (when
 * present), then the requested {@code purpose()}, then that purpose's fallback chain (e.g.
 * {@code HTTP_LOOKUP -> BINARY_CLIENT -> GENERIC}); if nothing along the chain yields material, the
 * returned future completes with material that mandates the platform-default trust store (for
 * {@code GENERIC}) or completes exceptionally.
 */
public interface PulsarTlsMaterialProvider extends AutoCloseable {

    /**
     * Initialize the provider with framework runtime services. Called once before any
     * {@link #getTlsMaterial} call.
     *
     * @param context the runtime services
     * @return a future completing when the provider is ready
     */
    CompletableFuture<Void> initialize(TlsMaterialProviderInitContext context);

    /**
     * Resolve the {@link TlsMaterial} for a purpose, applying the usage-identifier and purpose
     * fallback chain when none is configured for the exact context.
     *
     * @param purpose the TLS purpose context
     * @return a future of the resolved material
     */
    CompletableFuture<TlsMaterial> getTlsMaterial(TlsPurposeContext purpose);

    /**
     * Register a listener to be notified when material for a purpose first becomes ready and on every
     * subsequent change.
     *
     * @param purpose  the purpose to observe
     * @param listener the listener
     */
    void registerListener(TlsPurposeContext purpose, TlsMaterialListener listener);

    /**
     * Stop notifying a previously-registered listener for a purpose.
     *
     * @param purpose  the purpose
     * @param listener the listener
     */
    void unregisterListener(TlsPurposeContext purpose, TlsMaterialListener listener);

    /**
     * Release any resources held by the provider.
     */
    @Override
    void close();
}
