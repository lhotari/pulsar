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

import io.netty.handler.ssl.SslContext;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsEndpoint;
import org.apache.pulsar.common.tls.TlsHandle;
import org.apache.pulsar.common.tls.TlsPurpose;

/**
 * The single framework entry point every Netty-{@code SslContext} consumer uses to acquire its context from
 * a {@link PulsarTlsFactory}, applying the PIP-478 {@code SSLContext}-fallback synthesis (the "well-known
 * classes" tier-3 story).
 *
 * <p>For a {@code (purpose, class)} the framework first asks the factory for the Netty
 * {@link SslContext} a consumer actually needs; only when the factory returns {@link Optional#empty()} —
 * meaning it does not build the Netty class directly — does the framework request
 * {@code javax.net.ssl.SSLContext} for the same purpose (in the same one-shot vs. subscribing form) and
 * synthesize the Netty context from it. Because the default {@code FileBasedTlsFactory} natively supplies
 * the Netty class, the fallback fires only for a custom factory that implements only the JDK
 * {@code SSLContext}. If neither class is supported the returned {@link Optional} is empty, exactly as the
 * raw {@code createInstance} call would be — the consumer keeps its existing "supplied no context" failure.
 *
 * <p>The synthesized context bakes the role-appropriate settings the factory could not apply, carried by the
 * {@link TlsSynthesisSpec}: for a client purpose the hostname-verification algorithm (see
 * {@link TlsContexts#synthesizeNettyClientFromJdk}); for a server purpose the client-auth requirement (see
 * {@link TlsContexts#synthesizeNettyFromJdk}). For a subscribing acquisition the synthesis re-wraps on every
 * delivery, so rotated JDK material reaches the consumer as a freshly synthesized Netty context.
 */
public final class TlsContextAcquisition {

    private TlsContextAcquisition() {
    }

    /**
     * One-shot acquisition of the Netty {@link SslContext} for {@code purpose}, falling back to synthesis
     * from the JDK {@code SSLContext}.
     *
     * @param factory   the (initialized) TLS factory
     * @param purpose   the purpose to resolve
     * @param synthesis the settings to bake if the Netty class must be synthesized from the JDK context
     * @return a future of the handle, or {@link Optional#empty()} when the factory supports neither class
     */
    public static CompletableFuture<Optional<TlsHandle<SslContext>>> acquireNettyContext(
            PulsarTlsFactory factory, TlsPurpose purpose, TlsSynthesisSpec synthesis) {
        return factory.createInstance(purpose, SslContext.class)
                .thenCompose(direct -> direct.isPresent()
                        ? CompletableFuture.completedFuture(direct)
                        : factory.createInstance(purpose, SSLContext.class)
                                .thenApply(jdk -> jdk.map(handle -> synthesizeOneShot(handle, purpose, synthesis))));
    }

    /**
     * One-shot acquisition carrying the destination endpoint hint (client purposes), falling back to
     * synthesis from the JDK {@code SSLContext} requested with the same endpoint.
     *
     * @param factory   the (initialized) TLS factory
     * @param purpose   the purpose to resolve
     * @param endpoint  the destination host/port hint
     * @param synthesis the settings to bake if the Netty class must be synthesized from the JDK context
     * @return a future of the handle, or {@link Optional#empty()} when the factory supports neither class
     */
    public static CompletableFuture<Optional<TlsHandle<SslContext>>> acquireNettyContext(
            PulsarTlsFactory factory, TlsPurpose purpose, TlsEndpoint endpoint, TlsSynthesisSpec synthesis) {
        return factory.createInstance(purpose, endpoint, SslContext.class)
                .thenCompose(direct -> direct.isPresent()
                        ? CompletableFuture.completedFuture(direct)
                        : factory.createInstance(purpose, endpoint, SSLContext.class)
                                .thenApply(jdk -> jdk.map(handle -> synthesizeOneShot(handle, purpose, synthesis))));
    }

    /**
     * Subscribing acquisition of the Netty {@link SslContext} for {@code purpose}, falling back to a
     * subscription on the JDK {@code SSLContext} whose deliveries are re-wrapped into Netty contexts.
     *
     * @param factory        the (initialized) TLS factory
     * @param purpose        the purpose to resolve
     * @param synthesis      the settings to bake if the Netty class must be synthesized from the JDK context
     * @param onLoadOrReload receives the context on first load and on every rebuild (native or synthesized)
     * @return a future of the subscribing handle, or {@link Optional#empty()} when neither class is supported
     */
    public static CompletableFuture<Optional<TlsHandle<SslContext>>> acquireNettyContext(
            PulsarTlsFactory factory, TlsPurpose purpose, TlsSynthesisSpec synthesis,
            Consumer<SslContext> onLoadOrReload) {
        return factory.createInstance(purpose, SslContext.class, onLoadOrReload)
                .thenCompose(direct -> {
                    if (direct.isPresent()) {
                        return CompletableFuture.completedFuture(direct);
                    }
                    SynthesizingSubscription subscription =
                            new SynthesizingSubscription(purpose, synthesis, onLoadOrReload);
                    return factory.createInstance(purpose, SSLContext.class, subscription::onDelivery)
                            .thenApply(jdk -> jdk.map(subscription::bind));
                });
    }

    private static TlsHandle<SslContext> synthesizeOneShot(TlsHandle<SSLContext> jdkHandle, TlsPurpose purpose,
                                                           TlsSynthesisSpec synthesis) {
        SslContext context = synthesize(jdkHandle.get(), purpose, synthesis);
        return new TlsHandle<>() {
            @Override
            public SslContext get() {
                return context;
            }

            @Override
            public void dispose() {
                jdkHandle.dispose();
            }
        };
    }

    private static SslContext synthesize(SSLContext jdkContext, TlsPurpose purpose, TlsSynthesisSpec synthesis) {
        if (purpose.role() == TlsPurpose.Role.CLIENT) {
            return TlsContexts.synthesizeNettyClientFromJdk(jdkContext, synthesis.enableHostnameVerification());
        }
        return TlsContexts.synthesizeNettyFromJdk(jdkContext, false, synthesis.requireTrustedClientCert());
    }

    /**
     * A {@link TlsHandle} over a JDK {@code SSLContext} subscription that synthesizes a Netty context on each
     * delivery, forwards it to the consumer callback, and exposes the latest synthesized context via
     * {@link #get()}.
     */
    private static final class SynthesizingSubscription implements TlsHandle<SslContext> {

        private final TlsPurpose purpose;
        private final TlsSynthesisSpec synthesis;
        private final Consumer<SslContext> onLoadOrReload;
        private volatile SslContext latest;
        private volatile TlsHandle<SSLContext> underlying;

        SynthesizingSubscription(TlsPurpose purpose, TlsSynthesisSpec synthesis,
                                 Consumer<SslContext> onLoadOrReload) {
            this.purpose = purpose;
            this.synthesis = synthesis;
            this.onLoadOrReload = onLoadOrReload;
        }

        // Serial per subscription (SPI contract); the first delivery happens-before the subscribe future
        // completes, so `latest` is set by the time bind() runs.
        void onDelivery(SSLContext jdkContext) {
            SslContext wrapped = synthesize(jdkContext, purpose, synthesis);
            this.latest = wrapped;
            onLoadOrReload.accept(wrapped);
        }

        TlsHandle<SslContext> bind(TlsHandle<SSLContext> jdkHandle) {
            this.underlying = jdkHandle;
            return this;
        }

        @Override
        public SslContext get() {
            return latest;
        }

        @Override
        public void dispose() {
            TlsHandle<SSLContext> jdkHandle = underlying;
            if (jdkHandle != null) {
                jdkHandle.dispose();
            }
        }
    }
}
