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
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
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
 * {@link TlsContexts#synthesizeNettyServerFromJdk}). For a subscribing acquisition the synthesis re-wraps on
 * every delivery, so rotated JDK material reaches the consumer as a freshly synthesized Netty context.
 *
 * <p><b>{@code SSLParameters} companion (PIP-478).</b> On the synthesis path the framework additionally asks
 * the factory for {@code createInstance(purpose, SSLParameters.class)} — the optional engine-policy companion
 * carrying the factory's baseline a bare {@code SSLContext} cannot express (enabled protocols/ciphers,
 * algorithm constraints, application protocols, endpoint identification, and the server client-auth mode). It
 * is requested in the same form as the {@code SSLContext} (one-shot, or endpoint-carrying), and on a
 * subscribing acquisition it is re-requested with each {@code SSLContext} delivery so engine policy may rotate
 * with material. The merge is deterministic (see {@link TlsContexts#synthesizeNettyClientFromJdk} /
 * {@link TlsContexts#synthesizeNettyServerFromJdk}): the factory companion forms the engine baseline (non-null
 * members only); {@code endpointIdentificationAlgorithm} is the factory's when set, otherwise the consumer's
 * hostname-verification flag applies {@code "HTTPS"} on client purposes; SNI is always set per connection from
 * the target endpoint (never taken from the companion); and on server purposes the companion's
 * {@code needClientAuth}/{@code wantClientAuth} are authoritative when it is supplied, else the consumer's
 * client-auth flag maps as usual. A factory-supplied companion is mutable, so the framework snapshots it once
 * per acquisition (see {@link TlsContexts}); {@code empty()} means the consumer's configuration applies, as
 * before. The subscribing form requests the companion synchronously within the reload callback — which the SPI
 * runs off any event loop — so a factory that supplies it must complete that request without depending on the
 * thread delivering the {@code SSLContext}.
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
                        : synthesizeFromFallback(factory.createInstance(purpose, SSLContext.class),
                                () -> factory.createInstance(purpose, SSLParameters.class), purpose, synthesis));
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
                        : synthesizeFromFallback(factory.createInstance(purpose, endpoint, SSLContext.class),
                                () -> factory.createInstance(purpose, endpoint, SSLParameters.class), purpose,
                                synthesis));
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
                            new SynthesizingSubscription(factory, purpose, synthesis, onLoadOrReload);
                    return factory.createInstance(purpose, SSLContext.class, subscription::onDelivery)
                            .thenApply(jdk -> jdk.map(subscription::bind));
                });
    }

    /**
     * Complete the synthesis fallback once the JDK {@code SSLContext} is resolved: when present, request the
     * factory's optional {@code SSLParameters} companion (lazily, via {@code paramsSupplier}, so it is not
     * requested when the JDK context is unsupported) and synthesize the Netty context from both.
     */
    private static CompletableFuture<Optional<TlsHandle<SslContext>>> synthesizeFromFallback(
            CompletableFuture<Optional<TlsHandle<SSLContext>>> jdkFuture,
            Supplier<CompletableFuture<Optional<TlsHandle<SSLParameters>>>> paramsSupplier,
            TlsPurpose purpose, TlsSynthesisSpec synthesis) {
        return jdkFuture.thenCompose(jdk -> {
            if (jdk.isEmpty()) {
                return CompletableFuture.<Optional<TlsHandle<SslContext>>>completedFuture(Optional.empty());
            }
            return paramsSupplier.get().thenApply(paramsHandle ->
                    Optional.of(synthesizeOneShot(jdk.get(), purpose, synthesis, extractBaseline(paramsHandle))));
        });
    }

    private static TlsHandle<SslContext> synthesizeOneShot(TlsHandle<SSLContext> jdkHandle, TlsPurpose purpose,
                                                           TlsSynthesisSpec synthesis, SSLParameters factoryBaseline) {
        SslContext context = synthesize(jdkHandle.get(), purpose, synthesis, factoryBaseline);
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

    private static SslContext synthesize(SSLContext jdkContext, TlsPurpose purpose, TlsSynthesisSpec synthesis,
                                         SSLParameters factoryBaseline) {
        if (purpose.role() == TlsPurpose.Role.CLIENT) {
            return TlsContexts.synthesizeNettyClientFromJdk(jdkContext, synthesis.enableHostnameVerification(),
                    factoryBaseline);
        }
        return TlsContexts.synthesizeNettyServerFromJdk(jdkContext, synthesis.requireTrustedClientCert(),
                factoryBaseline);
    }

    /**
     * Extract the factory's {@code SSLParameters} companion from its handle, disposing the handle afterwards.
     * Returns {@code null} when the factory returned {@code empty()} (no companion for this purpose). The
     * synthesis takes its own defensive snapshot of the returned (mutable) object.
     */
    private static SSLParameters extractBaseline(Optional<TlsHandle<SSLParameters>> handle) {
        if (handle.isEmpty()) {
            return null;
        }
        TlsHandle<SSLParameters> paramsHandle = handle.get();
        try {
            return paramsHandle.get();
        } finally {
            paramsHandle.dispose();
        }
    }

    /**
     * A {@link TlsHandle} over a JDK {@code SSLContext} subscription that synthesizes a Netty context on each
     * delivery, forwards it to the consumer callback, and exposes the latest synthesized context via
     * {@link #get()}.
     */
    private static final class SynthesizingSubscription implements TlsHandle<SslContext> {

        private final PulsarTlsFactory factory;
        private final TlsPurpose purpose;
        private final TlsSynthesisSpec synthesis;
        private final Consumer<SslContext> onLoadOrReload;
        private volatile SslContext latest;
        private volatile TlsHandle<SSLContext> underlying;

        SynthesizingSubscription(PulsarTlsFactory factory, TlsPurpose purpose, TlsSynthesisSpec synthesis,
                                 Consumer<SslContext> onLoadOrReload) {
            this.factory = factory;
            this.purpose = purpose;
            this.synthesis = synthesis;
            this.onLoadOrReload = onLoadOrReload;
        }

        // Serial per subscription (SPI contract); the first delivery happens-before the subscribe future
        // completes, so `latest` is set by the time bind() runs. The SSLParameters companion is re-requested
        // with each delivery so engine policy rotates with material; the callback runs off any event loop
        // (SPI contract), so the synchronous request here never blocks a consumer event loop.
        void onDelivery(SSLContext jdkContext) {
            SSLParameters factoryBaseline = extractBaseline(
                    factory.createInstance(purpose, SSLParameters.class).join());
            SslContext wrapped = synthesize(jdkContext, purpose, synthesis, factoryBaseline);
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
