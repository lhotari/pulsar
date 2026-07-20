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

import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.tls.impl.TlsContexts;
import org.apache.pulsar.common.util.tls.JcaProviders;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * The framework's Jetty integration for the PIP-478 TLS SPI. Both {@link SslContextFactory.Server} and
 * {@link SslContextFactory.Client} are well-known SPI classes: the framework first asks the
 * {@link PulsarTlsFactory} to supply one natively (a custom factory may build and own it, reload
 * included); only when the factory returns {@code empty()} for the Jetty class does the framework
 * synthesize a <em>vanilla</em> (never subclassed) one from an {@code SSLContext} subscription. The
 * default {@code FileBasedTlsFactory} returns {@code empty()} for the Jetty classes, so the synthesized
 * path is the usual one.
 *
 * <p><b>Factory-supplied (native) instances.</b> When the factory supplies the Jetty factory directly it
 * is handed back <em>unstarted</em> (the connector / {@code HttpClient} starts it), is the factory's
 * same-instance-per-purpose, and the factory owns its reload on material rotation. The framework only
 * holds the returned {@link TlsHandle} for disposal and never drives {@code setSslContext}/{@code reload}
 * or overlays consumer configuration on such an instance.
 *
 * <p>This deliberately abandons the unsound {@code getSslContext()} override of the removed PIP-337
 * {@code JettySslContextFactory}. Instead it uses
 * Jetty's documented hot-reload API — the same one {@code KeyStoreScanner} uses: an {@code SSLContext}
 * subscription drives {@link SslContextFactory#setSslContext(SSLContext)} before start, and on each
 * later delivery {@link SslContextFactory#reload(Consumer)} atomically swaps the context and re-selects
 * protocols/ciphers. Existing connections keep their sessions; new connections use the new context.
 *
 * <p><b>{@code SSLParameters} companion (PIP-478).</b> Because these are synthesized paths (the factory
 * returned {@code empty()} for the Jetty class and supplies only the {@code SSLContext}), the framework also
 * asks the factory for its optional {@code javax.net.ssl.SSLParameters} companion — the engine-level baseline
 * a bare {@code SSLContext} cannot express — and maps its non-null members onto the Jetty setters: enabled
 * protocols ({@link SslContextFactory#setIncludeProtocols}) and cipher suites
 * ({@link SslContextFactory#setIncludeCipherSuites}), and (server side, merge rule 4) the authoritative
 * client-auth mode ({@link SslContextFactory.Server#setNeedClientAuth} /
 * {@link SslContextFactory.Server#setWantClientAuth}). The initial companion is overlaid before start (at build
 * time); on each subsequent <em>rotation</em> delivery it is re-requested <em>asynchronously</em> — the reload
 * callback composes {@code createInstance(purpose, SSLParameters.class)} with {@code whenComplete} and only then
 * calls {@code reload(...)}, so engine policy rotates with material (pip-478.md:736) yet the companion is
 * <b>never joined on the delivery thread</b>. Joining there would self-deadlock a custom factory that dispatches
 * companion creation to the same single-thread scheduler that runs the poll delivery — the exact hazard
 * {@code TlsContextAcquisition.SynthesizingSubscription} avoids by composing the companion asynchronously.
 * Consumer defaults are re-applied first inside every {@code reload(...)} lambda, so a companion member dropped
 * by a later delivery (notably client-auth) reverts to the consumer default rather than staying stuck from the
 * previous companion. {@code empty()} leaves the consumer's configuration in force, exactly as before.
 */
@CustomLog
public final class JettyTlsFactory {

    static {
        // DO NOT EDIT - Load Conscrypt provider
        if (JcaProviders.CONSCRYPT_PROVIDER != null) {
        }
    }

    private JettyTlsFactory() {
    }

    /**
     * A Jetty server factory (factory-supplied native, or framework-synthesized and self-reloading)
     * together with the handle backing it; dispose the {@link #subscription()} when the web service stops.
     *
     * @param sslContextFactory the (unstarted) Jetty server factory
     * @param subscription      the backing handle — the {@code SSLContext} reload subscription on the
     *                          synthesized path, or the native-instance handle when the factory supplied it
     */
    public record ReloadableServerTls(SslContextFactory.Server sslContextFactory,
                                      TlsHandle<?> subscription) {
    }

    /**
     * A Jetty client factory (factory-supplied native, or framework-synthesized and self-reloading)
     * together with the handle backing it; dispose the {@link #subscription()} when the owning HTTP client
     * / servlet is destroyed.
     *
     * @param sslContextFactory the (unstarted) Jetty client factory
     * @param subscription      the backing handle — the {@code SSLContext} reload subscription on the
     *                          synthesized path, or the native-instance handle when the factory supplied it
     */
    public record ReloadableClientTls(SslContextFactory.Client sslContextFactory,
                                      TlsHandle<?> subscription) {
    }

    /**
     * Build a self-reloading {@link SslContextFactory.Server} for a purpose, handed back <em>unstarted</em>
     * (Jetty starts it with the connector lifecycle) with its initial {@code SSLContext} already set.
     *
     * @param factory                  the TLS factory to subscribe to
     * @param purpose                  the server purpose (e.g. {@link TlsPurpose#WEB})
     * @param sslProviderString        the JCE provider name, or {@code null}/empty for the default
     * @param requireTrustedClientCert whether to require (vs. request) a trusted client certificate
     * @param allowInsecureConnection  whether an untrusted client cert is accepted under optional client auth
     * @param ciphers                  enabled cipher suites, or {@code null} for defaults
     * @param protocols                enabled protocols, or {@code null} for defaults
     * @return the reloading factory and its subscription handle
     */
    public static ReloadableServerTls createReloadingServerFactory(PulsarTlsFactory factory, TlsPurpose purpose,
                                                                   String sslProviderString,
                                                                   boolean requireTrustedClientCert,
                                                                   boolean allowInsecureConnection,
                                                                   Set<String> ciphers, Set<String> protocols) {
        // Ask the factory first: a custom factory may natively supply the Jetty server factory (a
        // well-known class). When it does, hand it back unstarted with the factory owning its reload and
        // configuration; the framework overlays no consumer config on it. The default file-based factory
        // returns empty() here, so the synthesized, self-reloading fallback below runs (the usual path).
        Optional<TlsHandle<SslContextFactory.Server>> nativeFactory =
                acquireNativeJettyFactory(factory, purpose, SslContextFactory.Server.class);
        if (nativeFactory.isPresent()) {
            TlsHandle<SslContextFactory.Server> handle = nativeFactory.get();
            return new ReloadableServerTls(handle.get(), handle);
        }

        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        // Consumer config plus the factory's engine-policy companion (if any), before start.
        configureServerBaseline(sslContextFactory, factory, purpose, sslProviderString, requireTrustedClientCert,
                allowInsecureConnection, ciphers, protocols);

        Consumer<SSLContext> onLoadOrReload = newContext -> {
            if (sslContextFactory.isStarted()) {
                // Rotation delivery. Re-request the companion so protocols, cipher suites, and the server
                // client-auth mode rotate with material (pip-478.md:736), symmetric with the Netty path — but
                // resolve it ASYNCHRONOUSLY, off this delivery thread. Joining here would self-deadlock a custom
                // factory that dispatches companion creation to the same single-thread scheduler that runs this
                // callback (the hazard TlsContextAcquisition.SynthesizingSubscription avoids). Only once the
                // companion has resolved do we reload under Jetty's lock.
                factory.createInstance(purpose, SSLParameters.class).whenComplete((companion, err) -> {
                    SSLParameters baseline = err == null ? extractBaseline(companion) : null;
                    try {
                        sslContextFactory.reload(f -> {
                            SslContextFactory.Server server = (SslContextFactory.Server) f;
                            server.setSslContext(newContext);
                            // Re-apply the consumer defaults first so a companion member dropped by this delivery
                            // (notably client-auth) reverts to the consumer default rather than staying stuck from
                            // the previous companion, then overlay the already-resolved companion.
                            applyServerConfig(server, sslProviderString, requireTrustedClientCert,
                                    allowInsecureConnection, ciphers, protocols);
                            if (baseline != null) {
                                applyServerBaseline(server, baseline);
                            }
                        });
                    } catch (Exception e) {
                        log.warn().attr("purpose", purpose).exception(e)
                                .log("Failed to reload Jetty SslContextFactory; keeping the running context");
                    }
                });
            } else {
                // Initial load (or a reload before the connector started): set directly before start. The initial
                // companion was already overlaid at build (configureServerBaseline), so the built factory is ready
                // before subscribe completes — mirroring SynthesizingSubscription's pre-fetched first delivery.
                sslContextFactory.setSslContext(newContext);
            }
        };

        TlsHandle<SSLContext> subscription = factory.createInstance(purpose, SSLContext.class, onLoadOrReload)
                .join()
                .orElseThrow(() -> new IllegalStateException(
                        "TLS factory supplied no SSLContext for purpose " + purpose));
        return new ReloadableServerTls(sslContextFactory, subscription);
    }

    /**
     * Build an {@link SslContextFactory.Client} for a purpose (used by the proxy's {@code AdminProxyHandler},
     * whose Jetty {@code HttpClient} outlives broker-client material rotation), handed back
     * <em>unstarted</em>. Mirroring {@link #createReloadingServerFactory}, the framework first asks the
     * factory to supply the Jetty client factory natively; a custom factory that does so owns its reload and
     * endpoint identification, and the framework overlays nothing on it. Otherwise a vanilla, self-reloading
     * one is synthesized: an {@code SSLContext} subscription drives
     * {@link SslContextFactory#setSslContext(SSLContext)} before start, and on each later delivery
     * {@link SslContextFactory#reload(Consumer)} atomically swaps the context so new connections use the
     * rotated material. Dispose the returned {@link ReloadableClientTls#subscription()} when the owning
     * client is destroyed.
     *
     * @param factory                   the TLS factory to acquire from / subscribe to
     * @param purpose                   the client purpose (e.g. {@link TlsPurpose#BROKER_CLIENT})
     * @param sslProviderString         the JCE provider name, or {@code null}/empty for the default
     * @param enableHostnameVerification whether to verify the peer hostname; when {@code false} the
     *                                  synthesized client's endpoint identification is disabled (a
     *                                  factory-supplied native client owns this itself and is left untouched)
     * @return the client factory and the handle backing it
     */
    public static ReloadableClientTls createReloadingClientFactory(PulsarTlsFactory factory, TlsPurpose purpose,
                                                                   String sslProviderString,
                                                                   boolean enableHostnameVerification) {
        // Ask the factory first: a custom factory may natively supply the Jetty client factory (a well-known
        // class, mirroring the Server variant) to customize proxy->broker admin TLS. When it does, hand it
        // back unstarted with the factory owning its reload and its own endpoint identification; the
        // framework overlays no consumer config on it. The default file-based factory returns empty() here,
        // so the SSLContext-synthesized, self-reloading fallback below runs (the usual path, including
        // rotation of the long-lived admin HttpClient's broker-client material).
        Optional<TlsHandle<SslContextFactory.Client>> nativeFactory =
                acquireNativeJettyFactory(factory, purpose, SslContextFactory.Client.class);
        if (nativeFactory.isPresent()) {
            TlsHandle<SslContextFactory.Client> handle = nativeFactory.get();
            return new ReloadableClientTls(handle.get(), handle);
        }

        SslContextFactory.Client client = new SslContextFactory.Client();
        if (StringUtils.isNotBlank(sslProviderString)) {
            client.setProvider(sslProviderString);
        }
        // Pin the {TLSv1.3, TLSv1.2} floor and overlay the factory's engine-policy companion (if any), before
        // start.
        configureClientBaseline(client, factory, purpose);
        // Hostname verification is a consumer (proxy) concern on the synthesized path: disable endpoint
        // identification when the consumer has it off (a native factory, handled above, owns this itself). This
        // is a per-consumer setting applied once at build, not part of the per-delivery companion overlay.
        if (!enableHostnameVerification) {
            client.setEndpointIdentificationAlgorithm(null);
        }

        Consumer<SSLContext> onLoadOrReload = newContext -> {
            if (client.isStarted()) {
                // Rotation delivery. Re-request the companion so protocols/ciphers rotate with material
                // (pip-478.md:736; client-auth is a server concept, endpoint identification stays as set at
                // build) — but resolve it ASYNCHRONOUSLY, off this delivery thread, never joined here (the
                // self-deadlock hazard, see the Server variant / SynthesizingSubscription). Only once the
                // companion has resolved do we reload under Jetty's lock.
                factory.createInstance(purpose, SSLParameters.class).whenComplete((companion, err) -> {
                    SSLParameters baseline = err == null ? extractBaseline(companion) : null;
                    try {
                        client.reload(f -> {
                            SslContextFactory.Client c = (SslContextFactory.Client) f;
                            c.setSslContext(newContext);
                            // Re-pin the {TLSv1.3, TLSv1.2} floor first (the consumer default), then overlay the
                            // already-resolved companion so a dropped companion reverts to that floor.
                            c.setIncludeProtocols(TlsContexts.DEFAULT_ENABLED_PROTOCOLS.toArray(new String[0]));
                            if (baseline != null) {
                                applyClientBaseline(c, baseline);
                            }
                        });
                    } catch (Exception e) {
                        log.warn().attr("purpose", purpose).exception(e)
                                .log("Failed to reload Jetty client SslContextFactory; keeping the running context");
                    }
                });
            } else {
                // Initial load (or a reload before the client started): set directly before start. The initial
                // companion was already overlaid at build (configureClientBaseline).
                client.setSslContext(newContext);
            }
        };

        TlsHandle<SSLContext> subscription = factory.createInstance(purpose, SSLContext.class, onLoadOrReload)
                .join()
                .orElseThrow(() -> new IllegalStateException(
                        "TLS factory supplied no SSLContext for purpose " + purpose));
        return new ReloadableClientTls(client, subscription);
    }

    /**
     * Apply the consumer config, then overlay the factory's engine-policy companion, onto a synthesized server
     * factory — at build time only (off any event loop; the companion is joined here). Each subsequent rotation
     * delivery re-requests the companion asynchronously and re-applies it inside the {@code reload(...)} lambda
     * (see {@code onLoadOrReload}), never joined on the delivery thread.
     */
    private static void configureServerBaseline(SslContextFactory.Server sslContextFactory, PulsarTlsFactory factory,
                                                TlsPurpose purpose, String sslProviderString,
                                                boolean requireTrustedClientCert, boolean allowInsecureConnection,
                                                Set<String> ciphers, Set<String> protocols) {
        applyServerConfig(sslContextFactory, sslProviderString, requireTrustedClientCert, allowInsecureConnection,
                ciphers, protocols);
        resolveBaselineParameters(factory, purpose)
                .ifPresent(baseline -> applyServerBaseline(sslContextFactory, baseline));
    }

    /**
     * Pin the {@code {TLSv1.3, TLSv1.2}} floor, then overlay the factory companion's protocols/ciphers, onto a
     * synthesized client factory — at build time only (off any event loop; the companion is joined here). Each
     * subsequent rotation delivery re-requests the companion asynchronously and re-pins the floor + overlays it
     * inside the {@code reload(...)} lambda (see {@code onLoadOrReload}), never joined on the delivery thread.
     * Client-auth is a server concept and is not mapped here; endpoint identification is a per-consumer setting
     * applied once at build.
     */
    private static void configureClientBaseline(SslContextFactory.Client client, PulsarTlsFactory factory,
                                                TlsPurpose purpose) {
        client.setIncludeProtocols(TlsContexts.DEFAULT_ENABLED_PROTOCOLS.toArray(new String[0]));
        resolveBaselineParameters(factory, purpose)
                .ifPresent(baseline -> applyClientBaseline(client, baseline));
    }

    private static void applyServerConfig(SslContextFactory.Server sslContextFactory, String sslProviderString,
                                          boolean requireTrustedClientCertOnConnect, boolean allowInsecureConnection,
                                          Set<String> ciphers, Set<String> protocols) {
        if (ciphers != null && !ciphers.isEmpty()) {
            sslContextFactory.setIncludeCipherSuites(ciphers.toArray(new String[0]));
        }
        // Pin the enabled protocols even when unconfigured, matching the {TLSv1.3, TLSv1.2} floor the native
        // Netty path applies rather than deferring to the provider default. A factory-supplied companion,
        // applied afterwards by applyServerBaseline, still overrides this.
        if (protocols != null && !protocols.isEmpty()) {
            sslContextFactory.setIncludeProtocols(protocols.toArray(new String[0]));
        } else {
            sslContextFactory.setIncludeProtocols(TlsContexts.DEFAULT_ENABLED_PROTOCOLS.toArray(new String[0]));
        }
        if (StringUtils.isNotBlank(sslProviderString)) {
            sslContextFactory.setProvider(sslProviderString);
        }
        if (requireTrustedClientCertOnConnect) {
            sslContextFactory.setNeedClientAuth(true);
            sslContextFactory.setTrustAll(false);
        } else {
            // PIP-478: optional client auth requests but does not require a client cert. An untrusted
            // client cert is accepted at the handshake only when tlsAllowInsecureConnection=true, aligning the
            // web listener with the Netty binary listener's semantics and diverging from the pre-5.0 Jetty
            // path, which trusted any presented client cert whenever client auth was optional (see PIP-478
            // Security Considerations).
            //
            // The actual enforcement is the trust managers baked into the SSLContext the framework hands to
            // Jetty via setSslContext (built from the WEB TlsPolicy's allowInsecureConnection: CA-validating
            // when secure, insecure-trust-all when insecure). Jetty's own setTrustAll only takes effect when
            // Jetty builds the SSLContext itself, so it is inert on this setSslContext path; we still scope it
            // to the insecure flag so the two never disagree (defence in depth) rather than leaving the
            // inherited unconditional setTrustAll(true).
            // Clear needClientAuth explicitly (not just rely on the fresh-factory default): this method is
            // re-applied on each reload, so a prior companion delivery that set needClientAuth must be reset to
            // the consumer default here before a later companion (or its absence) is overlaid.
            sslContextFactory.setNeedClientAuth(false);
            sslContextFactory.setWantClientAuth(true);
            sslContextFactory.setTrustAll(allowInsecureConnection);
        }
        // https://jetty.org/docs/jetty/12.1/operations-guide/protocols/index.html#ssl-sni
        // Set to false for backwards compatibility with Jetty 9.x
        sslContextFactory.setSniRequired(false);
    }

    /**
     * Ask the factory to supply a native Jetty {@code jettyClass} for {@code purpose} (one-shot). Jetty's
     * {@link SslContextFactory.Server} and {@link SslContextFactory.Client} are well-known SPI classes, so a
     * custom factory MAY build one directly; the default {@code FileBasedTlsFactory} returns
     * {@link Optional#empty()}, in which case the caller synthesizes the factory from an {@code SSLContext}
     * subscription. A supplied instance is the factory's own (unstarted, same-instance-per-purpose,
     * factory-driven reload) — the caller returns it verbatim and keeps the {@link TlsHandle} for disposal.
     * Resolved at factory-build time (off any event loop), mirroring {@link #resolveBaselineParameters}.
     */
    private static <T extends SslContextFactory> Optional<TlsHandle<T>> acquireNativeJettyFactory(
            PulsarTlsFactory factory, TlsPurpose purpose, Class<T> jettyClass) {
        return factory.createInstance(purpose, jettyClass).join();
    }

    /**
     * Request the factory's optional {@code SSLParameters} companion for a purpose (one-shot) at factory-build
     * time, joining off any event loop, and return the supplied instance or {@link Optional#empty()} when the
     * factory supplies none. Rotation deliveries do NOT use this: they re-request the companion asynchronously
     * inside {@code onLoadOrReload} (never joined on the delivery thread) and unwrap it via {@link #extractBaseline}.
     */
    private static Optional<SSLParameters> resolveBaselineParameters(PulsarTlsFactory factory, TlsPurpose purpose) {
        return Optional.ofNullable(extractBaseline(factory.createInstance(purpose, SSLParameters.class).join()));
    }

    /**
     * Unwrap the factory's {@code SSLParameters} companion from its handle, disposing the handle afterwards (a
     * companion carries no reference-counted state). Returns {@code null} when the factory supplied none
     * ({@link Optional#empty()}). Used both by the build-time {@link #resolveBaselineParameters} and by the
     * asynchronous per-rotation re-request in {@code onLoadOrReload}.
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
     * Overlay a factory-supplied engine baseline onto a synthesized server factory (PIP-478 merge order):
     * enabled protocols/cipher suites when the companion sets them, and the companion's client-auth mode as
     * authoritative (rule 4) — {@code needClientAuth} wins over {@code wantClientAuth}, neither means none.
     */
    private static void applyServerBaseline(SslContextFactory.Server sslContextFactory, SSLParameters baseline) {
        if (baseline.getProtocols() != null) {
            sslContextFactory.setIncludeProtocols(baseline.getProtocols());
        }
        if (baseline.getCipherSuites() != null) {
            sslContextFactory.setIncludeCipherSuites(baseline.getCipherSuites());
        }
        if (baseline.getNeedClientAuth()) {
            sslContextFactory.setNeedClientAuth(true);
        } else if (baseline.getWantClientAuth()) {
            sslContextFactory.setNeedClientAuth(false);
            sslContextFactory.setWantClientAuth(true);
        } else {
            sslContextFactory.setNeedClientAuth(false);
            sslContextFactory.setWantClientAuth(false);
        }
    }

    /**
     * Overlay a factory-supplied engine baseline onto a synthesized client factory: enabled protocols/cipher
     * suites only (client-auth is a server concept; SNI/hostname verification remain per-connection).
     */
    private static void applyClientBaseline(SslContextFactory.Client client, SSLParameters baseline) {
        if (baseline.getProtocols() != null) {
            client.setIncludeProtocols(baseline.getProtocols());
        }
        if (baseline.getCipherSuites() != null) {
            client.setIncludeCipherSuites(baseline.getCipherSuites());
        }
    }
}
