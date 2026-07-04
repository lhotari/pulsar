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
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsHandle;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.apache.pulsar.common.tls.impl.TlsContexts;
import org.apache.pulsar.common.util.SecurityUtility;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * The framework's Jetty integration for the PIP-478 TLS SPI: synthesizes a <em>vanilla</em> (never
 * subclassed) {@link SslContextFactory.Server} / {@link SslContextFactory.Client} from a
 * {@link PulsarTlsFactory} that returns {@code empty()} for the Jetty class.
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
 * {@link SslContextFactory.Server#setWantClientAuth}). The companion is requested one-shot (a
 * factory-supplied {@code SslContextFactory.Server} would reload internally, but this synthesized factory
 * applies protocols/ciphers/client-auth once at build time — they cannot change on a bare {@code reload});
 * {@code empty()} leaves the consumer's configuration in force, exactly as before.
 */
@CustomLog
public final class JettyTlsFactory {

    static {
        // DO NOT EDIT - Load Conscrypt provider
        if (SecurityUtility.CONSCRYPT_PROVIDER != null) {
        }
    }

    private JettyTlsFactory() {
    }

    /**
     * A synthesized, self-reloading Jetty server factory together with the subscription that drives it;
     * dispose the {@link #subscription()} when the web service stops.
     *
     * @param sslContextFactory the (unstarted) Jetty server factory
     * @param subscription      the reload subscription backing the factory
     */
    public record ReloadableServerTls(SslContextFactory.Server sslContextFactory,
                                      TlsHandle<SSLContext> subscription) {
    }

    /**
     * A synthesized, self-reloading Jetty client factory together with the subscription that drives it;
     * dispose the {@link #subscription()} when the owning HTTP client / servlet is destroyed.
     *
     * @param sslContextFactory the (unstarted) Jetty client factory
     * @param subscription      the reload subscription backing the factory
     */
    public record ReloadableClientTls(SslContextFactory.Client sslContextFactory,
                                      TlsHandle<SSLContext> subscription) {
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
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        applyServerConfig(sslContextFactory, sslProviderString, requireTrustedClientCert, allowInsecureConnection,
                ciphers, protocols);
        // Merge the factory's engine-policy companion (if any) over the consumer config, before start.
        resolveBaselineParameters(factory, purpose)
                .ifPresent(baseline -> applyServerBaseline(sslContextFactory, baseline));

        Consumer<SSLContext> onLoadOrReload = newContext -> {
            if (sslContextFactory.isStarted()) {
                try {
                    sslContextFactory.reload(f -> ((SslContextFactory.Server) f).setSslContext(newContext));
                } catch (Exception e) {
                    log.warn().attr("purpose", purpose).exception(e)
                            .log("Failed to reload Jetty SslContextFactory; keeping the running context");
                }
            } else {
                // Initial load (or a reload before the connector started): set directly before start.
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
     * Build a self-reloading {@link SslContextFactory.Client} for a purpose (used by the proxy's
     * {@code AdminProxyHandler}, whose Jetty {@code HttpClient} outlives broker-client material rotation),
     * handed back <em>unstarted</em> with its initial {@code SSLContext} already set. This mirrors
     * {@link #createReloadingServerFactory}: an {@code SSLContext} subscription drives
     * {@link SslContextFactory#setSslContext(SSLContext)} before start, and on each later delivery
     * {@link SslContextFactory#reload(Consumer)} atomically swaps the context so new connections use the
     * rotated material. Dispose the returned {@link ReloadableClientTls#subscription()} when the owning
     * client is destroyed.
     *
     * @param factory           the TLS factory to subscribe to
     * @param purpose           the client purpose (e.g. {@link TlsPurpose#BROKER_CLIENT})
     * @param sslProviderString the JCE provider name, or {@code null}/empty for the default
     * @return the reloading client factory and its subscription handle
     */
    public static ReloadableClientTls createReloadingClientFactory(PulsarTlsFactory factory, TlsPurpose purpose,
                                                                   String sslProviderString) {
        SslContextFactory.Client client = new SslContextFactory.Client();
        if (StringUtils.isNotBlank(sslProviderString)) {
            client.setProvider(sslProviderString);
        }
        // Pin the {TLSv1.3, TLSv1.2} floor (B3), matching the native path; a factory companion overrides it below.
        client.setIncludeProtocols(TlsContexts.DEFAULT_ENABLED_PROTOCOLS.toArray(new String[0]));
        // Merge the factory's engine-policy companion (if any): protocols/ciphers only — client-auth is a
        // server concept, and SNI/hostname verification stay per-connection.
        resolveBaselineParameters(factory, purpose)
                .ifPresent(baseline -> applyClientBaseline(client, baseline));

        Consumer<SSLContext> onLoadOrReload = newContext -> {
            if (client.isStarted()) {
                try {
                    client.reload(f -> ((SslContextFactory.Client) f).setSslContext(newContext));
                } catch (Exception e) {
                    log.warn().attr("purpose", purpose).exception(e)
                            .log("Failed to reload Jetty client SslContextFactory; keeping the running context");
                }
            } else {
                // Initial load (or a reload before the client started): set directly before start.
                client.setSslContext(newContext);
            }
        };

        TlsHandle<SSLContext> subscription = factory.createInstance(purpose, SSLContext.class, onLoadOrReload)
                .join()
                .orElseThrow(() -> new IllegalStateException(
                        "TLS factory supplied no SSLContext for purpose " + purpose));
        return new ReloadableClientTls(client, subscription);
    }

    private static void applyServerConfig(SslContextFactory.Server sslContextFactory, String sslProviderString,
                                          boolean requireTrustedClientCertOnConnect, boolean allowInsecureConnection,
                                          Set<String> ciphers, Set<String> protocols) {
        if (ciphers != null && !ciphers.isEmpty()) {
            sslContextFactory.setIncludeCipherSuites(ciphers.toArray(new String[0]));
        }
        // Pin the enabled protocols even when unconfigured, matching the {TLSv1.3, TLSv1.2} floor the native
        // Netty path applies rather than deferring to the provider default (B3). A factory-supplied companion,
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
            // PIP-478 (F2): optional client auth requests but does not require a client cert. An untrusted
            // client cert is accepted at the handshake only when tlsAllowInsecureConnection=true, aligning the
            // web listener with the Netty binary listener's D3 semantics and diverging from the pre-5.0 Jetty
            // path, which trusted any presented client cert whenever client auth was optional (see PIP-478
            // Security Considerations).
            //
            // The actual enforcement is the trust managers baked into the SSLContext the framework hands to
            // Jetty via setSslContext (built from the WEB TlsPolicy's allowInsecureConnection: CA-validating
            // when secure, insecure-trust-all when insecure). Jetty's own setTrustAll only takes effect when
            // Jetty builds the SSLContext itself, so it is inert on this setSslContext path; we still scope it
            // to the insecure flag so the two never disagree (defence in depth) rather than leaving the
            // inherited unconditional setTrustAll(true).
            sslContextFactory.setWantClientAuth(true);
            sslContextFactory.setTrustAll(allowInsecureConnection);
        }
        // https://jetty.org/docs/jetty/12.1/operations-guide/protocols/index.html#ssl-sni
        // Set to false for backwards compatibility with Jetty 9.x
        sslContextFactory.setSniRequired(false);
    }

    /**
     * Request the factory's optional {@code SSLParameters} companion for a purpose (one-shot), returning the
     * supplied instance or {@link Optional#empty()} when the factory supplies none. Runs at factory-build time,
     * off any event loop; the handle is disposed immediately (a companion carries no reference-counted state).
     */
    private static Optional<SSLParameters> resolveBaselineParameters(PulsarTlsFactory factory, TlsPurpose purpose) {
        return factory.createInstance(purpose, SSLParameters.class)
                .join()
                .map(handle -> {
                    try {
                        return handle.get();
                    } finally {
                        handle.dispose();
                    }
                });
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
