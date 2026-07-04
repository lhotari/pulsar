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

import java.util.Set;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsHandle;
import org.apache.pulsar.common.tls.TlsPurpose;
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
     * Build a self-reloading {@link SslContextFactory.Server} for a purpose, handed back <em>unstarted</em>
     * (Jetty starts it with the connector lifecycle) with its initial {@code SSLContext} already set.
     *
     * @param factory                  the TLS factory to subscribe to
     * @param purpose                  the server purpose (e.g. {@link TlsPurpose#WEB})
     * @param sslProviderString        the JCE provider name, or {@code null}/empty for the default
     * @param requireTrustedClientCert whether to require (vs. request) a trusted client certificate
     * @param ciphers                  enabled cipher suites, or {@code null} for defaults
     * @param protocols                enabled protocols, or {@code null} for defaults
     * @return the reloading factory and its subscription handle
     */
    public static ReloadableServerTls createReloadingServerFactory(PulsarTlsFactory factory, TlsPurpose purpose,
                                                                   String sslProviderString,
                                                                   boolean requireTrustedClientCert,
                                                                   Set<String> ciphers, Set<String> protocols) {
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        applyServerConfig(sslContextFactory, sslProviderString, requireTrustedClientCert, ciphers, protocols);

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
     * Build a plain {@link SslContextFactory.Client} from a one-shot {@code SSLContext} for the purpose
     * (used later by the proxy's {@code AdminProxyHandler}). Client-side, this is a one-shot snapshot —
     * no subscription.
     *
     * @param factory           the TLS factory
     * @param purpose           the client purpose (e.g. {@link TlsPurpose#BROKER_CLIENT})
     * @param sslProviderString the JCE provider name, or {@code null}/empty for the default
     * @return the configured client factory
     */
    public static SslContextFactory.Client createClientFactory(PulsarTlsFactory factory, TlsPurpose purpose,
                                                               String sslProviderString) {
        TlsHandle<SSLContext> handle = factory.createInstance(purpose, SSLContext.class).join()
                .orElseThrow(() -> new IllegalStateException(
                        "TLS factory supplied no SSLContext for purpose " + purpose));
        SslContextFactory.Client client = new SslContextFactory.Client();
        client.setSslContext(handle.get());
        if (StringUtils.isNotBlank(sslProviderString)) {
            client.setProvider(sslProviderString);
        }
        // The Jetty factory now holds the SSLContext; release this consumer's one-shot interest.
        handle.dispose();
        return client;
    }

    private static void applyServerConfig(SslContextFactory.Server sslContextFactory, String sslProviderString,
                                          boolean requireTrustedClientCertOnConnect,
                                          Set<String> ciphers, Set<String> protocols) {
        if (ciphers != null && !ciphers.isEmpty()) {
            sslContextFactory.setIncludeCipherSuites(ciphers.toArray(new String[0]));
        }
        if (protocols != null && !protocols.isEmpty()) {
            sslContextFactory.setIncludeProtocols(protocols.toArray(new String[0]));
        }
        if (StringUtils.isNotBlank(sslProviderString)) {
            sslContextFactory.setProvider(sslProviderString);
        }
        if (requireTrustedClientCertOnConnect) {
            sslContextFactory.setNeedClientAuth(true);
            sslContextFactory.setTrustAll(false);
        } else {
            sslContextFactory.setWantClientAuth(true);
            sslContextFactory.setTrustAll(true);
        }
        // https://jetty.org/docs/jetty/12.1/operations-guide/protocols/index.html#ssl-sni
        // Set to false for backwards compatibility with Jetty 9.x
        sslContextFactory.setSniRequired(false);
    }
}
