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
package org.apache.pulsar.client.impl.v5.http;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.http.ClientHttpTlsPurpose;
import org.apache.pulsar.client.api.v5.http.ProxyConfig;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClient;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientConfig;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientConfig.UsageIdentifier;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactory;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactoryConfig;
import org.apache.pulsar.client.util.PulsarTlsAsyncHttpSslEngineFactory;
import org.apache.pulsar.common.tls.ClientTlsPurposeContext;
import org.apache.pulsar.common.tls.PulsarTlsEngineProvider;
import org.apache.pulsar.common.tls.PulsarTlsMaterialProvider;
import org.apache.pulsar.common.tls.TlsPurposeContext;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Realm;
import org.asynchttpclient.SslEngineFactory;
import org.asynchttpclient.proxy.ProxyServer;
import org.asynchttpclient.proxy.ProxyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default {@link PulsarHttpClientFactory} (PIP-478), building AsyncHttpClient-backed
 * {@link PulsarHttpClient} instances that share this factory's Netty event loop and timer.
 *
 * <p>One factory exists per {@code PulsarClient}. Because the public
 * {@link PulsarHttpClientFactoryConfig} SPI does not yet expose the framework's Netty event-loop
 * group or timer (only a {@code clientInstanceId} and an {@code OpenTelemetry} handle), this factory
 * creates its own {@link NioEventLoopGroup} + {@link HashedWheelTimer} lazily on first use and shares
 * them across every {@link PulsarHttpClient} it builds. They are released by {@link #close()}; the
 * per-client {@link PulsarHttpClient#close()} only closes that client's own AsyncHttpClient instance,
 * matching the SPI contract that shared resources are factory/framework-owned.
 *
 * <p>If a {@link PulsarTlsMaterialProvider} is wired in, HTTPS clients resolve their {@code SSLContext}
 * from it via {@link PulsarTlsAsyncHttpSslEngineFactory}; otherwise HTTPS uses the platform default
 * trust store and no client certificate.
 */
public class AsyncHttpClientFactory implements PulsarHttpClientFactory, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AsyncHttpClientFactory.class);

    private final String clientInstanceId;
    private final PulsarTlsMaterialProvider tlsMaterialProvider;
    private final PulsarTlsEngineProvider tlsEngineProvider;

    private final Object lifecycleLock = new Object();
    private EventLoopGroup eventLoopGroup;
    private Timer timer;
    private boolean closed;

    AsyncHttpClientFactory(PulsarHttpClientFactoryConfig sharedResources,
                           PulsarTlsMaterialProvider tlsMaterialProvider) {
        this.clientInstanceId =
                sharedResources != null ? sharedResources.clientInstanceId() : "unknown";
        this.tlsMaterialProvider = tlsMaterialProvider;
        this.tlsEngineProvider =
                tlsMaterialProvider != null ? new PulsarTlsEngineProvider(tlsMaterialProvider) : null;
    }

    @Override
    public PulsarHttpClient newHttpClient(PulsarHttpClientConfig config) {
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("AsyncHttpClientFactory is closed");
            }
            ensureSharedResources();
            AsyncHttpClient asyncHttpClient = new DefaultAsyncHttpClient(buildConfig(config));
            return new AsyncHttpPulsarClient(asyncHttpClient, config);
        }
    }

    private void ensureSharedResources() {
        if (eventLoopGroup == null) {
            // The public SPI does not hand us the framework's shared event loop / timer yet, so we own
            // them here and close them in close(). AsyncHttpClient would otherwise create one set per
            // client; sharing one set across all clients of this PulsarClient avoids that fan-out.
            eventLoopGroup = new NioEventLoopGroup(
                    new DefaultThreadFactory("pulsar-v5-httpclient-" + clientInstanceId));
            timer = new HashedWheelTimer(
                    new DefaultThreadFactory("pulsar-v5-httpclient-timer-" + clientInstanceId));
        }
    }

    private DefaultAsyncHttpClientConfig buildConfig(PulsarHttpClientConfig config) {
        DefaultAsyncHttpClientConfig.Builder b = new DefaultAsyncHttpClientConfig.Builder();
        // Mirror org.apache.pulsar.client.impl.HttpClient's configuration approach.
        b.setCookieStore(null);
        b.setUseProxyProperties(true);
        // Redirects are surfaced to the caller as-is; the plugin layer decides how to follow them so
        // that the Authorization header is re-derived per hop (AHC >= 2.14.5 strips it on redirect).
        b.setFollowRedirect(false);
        b.setConnectTimeout((int) config.connectTimeout().toMillis());
        b.setReadTimeout((int) config.readTimeout().toMillis());
        b.setRequestTimeout((int) config.requestTimeout().toMillis());
        b.setUserAgent(config.userAgent());

        // TLS. When a material provider is wired, bridge to it; otherwise rely on platform trust.
        if (tlsEngineProvider != null) {
            TlsPurposeContext purpose = toPurposeContext(config.tlsPurpose(), config.usageIdentifier());
            // Host is left null: the AsyncHttpClient SslEngineFactory sets SNI from the connection's
            // peer host per request, and endpoint identification is governed by the flag below.
            SslEngineFactory sslEngineFactory =
                    new PulsarTlsAsyncHttpSslEngineFactory(tlsEngineProvider, purpose, null);
            b.setSslEngineFactory(sslEngineFactory);
        }
        if (config.allowInsecureConnection()) {
            log.warn("HTTP client for {} configured to trust any server certificate (insecure)",
                    clientInstanceId);
            b.setUseInsecureTrustManager(true);
        }
        b.setDisableHttpsEndpointIdentificationAlgorithm(!config.hostnameVerificationEnabled());

        config.proxy().ifPresent(proxy -> b.setProxyServer(toProxyServer(proxy)));

        b.setEventLoopGroup(eventLoopGroup);
        b.setNettyTimer(timer);
        return b.build();
    }

    /**
     * Map the Tier-0 {@link ClientHttpTlsPurpose} one-to-one (by name) onto a framework
     * {@link ClientTlsPurposeContext}, threading any {@link UsageIdentifier} through.
     */
    private static ClientTlsPurposeContext toPurposeContext(ClientHttpTlsPurpose purpose,
                                                            Optional<UsageIdentifier> usageIdentifier) {
        ClientTlsPurposeContext.ClientPurpose mapped =
                ClientTlsPurposeContext.ClientPurpose.valueOf(purpose.name());
        return usageIdentifier
                .map(u -> new ClientTlsPurposeContext(mapped,
                        new TlsPurposeContext.UsageIdentifier(u.owner(), u.identifier())))
                .orElseGet(() -> new ClientTlsPurposeContext(mapped));
    }

    private static ProxyServer toProxyServer(ProxyConfig proxy) {
        ProxyServer.Builder builder = new ProxyServer.Builder(proxy.host(), proxy.port())
                .setProxyType(proxy.type() == ProxyConfig.Type.SOCKS5 ? ProxyType.SOCKS_V5 : ProxyType.HTTP);
        proxy.usernameOptional().ifPresent(username -> {
            Realm realm = new Realm.Builder(username, proxy.password())
                    .setScheme(Realm.AuthScheme.BASIC)
                    .build();
            builder.setRealm(realm);
        });
        return builder.build();
    }

    /**
     * Release this factory's shared event loop and timer. Idempotent.
     */
    @Override
    public void close() {
        EventLoopGroup elgToClose;
        Timer timerToClose;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            elgToClose = eventLoopGroup;
            timerToClose = timer;
            eventLoopGroup = null;
            timer = null;
        }
        if (timerToClose != null) {
            timerToClose.stop();
        }
        if (elgToClose != null) {
            elgToClose.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        }
    }

    Map<String, String> describe() {
        return Map.of("clientInstanceId", clientInstanceId,
                "tlsMaterialProvider", String.valueOf(tlsMaterialProvider != null));
    }
}
