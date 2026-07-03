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
package org.apache.pulsar.proxy.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.opentelemetry.api.OpenTelemetry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.pulsar.broker.tls.TlsFactorySupport;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.protocol.OptionalProxyProtocolDecoder;
import org.apache.pulsar.common.tls.PulsarTlsFactory;
import org.apache.pulsar.common.tls.TlsFactoryInitContext;
import org.apache.pulsar.common.tls.TlsHandle;
import org.apache.pulsar.common.tls.TlsPurpose;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;

/**
 * Initialize service channel handlers.
 *
 */
@CustomLog
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";
    private final ProxyService proxyService;
    private final boolean enableTls;
    private final boolean tlsEnabledWithKeyStore;
    private final int brokerProxyReadTimeoutMs;
    private final int maxMessageSize;

    // PIP-337 legacy path.
    private PulsarSslFactory sslFactory;
    // PIP-478 path (used when selected instead of the legacy PulsarSslFactory).
    private PulsarTlsFactory tlsFactory;
    private TlsHandle<SslContext> tlsSubscription;
    private volatile SslContext tlsServerContext;

    public ServiceChannelInitializer(ProxyService proxyService, ProxyConfiguration serviceConfig,
                                     boolean enableTls, ScheduledExecutorService sslContextRefresher)
            throws Exception {
        super();
        this.proxyService = proxyService;
        this.enableTls = enableTls;
        this.tlsEnabledWithKeyStore = serviceConfig.isTlsEnabledWithKeyStore();
        this.brokerProxyReadTimeoutMs = serviceConfig.getBrokerProxyReadTimeoutMs();
        this.maxMessageSize = serviceConfig.getMaxMessageSize();

        if (enableTls) {
            if (TlsFactorySupport.selectPath(serviceConfig.getSslFactoryPlugin(),
                    serviceConfig.getTlsFactoryClassName()) == TlsFactorySupport.TlsPath.NEW) {
                initializeTlsFactory(serviceConfig, sslContextRefresher);
            } else {
                initializeLegacySslFactory(serviceConfig, sslContextRefresher);
            }
        }
    }

    private void initializeLegacySslFactory(ProxyConfiguration serviceConfig,
                                            ScheduledExecutorService sslContextRefresher) throws Exception {
        PulsarSslConfiguration sslConfiguration = buildSslConfiguration(serviceConfig);
        this.sslFactory = (PulsarSslFactory) Class.forName(serviceConfig.getSslFactoryPlugin())
                .getConstructor().newInstance();
        this.sslFactory.initialize(sslConfiguration);
        this.sslFactory.createInternalSslContext();
        if (serviceConfig.getTlsCertRefreshCheckDurationSec() > 0) {
            sslContextRefresher.scheduleWithFixedDelay(this::refreshSslContext,
                    serviceConfig.getTlsCertRefreshCheckDurationSec(),
                    serviceConfig.getTlsCertRefreshCheckDurationSec(), TimeUnit.SECONDS);
        }
    }

    // PIP-478: build the PulsarTlsFactory and subscribe to the PROXY purpose; the volatile Netty SslContext
    // holds the latest instance (rotation delivered by the subscription; no cert-refresh task).
    private void initializeTlsFactory(ProxyConfiguration serviceConfig,
                                      ScheduledExecutorService scheduler) throws Exception {
        this.tlsFactory = TlsFactorySupport.createFactory(serviceConfig.getTlsFactoryClassName(), null,
                () -> ProxyTlsFactories.serverFactory(serviceConfig, TlsPurpose.PROXY,
                        serviceConfig.getTlsCiphers(), serviceConfig.getTlsProtocols()));
        TlsFactoryInitContext initContext = TlsFactorySupport.initContext(
                TlsFactorySupport.parseFactoryConfig(serviceConfig.getTlsFactoryConfig()),
                scheduler, scheduler, OpenTelemetry.noop());
        TlsFactorySupport.initializeBlocking(this.tlsFactory, initContext);
        this.tlsSubscription = this.tlsFactory
                .createInstance(TlsPurpose.PROXY, SslContext.class, ctx -> this.tlsServerContext = ctx)
                .get()
                .orElseThrow(() -> new IllegalStateException(
                        "TLS factory supplied no Netty SslContext for purpose " + TlsPurpose.PROXY));
    }

    /** Dispose the PIP-478 TLS factory and its subscription, if any. */
    public void close() {
        TlsHandle<SslContext> subscription = this.tlsSubscription;
        if (subscription != null) {
            this.tlsSubscription = null;
            subscription.dispose();
        }
        PulsarTlsFactory factory = this.tlsFactory;
        if (factory != null) {
            this.tlsFactory = null;
            factory.close();
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(1024,
                true));
        if (this.enableTls) {
            final SslHandler tlsHandler = this.tlsFactory != null
                    ? this.tlsServerContext.newHandler(ch.alloc())
                    : new SslHandler(this.sslFactory.createServerSslEngine(ch.alloc()));
            ch.pipeline().addLast(TLS_HANDLER, tlsHandler);
        }
        if (brokerProxyReadTimeoutMs > 0) {
            ch.pipeline().addLast("readTimeoutHandler",
                    new ReadTimeoutHandler(brokerProxyReadTimeoutMs, TimeUnit.MILLISECONDS));
        }
        if (proxyService.getConfiguration().isHaProxyProtocolEnabled()) {
            ch.pipeline().addLast(OptionalProxyProtocolDecoder.NAME, new OptionalProxyProtocolDecoder());
        }
        FrameDecoderUtil.addFrameDecoder(ch.pipeline(), maxMessageSize);
        ch.pipeline().addLast("handler", new ProxyConnection(proxyService, proxyService.getDnsAddressResolverGroup()));
    }

    protected PulsarSslConfiguration buildSslConfiguration(ProxyConfiguration config) {
        return PulsarSslConfiguration.builder()
                .tlsProvider(config.getTlsProvider())
                .tlsKeyStoreType(config.getTlsKeyStoreType())
                .tlsKeyStorePath(config.getTlsKeyStore())
                .tlsKeyStorePassword(config.getTlsKeyStorePassword())
                .tlsTrustStoreType(config.getTlsTrustStoreType())
                .tlsTrustStorePath(config.getTlsTrustStore())
                .tlsTrustStorePassword(config.getTlsTrustStorePassword())
                .tlsCiphers(config.getTlsCiphers())
                .tlsProtocols(config.getTlsProtocols())
                .tlsTrustCertsFilePath(config.getTlsTrustCertsFilePath())
                .tlsCertificateFilePath(config.getTlsCertificateFilePath())
                .tlsKeyFilePath(config.getTlsKeyFilePath())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(config.isTlsRequireTrustedClientCertOnConnect())
                .tlsEnabledWithKeystore(config.isTlsEnabledWithKeyStore())
                .tlsCustomParams(config.getSslFactoryPluginParams())
                .authData(null)
                .serverMode(true)
                .build();
    }

    protected void refreshSslContext() {
        try {
            this.sslFactory.update();
        } catch (Exception e) {
            log.error().exception(e).log("Failed to refresh SSL context");
        }
    }
}
