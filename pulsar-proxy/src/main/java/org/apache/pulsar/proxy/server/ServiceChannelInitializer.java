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
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.protocol.OptionalProxyProtocolDecoder;
import org.apache.pulsar.common.tls.DefaultTlsMaterialProviderInitContext;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialProvider;
import org.apache.pulsar.common.tls.PulsarTlsEngineProvider;
import org.apache.pulsar.common.tls.ServerTlsPurposeContext;
import org.apache.pulsar.common.tls.ServerTlsPurposeContext.ServerPurpose;

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

    private FileBasedTlsMaterialProvider tlsMaterialProvider;
    private PulsarTlsEngineProvider tlsEngineProvider;

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
            // PIP-478: resolve the proxy server TLS material through the framework provider/engine.
            // The provider owns periodic file-rotation polling (using the supplied scheduler), so no
            // separate refresh task is scheduled here.
            this.tlsMaterialProvider = ProxyTlsMaterialProviders.forServer(serviceConfig);
            this.tlsMaterialProvider.initialize(new DefaultTlsMaterialProviderInitContext(
                    sslContextRefresher, Clock.systemUTC(), "pulsar-proxy")).join();
            this.tlsEngineProvider = new PulsarTlsEngineProvider(tlsMaterialProvider);
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(1024,
                true));
        if (this.enableTls) {
            ch.pipeline().addLast(TLS_HANDLER, new SslHandler(this.tlsEngineProvider
                    .createServerSslEngine(ServerTlsPurposeContext.of(ServerPurpose.PROXY), ch.alloc()).join()));
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

    /**
     * Release the TLS material provider (and its rotation poll task), if any. Safe to call multiple
     * times and when TLS is disabled.
     */
    public void close() {
        if (tlsMaterialProvider != null) {
            tlsMaterialProvider.close();
            tlsMaterialProvider = null;
        }
    }
}
