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
package org.apache.pulsar.client.impl;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.proxy.Socks5ProxyHandler;
import io.netty.handler.ssl.SslHandler;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import lombok.CustomLog;
import lombok.Getter;
import org.apache.pulsar.client.api.Socks5ProxyScope;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ClientTlsMaterialProviders;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.tls.ClientTlsPurposeContext;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialProvider;
import org.apache.pulsar.common.tls.PulsarTlsEngineProvider;
import org.apache.pulsar.common.tls.TlsPurposeContext;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.common.util.netty.NettyFutureUtil;

@CustomLog
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final Supplier<ClientCnx> clientCnxSupplier;
    @Getter
    private final boolean tlsEnabled;
    private final boolean tlsHostnameVerificationEnabled;
    private final InetSocketAddress socks5ProxyAddress;
    private final String socks5ProxyUsername;
    private final String socks5ProxyPassword;
    private final Socks5ProxyScope socks5ProxyScope;
    private final ClientConfigurationData conf;
    private final FileBasedTlsMaterialProvider tlsMaterialProvider;
    private final PulsarTlsEngineProvider tlsEngineProvider;
    private final TlsPurposeContext tlsPurpose = ClientTlsPurposeContext.of(
            ClientTlsPurposeContext.ClientPurpose.BINARY_CLIENT);

    public PulsarChannelInitializer(ClientConfigurationData conf, Supplier<ClientCnx> clientCnxSupplier,
                                    ScheduledExecutorService scheduledExecutorService) throws Exception {
        super();
        this.clientCnxSupplier = clientCnxSupplier;
        this.tlsEnabled = conf.isUseTls();
        this.tlsHostnameVerificationEnabled = conf.isTlsHostnameVerificationEnable();
        this.socks5ProxyAddress = conf.getSocks5ProxyAddress();
        this.socks5ProxyUsername = conf.getSocks5ProxyUsername();
        this.socks5ProxyPassword = conf.getSocks5ProxyPassword();
        this.socks5ProxyScope = conf.getSocks5ProxyScope();
        this.conf = conf.clone();
        if (tlsEnabled) {
            // PIP-478: the framework owns TLS material loading, caching and rotation. The provider polls
            // the configured files on the supplied scheduler and the engine provider rebuilds the cached
            // SSLEngine only when the material changes.
            this.tlsMaterialProvider = ClientTlsMaterialProviders.createInitializedProvider(this.conf,
                    ClientTlsPurposeContext.ClientPurpose.BINARY_CLIENT, null, scheduledExecutorService,
                    "pulsar-client-binary");
            this.tlsEngineProvider = new PulsarTlsEngineProvider(tlsMaterialProvider);
        } else {
            this.tlsMaterialProvider = null;
            this.tlsEngineProvider = null;
        }
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(1024, true));

        // Setup channel except for the SsHandler for TLS enabled connections
        ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.getEncoder(tlsEnabled));
        FrameDecoderUtil.addFrameDecoder(ch.pipeline(), Commands.DEFAULT_MAX_MESSAGE_SIZE);
        ChannelHandler clientCnx = clientCnxSupplier.get();
        ch.pipeline().addLast("handler", clientCnx);
    }

    /**
     * Initialize TLS for a channel. Should be invoked before the channel is connected to the remote address.
     *
     * @param ch      the channel
     * @param sniHost the value of this argument will be passed as peer host and port when creating the SSLEngine (which
     *                in turn will use these values to set SNI header when doing the TLS handshake). Cannot be
     *                <code>null</code>.
     * @return a {@link CompletableFuture} that completes when the TLS is set up.
     */
    CompletableFuture<Channel> initTls(Channel ch, InetSocketAddress sniHost) {
        if (ch == null) {
            return FutureUtil.failedFuture(new NullPointerException("A channel is required"));
        }
        if (sniHost == null) {
            return FutureUtil.failedFuture(new NullPointerException("A sniHost is required"));
        }
        if (!tlsEnabled) {
            return FutureUtil.failedFuture(new IllegalStateException("TLS is not enabled in client configuration"));
        }
        CompletableFuture<Channel> initTlsFuture = new CompletableFuture<>();
        ch.eventLoop().execute(() -> {
            try {
                SslHandler handler = new SslHandler(tlsEngineProvider
                        .createClientSslEngine(tlsPurpose, ch.alloc(), sniHost.getHostName(), sniHost.getPort())
                        .join());

                if (tlsHostnameVerificationEnabled) {
                    SecurityUtility.configureSSLHandler(handler);
                }

                ch.pipeline().addFirst(TLS_HANDLER, handler);
                initTlsFuture.complete(ch);
            } catch (Throwable t) {
                initTlsFuture.completeExceptionally(t);
            }
        });

        return initTlsFuture;
    }

    CompletableFuture<Channel> initSocks5IfConfig(Channel ch) {
        CompletableFuture<Channel> initSocks5Future = new CompletableFuture<>();
        // Only apply SOCKS5 to the binary protocol path when the scope includes binary connections.
        if (socks5ProxyAddress != null && socks5ProxyScope.appliesToBinary()) {
            ch.eventLoop().execute(() -> {
                try {
                    Socks5ProxyHandler socks5ProxyHandler =
                            new Socks5ProxyHandler(socks5ProxyAddress, socks5ProxyUsername, socks5ProxyPassword);
                    ch.pipeline().addFirst(socks5ProxyHandler.protocol(), socks5ProxyHandler);
                    initSocks5Future.complete(ch);
                } catch (Throwable t) {
                    initSocks5Future.completeExceptionally(t);
                }
            });
        } else {
            initSocks5Future.complete(ch);
        }

        return initSocks5Future;
    }

    /**
     * Sentinel logical address marking a connection that the proxy should pair to any broker it
     * selects (the client sends an empty proxyToBrokerUrl). It is never resolved or dialed — only
     * the physical address (the proxy) is — and is matched by identity.
     */
    static final InetSocketAddress PROXY_TO_ANY_BROKER =
            InetSocketAddress.createUnresolved("proxy-to-any-broker.pulsar.invalid", 0);

    CompletableFuture<Channel> initializeClientCnx(Channel ch,
                                                   InetSocketAddress logicalAddress,
                                                   InetSocketAddress unresolvedPhysicalAddress) {
        return NettyFutureUtil.toCompletableFuture(ch.eventLoop().submit(() -> {
            final ClientCnx cnx = (ClientCnx) ch.pipeline().get("handler");

            if (cnx == null) {
                throw new IllegalStateException("Missing ClientCnx. This should not happen.");
            }

            if (logicalAddress == PROXY_TO_ANY_BROKER) {
                // Pair through the proxy to any broker: send an empty proxyToBrokerUrl so the proxy
                // selects a broker and bridges this connection to it.
                cnx.setProxyToAnyBroker();
            } else if (!logicalAddress.equals(unresolvedPhysicalAddress)) {
                // We are connecting through a proxy. We need to set the target broker in the ClientCnx object so that
                // it can be specified when sending the CommandConnect.
                cnx.setTargetBroker(logicalAddress);
            }

            cnx.setRemoteHostName(unresolvedPhysicalAddress.getHostString());

            return ch;
        }));
    }

    /**
     * Release the TLS material provider (and its rotation polling task), if TLS was enabled.
     */
    public void close() {
        if (tlsMaterialProvider != null) {
            tlsMaterialProvider.close();
        }
    }

}

