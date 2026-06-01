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
package org.apache.pulsar.broker.service;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flow.FlowControlHandler;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.handler.ssl.SslHandler;
import java.time.Clock;
import lombok.Builder;
import lombok.CustomLog;
import lombok.Data;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.tls.DefaultBrokerTlsMaterialProvider;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.FrameDecoderUtil;
import org.apache.pulsar.common.protocol.OptionalProxyProtocolDecoder;
import org.apache.pulsar.common.tls.DefaultTlsMaterialProviderInitContext;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialProvider;
import org.apache.pulsar.common.tls.PulsarTlsEngineProvider;
import org.apache.pulsar.common.tls.ServerTlsPurposeContext;
import org.apache.pulsar.common.tls.ServerTlsPurposeContext.ServerPurpose;

@CustomLog
public class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final String TLS_HANDLER = "tls";

    private final PulsarService pulsar;
    private final String listenerName;
    private final boolean enableTls;
    private final ServiceConfiguration brokerConf;
    private FileBasedTlsMaterialProvider tlsMaterialProvider;
    private PulsarTlsEngineProvider tlsEngineProvider;

    /**
     * @param pulsar
     *              An instance of {@link PulsarService}
     * @param opts
     *              Channel options
     */
    public PulsarChannelInitializer(PulsarService pulsar, PulsarChannelOptions opts) throws Exception {
        super();
        this.pulsar = pulsar;
        this.listenerName = opts.getListenerName();
        this.enableTls = opts.isEnableTLS();
        ServiceConfiguration serviceConfig = pulsar.getConfiguration();
        if (this.enableTls) {
            // PIP-478: resolve the broker server TLS material through the framework provider/engine.
            // The provider owns periodic file-rotation polling (using the broker executor), so no
            // separate refresh task is scheduled here.
            this.tlsMaterialProvider = DefaultBrokerTlsMaterialProvider.forServer(serviceConfig);
            this.tlsMaterialProvider.initialize(new DefaultTlsMaterialProviderInitContext(
                    pulsar.getExecutor(), Clock.systemUTC(), "pulsar-broker")).join();
            this.tlsEngineProvider = new PulsarTlsEngineProvider(tlsMaterialProvider);
        }
        this.brokerConf = pulsar.getConfiguration();
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        // disable auto read explicitly so that requests aren't served until auto read is enabled
        // ServerCnx must enable auto read in channelActive after PulsarService is ready to accept incoming requests
        ch.config().setAutoRead(false);
        ch.config().setWriteBufferHighWaterMark(pulsar.getConfig().getPulsarChannelWriteBufferHighWaterMark());
        ch.config().setWriteBufferLowWaterMark(pulsar.getConfig().getPulsarChannelWriteBufferLowWaterMark());
        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(1024, true));
        if (this.enableTls) {
            ch.pipeline().addLast(TLS_HANDLER, new SslHandler(this.tlsEngineProvider
                    .createServerSslEngine(ServerTlsPurposeContext.of(ServerPurpose.BROKER), ch.alloc()).join()));
        }
        ch.pipeline().addLast("ByteBufPairEncoder", ByteBufPair.getEncoder(this.enableTls));

        if (pulsar.getConfiguration().isHaProxyProtocolEnabled()) {
            ch.pipeline().addLast(OptionalProxyProtocolDecoder.NAME, new OptionalProxyProtocolDecoder());
        }
        FrameDecoderUtil.addFrameDecoder(ch.pipeline(), brokerConf.getMaxMessageSize());
        // https://stackoverflow.com/questions/37535482/netty-disabling-auto-read-doesnt-work-for-bytetomessagedecoder
        // Classes such as {@link ByteToMessageDecoder} or {@link MessageToByteEncoder} are free to emit as many events
        // as they like for any given input. so, disabling auto-read on `ByteToMessageDecoder` doesn't work properly and
        // ServerCnx ends up reading higher number of messages and broker can not throttle the messages by disabling
        // auto-read.
        ch.pipeline().addLast("flowController", new FlowControlHandler());
        // using "ChannelHandler" type to workaround an IntelliJ bug that shows a false positive error
        ChannelHandler cnx = newServerCnx(pulsar, listenerName);
        ch.pipeline().addLast("handler", cnx);
    }

    @VisibleForTesting
    protected ServerCnx newServerCnx(PulsarService pulsar, String listenerName) throws Exception {
        return new ServerCnx(pulsar, listenerName);
    }

    public interface Factory {
        PulsarChannelInitializer newPulsarChannelInitializer(
                PulsarService pulsar, PulsarChannelOptions opts) throws Exception;
    }

    public static final Factory DEFAULT_FACTORY = PulsarChannelInitializer::new;

    @Data
    @Builder
    public static class PulsarChannelOptions {

        /**
         * Indicates whether to enable TLS on the channel.
         */
        private boolean enableTLS;

        /**
         * The name of the listener to associate with the channel (optional).
         */
        private String listenerName;
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
