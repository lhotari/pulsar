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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478: verifies the {@code ClientCnx} async authentication carve-out against an
 * {@link AsyncAuthenticationDriver}-capable built-in plugin. Guards the acceptance invariants of the
 * single-continuation refactor — the connection's {@code authenticationDataProvider} observable
 * ({@code getCommandData()}) reflects the freshly-resolved credential on connect and on a broker-pushed
 * REFRESH, and a REFRESH does not disconnect ({@code getLastDisconnectedTimestamp()} unchanged). This is
 * the regression the prior implementation attempt dropped when it duplicated the async path.
 */
public class ClientCnxAsyncAuthTest {

    private EventLoopGroup eventLoop;
    private ClientCnx cnx;
    private AtomicInteger tokenCounter;

    @BeforeMethod
    void setup() throws Exception {
        eventLoop = EventLoopUtil.newEventLoopGroup(1, false, new DefaultThreadFactory("testClientCnxAsyncAuth"));
        tokenCounter = new AtomicInteger();

        // A real async-capable built-in plugin (AuthenticationToken implements AsyncAuthenticationDriver via
        // its v5-native body); the supplier hands out a fresh token on every credential resolution, so the
        // connect round and each subsequent REFRESH round observe distinct command data.
        AuthenticationToken auth = new AuthenticationToken(() -> "token-" + tokenCounter.incrementAndGet());
        assertThat(auth).isInstanceOf(AsyncAuthenticationDriver.class);

        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setKeepAliveIntervalSeconds(0);
        conf.setOperationTimeoutMs(1);
        conf.setAuthentication(auth);
        cnx = new ClientCnx(InstrumentProvider.NOOP, conf, eventLoop);
        cnx.setRemoteHostName("localhost");

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(ctx.writeAndFlush(any())).thenAnswer(args -> mock(ChannelFuture.class));
        when(ctx.channel()).thenReturn(channel);
        when(ctx.executor()).thenReturn(eventLoop.next());
        when(channel.remoteAddress()).thenReturn(new InetSocketAddress("localhost", 6650));
        cnx.channelActive(ctx);
    }

    @AfterMethod(alwaysRun = true)
    void cleanup() {
        if (eventLoop != null) {
            eventLoop.shutdownGracefully();
        }
    }

    @Test
    void refreshRepublishesCredentialWithoutDisconnect() {
        // Connect resolved the first credential and published it as the observable data provider.
        assertThat(cnx.getAuthenticationDataProvider().getCommandData()).isEqualTo("token-1");
        long lastDisconnect = cnx.getLastDisconnectedTimestamp();

        // Broker pushes the REFRESH sentinel: the async path must open a fresh exchange, re-produce the
        // current credential, and republish the data provider — without disconnecting.
        cnx.handleAuthChallenge(refreshChallenge());

        assertThat(cnx.getAuthenticationDataProvider().getCommandData()).isEqualTo("token-2");
        assertThat(cnx.getLastDisconnectedTimestamp()).isEqualTo(lastDisconnect);

        // A second REFRESH re-produces again, still on the single assignment site.
        cnx.handleAuthChallenge(refreshChallenge());
        assertThat(cnx.getAuthenticationDataProvider().getCommandData()).isEqualTo("token-3");
        assertThat(cnx.getLastDisconnectedTimestamp()).isEqualTo(lastDisconnect);
    }

    private static CommandAuthChallenge refreshChallenge() {
        CommandAuthChallenge challenge = new CommandAuthChallenge();
        challenge.setChallenge()
                .setAuthData(AuthData.REFRESH_AUTH_DATA_BYTES)
                .setAuthMethodName("token");
        return challenge;
    }
}
