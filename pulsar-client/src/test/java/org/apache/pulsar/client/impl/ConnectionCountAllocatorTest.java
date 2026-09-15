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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@SuppressWarnings("deprecation")
public class ConnectionCountAllocatorTest {
    private Bootstrap bootstrap(NioEventLoopGroup group) {
        return new Bootstrap().group(group).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) {
                    }
                });
    }

    @Test
    public void balancesAndReusesClosedSlots() {
        var group = new NioEventLoopGroup(4);
        try {
            var allocator = ConnectionCountAllocator.forGroup(group);
            assertSame(ConnectionCountAllocator.forGroup(group), allocator);
            Bootstrap bootstrap = bootstrap(group);
            List<Channel> channels = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                // Unrelated group selections must not affect connection placement.
                group.next();
                group.next();
                group.next();
                channels.add(allocator.register(bootstrap).syncUninterruptibly().channel());
            }
            assertEquals(allocator.counts(), new int[]{10, 10, 10, 10});
            Channel closed = channels.remove(0);
            var loop = closed.eventLoop();
            closed.close().syncUninterruptibly();
            awaitCount(allocator, 39);
            Channel replacement = allocator.register(bootstrap).syncUninterruptibly().channel();
            assertSame(replacement.eventLoop(), loop);
            channels.add(replacement);
            channels.forEach(channel -> channel.close().syncUninterruptibly());
            awaitCount(allocator, 0);
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    @Test
    public void balancesConcurrentRegistrations() throws Exception {
        var group = new NioEventLoopGroup(4);
        var callers = Executors.newFixedThreadPool(8);
        try {
            var allocator = ConnectionCountAllocator.forGroup(group);
            Bootstrap bootstrap = bootstrap(group);
            List<Callable<ChannelFuture>> attempts = new ArrayList<>();
            for (int i = 0; i < 128; i++) {
                attempts.add(() -> allocator.register(bootstrap));
            }
            var registrations = callers.invokeAll(attempts, 10, TimeUnit.SECONDS);
            for (var registration : registrations) {
                registration.get().syncUninterruptibly();
            }
            assertEquals(allocator.counts(), new int[]{32, 32, 32, 32});
            for (var registration : registrations) {
                registration.get().channel().close().syncUninterruptibly();
            }
            awaitCount(allocator, 0);
        } finally {
            callers.shutdownNow();
            group.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    @Test
    public void releasesFailedRegistrations() {
        var group = new NioEventLoopGroup(2);
        try {
            var allocator = ConnectionCountAllocator.forGroup(group);
            expectThrows(IllegalStateException.class, () -> allocator.register(new Bootstrap().group(group)));
            awaitCount(allocator, 0);
            Bootstrap bootstrap = bootstrap(group);
            group.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
            ChannelFuture registration = allocator.register(bootstrap).awaitUninterruptibly();
            assertEquals(registration.isSuccess(), false);
            registration.channel().close();
            awaitCount(allocator, 0);
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
        }
    }

    private void awaitCount(ConnectionCountAllocator allocator, int expected) {
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertEquals(Arrays.stream(allocator.counts()).sum(), expected));
    }
}
