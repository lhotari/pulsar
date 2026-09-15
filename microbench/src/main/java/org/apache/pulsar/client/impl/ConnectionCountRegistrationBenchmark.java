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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** Measures registration/close accounting overhead, excluding TCP connect, TLS and application traffic. */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@SuppressWarnings("deprecation")
public class ConnectionCountRegistrationBenchmark {
    @Param({"false", "true"})
    public boolean balanced;
    private NioEventLoopGroup group;
    private Bootstrap bootstrap;
    private ConnectionCountAllocator allocator;

    @Setup(Level.Trial)
    public void setup() {
        group = new NioEventLoopGroup(4);
        allocator = ConnectionCountAllocator.forGroup(group);
        bootstrap = new Bootstrap().group(group).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel channel) {
                    }
                });
    }

    @Benchmark
    public void registerAndClose() {
        Channel channel = (balanced ? allocator.register(bootstrap) : bootstrap.register())
                .syncUninterruptibly().channel();
        channel.close().syncUninterruptibly();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        group.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
    }
}
