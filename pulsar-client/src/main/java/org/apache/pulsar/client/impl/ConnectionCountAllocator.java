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

import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Least-connection selection for clients sharing a fixed event-loop group.
 * Counts include pending registrations and exclude ordinary tasks.
 * Established channels never migrate between event loops. Accounting is outside the message path.
 */
final class ConnectionCountAllocator {
    // Weak values are needed too: an allocator retains loops, which can retain their parent group.
    private static final Map<EventLoopGroup, WeakReference<ConnectionCountAllocator>> ALLOCATORS = new WeakHashMap<>();
    private final List<EventLoop> loops = new ArrayList<>();
    private final int[] counts;
    private int nextTie;

    static synchronized ConnectionCountAllocator forGroup(EventLoopGroup group) {
        WeakReference<ConnectionCountAllocator> reference = ALLOCATORS.get(group);
        ConnectionCountAllocator allocator = reference == null ? null : reference.get();
        if (allocator == null) {
            allocator = new ConnectionCountAllocator(group);
            ALLOCATORS.put(group, new WeakReference<>(allocator));
        }
        return allocator;
    }

    private ConnectionCountAllocator(EventLoopGroup group) {
        group.forEach(executor -> loops.add((EventLoop) executor));
        if (loops.isEmpty()) {
            throw new IllegalArgumentException("An event loop group must contain at least one loop");
        }
        counts = new int[loops.size()];
    }

    private synchronized int reserve() {
        int selected = nextTie;
        for (int offset = 1; offset < counts.length; offset++) {
            int candidate = (nextTie + offset) % counts.length;
            if (counts[candidate] < counts[selected]) {
                selected = candidate;
            }
        }
        counts[selected]++;
        nextTie = (selected + 1) % counts.length;
        return selected;
    }

    private synchronized void release(int selected) {
        counts[selected]--;
    }

    ChannelFuture register(Bootstrap bootstrap) {
        int selected = reserve();
        AtomicBoolean released = new AtomicBoolean();
        Runnable release = () -> {
            if (released.compareAndSet(false, true)) {
                release(selected);
            }
        };
        try {
            ChannelFuture registration = bootstrap.clone(loops.get(selected)).register();
            // A terminated event loop cannot deliver promise listeners. Handle immediate failures here.
            if (registration.isDone() && !registration.isSuccess()) {
                release.run();
                return registration;
            }
            ChannelFuture closed = registration.channel().closeFuture();
            closed.addListener(ignored -> release.run());
            registration.addListener(result -> {
                if (!result.isSuccess()) {
                    release.run();
                }
            });
            // Completion can race listener installation while the loop is terminating.
            if (closed.isDone() || (registration.isDone() && !registration.isSuccess())) {
                release.run();
            }
            return registration;
        } catch (RuntimeException | Error e) {
            release.run();
            throw e;
        }
    }

    @VisibleForTesting
    synchronized int[] counts() {
        return counts.clone();
    }
}
