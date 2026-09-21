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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class GracefulBundleUnloadTest {
    @Test
    public void slowBundleDoesNotSerializeOtherUnloads() throws Exception {
        NamespaceService namespaces = mock(NamespaceService.class);
        Map<NamespaceBundle, CompletableFuture<Void>> unloads = new LinkedHashMap<>();
        for (int i = 0; i < 4; i++) {
            unloads.put(mock(NamespaceBundle.class), new CompletableFuture<>());
        }
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        AtomicInteger started = new AtomicInteger();
        doAnswer(invocation -> {
            maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
            started.incrementAndGet();
            return unloads.get(invocation.getArgument(0)).whenComplete((__, error) -> active.decrementAndGet());
        }).when(namespaces).unloadNamespaceBundle(any(), anyLong(), any(), anyBoolean());
        CompletableFuture<Void> draining = CompletableFuture.runAsync(() -> {
            try {
                GracefulBundleUnload.unload(namespaces, unloads.keySet(), 2, 0, true, 10000,
                        () -> TimeUnit.SECONDS.toNanos(10));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        try {
            Awaitility.await().until(() -> started.get() == 2);
            assertThat(maximum.get()).isEqualTo(2);
            // Leave the first bundle blocked; freeing the second slot must start a third bundle.
            unloads.values().stream().skip(1).findFirst().orElseThrow().complete(null);
            Awaitility.await().until(() -> started.get() == 3);
            assertThat(draining.isDone()).isFalse();
        } finally {
            unloads.values().forEach(future -> future.complete(null));
            draining.get(10, TimeUnit.SECONDS);
        }
        assertThat(started.get()).isEqualTo(4);
        assertThat(maximum.get()).isEqualTo(2);
    }

    @Test
    public void deadlineInterruptsBlockingUnloadInvocation() throws Exception {
        NamespaceService namespaces = mock(NamespaceService.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
            return CompletableFuture.completedFuture(null);
        }).when(namespaces).unloadNamespaceBundle(any(), anyLong(), any(), anyBoolean());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        try {
            GracefulBundleUnload.unload(namespaces, Set.of(mock(NamespaceBundle.class)), 2, 0, true, 10000,
                    () -> Math.max(0, deadline - System.nanoTime()));
        } catch (TimeoutException expected) {
            // The caller proceeds to service cleanup when the shared deadline expires.
        } finally {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
            release.countDown();
        }
    }
}
