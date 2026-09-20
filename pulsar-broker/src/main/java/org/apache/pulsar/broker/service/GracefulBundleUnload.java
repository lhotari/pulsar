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

import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import lombok.CustomLog;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.util.FutureUtil;

/** Bounded parallel bundle draining, including unload calls that block before returning a future. */
@CustomLog
final class GracefulBundleUnload {
    private GracefulBundleUnload() {
    }

    static void unload(NamespaceService namespaceService, Set<NamespaceBundle> bundles, int concurrency,
                       int startsPerSecond, boolean force, long bundleTimeoutMs, LongSupplier remainingNanos)
            throws Exception {
        drain(bundles, (bundle, timeout) -> namespaceService.unloadNamespaceBundle(
                bundle, timeout, TimeUnit.NANOSECONDS, force), concurrency, startsPerSecond,
                bundleTimeoutMs, remainingNanos);
    }

    static <T> void drain(Collection<T> bundles, BiFunction<T, Long, CompletableFuture<Void>> close,
                         int concurrency, int startsPerSecond, long bundleTimeoutMs, LongSupplier remainingNanos)
            throws Exception {
        if (bundles.isEmpty() || remainingNanos.getAsLong() <= 0) {
            return;
        }
        ExecutorService workers = Executors.newFixedThreadPool(Math.min(concurrency, bundles.size()),
                new DefaultThreadFactory("broker-bundle-drain", true));
        RateLimiter rate = startsPerSecond > 0 ? RateLimiter.create(startsPerSecond) : null;
        AtomicInteger attempted = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger timedOut = new AtomicInteger();
        try {
            List<CompletableFuture<Void>> unloads = new ArrayList<>();
            for (T bundle : bundles) {
                if (bundle == null) {
                    continue;
                }
                unloads.add(CompletableFuture.runAsync(() -> {
                    try {
                        long remaining = remainingNanos.getAsLong();
                        if (remaining <= 0 || Thread.currentThread().isInterrupted()
                                || (rate != null && !rate.tryAcquire(1, remaining, TimeUnit.NANOSECONDS))) {
                            return;
                        }
                        long timeout = Math.min(TimeUnit.MILLISECONDS.toNanos(bundleTimeoutMs),
                                remainingNanos.getAsLong());
                        if (timeout <= 0 || Thread.currentThread().isInterrupted()) {
                            return;
                        }
                        attempted.incrementAndGet();
                        long started = System.nanoTime();
                        CompletableFuture<Void> unload = close.apply(bundle, timeout);
                        // The call itself can wait for a bundle's ownership lock. Include that time in the budget.
                        unload.get(Math.max(0, Math.min(timeout - (System.nanoTime() - started),
                                remainingNanos.getAsLong())), TimeUnit.NANOSECONDS);
                        completed.incrementAndGet();
                    } catch (TimeoutException e) {
                        timedOut.incrementAndGet();
                    } catch (InterruptedException e) {
                        timedOut.incrementAndGet();
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        failed.incrementAndGet();
                        log.warn().attr("bundle", bundle).exceptionMessage(e).log("Failed to unload namespace bundle");
                    }
                }, workers));
            }
            FutureUtil.waitForAll(unloads).get(Math.max(0, remainingNanos.getAsLong()), TimeUnit.NANOSECONDS);
        } finally {
            workers.shutdownNow();
            log.info().attr("attempted", attempted.get()).attr("completed", completed.get())
                    .attr("failed", failed.get()).attr("timedOut", timedOut.get())
                    .attr("unfinished", Math.max(0,
                            attempted.get() - completed.get() - failed.get() - timedOut.get()))
                    .attr("notStarted", Math.max(0, bundles.size() - attempted.get()))
                    .log("Bundle shutdown drain summary");
        }
    }
}
