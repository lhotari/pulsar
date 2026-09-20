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
package org.apache.pulsar.broker;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.common.util.FutureUtil;

/** One deadline, independent of the broker executors and of potentially blocking component closes. */
@CustomLog
final class BrokerShutdown {
    private final long startedNanos = System.nanoTime();
    private final long timeoutNanos;
    private final long cleanupReserveNanos;
    private final CompletableFuture<Void> result = new CompletableFuture<>();
    private final AtomicBoolean closingSessions = new AtomicBoolean();
    private final Supplier<CompletableFuture<Void>> closeSessions;
    private final ScheduledExecutorService watchdog;
    private volatile Throwable failure;
    private volatile String phase = "bundle drain";

    BrokerShutdown(long timeoutMs, Supplier<CompletableFuture<Void>> closeSessions) {
        timeoutNanos = timeoutMs > 0 ? TimeUnit.MILLISECONDS.toNanos(timeoutMs) : Long.MAX_VALUE;
        cleanupReserveNanos = Math.min(TimeUnit.SECONDS.toNanos(5), timeoutNanos / 5);
        this.closeSessions = closeSessions;
        watchdog = timeoutMs > 0 ? Executors.newSingleThreadScheduledExecutor(
                runnable -> newDaemonThread("pulsar-shutdown-watchdog", runnable)) : null;
    }

    CompletableFuture<Void> start(Supplier<CompletableFuture<Void>> closeServices) {
        if (watchdog != null) {
            watchdog.schedule(() -> {
                failure = new TimeoutException("Broker shutdown budget exhausted during " + phase);
                String exhaustedPhase = phase;
                finish();
                log.warn().attr("phase", exhaustedPhase).log("Shutdown budget exhausted; closing metadata sessions");
            }, remainingDrainNanos(), TimeUnit.NANOSECONDS);
            watchdog.schedule(() -> {
                result.completeExceptionally(new TimeoutException("Broker shutdown deadline expired during " + phase));
                log.warn().attr("phase", phase).log("Overall broker shutdown deadline expired");
            }, remainingNanos(), TimeUnit.NANOSECONDS);
            result.whenComplete((__, error) -> watchdog.shutdownNow());
        }
        newDaemonThread("pulsar-service-close", () -> FutureUtil.supplySafely(closeServices)
                .whenComplete((__, error) -> {
                    if (error != null) {
                        failure = error;
                    }
                    finish();
                })).start();
        return result;
    }

    long remainingDrainNanos() {
        return closingSessions.get() ? 0 : timeoutNanos == Long.MAX_VALUE ? Long.MAX_VALUE
                : Math.max(0, remainingNanos() - cleanupReserveNanos);
    }

    long deadlineNanos() {
        return startedNanos + timeoutNanos;
    }

    void finishImmediately(long timeoutMs) {
        // This also bounds escalation of an already-running, otherwise unbounded embedded shutdown.
        ScheduledExecutorService immediateWatchdog = Executors.newSingleThreadScheduledExecutor(
                runnable -> newDaemonThread("pulsar-immediate-shutdown-watchdog", runnable));
        immediateWatchdog.schedule(() -> result.completeExceptionally(
                new TimeoutException("Immediate shutdown metadata-session cleanup timed out")),
                timeoutMs, TimeUnit.MILLISECONDS);
        result.whenComplete((__, error) -> immediateWatchdog.shutdownNow());
        finish();
    }

    private long remainingNanos() {
        return Math.max(0, timeoutNanos - (System.nanoTime() - startedNanos));
    }

    void servicesClosing() {
        if (!closingSessions.get()) {
            phase = "service cleanup";
        }
    }

    void finish() {
        if (!closingSessions.compareAndSet(false, true)) {
            return;
        }
        phase = "metadata session cleanup";
        // The supplier only fences the broker and starts independent daemon threads. It must never wait for IO.
        FutureUtil.supplySafely(closeSessions).whenComplete((__, error) -> {
            Throwable cause = failure != null ? failure : error;
            if (cause == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(cause);
            }
        });
    }

    static Thread newDaemonThread(String name, Runnable runnable) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(true);
        return thread;
    }
}
