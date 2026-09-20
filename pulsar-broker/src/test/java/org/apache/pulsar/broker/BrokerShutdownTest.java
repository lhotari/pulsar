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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerShutdownTest {
    @Test
    public void blockedServiceCloseDoesNotBlockSessions() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        AtomicInteger sessionsClosed = new AtomicInteger();
        BrokerShutdown shutdown = new BrokerShutdown(500, () -> {
            sessionsClosed.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        CompletableFuture<Void> result = shutdown.start(() -> {
            entered.countDown();
            try {
                release.await();
                return CompletableFuture.completedFuture(null);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return CompletableFuture.failedFuture(e);
            } finally {
                exited.countDown();
            }
        });
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
            assertThat(sessionsClosed).hasValue(1);
            assertThat(shutdown.remainingDrainNanos()).isZero();
        } finally {
            release.countDown();
            assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(sessionsClosed).hasValue(1);
    }

    @Test
    public void hungSessionCloseIsBounded() {
        CountDownLatch attempted = new CountDownLatch(1);
        BrokerShutdown shutdown = new BrokerShutdown(500, () -> {
            attempted.countDown();
            return new CompletableFuture<>();
        });
        CompletableFuture<Void> result = shutdown.start(() -> CompletableFuture.completedFuture(null));
        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
        assertThat(attempted.getCount()).isZero();
    }

    @Test
    public void failureStillClosesSessions() {
        AtomicInteger sessionsClosed = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("injected close failure");
        BrokerShutdown shutdown = new BrokerShutdown(5000, () -> {
            sessionsClosed.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        CompletableFuture<Void> result = shutdown.start(() -> {
            throw failure;
        });
        assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasCause(failure);
        assertThat(sessionsClosed).hasValue(1);
    }

    @Test
    public void healthyShutdownKeepsSessionsUntilServicesClose() throws Exception {
        CompletableFuture<Void> services = new CompletableFuture<>();
        AtomicInteger sessionsClosed = new AtomicInteger();
        BrokerShutdown shutdown = new BrokerShutdown(5000, () -> {
            sessionsClosed.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        CompletableFuture<Void> result = shutdown.start(() -> services);
        assertThat(result).isNotDone();
        assertThat(sessionsClosed).hasValue(0);
        services.complete(null);
        result.get(5, TimeUnit.SECONDS);
        assertThat(sessionsClosed).hasValue(1);
    }

    @Test
    public void nonPositiveTimeoutDisablesDeadline() throws Exception {
        for (long timeout : new long[]{0, -1}) {
            CompletableFuture<Void> services = new CompletableFuture<>();
            BrokerShutdown shutdown = new BrokerShutdown(timeout, () -> CompletableFuture.completedFuture(null));
            CompletableFuture<Void> result = shutdown.start(() -> services);
            assertThat(shutdown.remainingDrainNanos()).isEqualTo(Long.MAX_VALUE);
            assertThat(result).isNotDone();
            services.complete(null);
            result.get(5, TimeUnit.SECONDS);
        }
    }
}
