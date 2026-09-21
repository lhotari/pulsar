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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ShutdownTopicCloseLimiterTest {
    @Test
    public void testTimeoutAndCancellationKeepPhysicalSlots() throws Exception {
        QueuedExecutor executor = new QueuedExecutor();
        try (ShutdownTopicCloseLimiter limiter = new ShutdownTopicCloseLimiter(2, executor, () -> Long.MAX_VALUE)) {
            List<CompletableFuture<Void>> physical = new ArrayList<>();
            List<CompletableFuture<Void>> observers = new ArrayList<>();
            AtomicInteger started = new AtomicInteger();
            for (int i = 0; i < 6; i++) {
                CompletableFuture<Void> close = new CompletableFuture<>();
                physical.add(close);
                observers.add(limiter.run(() -> {
                    assertThat(Thread.holdsLock(limiter)).isFalse();
                    started.incrementAndGet();
                    return close;
                }));
            }
            executor.runAll();
            assertThat(started).hasValue(2);
            assertThat(limiter.activeCount()).isEqualTo(2);
            observers.get(0).cancel(false);
            assertThatThrownBy(() -> observers.get(1).orTimeout(1, TimeUnit.MILLISECONDS).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TimeoutException.class);
            executor.runAll();
            assertThat(started).hasValue(2);
            assertThat(limiter.activeCount()).isEqualTo(2);
            physical.get(0).complete(null);
            executor.runAll();
            assertThat(started).hasValue(3);
            assertThat(limiter.activeCount()).isEqualTo(2);
            physical.get(1).completeExceptionally(new IllegalStateException("storage failed"));
            executor.runAll();
            assertThat(started).hasValue(4);
            assertThat(limiter.activeCount()).isEqualTo(2);
            for (int i = 2; i < physical.size(); i++) {
                physical.get(i).complete(null);
                executor.runAll();
            }
            assertThat(started).hasValue(6);
            assertThat(limiter.activeCount()).isZero();
            for (int i = 2; i < observers.size(); i++) {
                observers.get(i).get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void testCloseRejectsQueueWithoutCompletingPhysicalWork() throws Exception {
        QueuedExecutor executor = new QueuedExecutor();
        ShutdownTopicCloseLimiter limiter = new ShutdownTopicCloseLimiter(1, executor, () -> Long.MAX_VALUE);
        CompletableFuture<Void> physical = new CompletableFuture<>();
        CompletableFuture<Void> first = limiter.run(() -> physical);
        AtomicInteger unexpected = new AtomicInteger();
        CompletableFuture<Void> second = limiter.run(() -> {
            unexpected.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        executor.runAll();
        CompletableFuture<Void> callback = second.whenComplete((ignored, error) ->
                assertThat(Thread.holdsLock(limiter)).isFalse());
        limiter.close();
        assertThat(first).isNotDone();
        assertThat(limiter.activeCount()).isEqualTo(1);
        assertThatThrownBy(() -> callback.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
        physical.complete(null);
        first.get(5, TimeUnit.SECONDS);
        executor.runAll();
        assertThat(limiter.activeCount()).isZero();
        assertThat(unexpected).hasValue(0);
    }

    @Test
    public void testExpiredDeadlinePreventsQueuedClose() throws Exception {
        QueuedExecutor executor = new QueuedExecutor();
        AtomicLong remaining = new AtomicLong(10);
        try (ShutdownTopicCloseLimiter limiter = new ShutdownTopicCloseLimiter(1, executor, remaining::get)) {
            CompletableFuture<Void> physical = new CompletableFuture<>();
            limiter.run(() -> physical);
            AtomicInteger started = new AtomicInteger();
            CompletableFuture<Void> queued = limiter.run(() -> {
                started.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            });
            executor.runAll();
            remaining.set(0);
            physical.complete(null);
            executor.runAll();
            assertThatThrownBy(() -> queued.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
            assertThat(started).hasValue(0);
            assertThat(limiter.activeCount()).isZero();
        }
    }

    @Test
    public void testSynchronousFailureAndRejectedDispatchReturnPermits() throws Exception {
        QueuedExecutor executor = new QueuedExecutor();
        try (ShutdownTopicCloseLimiter limiter = new ShutdownTopicCloseLimiter(1, executor, () -> Long.MAX_VALUE)) {
            CompletableFuture<Void> failed = limiter.run(() -> {
                throw new IllegalStateException("close failed");
            });
            CompletableFuture<Void> next = limiter.run(() -> CompletableFuture.completedFuture(null));
            executor.runAll();
            assertThatThrownBy(() -> failed.get(5, TimeUnit.SECONDS)).hasRootCauseMessage("close failed");
            next.get(5, TimeUnit.SECONDS);
            assertThat(limiter.activeCount()).isZero();
            CompletableFuture<Void> rejected = limiter.run(() -> CompletableFuture.completedFuture(null));
            executor.reject = true;
            executor.runAll();
            assertThatThrownBy(() -> rejected.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(limiter.activeCount()).isZero();
        }
    }

    @Test
    public void testReservationCancellationAndRepeatedRelease() throws Exception {
        QueuedExecutor executor = new QueuedExecutor();
        try (ShutdownTopicCloseLimiter limiter = new ShutdownTopicCloseLimiter(1, executor, () -> Long.MAX_VALUE)) {
            CompletableFuture<ShutdownTopicCloseLimiter.Permit> canceled = limiter.reserve();
            CompletableFuture<ShutdownTopicCloseLimiter.Permit> next = limiter.reserve();
            canceled.cancel(false);
            executor.runAll();
            ShutdownTopicCloseLimiter.Permit permit = next.get(5, TimeUnit.SECONDS);
            assertThat(limiter.activeCount()).isEqualTo(1);
            permit.close();
            permit.close();
            assertThat(limiter.activeCount()).isZero();
        }
    }

    @Test
    public void testExistingFactoryCleanupConsumesCapacityUntilPhysicalCompletion() throws Exception {
        QueuedExecutor executor = new QueuedExecutor();
        try (ShutdownTopicCloseLimiter limiter = new ShutdownTopicCloseLimiter(1, executor, () -> Long.MAX_VALUE)) {
            CompletableFuture<Void> factoryCleanup = new CompletableFuture<>();
            limiter.observeExisting(factoryCleanup.minimalCompletionStage());
            AtomicInteger started = new AtomicInteger();
            CompletableFuture<Void> queued = limiter.run(() -> {
                started.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            });
            executor.runAll();
            assertThat(limiter.activeCount()).isEqualTo(1);
            assertThat(started).hasValue(0);
            assertThat(queued).isNotDone();
            factoryCleanup.completeExceptionally(new IllegalStateException("cleanup failed"));
            executor.runAll();
            queued.get(5, TimeUnit.SECONDS);
            assertThat(started).hasValue(1);
            assertThat(limiter.activeCount()).isZero();
        }
    }

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> tasks = new ArrayDeque<>();
        private boolean reject;

        @Override
        public void execute(Runnable task) {
            if (reject) {
                throw new RejectedExecutionException("executor closed");
            }
            tasks.add(task);
        }

        private void runAll() {
            Runnable next;
            while ((next = tasks.poll()) != null) {
                next.run();
            }
        }
    }
}
