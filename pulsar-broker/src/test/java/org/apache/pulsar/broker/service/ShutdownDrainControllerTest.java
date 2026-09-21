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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ShutdownDrainControllerTest {
    private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

    @Test
    public void testWeightedStartsAndIdleFastLane() throws Exception {
        try (Harness harness = new Harness(41 * SECOND, 4, 0, 10 * SECOND)) {
            Work heavy = harness.work("heavy", 8);
            Work light = harness.work("light-a", 1);
            Work last = harness.work("light-b", 1);
            Work idle = harness.work("idle", 0);
            harness.start();
            assertThat(heavy.starts).isEqualTo(1);
            assertThat(idle.starts).isEqualTo(1);
            assertThat(light.starts + last.starts).isZero();
            heavy.closed.complete(null);
            idle.closed.complete(null);
            harness.advance(31 * SECOND);
            assertThat(light.starts + last.starts).isZero();
            harness.advance(32 * SECOND);
            assertThat(light.starts).isEqualTo(1);
            assertThat(last.starts).isZero();
            light.closed.complete(null);
            harness.advance(36 * SECOND);
            assertThat(last.starts).isEqualTo(1);
            last.closed.complete(null);
            harness.flush();
            harness.result.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testTimeoutAndObserverCancellationDoNotFreeBundleSlots() throws Exception {
        try (Harness harness = new Harness(30 * SECOND, 2, 0, 1)) {
            Work first = harness.work("a", 0);
            Work second = harness.work("b", 0);
            Work third = harness.work("c", 0);
            harness.start();
            assertThat(first.starts + second.starts).isEqualTo(2);
            assertThat(third.starts).isZero();
            assertThat(harness.result.cancel(false)).isTrue();
            harness.advance(SECOND);
            assertThat(third.starts).isZero();
            assertThat(first.closed).isNotDone();
            second.closed.complete(null);
            harness.flush();
            assertThat(third.starts).isEqualTo(1);
            first.closed.complete(null);
            third.closed.complete(null);
            harness.controller.start().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testPreparationDoesNotConsumeBundleBudget() throws Exception {
        try (Harness harness = new Harness(60 * SECOND, 1, 0, 5 * SECOND)) {
            Work work = harness.work("a", 8);
            Work next = harness.work("b", 1);
            work.prepared = new CompletableFuture<>();
            harness.start();
            harness.advance(20 * SECOND);
            work.producers = 0;
            assertThat(work.starts).isZero();
            work.prepared.complete(null);
            harness.flush();
            assertThat(work.starts).isEqualTo(1);
            assertThat(work.budget).isEqualTo(5 * SECOND);
            work.closed.complete(null);
            harness.flush();
            // The first job became idle while waiting. Its original impact must not postpone the next job.
            assertThat(next.starts).isEqualTo(1);
            next.closed.complete(null);
            harness.result.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testGlobalDeadlineCancelsPreparationButNotPhysicalClosure() throws Exception {
        try (Harness harness = new Harness(SECOND, 2, 0, SECOND)) {
            Work active = harness.work("a", 0);
            Work preparing = harness.work("b", 0);
            preparing.prepared = new CompletableFuture<>();
            harness.start();
            harness.advance(SECOND);
            assertThatThrownBy(() -> harness.result.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TimeoutException.class);
            assertThat(active.closed).isNotDone();
            assertThat(preparing.canceled).isTrue();
            preparing.prepared.complete(null);
            harness.flush();
            assertThat(preparing.starts).isZero();
            active.closed.complete(null);
        }
    }

    @Test
    public void testDeadlineReleasesUnusedReservationWhileTargetSelectionIsPending() throws Exception {
        try (Harness harness = new Harness(SECOND, 1, 0, SECOND)) {
            Work work = harness.work("a", 0);
            var reservation = harness.limiter.reserve().get(5, TimeUnit.SECONDS);
            work.cancelReservation = reservation::close;
            harness.start();
            assertThat(work.starts).isEqualTo(1);
            assertThat(harness.limiter.activeCount()).isEqualTo(1);
            harness.advance(SECOND);
            assertThatThrownBy(() -> harness.result.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TimeoutException.class);
            assertThat(harness.limiter.activeCount()).isZero();
            assertThat(work.closed).isNotDone();
        }
    }

    @Test
    public void testExplicitRateCapUsesActualStartAfterPreparation() throws Exception {
        try (Harness harness = new Harness(10 * SECOND, 3, 2, SECOND)) {
            Work first = harness.work("a", 0);
            first.prepared = new CompletableFuture<>();
            Work second = harness.work("b", 0);
            Work third = harness.work("c", 0);
            harness.start();
            harness.advance(3 * SECOND);
            assertThat(second.starts).isZero();
            first.prepared.complete(null);
            harness.flush();
            assertThat(first.starts).isEqualTo(1);
            assertThat(second.starts).isZero();
            harness.advance(3 * SECOND + SECOND / 2);
            assertThat(second.starts).isEqualTo(1);
            assertThat(third.starts).isZero();
            harness.advance(4 * SECOND);
            assertThat(third.starts).isEqualTo(1);
            harness.work.forEach(item -> item.closed.complete(null));
            harness.result.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testSystemDependenciesWaitForPhysicalUserCompletion() throws Exception {
        try (Harness harness = new Harness(30 * SECOND, 2, 0, SECOND)) {
            Work user = harness.work("user", 1);
            Work system = harness.work("system", 100);
            system.dependent = true;
            harness.start();
            assertThat(user.starts).isEqualTo(1);
            assertThat(system.starts).isZero();
            harness.advance(2 * SECOND);
            assertThat(system.starts).isZero();
            user.closed.complete(null);
            harness.flush();
            assertThat(system.starts).isEqualTo(1);
            system.closed.complete(null);
            harness.result.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testObservedHandoffDurationAdvancesRemainingBundles() throws Exception {
        try (Harness harness = new Harness(11 * SECOND, 1, 0, 10 * SECOND)) {
            Work first = harness.work("first", 8);
            Work second = harness.work("second", 1);
            Work third = harness.work("third", 1);
            harness.work.forEach(work -> work.hasHandoff = true);
            first.handoffStarted = 0;
            harness.start();
            assertThat(first.starts).isEqualTo(1);
            harness.advance(2 * SECOND);
            first.closed.complete(null);
            harness.flush();
            harness.advance(5 * SECOND);
            assertThat(second.starts).isZero();
            harness.advance(6 * SECOND);
            // Nominal impact pacing is at8s, but two observed two-second handoffs need4s before the10s cutoff.
            assertThat(second.starts).isEqualTo(1);
            assertThat(third.starts).isZero();
            second.handoffStarted = 6 * SECOND;
            harness.advance(8 * SECOND);
            second.closed.complete(null);
            harness.flush();
            assertThat(third.starts).isEqualTo(1);
            third.closed.complete(null);
            harness.result.get(5, TimeUnit.SECONDS);
        }
    }

    private static final class Work implements ShutdownDrainController.Work {
        final String id;
        long producers;
        final CompletableFuture<Void> closed = new CompletableFuture<>();
        CompletableFuture<Void> prepared = CompletableFuture.completedFuture(null);
        int starts;
        long budget;
        boolean canceled;
        boolean dependent;
        boolean hasHandoff;
        volatile long handoffStarted = Long.MIN_VALUE;
        Runnable cancelReservation = () -> { };

        Work(String id, long producers) {
            this.id = id;
            this.producers = producers;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public ShutdownBundleCost.Load load() {
            return new ShutdownBundleCost.Load(producers, 0, 0, 0, false, false, 0, 0, false);
        }

        @Override
        public long remainingTopics() {
            return 0;
        }

        @Override
        public boolean dependent() {
            return dependent;
        }

        @Override
        public boolean hasHandoff() {
            return hasHandoff;
        }

        @Override
        public long handoffStartedNanos() {
            return handoffStarted;
        }

        @Override
        public CompletableFuture<Void> prepare() {
            return prepared;
        }

        @Override
        public CompletableFuture<Void> start(long budgetNanos) {
            starts++;
            budget = budgetNanos;
            return closed;
        }

        @Override
        public void cancelPreparation() {
            canceled = true;
            cancelReservation.run();
        }
    }

    private static final class Harness implements AutoCloseable {
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        final AtomicLong clock = new AtomicLong();
        final List<Work> work = new ArrayList<>();
        final long budget;
        final int concurrency;
        final int rate;
        final long bundleBudget;
        final ShutdownTopicCloseLimiter limiter;
        ShutdownDrainController controller;
        CompletableFuture<Void> result;

        Harness(long budget, int concurrency, int rate, long bundleBudget) {
            this.budget = budget;
            this.concurrency = concurrency;
            this.rate = rate;
            this.bundleBudget = bundleBudget;
            limiter = new ShutdownTopicCloseLimiter(2, executor, () -> budget - clock.get());
        }

        Work work(String id, long producers) {
            Work item = new Work(id, producers);
            work.add(item);
            return item;
        }

        void start() throws Exception {
            controller = new ShutdownDrainController(work, executor, limiter, 0, clock::get,
                    () -> budget - clock.get(), concurrency, 2, rate, bundleBudget);
            result = controller.start();
            flush();
        }

        void advance(long now) throws Exception {
            clock.set(now);
            controller.start();
            flush();
        }

        void flush() throws Exception {
            executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
            executor.submit(() -> { }).get(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            clock.set(budget);
            if (controller != null) {
                controller.start();
                flush();
            }
            work.forEach(item -> item.closed.complete(null));
            limiter.close();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
