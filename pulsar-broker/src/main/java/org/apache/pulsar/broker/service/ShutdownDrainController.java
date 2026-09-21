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

import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import lombok.CustomLog;
import org.apache.pulsar.common.util.FutureUtil;

/** Serializes planning decisions, never waiting for a storage or ownership operation on its executor. */
@CustomLog
final class ShutdownDrainController {
    interface Work {
        String id();
        ShutdownBundleCost.Load load();
        long remainingTopics();
        default boolean hasHandoff() {
            return false;
        }
        /** Monotonic time when all storage closed, or Long.MIN_VALUE while it remains pending. */
        default long handoffStartedNanos() {
            return Long.MIN_VALUE;
        }
        default boolean unpaced() {
            return false;
        }
        default boolean dependent() {
            return false;
        }
        CompletableFuture<Void> prepare();
        CompletableFuture<Void> start(long budgetNanos);
        void cancelPreparation();
    }

    private enum State { WAITING, PREPARING, RUNNING, DONE }

    private static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final long REFRESH_NANOS = TimeUnit.SECONDS.toNanos(1);
    private final class Entry {
        final Work work;
        final long observationId;
        State state = State.WAITING;
        ShutdownBundleCost.Load load;
        ShutdownDrainPlanner.Job job;

        Entry(Work work) {
            this.work = work;
            observationId = entries.size();
        }
    }

    private final Map<String, Entry> entries = new LinkedHashMap<>();
    private final ScheduledExecutorService timer;
    private final Executor serial;
    private final LongSupplier clock;
    private final LongSupplier remaining;
    private final ShutdownTopicCloseLimiter limiter;
    private final long origin;
    private final long bundleBudget;
    private final int concurrency;
    private final boolean rateLimited;
    private final ShutdownDrainPlanner planner;
    private final ShutdownCloseTimeEstimator estimator = new ShutdownCloseTimeEstimator(
            TimeUnit.MILLISECONDS.toNanos(100));
    private final ShutdownCloseTimeEstimator handoffEstimator = new ShutdownCloseTimeEstimator(
            TimeUnit.MILLISECONDS.toNanos(100));
    private final CompletableFuture<Void> result = new CompletableFuture<>();
    private final AtomicBoolean requested = new AtomicBoolean();
    private ShutdownBundleCost.Normalizer normalizer;
    private ScheduledFuture<?> wakeup;
    private long nextRefresh;
    private Throwable failure;

    ShutdownDrainController(Collection<? extends Work> work, ScheduledExecutorService executor,
                            ShutdownTopicCloseLimiter limiter, long origin, LongSupplier clock,
                            LongSupplier remaining, int concurrency, int topicConcurrency,
                            int startsPerSecond, long bundleBudget) {
        timer = executor;
        serial = MoreExecutors.newSequentialExecutor(executor);
        this.limiter = limiter;
        this.origin = origin;
        this.clock = clock;
        this.remaining = remaining;
        this.concurrency = Math.max(1, concurrency);
        this.bundleBudget = bundleBudget;
        rateLimited = startsPerSecond > 0;
        long left = Math.max(0, remaining.getAsLong());
        long elapsed = Math.max(0, clock.getAsLong() - origin);
        long budget = left >= Long.MAX_VALUE - elapsed ? Long.MAX_VALUE : left + elapsed;
        planner = new ShutdownDrainPlanner(budget, budget == Long.MAX_VALUE ? 0
                : Math.min(TimeUnit.SECONDS.toNanos(1), left / 10), Math.max(1, topicConcurrency),
                this.concurrency, startsPerSecond > 0 ? Math.max(1,
                        TimeUnit.SECONDS.toNanos(1) / startsPerSecond) : 0);
        for (Work item : work) {
            if (entries.putIfAbsent(item.id(), new Entry(item)) != null) {
                throw new IllegalArgumentException("Duplicate shutdown work: " + item.id());
            }
        }
    }

    CompletableFuture<Void> start() {
        requestTick();
        // Cancellation by an observer cannot make physical work disappear or cancel shared closure.
        return result.copy();
    }

    private void requestTick() {
        if (!result.isDone() && requested.compareAndSet(false, true)) {
            execute(() -> {
                requested.set(false);
                tick();
            });
        }
    }

    private void execute(Runnable action) {
        try {
            serial.execute(() -> {
                try {
                    action.run();
                } catch (Throwable error) {
                    finish(error);
                }
            });
        } catch (RuntimeException error) {
            result.completeExceptionally(error);
            // The executor can reject a callback after a permit was granted. These operations are thread-safe
            // and release only unused reservations; no physical closure is canceled here.
            entries.values().forEach(entry -> entry.work.cancelPreparation());
        }
    }

    private void tick() {
        if (result.isDone()) {
            return;
        }
        if (wakeup != null) {
            wakeup.cancel(false);
            wakeup = null;
        }
        if (entries.values().stream().allMatch(entry -> entry.state == State.DONE)) {
            finish(failure);
            return;
        }
        if (remaining.getAsLong() <= 0) {
            finish(new TimeoutException("Broker shutdown drain deadline expired"));
            return;
        }
        long now = clock.getAsLong();
        long elapsed = Math.max(0, now - origin);
        if (normalizer == null || elapsed >= nextRefresh) {
            List<ShutdownBundleCost.Load> loads = new ArrayList<>();
            for (Entry entry : entries.values()) {
                if (entry.state == State.WAITING) {
                    entry.load = entry.work.load();
                    if (!entry.work.dependent() && !entry.work.unpaced()) {
                        loads.add(entry.load);
                    }
                }
            }
            if (normalizer == null) {
                normalizer = ShutdownBundleCost.Normalizer.capture(loads);
            }
            nextRefresh = elapsed + REFRESH_NANOS;
        }
        ShutdownTopicCloseLimiter.Progress progress = limiter.progress();
        progress.completed().forEach((id, done) -> estimator.observe(id, done.nanos(), done.successful()
                ? ShutdownCloseTimeEstimator.Outcome.SUCCESS : ShutdownCloseTimeEstimator.Outcome.FAILURE));
        Map<Long, Long> outstanding = new HashMap<>();
        progress.running().forEach((id, running) -> outstanding.put(id, Math.max(0, now - running.startedNanos())));
        ShutdownCloseTimeEstimator.Estimate estimate = estimator.estimate(outstanding);
        Map<String, List<Long>> activeTopics = new HashMap<>();
        List<Long> background = new ArrayList<>();
        progress.running().forEach((id, running) -> {
            // An overrun remains occupied. The forecast is a lower bound, not a newly granted physical slot.
            long prediction = Math.max(1, estimate.nanos() - outstanding.get(id));
            Entry owner = entries.get(running.bundle());
            if (owner == null || owner.state == State.DONE) {
                background.add(prediction);
            } else {
                activeTopics.computeIfAbsent(running.bundle(), ignored -> new ArrayList<>()).add(prediction);
            }
        });
        for (int i = 0; i < progress.reserved(); i++) {
            background.add(estimate.nanos());
        }
        Map<Long, Long> outstandingHandoffs = new HashMap<>();
        for (Entry entry : entries.values()) {
            long started = entry.work.handoffStartedNanos();
            if (entry.state == State.RUNNING && entry.work.hasHandoff() && started != Long.MIN_VALUE) {
                outstandingHandoffs.put(entry.observationId, Math.max(0, now - started));
            }
        }
        long handoffEstimate = handoffEstimator.estimate(outstandingHandoffs).nanos();
        List<ShutdownDrainPlanner.Job> pending = new ArrayList<>();
        List<ShutdownDrainPlanner.Active> active = new ArrayList<>();
        int preparing = 0;
        for (Entry entry : entries.values()) {
            if (entry.state == State.WAITING) {
                // Dependency work reserves storage capacity in the tail, without delaying user work by its impact.
                boolean unpaced = entry.load.idle() || entry.work.dependent() || entry.work.unpaced();
                entry.job = new ShutdownDrainPlanner.Job(entry.work.id(), unpaced ? 0 : normalizer.impact(entry.load),
                        entry.work.remainingTopics(), estimate.nanos(), 0, unpaced,
                        entry.work.hasHandoff() ? handoffEstimate : 0);
                pending.add(entry.job);
            } else if (entry.state != State.DONE) {
                List<Long> topics = activeTopics.getOrDefault(entry.work.id(), List.of());
                active.add(new ShutdownDrainPlanner.Active(entry.work.id(), topics,
                        Math.max(0, entry.work.remainingTopics() - topics.size()), 0,
                        entry.work.hasHandoff() ? Math.max(1, handoffEstimate
                                - outstandingHandoffs.getOrDefault(entry.observationId, 0L)) : 0));
                preparing += entry.state == State.PREPARING ? 1 : 0;
            }
        }
        ShutdownDrainPlanner.Plan plan = planner.plan(elapsed, pending, active, background, estimate);
        if (plan.newExhaustion()) {
            log.info().attr("reasons", plan.reasons()).attr("shortfallNanos", plan.shortfallLowerBoundNanos())
                    .log("Shutdown drain forecast exhausted; using available bounded capacity");
        }
        boolean dependenciesRequired = entries.values().stream()
                .anyMatch(entry -> !entry.work.dependent() && entry.state != State.DONE);
        int available = concurrency - active.size();
        long delay = TICK_NANOS;
        for (ShutdownDrainPlanner.Scheduled scheduled : plan.jobs()) {
            if (available <= 0 || (rateLimited && preparing > 0)) {
                break;
            }
            if (scheduled.notBeforeNanos() > elapsed) {
                delay = Math.min(delay, scheduled.notBeforeNanos() - elapsed);
                continue;
            }
            Entry entry = entries.get(scheduled.job().id());
            if (entry.work.dependent() && dependenciesRequired) {
                continue;
            }
            entry.state = State.PREPARING;
            available--;
            preparing++;
            FutureUtil.supplySafely(entry.work::prepare).whenComplete((ignored, error) -> execute(() -> {
                if (result.isDone()) {
                    entry.work.cancelPreparation();
                } else if (error != null) {
                    completed(entry, error);
                } else if (remaining.getAsLong() <= 0) {
                    entry.work.cancelPreparation();
                    requestTick();
                } else {
                    // A queued reservation can outlive a load refresh. Freeze cost at actual admission.
                    entry.load = entry.work.load();
                    boolean unpaced = entry.load.idle() || entry.work.dependent() || entry.work.unpaced();
                    entry.job = new ShutdownDrainPlanner.Job(entry.work.id(), unpaced ? 0
                            : normalizer.impact(entry.load), entry.work.remainingTopics(),
                            entry.job.longestTopicNanos(), entry.job.minimumDurationNanos(), unpaced,
                            entry.job.handoffNanos());
                    entry.state = State.RUNNING;
                    planner.started(entry.job, Math.max(0, clock.getAsLong() - origin));
                    FutureUtil.supplySafely(() -> entry.work.start(Math.min(bundleBudget, remaining.getAsLong())))
                            .whenComplete((closed, closeError) -> execute(() -> completed(entry, closeError)));
                    requestTick();
                }
            }));
        }
        wakeup = timer.schedule(this::requestTick, Math.max(1, Math.min(delay, remaining.getAsLong())),
                TimeUnit.NANOSECONDS);
    }

    private void completed(Entry entry, Throwable error) {
        long started = entry.work.handoffStartedNanos();
        if (entry.work.hasHandoff() && started != Long.MIN_VALUE) {
            ShutdownCloseTimeEstimator.Outcome outcome;
            if (error == null) {
                outcome = ShutdownCloseTimeEstimator.Outcome.SUCCESS;
            } else if (FutureUtil.unwrapCompletionException(error) instanceof TimeoutException) {
                outcome = ShutdownCloseTimeEstimator.Outcome.CENSORED;
            } else {
                outcome = ShutdownCloseTimeEstimator.Outcome.FAILURE;
            }
            handoffEstimator.observe(entry.observationId, Math.max(0, clock.getAsLong() - started), outcome);
        }
        entry.work.cancelPreparation();
        entry.state = State.DONE;
        if (error != null && failure == null) {
            failure = error;
        }
        requestTick();
    }

    private void finish(Throwable error) {
        if (result.isDone()) {
            return;
        }
        if (wakeup != null) {
            wakeup.cancel(false);
        }
        for (Entry entry : entries.values()) {
            // A started bundle can still own an unused first slot while selecting/publishing its target.
            // The handle releases only unused reservations, never a physical operation already in progress.
            entry.work.cancelPreparation();
        }
        if (error == null) {
            result.complete(null);
        } else {
            result.completeExceptionally(error);
        }
    }
}
