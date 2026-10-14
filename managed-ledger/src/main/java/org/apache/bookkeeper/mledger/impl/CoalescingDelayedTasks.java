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
package org.apache.bookkeeper.mledger.impl;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import lombok.CustomLog;

/**
 * Runs tasks after a delay with one scheduled task for the tasks that fall due together, instead of one scheduled task
 * per task.
 *
 * <p>A managed ledger's cursors that wait for new entries check again after the same delay, and the subscriptions of a
 * topic usually reach its end at about the same time, after the dispatch of the same entry. Scheduling each check on
 * its own costs a scheduled task, a sift of the scheduler's queue and a wake-up of a scheduler thread for every check.
 * Here the tasks are queued, and one scheduled drain runs the queued tasks that are due, or due within
 * {@link #TOLERANCE_NANOS}, and then schedules the next drain for the earliest queued task.
 *
 * <p>Tasks are expected to be added with the same delay, so that the queue's order is about the order of their
 * deadlines: tasks added concurrently can be queued in another order than their deadlines', by the time between taking
 * the time and queueing, and a task added with a shorter delay than the tasks before it, after a change of the delay,
 * runs when they are due.
 *
 * <p>The tasks run on the scheduler's thread, one after the other, at most {@link #MAX_TASKS_PER_DRAIN} in a drain,
 * so that other work on the scheduler's thread doesn't wait for a long drain; a drain that leaves due tasks schedules
 * the next one without a delay. A task that throws is logged, and the others still run. When the scheduler rejects a
 * drain, such as after its shutdown, the queued tasks run at once on the thread that scheduled it, as they would
 * without a delay.
 */
@CustomLog
class CoalescingDelayedTasks {
    // A task due within this time of the drain's run runs in that drain, so that the tasks added within a short time of
    // each other share one drain instead of a drain each
    static final long TOLERANCE_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    static final int MAX_TASKS_PER_DRAIN = 256;

    private record DelayedTask(long deadlineNanos, Runnable task) {
    }

    private final ScheduledExecutorService scheduler;
    private final LongSupplier nanoTime;
    private final ConcurrentLinkedQueue<DelayedTask> queue = new ConcurrentLinkedQueue<>();
    // Whether a drain is scheduled, or a thread is about to schedule one or to find the queue empty
    private final AtomicBoolean drainScheduled = new AtomicBoolean();

    CoalescingDelayedTasks(ScheduledExecutorService scheduler) {
        this(scheduler, System::nanoTime);
    }

    @VisibleForTesting
    CoalescingDelayedTasks(ScheduledExecutorService scheduler, LongSupplier nanoTime) {
        this.scheduler = scheduler;
        this.nanoTime = nanoTime;
    }

    /** Runs {@code task} after {@code delayMillis}. */
    void schedule(Runnable task, long delayMillis) {
        queue.offer(new DelayedTask(nanoTime.getAsLong() + TimeUnit.MILLISECONDS.toNanos(delayMillis), task));
        if (drainScheduled.compareAndSet(false, true)) {
            scheduleDrain();
        }
    }

    /**
     * Schedules a drain for the earliest queued task. Called by the thread that set {@link #drainScheduled}, which
     * keeps it set while a drain is scheduled and clears it when it finds the queue empty.
     */
    private void scheduleDrain() {
        while (true) {
            DelayedTask next = queue.peek();
            if (next != null) {
                try {
                    // The earliest task's deadline, not the delay of the task that is added, which can be later
                    scheduler.schedule(this::drain, Math.max(0, next.deadlineNanos() - nanoTime.getAsLong()),
                            TimeUnit.NANOSECONDS);
                    return;
                } catch (RejectedExecutionException e) {
                    log.debug().exception(e).log("The scheduler rejected the delayed tasks' drain, running them now");
                    runQueued(false);
                }
            }
            // A task added before this finds the drain scheduled and relies on the check below; one added after it
            // schedules a drain itself
            drainScheduled.set(false);
            if (queue.isEmpty() || !drainScheduled.compareAndSet(false, true)) {
                return;
            }
        }
    }

    @VisibleForTesting
    void drain() {
        runQueued(true);
        drainScheduled.set(false);
        if (!queue.isEmpty() && drainScheduled.compareAndSet(false, true)) {
            scheduleDrain();
        }
    }

    // Runs the due queued tasks, at most MAX_TASKS_PER_DRAIN, or with dueOnly false every queued task
    private void runQueued(boolean dueOnly) {
        long now = nanoTime.getAsLong();
        DelayedTask next;
        for (int ran = 0; (next = queue.peek()) != null; ran++) {
            if (dueOnly && (ran == MAX_TASKS_PER_DRAIN || next.deadlineNanos() - now > TOLERANCE_NANOS)) {
                return;
            }
            queue.poll();
            try {
                next.task().run();
            } catch (Throwable t) {
                log.warn().exception(t).log("A delayed task failed");
            }
        }
    }

    @VisibleForTesting
    int size() {
        return queue.size();
    }
}
