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
 * Here the tasks are queued, and one scheduled drain runs every queued task that is due, or due within
 * {@link #TOLERANCE_NANOS}, and then schedules itself for the next one.
 *
 * <p>Tasks are expected to be added with the same delay, so that the queue's order is the order of their deadlines. A
 * task added with a shorter delay than the tasks before it runs when they are due, which delays it by at most the
 * difference; this only happens when the delay is changed.
 *
 * <p>The tasks run on the scheduler's thread, one after the other. A task that throws is logged, and the others still
 * run.
 */
@CustomLog
class CoalescingDelayedTasks {
    // A task due within this time of the drain's run runs in that drain, so that the tasks added within a short time of
    // each other share one drain instead of a drain each
    static final long TOLERANCE_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    private record DelayedTask(long deadlineNanos, Runnable task) {
    }

    private final ScheduledExecutorService scheduler;
    private final LongSupplier nanoTime;
    private final ConcurrentLinkedQueue<DelayedTask> queue = new ConcurrentLinkedQueue<>();
    // Whether a drain is scheduled, or running and yet to check the queue for the next one
    private final AtomicBoolean drainScheduled = new AtomicBoolean();

    CoalescingDelayedTasks(ScheduledExecutorService scheduler) {
        this(scheduler, System::nanoTime);
    }

    @VisibleForTesting
    CoalescingDelayedTasks(ScheduledExecutorService scheduler, LongSupplier nanoTime) {
        this.scheduler = scheduler;
        this.nanoTime = nanoTime;
    }

    /**
     * Runs {@code task} after {@code delayMillis}.
     *
     * @throws RejectedExecutionException when the scheduler rejects the drain, such as after its shutdown
     */
    void schedule(Runnable task, long delayMillis) {
        long delayNanos = TimeUnit.MILLISECONDS.toNanos(delayMillis);
        queue.offer(new DelayedTask(nanoTime.getAsLong() + delayNanos, task));
        if (drainScheduled.compareAndSet(false, true)) {
            scheduleDrain(delayNanos);
        }
    }

    private void scheduleDrain(long delayNanos) {
        try {
            scheduler.schedule(this::drain, delayNanos, TimeUnit.NANOSECONDS);
        } catch (RejectedExecutionException e) {
            // A later task schedules a drain again
            drainScheduled.set(false);
            throw e;
        }
    }

    @VisibleForTesting
    void drain() {
        long now = nanoTime.getAsLong();
        DelayedTask next;
        while ((next = queue.peek()) != null && next.deadlineNanos() - now <= TOLERANCE_NANOS) {
            queue.poll();
            try {
                next.task().run();
            } catch (Throwable t) {
                log.warn().exception(t).log("A delayed task failed");
            }
        }
        // A task added before this finds the drain scheduled and relies on this check; one added after it schedules
        // a drain itself
        drainScheduled.set(false);
        next = queue.peek();
        if (next != null && drainScheduled.compareAndSet(false, true)) {
            try {
                scheduleDrain(Math.max(0, next.deadlineNanos() - nanoTime.getAsLong()));
            } catch (RejectedExecutionException e) {
                log.debug().exception(e).log("Couldn't schedule the delayed tasks' next drain");
            }
        }
    }

    @VisibleForTesting
    int size() {
        return queue.size();
    }
}
