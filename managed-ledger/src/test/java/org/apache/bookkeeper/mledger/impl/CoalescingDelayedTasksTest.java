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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CoalescingDelayedTasksTest {
    private static final long START = 1_000_000_000L;

    private record Scheduled(Runnable drain, long delayNanos) {
    }

    private final AtomicLong now = new AtomicLong();
    // The drains the scheduler accepted and hasn't run yet
    private final Deque<Scheduled> scheduled = new ArrayDeque<>();
    private final List<String> ran = new ArrayList<>();
    private boolean rejecting;
    private CoalescingDelayedTasks tasks;

    @BeforeMethod
    public void setUp() {
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        doAnswer(invocation -> {
            if (rejecting) {
                throw new RejectedExecutionException("shut down");
            }
            long delay = ((TimeUnit) invocation.getArgument(2)).toNanos(invocation.getArgument(1));
            scheduled.add(new Scheduled(invocation.getArgument(0), delay));
            return null;
        }).when(scheduler).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        now.set(START);
        scheduled.clear();
        ran.clear();
        rejecting = false;
        tasks = new CoalescingDelayedTasks(scheduler, now::get);
    }

    private Runnable task(String name) {
        return () -> ran.add(name);
    }

    private static long millis(double millis) {
        return (long) (millis * 1_000_000);
    }

    // Runs the next scheduled drain at its time
    private void runNextDrain() {
        Scheduled next = scheduled.poll();
        assertThat(next).isNotNull();
        now.addAndGet(next.delayNanos());
        next.drain().run();
    }

    @Test
    public void tasksDueTogetherShareOneDrain() {
        tasks.schedule(task("a"), 10);
        now.addAndGet(millis(0.3));
        tasks.schedule(task("b"), 10);
        now.addAndGet(millis(5));
        tasks.schedule(task("c"), 10);
        // one drain, after the first task's delay
        assertThat(scheduled).extracting(Scheduled::delayNanos).containsExactly(millis(10));

        // at a's deadline, 4.7 ms later: a, and b within the tolerance; c is due 5 ms later
        now.set(START);
        runNextDrain();
        assertThat(ran).containsExactly("a", "b");
        assertThat(scheduled).extracting(Scheduled::delayNanos).containsExactly(millis(5.3));

        runNextDrain();
        assertThat(ran).containsExactly("a", "b", "c");
        assertThat(tasks.size()).isZero();
        assertThat(scheduled).isEmpty();
    }

    @Test
    public void aTaskAddedAfterADrainLeftTasksSchedulesForTheEarliest() {
        tasks.schedule(task("a"), 10);
        now.addAndGet(millis(5));
        tasks.schedule(task("b"), 10);
        Scheduled first = scheduled.poll();
        now.set(START + first.delayNanos());
        // c is added while the first drain is pending, which runs a and leaves b, due 5 ms later, and c
        tasks.schedule(task("c"), 10);
        first.drain().run();
        assertThat(ran).containsExactly("a");
        // whoever scheduled the next drain scheduled it for b, not for c's own delay
        assertThat(scheduled).extracting(Scheduled::delayNanos).containsExactly(millis(5));
        runNextDrain();
        assertThat(ran).containsExactly("a", "b");
        runNextDrain();
        assertThat(ran).containsExactly("a", "b", "c");
        assertThat(scheduled).isEmpty();
    }

    @Test
    public void aTaskAfterADrainSchedulesTheNext() {
        tasks.schedule(task("a"), 10);
        runNextDrain();
        tasks.schedule(task("b"), 10);
        assertThat(scheduled).extracting(Scheduled::delayNanos).containsExactly(millis(10));
        runNextDrain();
        assertThat(ran).containsExactly("a", "b");
    }

    @Test
    public void aTaskAddedByARunningTaskRunsInALaterDrain() {
        tasks.schedule(() -> {
            ran.add("a");
            tasks.schedule(task("b"), 10);
        }, 10);
        runNextDrain();
        assertThat(ran).containsExactly("a");
        runNextDrain();
        assertThat(ran).containsExactly("a", "b");
        assertThat(scheduled).isEmpty();
    }

    @Test
    public void aFailingTaskDoesntStopTheOthers() {
        tasks.schedule(() -> {
            throw new IllegalStateException("failing task");
        }, 10);
        tasks.schedule(task("b"), 10);
        runNextDrain();
        assertThat(ran).containsExactly("b");
        assertThat(tasks.size()).isZero();
    }

    @Test
    public void aDrainRunsAtMostItsLimitAndContinuesAtOnce() {
        int count = CoalescingDelayedTasks.MAX_TASKS_PER_DRAIN + 10;
        for (int i = 0; i < count; i++) {
            tasks.schedule(task("t" + i), 10);
        }
        runNextDrain();
        assertThat(ran).hasSize(CoalescingDelayedTasks.MAX_TASKS_PER_DRAIN);
        assertThat(scheduled).extracting(Scheduled::delayNanos).containsExactly(0L);
        runNextDrain();
        assertThat(ran).hasSize(count);
        assertThat(scheduled).isEmpty();
    }

    @Test
    public void rejectedTasksRunAtOnce() {
        rejecting = true;
        tasks.schedule(task("a"), 10);
        assertThat(ran).containsExactly("a");
        // a later task finds no drain scheduled, and runs at once too
        tasks.schedule(task("b"), 10);
        assertThat(ran).containsExactly("a", "b");
        assertThat(tasks.size()).isZero();
        // and once the scheduler accepts drains again, tasks wait for their delay
        rejecting = false;
        tasks.schedule(task("c"), 10);
        assertThat(ran).containsExactly("a", "b");
        runNextDrain();
        assertThat(ran).containsExactly("a", "b", "c");
    }

    @Test
    public void aRejectedNextDrainRunsTheRemainingTasks() {
        tasks.schedule(task("a"), 10);
        now.addAndGet(millis(5));
        tasks.schedule(task("b"), 10);
        Scheduled first = scheduled.poll();
        now.set(START + first.delayNanos());
        rejecting = true;
        first.drain().run();
        // b, due 5 ms later, runs at once instead of being left without a drain
        assertThat(ran).containsExactly("a", "b");
        assertThat(tasks.size()).isZero();
    }

    @Test
    public void anEarlyDrainRunsNothingAndReschedules() {
        tasks.schedule(task("a"), 10);
        Scheduled first = scheduled.poll();
        now.addAndGet(millis(2));
        first.drain().run();
        assertThat(ran).isEmpty();
        assertThat(scheduled).extracting(Scheduled::delayNanos).containsExactly(millis(8));
        runNextDrain();
        assertThat(ran).containsExactly("a");
    }
}
