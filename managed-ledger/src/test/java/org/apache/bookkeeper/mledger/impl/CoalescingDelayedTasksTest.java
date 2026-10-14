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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CoalescingDelayedTasksTest {
    private ScheduledExecutorService scheduler;
    private final AtomicLong now = new AtomicLong();
    private CoalescingDelayedTasks tasks;
    private final List<String> ran = new ArrayList<>();

    @BeforeMethod
    public void setUp() {
        scheduler = mock(ScheduledExecutorService.class);
        now.set(1_000_000_000L);
        ran.clear();
        tasks = new CoalescingDelayedTasks(scheduler, now::get);
    }

    private Runnable task(String name) {
        return () -> ran.add(name);
    }

    @Test
    public void tasksDueTogetherShareOneDrain() {
        tasks.schedule(task("a"), 10);
        now.addAndGet(TimeUnit.MICROSECONDS.toNanos(300));
        tasks.schedule(task("b"), 10);
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(5));
        tasks.schedule(task("c"), 10);
        // one drain, after the first task's delay
        verify(scheduler, times(1)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        verify(scheduler).schedule(any(Runnable.class), eq(TimeUnit.MILLISECONDS.toNanos(10)),
                eq(TimeUnit.NANOSECONDS));

        // at a's deadline: a, and b within the tolerance; c is due 5 ms later
        now.set(1_000_000_000L + TimeUnit.MILLISECONDS.toNanos(10));
        tasks.drain();
        assertEquals(ran, List.of("a", "b"));
        assertEquals(tasks.size(), 1);
        verify(scheduler).schedule(any(Runnable.class), eq(TimeUnit.MICROSECONDS.toNanos(5_300)),
                eq(TimeUnit.NANOSECONDS));

        now.addAndGet(TimeUnit.MICROSECONDS.toNanos(5_300));
        tasks.drain();
        assertEquals(ran, List.of("a", "b", "c"));
        assertEquals(tasks.size(), 0);
        // nothing left: no further drain
        verify(scheduler, times(2)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void aTaskAfterADrainSchedulesTheNext() {
        tasks.schedule(task("a"), 10);
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
        tasks.drain();
        tasks.schedule(task("b"), 10);
        verify(scheduler, times(2)).schedule(any(Runnable.class), eq(TimeUnit.MILLISECONDS.toNanos(10)),
                eq(TimeUnit.NANOSECONDS));
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
        tasks.drain();
        assertEquals(ran, List.of("a", "b"));
    }

    @Test
    public void aFailingTaskDoesntStopTheOthers() {
        tasks.schedule(() -> {
            throw new IllegalStateException("failing task");
        }, 10);
        tasks.schedule(task("b"), 10);
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(10));
        tasks.drain();
        assertEquals(ran, List.of("b"));
        assertEquals(tasks.size(), 0);
    }

    @Test
    public void aRejectedDrainIsScheduledAgainByTheNextTask() {
        doThrow(new RejectedExecutionException("shut down")).when(scheduler)
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        expectThrows(RejectedExecutionException.class, () -> tasks.schedule(task("a"), 10));
        expectThrows(RejectedExecutionException.class, () -> tasks.schedule(task("b"), 10));
        verify(scheduler, times(2)).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void aDrainBeforeTheDeadlineRunsNothingAndReschedules() {
        tasks.schedule(task("a"), 10);
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(2));
        tasks.drain();
        assertEquals(ran, List.of());
        verify(scheduler).schedule(any(Runnable.class), eq(TimeUnit.MILLISECONDS.toNanos(8)),
                eq(TimeUnit.NANOSECONDS));
        verify(scheduler, never()).schedule(any(Runnable.class), eq(0L), any(TimeUnit.class));
    }
}
