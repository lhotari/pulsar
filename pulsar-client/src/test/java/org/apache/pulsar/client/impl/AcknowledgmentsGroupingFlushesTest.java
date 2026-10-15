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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AcknowledgmentsGroupingFlushesTest {
    private static final long GROUP_TIME_MICROS = TimeUnit.MILLISECONDS.toMicros(10);

    private ScheduledExecutorService executor;

    @BeforeMethod
    public void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void flushesOfAnExecutorAndGroupTimeShareOneTask() {
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        AtomicInteger otherGroupTime = new AtomicInteger();
        AcknowledgmentsGroupingFlushes.Registration firstRegistration =
                AcknowledgmentsGroupingFlushes.register(executor, GROUP_TIME_MICROS, first::incrementAndGet);
        AcknowledgmentsGroupingFlushes.Registration secondRegistration =
                AcknowledgmentsGroupingFlushes.register(executor, GROUP_TIME_MICROS, second::incrementAndGet);
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isEqualTo(1);
        AcknowledgmentsGroupingFlushes.Registration otherRegistration = AcknowledgmentsGroupingFlushes.register(
                executor, 2 * GROUP_TIME_MICROS, otherGroupTime::incrementAndGet);
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isEqualTo(2);

        Awaitility.await().untilAsserted(() -> {
            assertThat(first.get()).isGreaterThan(2);
            assertThat(second.get()).isGreaterThan(2);
            assertThat(otherGroupTime.get()).isGreaterThan(2);
        });

        // a closed registration isn't flushed any more, and the others still are
        firstRegistration.close();
        // closing again does nothing
        firstRegistration.close();
        int firstAtClose = first.get();
        waitForTheNextRuns(second);
        // a run that had started when it was closed may complete
        assertThat(first.get()).isLessThanOrEqualTo(firstAtClose + 1);

        // the last registration of a group time removes its task
        secondRegistration.close();
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isEqualTo(1);
        otherRegistration.close();
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isZero();
        int otherAfterClose = otherGroupTime.get();
        sleepGroupTimes(5);
        assertThat(otherGroupTime.get()).isLessThanOrEqualTo(otherAfterClose + 1);
    }

    @Test
    public void aFailingFlushDoesntStopTheOthers() {
        AtomicInteger failing = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        AcknowledgmentsGroupingFlushes.Registration failingRegistration =
                AcknowledgmentsGroupingFlushes.register(executor, GROUP_TIME_MICROS, () -> {
                    failing.incrementAndGet();
                    throw new IllegalStateException("failing flush");
                });
        AcknowledgmentsGroupingFlushes.Registration otherRegistration =
                AcknowledgmentsGroupingFlushes.register(executor, GROUP_TIME_MICROS, other::incrementAndGet);
        try {
            Awaitility.await().untilAsserted(() -> {
                assertThat(failing.get()).isGreaterThan(2);
                assertThat(other.get()).isGreaterThan(2);
            });
        } finally {
            failingRegistration.close();
            otherRegistration.close();
        }
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isZero();
    }

    @Test
    public void aRejectingExecutorFailsTheRegistration() {
        executor.shutdown();
        assertThatThrownBy(() -> AcknowledgmentsGroupingFlushes.register(executor, GROUP_TIME_MICROS, () -> { }))
                .isInstanceOf(RejectedExecutionException.class);
        assertThat(AcknowledgmentsGroupingFlushes.sharedTaskCount(executor)).isZero();
    }

    // Waits until the counter has advanced twice more
    private static void waitForTheNextRuns(AtomicInteger counter) {
        int start = counter.get();
        Awaitility.await().until(() -> counter.get() >= start + 2);
    }

    private static void sleepGroupTimes(int groupTimes) {
        try {
            Thread.sleep(TimeUnit.MICROSECONDS.toMillis(GROUP_TIME_MICROS) * groupTimes);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
