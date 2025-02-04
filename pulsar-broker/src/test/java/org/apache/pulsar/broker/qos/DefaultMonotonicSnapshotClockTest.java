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

package org.apache.pulsar.broker.qos;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.data.Offset;
import org.testng.annotations.Test;

@Slf4j
public class DefaultMonotonicSnapshotClockTest {
    @Test
    void testClockHandlesTimeLeapsBackwardsOrForward() throws InterruptedException {
        long snapshotIntervalMillis = 50;
        AtomicLong offsetValue = new AtomicLong(0);
        @Cleanup
        DefaultMonotonicSnapshotClock clock =
                new DefaultMonotonicSnapshotClock(Duration.ofMillis(snapshotIntervalMillis).toNanos(),
                        () -> System.nanoTime() + offsetValue.get());

        long previousTick = -1;
        boolean leapDirection = true;
        for (int i = 0; i < 10; i++) {
            long tick = clock.getTickNanos(false);
            if ((i + 1) % 4 == 0) {
                leapDirection = !leapDirection;
                log.info("Time leap 5 minutes {}", leapDirection ? "forward" : "backwards");
                // make the clock leap 5 minute forward or backwards
                offsetValue.set((leapDirection ? 1L : -1L) * Duration.ofMinutes(5).toNanos());
                Thread.sleep(2 * snapshotIntervalMillis);
            } else {
                Thread.sleep(snapshotIntervalMillis);
            }
            if (previousTick != -1) {
                assertThat(tick)
                        .isGreaterThanOrEqualTo(previousTick)
                        .isCloseTo(previousTick,
                                Offset.offset(4 * TimeUnit.MILLISECONDS.toNanos(snapshotIntervalMillis)));
            }
            previousTick = tick;
        }
    }
}