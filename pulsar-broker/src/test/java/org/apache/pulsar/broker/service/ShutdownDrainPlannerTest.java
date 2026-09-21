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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.ShutdownCloseTimeEstimator.Estimate;
import org.apache.pulsar.broker.service.ShutdownDrainPlanner.Active;
import org.apache.pulsar.broker.service.ShutdownDrainPlanner.Job;
import org.apache.pulsar.broker.service.ShutdownDrainPlanner.Plan;
import org.apache.pulsar.broker.service.ShutdownDrainPlanner.Reason;
import org.apache.pulsar.broker.service.ShutdownDrainPlanner.Scheduled;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ShutdownDrainPlannerTest {
    private static long seconds(long seconds) {
        return TimeUnit.SECONDS.toNanos(seconds);
    }

    private static Estimate estimate(long seconds) {
        return new Estimate(seconds(seconds), false, false, 32, 0);
    }

    private static Job job(String id, double impact, long topics) {
        return new Job(id, impact, topics, 0, 0, impact == 0);
    }

    private static Scheduled scheduled(Plan plan, String id) {
        return plan.jobs().stream().filter(entry -> entry.job().id().equals(id)).findFirst().orElseThrow();
    }

    @Test
    public void testWeightedStartsIdleLaneAndFixedAnchor() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(50), seconds(10), 16, 16, 0);
        Job a = job("a", 8, 1);
        Job b = job("b", 1, 1);
        Job c = job("c", 1, 1);
        Job idle = job("idle", 0, 1);
        Plan first = planner.plan(0, List.of(a, b, c, idle), List.of(), List.of(), estimate(1));
        assertThat(first.workConserving()).isFalse();
        assertThat(scheduled(first, "a").dueNanos()).isZero();
        assertThat(scheduled(first, "b").dueNanos()).isEqualTo(seconds(32));
        assertThat(scheduled(first, "c").dueNanos()).isEqualTo(seconds(36));
        assertThat(scheduled(first, "idle").dueNanos()).isZero();
        planner.started(a, 0);
        for (long now : new long[]{1, 5, 10, 20}) {
            Plan next = planner.plan(seconds(now), List.of(b, c, idle), List.of(), List.of(), estimate(1));
            assertThat(scheduled(next, "b").nominalNanos()).isEqualTo(seconds(32));
            assertThat(scheduled(next, "c").nominalNanos()).isEqualTo(seconds(36));
            assertThat(next.workConserving()).isFalse();
        }
    }

    @Test
    public void testTwoLargeColdBundlesShareTheCapacityForecast() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(40), 0, 1, 3, 0);
        Plan plan = planner.plan(0, List.of(job("a", 8, 1), job("b", 1, 15), job("c", 1, 15)),
                List.of(), List.of(), estimate(1));
        assertThat(plan.workConserving()).isFalse();
        assertThat(scheduled(plan, "b").latestNanos()).isEqualTo(seconds(10));
        assertThat(scheduled(plan, "c").latestNanos()).isEqualTo(seconds(25));
        assertThat(scheduled(plan, "b").dueNanos()).isEqualTo(seconds(10));
        assertThat(scheduled(plan, "c").dueNanos()).isEqualTo(seconds(25));
    }

    @Test
    public void testLongStorageJobAdvancesWithoutCollapsingOtherSpacing() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(100), 0, 10, 10, 0);
        Plan plan = planner.plan(0, List.of(job("hot", 8, 1), job("storage", 1, 600), job("small", 1, 1)),
                List.of(), List.of(), estimate(1));
        assertThat(plan.workConserving()).isFalse();
        assertThat(scheduled(plan, "storage").dueNanos()).isLessThan(seconds(40));
        assertThat(scheduled(plan, "small").dueNanos()).isEqualTo(seconds(90));
    }

    @Test
    public void testFasterCompletionRestoresSpacingOnlyForNotYetDueJobs() {
        List<Job> jobs = List.of(job("a", 8, 1), job("b", 1, 20), job("c", 1, 1));
        ShutdownDrainPlanner beforeDue = new ShutdownDrainPlanner(seconds(100), 0, 1, 3, 0);
        Plan slow = beforeDue.plan(0, jobs, List.of(), List.of(), estimate(4));
        assertThat(scheduled(slow, "b").dueNanos()).isEqualTo(seconds(16));
        Plan faster = beforeDue.plan(seconds(1), jobs, List.of(), List.of(), estimate(1));
        assertThat(scheduled(faster, "b").dueNanos()).isEqualTo(seconds(79));
        ShutdownDrainPlanner afterDue = new ShutdownDrainPlanner(seconds(100), 0, 1, 3, 0);
        afterDue.plan(0, jobs, List.of(), List.of(), estimate(4));
        Plan sticky = afterDue.plan(seconds(17), jobs, List.of(), List.of(), estimate(1));
        assertThat(scheduled(sticky, "b").dueNanos()).isEqualTo(seconds(16));
        assertThat(sticky.workConserving()).isFalse();
    }

    @Test
    public void testColdStartPacesAndExhaustionIsOneWay() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(100), 0, 8, 8, 0);
        List<Job> jobs = List.of(job("a", 8, 1), job("b", 1, 1));
        Plan cold = planner.plan(0, jobs, List.of(), List.of(), new Estimate(seconds(1), true, false, 0, 0));
        assertThat(cold.workConserving()).isFalse();
        assertThat(scheduled(cold, "b").dueNanos()).isPositive();
        Plan slow = planner.plan(seconds(2), jobs, List.of(), List.of(),
                new Estimate(seconds(200), true, true, 1, 0));
        assertThat(slow.workConserving()).isTrue();
        assertThat(slow.newExhaustion()).isTrue();
        assertThat(slow.reasons()).contains(Reason.CAPACITY);
        assertThat(slow.jobs()).allSatisfy(entry -> assertThat(entry.dueNanos()).isLessThanOrEqualTo(seconds(2)));
        Plan recovered = planner.plan(seconds(3), jobs, List.of(), List.of(), estimate(1));
        assertThat(recovered.workConserving()).isTrue();
        assertThat(recovered.newExhaustion()).isFalse();
    }

    @Test
    public void testFirstFastCompletionDoesNotEndPacingForSlowerSiblings() {
        ShutdownCloseTimeEstimator estimator = new ShutdownCloseTimeEstimator(seconds(1));
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(10), 0, 8, 8, 0);
        Job first = job("first", 8, 8);
        Job second = job("second", 1, 1);
        Job third = job("third", 1, 1);
        planner.plan(0, List.of(first, second, third), List.of(), List.of(), estimator.estimate(Map.of()));
        planner.started(first, 0);
        estimator.observe(0, TimeUnit.MILLISECONDS.toNanos(100), ShutdownCloseTimeEstimator.Outcome.SUCCESS);
        Map<Long, Long> outstanding = new HashMap<>();
        for (long id = 1; id < 8; id++) {
            outstanding.put(id, TimeUnit.MILLISECONDS.toNanos(101));
        }
        Estimate observed = estimator.estimate(outstanding);
        assertThat(observed.censored()).isTrue();
        Plan plan = planner.plan(TimeUnit.MILLISECONDS.toNanos(101), List.of(second, third),
                List.of(new Active("first", Collections.nCopies(7, 1L), 0, 0)), List.of(), observed);
        assertThat(plan.workConserving()).isFalse();
        assertThat(plan.reasons()).isEmpty();
        assertThat(scheduled(plan, "second").dueNanos()).isEqualTo(seconds(8));
        assertThat(scheduled(plan, "third").dueNanos()).isEqualTo(seconds(9));
    }

    @Test
    public void testRunningSlotsAreReservedOnceAtTheirPredictedReleaseTimes() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(20), 0, 2, 3, 0);
        Active running = new Active("running", List.of(seconds(10)), 0, seconds(10));
        Plan plan = planner.plan(0, List.of(job("a", 1, 1), job("b", 1, 1)), List.of(running), List.of(), estimate(10));
        assertThat(plan.workConserving()).isFalse();
        assertThat(scheduled(plan, "a").latestNanos()).isEqualTo(seconds(10));
        assertThat(scheduled(plan, "b").latestNanos()).isEqualTo(seconds(10));
    }

    @Test
    public void testQueuedWorkOfStartedBundleHasCapacityBeforeNewBundles() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(20), 0, 1, 1, 0);
        Active running = new Active("running", List.of(seconds(5)), 5, 0);
        Plan plan = planner.plan(0, List.of(job("next", 1, 10)), List.of(running), List.of(), estimate(1));
        assertThat(plan.workConserving()).isFalse();
        assertThat(scheduled(plan, "next").latestNanos()).isEqualTo(seconds(10));
    }

    @Test
    public void testNondivisibleLongTopicCannotFitAfterAllSlotsWereBusy() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(20), 0, 10, 2, 0);
        List<Long> busy = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            busy.add(seconds(10));
        }
        Job longTopic = new Job("long", 1, 1, seconds(15), 0, false);
        Plan plan = planner.plan(0, List.of(longTopic), List.of(), busy, estimate(1));
        assertThat(plan.workConserving()).isTrue();
        assertThat(plan.reasons()).containsExactly(Reason.CAPACITY);
        assertThat(plan.shortfallLowerBoundNanos()).isEqualTo(seconds(5));
    }

    @Test
    public void testBundleLimitAndDependencyTailConstrainForecast() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(30), seconds(10), 10, 1, 0);
        Job a = new Job("a", 1, 1, seconds(15), 0, false);
        Job b = new Job("b", 1, 1, seconds(15), 0, false);
        Plan plan = planner.plan(0, List.of(a, b), List.of(), List.of(), estimate(1));
        assertThat(plan.workConserving()).isTrue();
        assertThat(plan.shortfallLowerBoundNanos()).isGreaterThanOrEqualTo(seconds(10));
        assertThat(plan.reasons()).containsExactly(Reason.CAPACITY);
    }

    @Test
    public void testHungCapacityNeverBecomesFreeFromAWrapperTimeout() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(30), 0, 2, 2, 0);
        Plan plan = planner.plan(seconds(5), List.of(job("pending", 1, 1)), List.of(),
                List.of(Long.MAX_VALUE, Long.MAX_VALUE), estimate(1));
        assertThat(plan.workConserving()).isTrue();
        assertThat(plan.reasons()).containsExactly(Reason.CAPACITY);
        assertThat(plan.shortfallLowerBoundNanos()).isPositive();
    }

    @Test
    public void testPositiveCompatibilityRateCapRemainsBindingWhenInfeasible() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(10), 0, 10, 10, seconds(15));
        Plan plan = planner.plan(0, List.of(job("a", 0, 1), job("b", 0, 1), job("c", 0, 1)),
                List.of(), List.of(), estimate(1));
        assertThat(plan.workConserving()).isTrue();
        assertThat(plan.reasons()).contains(Reason.RATE_CAP);
        assertThat(plan.shortfallLowerBoundNanos()).isPositive();
        assertThat(plan.jobs()).extracting(Scheduled::notBeforeNanos).containsExactly(0L, seconds(15), seconds(30));
    }

    @Test
    public void testUnboundedShutdownHasNoSyntheticPacingHorizon() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(Long.MAX_VALUE, 0, 2, 2, 0);
        Plan plan = planner.plan(seconds(5), List.of(job("a", 8, 1), job("b", 1, 100)),
                List.of(), List.of(), estimate(1));
        assertThat(plan.workConserving()).isTrue();
        assertThat(plan.newExhaustion()).isFalse();
        assertThat(plan.jobs()).allSatisfy(entry -> assertThat(entry.notBeforeNanos()).isEqualTo(seconds(5)));
    }

    @Test
    public void testEmptyPlanAndLargeIdlePopulation() {
        ShutdownDrainPlanner empty = new ShutdownDrainPlanner(0, 0, 1, 1, 0);
        Plan none = empty.plan(0, List.of(), List.of(), List.of(), estimate(1));
        assertThat(none.jobs()).isEmpty();
        assertThat(none.workConserving()).isFalse();
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(1), 0, 32, 32, 0);
        List<Job> jobs = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            jobs.add(new Job("idle-" + i, 0, 0, 0, 10000, true));
        }
        Plan idle = planner.plan(0, jobs, List.of(), List.of(), estimate(1));
        assertThat(idle.jobs()).hasSize(10000);
        assertThat(idle.workConserving()).isFalse();
        assertThat(idle.jobs()).allSatisfy(entry -> assertThat(entry.dueNanos()).isZero());
    }

    @Test
    public void testOverflowAndExtremeFiniteImpactsDoNotCreateFeasibility() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(Long.MAX_VALUE - 1, 0, 1, 1, 0);
        Job a = new Job("a", Double.MAX_VALUE, 3, Long.MAX_VALUE / 2, 0, false);
        Job b = new Job("b", Double.MAX_VALUE, 3, Long.MAX_VALUE / 2, 0, false);
        Plan plan = planner.plan(0, List.of(a, b), List.of(), List.of(),
                new Estimate(Long.MAX_VALUE / 2, true, false, 0, 0));
        assertThat(plan.workConserving()).isTrue();
        assertThat(plan.shortfallLowerBoundNanos()).isPositive();
        assertThat(plan.jobs()).allSatisfy(entry -> assertThat(entry.notBeforeNanos()).isGreaterThanOrEqualTo(0));
    }

    @Test
    public void testStartedIdentityCannotBeScheduledAgain() {
        ShutdownDrainPlanner planner = new ShutdownDrainPlanner(seconds(10), 0, 2, 2, 0);
        Job job = job("a", 1, 1);
        planner.started(job, 0);
        assertThatThrownBy(() -> planner.plan(0, List.of(job), List.of(), List.of(), estimate(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> planner.started(job, 1)).isInstanceOf(IllegalArgumentException.class);
    }
}
