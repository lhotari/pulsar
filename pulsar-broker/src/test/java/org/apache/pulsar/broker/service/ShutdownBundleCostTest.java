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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.ShutdownBundleCost.Load;
import org.apache.pulsar.broker.service.ShutdownBundleCost.Normalizer;
import org.apache.pulsar.broker.service.ShutdownCloseTimeEstimator.Estimate;
import org.apache.pulsar.broker.service.ShutdownCloseTimeEstimator.Outcome;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ShutdownBundleCostTest {
    @Test
    public void testImpactUsesNormalizedDimensionsAndLiveFallback() {
        Load a = new Load(8, 0, 0, 0, false, false, 10, 10, true);
        Load b = new Load(1, 0, 0, 0, false, false, 90, 90, true);
        Load c = new Load(1, 0, 0, 0, false, false, Double.NaN, Double.POSITIVE_INFINITY, true);
        Normalizer normalizer = Normalizer.capture(List.of(a, b, c));
        assertThat(normalizer.impact(a)).isEqualTo(0.8);
        assertThat(normalizer.impact(b)).isEqualTo(0.9);
        assertThat(normalizer.impact(c)).isEqualTo(0.1);
        Load stale = new Load(1, 0, 0, 0, false, false, 10000, 10000, false);
        assertThat(normalizer.impact(stale)).isEqualTo(0.1);
        Load naturallyClosed = new Load(0, 0, 0, 0, false, false, 10000, 10000, true);
        assertThat(normalizer.impact(naturallyClosed)).isZero();
    }

    @Test
    public void testBackgroundOnlyBundlesHavePositiveFiniteCost() {
        List<Load> loads = List.of(new Load(0, 0, 1, 0, false, false, 0, 0, false),
                new Load(0, 0, 0, 1, false, false, 0, 0, false),
                new Load(0, 0, 0, 0, true, false, 0, 0, false),
                new Load(0, 0, 0, 0, false, true, 0, 0, false));
        Normalizer normalizer = Normalizer.capture(loads);
        loads.forEach(load -> {
            assertThat(load.idle()).isFalse();
            assertThat(normalizer.impact(load)).isEqualTo(0.25);
        });
        assertThat(Normalizer.capture(List.of()).impact(loads.get(0))).isEqualTo(1);
    }

    @Test
    public void testInvalidRatesAndLargeCountersCannotProduceNaNOrInfinity() {
        Load a = new Load(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 0,
                false, false, Double.MAX_VALUE, Double.MAX_VALUE, true);
        Normalizer normalizer = Normalizer.capture(List.of(a, a));
        assertThat(normalizer.impact(a)).isEqualTo(0.5);
        Load tiny = new Load(1, 0, 0, 0, false, false, Double.MIN_VALUE, Double.MIN_VALUE, true);
        assertThat(Normalizer.capture(List.of(tiny)).impact(a)).isFinite();
    }

    @Test
    public void testFourLongCensoredSamplesCannotBecomeConfidentFastP90() {
        ShutdownCloseTimeEstimator estimator = new ShutdownCloseTimeEstimator(TimeUnit.SECONDS.toNanos(1));
        for (int i = 0; i < 28; i++) {
            estimator.observe(i, TimeUnit.MILLISECONDS.toNanos(50), Outcome.SUCCESS);
        }
        for (int i = 28; i < 32; i++) {
            estimator.observe(i, TimeUnit.SECONDS.toNanos(60), Outcome.CENSORED);
        }
        Estimate estimate = estimator.estimate(Map.of());
        assertThat(estimate.nanos()).isEqualTo(TimeUnit.SECONDS.toNanos(60));
        assertThat(estimate.censored()).isTrue();
        assertThat(estimate.exhausted()).isTrue();
        assertThat(estimate.successfulSamples()).isEqualTo(28);
    }

    @Test
    public void testShortCensoringStillMakesQuantileUncertainAndLaterProgressExhaustsEstimate() {
        ShutdownCloseTimeEstimator estimator = new ShutdownCloseTimeEstimator(100);
        for (int i = 0; i < 28; i++) {
            estimator.observe(i, 50, Outcome.SUCCESS);
        }
        for (int i = 28; i < 32; i++) {
            estimator.observe(i, 1, Outcome.CENSORED);
        }
        Estimate shortBounds = estimator.estimate(Map.of());
        assertThat(shortBounds.nanos()).isEqualTo(50);
        assertThat(shortBounds.censored()).isTrue();
        assertThat(shortBounds.exhausted()).isFalse();
        Estimate overdue = estimator.estimate(Map.of(28L, 60L, 29L, 60L, 30L, 60L, 31L, 60L));
        assertThat(overdue.exhausted()).isTrue();
        assertThat(overdue.nanos()).isEqualTo(60);
    }

    @Test
    public void testPhysicalCompletionReplacesTimeoutAndFailuresAreNotFastSuccesses() {
        ShutdownCloseTimeEstimator estimator = new ShutdownCloseTimeEstimator(100);
        assertThat(estimator.estimate(Map.of()).provisional()).isTrue();
        assertThat(estimator.estimate(Map.of()).exhausted()).isFalse();
        estimator.observe(1, 200, Outcome.CENSORED);
        assertThat(estimator.estimate(Map.of()).exhausted()).isTrue();
        estimator.observe(1, 250, Outcome.SUCCESS);
        estimator.observe(2, 1, Outcome.FAILURE);
        Estimate complete = estimator.estimate(Map.of());
        assertThat(complete.nanos()).isEqualTo(250);
        assertThat(complete.successfulSamples()).isEqualTo(1);
        assertThat(complete.provisional()).isTrue();
        assertThat(complete.failedSamples()).isEqualTo(1);
        assertThat(complete.censored()).isFalse();
        assertThat(complete.exhausted()).isFalse();
        for (int i = 3; i < 35; i++) {
            estimator.observe(i, 50, Outcome.SUCCESS);
        }
        Estimate rolled = estimator.estimate(Map.of());
        assertThat(rolled.successfulSamples()).isEqualTo(32);
        assertThat(rolled.provisional()).isFalse();
        assertThat(rolled.failedSamples()).isZero();
        assertThat(rolled.nanos()).isEqualTo(50);
    }
}
