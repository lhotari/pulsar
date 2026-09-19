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
package org.apache.pulsar.tests.performance.tools;

import java.util.Random;
import java.util.concurrent.locks.LockSupport;

/** Deterministic processing-delay samples with a normal path and a long-tail outlier path. */
final class ProcessingDelaySampler {
    private static final int SAMPLE_COUNT = 1 << 16;
    private static final int SAMPLE_MASK = SAMPLE_COUNT - 1;
    private static final long RANDOM_SEED = 0x4f1bbcdc5a17d3e9L;
    private static final ProcessingDelaySampler NONE = new ProcessingDelaySampler(new long[0]);

    private final long[] delayNanos;

    private ProcessingDelaySampler(long[] delayNanos) {
        this.delayNanos = delayNanos;
    }

    static ProcessingDelaySampler from(IotScenario scenario) {
        return create(scenario.processingDelayMeanMicros(), scenario.processingDelayStdDevMicros(),
                scenario.processingDelayOutlierProbability(), scenario.processingDelayOutlierMeanMicros(),
                scenario.processingDelayOutlierStdDevMicros());
    }

    static ProcessingDelaySampler create(double meanMicros, double stdDevMicros, double outlierProbability,
                                         double outlierMeanMicros, double outlierStdDevMicros) {
        if (meanMicros == 0 && stdDevMicros == 0 && outlierProbability == 0) {
            return NONE;
        }
        Random random = new Random(RANDOM_SEED);
        long[] samples = new long[SAMPLE_COUNT];
        for (int i = 0; i < samples.length; i++) {
            boolean outlier = random.nextDouble() < outlierProbability;
            double mean = outlier ? outlierMeanMicros : meanMicros;
            double stdDev = outlier ? outlierStdDevMicros : stdDevMicros;
            samples[i] = Math.round(Math.max(0, mean + random.nextGaussian() * stdDev) * 1_000);
        }
        return new ProcessingDelaySampler(samples);
    }

    void apply(long deviceId, long sequence, int applicationIndex) {
        long delay = delayNanos(deviceId, sequence, applicationIndex);
        if (delay > 0) {
            LockSupport.parkNanos(delay);
        }
    }

    long delayNanos(long deviceId, long sequence, int applicationIndex) {
        if (delayNanos.length == 0) {
            return 0;
        }
        long mixed = mix64(deviceId ^ Long.rotateLeft(sequence, 21)
                ^ Long.rotateLeft(Integer.toUnsignedLong(applicationIndex), 42));
        return delayNanos[(int) mixed & SAMPLE_MASK];
    }

    private static long mix64(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }
}
