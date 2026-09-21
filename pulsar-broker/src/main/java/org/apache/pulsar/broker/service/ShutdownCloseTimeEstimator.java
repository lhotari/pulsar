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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Physical-duration evidence. A timeout is a lower bound, never a successful completion. */
final class ShutdownCloseTimeEstimator {
    private static final int WINDOW_SIZE = 32;

    enum Outcome { SUCCESS, CENSORED, FAILURE }

    record Observation(long nanos, Outcome outcome) { }

    record Estimate(long nanos, boolean provisional, boolean censored,
                    int successfulSamples, int failedSamples) { }

    private final long provisionalNanos;
    private final Map<Long, Observation> window = new LinkedHashMap<>();

    ShutdownCloseTimeEstimator(long provisionalNanos) {
        if (provisionalNanos <= 0) {
            throw new IllegalArgumentException("A positive provisional close duration is required");
        }
        this.provisionalNanos = provisionalNanos;
    }

    void observe(long operation, long durationNanos, Outcome outcome) {
        // A later physical completion replaces this operation's earlier timeout observation.
        window.remove(operation);
        window.put(operation, new Observation(Math.max(1, durationNanos), outcome));
        if (window.size() > WINDOW_SIZE) {
            window.remove(window.keySet().iterator().next());
        }
    }

    Estimate estimate(Map<Long, Long> outstandingElapsedNanos) {
        List<Long> exact = new ArrayList<>();
        List<Long> bounds = new ArrayList<>();
        int failures = 0;
        for (var entry : window.entrySet()) {
            if (outstandingElapsedNanos.containsKey(entry.getKey())) {
                continue;
            }
            Observation observation = entry.getValue();
            if (observation.outcome() == Outcome.FAILURE) {
                failures++;
            } else {
                bounds.add(observation.nanos());
                if (observation.outcome() == Outcome.SUCCESS) {
                    exact.add(observation.nanos());
                }
            }
        }
        exact.sort(Comparator.naturalOrder());
        for (long elapsed : outstandingElapsedNanos.values()) {
            bounds.add(Math.max(0, elapsed));
        }
        if (bounds.isEmpty()) {
            return new Estimate(provisionalNanos, true, false, 0, failures);
        }
        bounds.sort(Comparator.naturalOrder());
        int rank = rank(bounds.size());
        long lowerBound = bounds.get(rank - 1);
        // Replacing every right-censored observation with infinity gives the quantile's upper bound.
        // Even short censored samples can leave p90 unknown; do not inspect only the selected sample.
        boolean censored = rank > exact.size() || exact.get(rank - 1) > lowerBound;
        return new Estimate(exact.isEmpty() ? Math.max(provisionalNanos, lowerBound) : lowerBound,
                exact.size() < WINDOW_SIZE, censored, exact.size(), failures);
    }

    private static int rank(int size) {
        return (int) Math.ceil(size * 0.9);
    }
}
