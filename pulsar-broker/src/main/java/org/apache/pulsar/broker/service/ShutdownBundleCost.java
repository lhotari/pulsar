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

import java.util.Collection;

/** Dimensionless transfer impact. Storage work and historical backlog are intentionally separate. */
final class ShutdownBundleCost {
    record Load(long producers, long consumers, long replicationEndpoints, long unresolvedLoads,
                boolean pendingPersistence, boolean transactionWork, double messageRate, double byteRate,
                boolean ratesFresh) {
        boolean idle() {
            return producers == 0 && consumers == 0 && replicationEndpoints == 0 && unresolvedLoads == 0
                    && !pendingPersistence && !transactionWork;
        }

        double entityUnits() {
            double entities = Math.max(0, producers) + (double) Math.max(0, consumers)
                    + Math.max(0, replicationEndpoints);
            return idle() ? 0 : Math.max(1, entities);
        }
    }

    /** Denominators are fixed at shutdown-plan creation, including when pending loads are refreshed. */
    record Normalizer(double entities, double messages, double bytes) {
        static Normalizer capture(Collection<Load> loads) {
            double entities = 0;
            double messages = 0;
            double bytes = 0;
            for (Load load : loads) {
                entities += load.entityUnits();
                if (!load.idle() && load.ratesFresh()) {
                    messages += usable(load.messageRate()) ? load.messageRate() : 0;
                    bytes += usable(load.byteRate()) ? load.byteRate() : 0;
                }
            }
            return new Normalizer(Math.max(1, entities), messages, bytes);
        }

        double impact(Load load) {
            if (load.idle()) {
                return 0;
            }
            double impact = load.entityUnits() / entities;
            if (load.ratesFresh()) {
                if (usable(messages) && usable(load.messageRate())) {
                    impact = Math.max(impact, Math.min(Double.MAX_VALUE, load.messageRate() / messages));
                }
                if (usable(bytes) && usable(load.byteRate())) {
                    impact = Math.max(impact, Math.min(Double.MAX_VALUE, load.byteRate() / bytes));
                }
            }
            return impact;
        }

        private static boolean usable(double value) {
            return Double.isFinite(value) && value > 0;
        }
    }

    private ShutdownBundleCost() {
    }
}
