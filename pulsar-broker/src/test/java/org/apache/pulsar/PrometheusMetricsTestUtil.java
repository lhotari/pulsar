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
package org.apache.pulsar;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;

public class PrometheusMetricsTestUtil {

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                boolean includeProducerMetrics, OutputStream out) throws IOException {
        new PrometheusMetricsGenerator(pulsar, includeTopicMetrics, includeConsumerMetrics, includeProducerMetrics, false).generate(out, null);
    }

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                boolean includeProducerMetrics, boolean splitTopicAndPartitionIndexLabel,
                                OutputStream out) throws IOException {
        new PrometheusMetricsGenerator(pulsar, includeTopicMetrics, includeConsumerMetrics, includeProducerMetrics,
                splitTopicAndPartitionIndexLabel).generate(out, null);
    }
}
