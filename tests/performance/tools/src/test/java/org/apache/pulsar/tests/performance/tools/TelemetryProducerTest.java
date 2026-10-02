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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.stream.IntStream;
import org.testng.annotations.Test;

public class TelemetryProducerTest {
    @Test
    public void precreatesInOrderOneAtATimeAndInARepeatableRandomOrderConcurrently() {
        assertThat(TelemetryProducer.precreateOrder(6, 1)).containsExactly(0, 1, 2, 3, 4, 5);

        int[] concurrent = TelemetryProducer.precreateOrder(500, 32);
        // every producer once, not in the order of the gateways, and the same order for every run
        assertThat(concurrent).containsExactlyInAnyOrder(IntStream.range(0, 500).toArray());
        assertThat(concurrent).isNotEqualTo(IntStream.range(0, 500).toArray());
        assertThat(TelemetryProducer.precreateOrder(500, 32)).isEqualTo(concurrent);
    }
}
