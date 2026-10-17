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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.apache.pulsar.tests.performance.tools.IotScenario;
import org.testng.annotations.Test;

public class TopicUnloadsTest {
    @Test(timeOut = 30_000)
    public void unloadsAFractionOfTheTopicsEachRoundAndRecordsEachUnload() throws Exception {
        Path runOutput = Files.createTempDirectory("topic-unloads");
        List<String> unloaded = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean closed = new AtomicBoolean();
        TopicUnloads unloads = TopicUnloads.start(topic -> {
            unloaded.add(topic);
            // the second unload fails
            if (unloaded.size() == 2) {
                throw new IOException("unload failed, retry");
            }
        }, () -> closed.set(true), List.of("t-0", "t-1", "t-2", "t-3"), new IotScenario.TopicUnloads(1, 0.5),
                runOutput, status -> { });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (unloaded.size() < 2 && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        unloads.close();

        assertThat(closed).isTrue();
        // two of the four topics a round, each once
        assertThat(unloaded.subList(0, 2)).doesNotHaveDuplicates();
        List<String> lines = Files.readAllLines(runOutput.resolve(RunReport.TOPIC_UNLOADS_FILE));
        assertThat(lines.get(0)).isEqualTo(RunReport.TOPIC_UNLOADS_HEADER);
        assertThat(lines.get(1)).matches("\\d+,t-\\d,\\d+,");
        assertThat(lines.get(2)).matches("\\d+,t-\\d,\\d+,unload failed  retry");
    }

    @Test
    public void unloadsAtLeastOneTopicARound() {
        assertThat(TopicUnloads.unloadsPerRound(10, new IotScenario.TopicUnloads(10, 0.01))).isEqualTo(1);
        assertThat(TopicUnloads.unloadsPerRound(10, new IotScenario.TopicUnloads(10, 0.25))).isEqualTo(3);
        assertThat(TopicUnloads.unloadsPerRound(2, new IotScenario.TopicUnloads(10, 1))).isEqualTo(2);
    }
}
