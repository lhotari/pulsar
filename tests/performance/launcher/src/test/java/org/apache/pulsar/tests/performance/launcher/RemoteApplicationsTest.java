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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.Map;
import org.testng.annotations.Test;

public class RemoteApplicationsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void givesTheRemoteApplicationsTheRemoteClusterAndTheirSettings() throws Exception {
        ObjectNode workload = (ObjectNode) mapper.readTree("""
                serviceUrl: pulsar://iot-1-pulsar-broker-0:6650
                rate: 1000
                applications:
                  count: 20
                  podsPerApplication: 10
                  joinSeconds: [0, 30]
                  env:
                    PULSAR_MEM: "-Xmx1g"
                """);
        ClusterSettings.Remote remote = remote("""
                  applications:
                    count: 4
                    env:
                      PULSAR_MEM: "-Xmx256m"
                """);

        ObjectNode remoteWorkload = PerformanceLauncher.remoteApplicationsWorkload(workload, remote,
                remote.serviceUrl("iot-1"));

        assertThat(remoteWorkload.path("serviceUrl").asText()).isEqualTo("pulsar://iot-1-remote-pulsar-broker-0:6650");
        assertThat(remoteWorkload.path("rate").asInt()).isEqualTo(1000);
        assertThat(remoteWorkload.path("applications").path("count").asInt()).isEqualTo(4);
        assertThat(remoteWorkload.path("applications").path("podsPerApplication").asInt()).isEqualTo(10);
        assertThat(remoteWorkload.path("applications").has("joinSeconds")).isFalse();
        // The workload itself is unchanged
        assertThat(workload.path("applications").path("count").asInt()).isEqualTo(20);
        assertThat(PerformanceLauncher.remoteApplicationsEnv(Map.of("PULSAR_MEM", "-Xmx1g"), remote))
                .containsExactly(Map.entry("PULSAR_MEM", "-Xmx256m"));
    }

    @Test
    public void runsTheWorkloadsApplicationsWithoutSettingsOfTheirOwn() throws Exception {
        ObjectNode workload = (ObjectNode) mapper.readTree("""
                applications:
                  count: 20
                  podsPerApplication: 10
                """);
        ClusterSettings.Remote remote = remote("  {}\n");

        ObjectNode remoteWorkload = PerformanceLauncher.remoteApplicationsWorkload(workload, remote, "pulsar://x");

        assertThat(remoteWorkload.path("applications").path("count").asInt()).isEqualTo(20);
        assertThat(PerformanceLauncher.remoteApplicationsEnv(Map.of("PULSAR_MEM", "-Xmx1g"), remote))
                .containsExactly(Map.entry("PULSAR_MEM", "-Xmx1g"));
    }

    @Test
    public void namesTheContainersOfBothClustersWithoutTheClusters() {
        assertThat(PerformanceLauncher.componentName("/iot-1-pulsar-broker-0", "iot-1")).isEqualTo("broker-0");
        assertThat(PerformanceLauncher.componentName("/iot-1-iot-1-pulsar-broker-0", "iot-1")).isEqualTo("broker-0");
        assertThat(PerformanceLauncher.componentName("/iot-1-remote-iot-1-remote-pulsar-broker-1", "iot-1-remote"))
                .isEqualTo("broker-1");
        assertThat(PerformanceLauncher.componentName("/iot-1-remote-bookie-0", "iot-1-remote")).isEqualTo("bookie-0");
        assertThat(PerformanceLauncher.componentName("/iot-1-zookeeper", "iot-1")).isEqualTo("zookeeper");
    }

    private ClusterSettings.Remote remote(String section) throws Exception {
        return ClusterSettings.read(mapper, mapper.readTree("""
                brokers:
                  replicas: 1
                bookies:
                  replicas: 1
                remote:
                """ + section)).remote();
    }
}
