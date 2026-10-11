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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.time.Duration;
import java.util.List;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Anchor;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Impairment;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Kind;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Offloads;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Pair;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Topology;
import org.testng.annotations.Test;

public class NetworkSettingsTest {
    private static final Topology TOPOLOGY = new Topology(2, 3);
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void readsConditionsPartitionsAndResets() throws Exception {
        NetworkSettings settings = read("""
                offloads: off
                impairments:
                  gateway-wan:
                    between: [gateways, brokers]
                    delay: 20ms
                    jitter: 5ms
                    loss: 0.1%
                    lossCorrelation: 25
                  gateway-uplink:
                    from: gateways
                    to: brokers
                    rate: 20mbit
                  broker-1-isolated:
                    between: [clients, broker-1]
                    partition: true
                    start: 30s
                    duration: 1.5s
                  reset-gateways:
                    from: gateways
                    to: brokers
                    reset: true
                    anchor: run
                    start: 2m
                """);

        assertThat(settings.offloads()).isEqualTo(Offloads.OFF);
        assertThat(settings.impairments()).extracting(Impairment::name)
                .containsExactly("gateway-wan", "gateway-uplink", "broker-1-isolated", "reset-gateways");
        Impairment wan = settings.impairments().get(0);
        assertThat(wan.kind()).isEqualTo(Kind.CONDITION);
        assertThat(wan.bothDirections()).isTrue();
        assertThat(wan.delay()).isEqualTo(Duration.ofMillis(20));
        assertThat(wan.jitter()).isEqualTo(Duration.ofMillis(5));
        assertThat(wan.lossPercent()).isEqualTo(0.1);
        assertThat(wan.lossCorrelationPercent()).isEqualTo(25);
        assertThat(wan.wholeRun()).isTrue();
        assertThat(wan.pathImpairment()).isTrue();
        assertThat(wan.rateLimited()).isFalse();
        assertThat(wan.needsSegmentOffloadsOff()).isTrue();
        assertThat(wan.description()).isEqualTo("20 ms ± 5 ms delay, 0.1 % loss (25 % correlated)");
        assertThat(wan.endpoints()).isEqualTo("gateways ↔ brokers");
        Impairment uplink = settings.impairments().get(1);
        assertThat(uplink.rateBitsPerSecond()).isEqualTo(20_000_000);
        assertThat(uplink.queue()).isEqualTo(NetworkSettings.DEFAULT_QUEUE);
        assertThat(uplink.pathImpairment()).isFalse();
        assertThat(uplink.description()).isEqualTo("20 Mbit/s with a 100 ms queue");
        assertThat(uplink.endpoints()).isEqualTo("gateways → brokers");
        Impairment partition = settings.impairments().get(2);
        assertThat(partition.kind()).isEqualTo(Kind.PARTITION);
        assertThat(partition.anchor()).isEqualTo(Anchor.MEASUREMENT);
        assertThat(partition.start()).isEqualTo(Duration.ofSeconds(30));
        assertThat(partition.duration()).isEqualTo(Duration.ofMillis(1500));
        Impairment reset = settings.impairments().get(3);
        assertThat(reset.kind()).isEqualTo(Kind.RESET);
        assertThat(reset.anchor()).isEqualTo(Anchor.RUN);
        assertThat(reset.start()).isEqualTo(Duration.ofMinutes(2));
        assertThat(settings.anyMeasurementAnchored()).isTrue();
    }

    @Test
    public void isEmptyWithoutTheSection() {
        NetworkSettings settings = NetworkSettings.read(mapper.missingNode(), TOPOLOGY, 60);
        assertThat(settings.any()).isFalse();
        assertThat(settings.anyMeasurementAnchored()).isFalse();
    }

    @Test
    public void expandsTheEndpointsIntoContainers() {
        assertThat(NetworkSettings.containers(List.of("clients", "brokers", "bookie-2", "metadata"), TOPOLOGY))
                .containsExactly("gateways", "applications", "broker-0", "broker-1", "bookie-2", "zookeeper",
                        "configuration-store");
        assertThatThrownBy(() -> NetworkSettings.containers("broker-2", TOPOLOGY))
                .hasMessageContaining("the cluster has 2 broker(s)");
        assertThatThrownBy(() -> NetworkSettings.containers("proxy", TOPOLOGY))
                .hasMessageContaining("isn't an endpoint");
    }

    @Test
    public void pairsBothDirectionsOfBetween() throws Exception {
        Impairment impairment = read("""
                impairments:
                  slow:
                    between: [broker-0, bookies]
                    delay: 1ms
                """).impairments().get(0);

        assertThat(NetworkSettings.pairs(impairment, TOPOLOGY)).containsExactly(
                new Pair("broker-0", "bookie-0"), new Pair("broker-0", "bookie-1"), new Pair("broker-0", "bookie-2"),
                new Pair("bookie-0", "broker-0"), new Pair("bookie-1", "broker-0"), new Pair("bookie-2", "broker-0"));
    }

    @Test
    public void rejectsOverlappingConditionsInTheSamePlace() throws Exception {
        assertThatThrownBy(() -> read("""
                impairments:
                  a:
                    from: gateways
                    to: brokers
                    delay: 20ms
                  b:
                    from: gateways
                    to: broker-1
                    loss: 1%
                    start: 10s
                    anchor: run
                """)).hasMessageContaining("a and b both impair the packets from gateways to broker-1 at the same"
                + " time");
        assertThatThrownBy(() -> read("""
                impairments:
                  a:
                    from: gateways
                    to: brokers
                    delay: 20ms
                  b:
                    from: gateways
                    to: broker-1
                    loss: 1%
                    start: 10s
                """)).hasMessageContaining("with starts that count from different anchors");
        // One after the other, as half-open intervals
        assertThat(read("""
                impairments:
                  a:
                    from: gateways
                    to: brokers
                    delay: 20ms
                    start: 0s
                    duration: 10s
                  b:
                    from: gateways
                    to: brokers
                    delay: 50ms
                    start: 10s
                """).impairments()).hasSize(2);
        // A path condition and a rate limit are in different places, and partitions take precedence
        assertThat(read("""
                impairments:
                  wan:
                    between: [gateways, brokers]
                    delay: 20ms
                  uplink:
                    from: gateways
                    to: brokers
                    rate: 10mbit
                  cut:
                    between: [gateways, brokers]
                    partition: true
                    start: 10s
                    duration: 5s
                """).impairments()).hasSize(3);
    }

    @Test
    public void rejectsInvalidImpairments() {
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways]
                    delay: 1ms
                """, "must be a list of two sides");
        assertInvalid("""
                impairments:
                  a:
                    from: brokers
                    to: [broker-0, bookies]
                    delay: 1ms
                """, "both sides have [broker-0]");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    jitter: 1ms
                """, "jitter needs a delay");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    delay: 1ms
                    jitter: 2ms
                """, "the jitter can't be larger than the delay");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    partition: true
                    delay: 1ms
                """, "is a partition, which takes no [delay]");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    reset: true
                    start: 1s
                    duration: 1s
                """, "a reset happens once");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                """, "does nothing");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    delay: 20
                """, "must be a duration with its unit");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    rate: 10mbps
                """, "must be a rate in bits per second");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    loss: 101%
                """, "must be a percentage from 0 to 100");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    delay: 1ms
                    queue: 10ms
                """, "queue is the queue of a rate limit");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    delay: 1ms
                    anchor: run
                """, "anchor is what start counts from");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    partition: true
                    start: 90s
                """, "the measurement lasts about 60 s");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    delay: 1ms
                    jiter: 1ms
                """, "jiter isn't a setting");
        assertInvalid("""
                offloads: maybe
                """, "network.offloads must be auto, off or on");
    }

    @Test
    public void limitsTheImpairmentsPerDevice() {
        StringBuilder yaml = new StringBuilder("impairments:\n");
        for (int index = 0; index <= NetworkSettings.MAX_IMPAIRMENTS_PER_DEVICE; index++) {
            yaml.append("  p").append(index).append(":\n    from: gateways\n    to: broker-0\n    partition: true\n");
        }
        assertInvalid(yaml.toString(), "14 impairments act on the packets to broker-0");
    }

    private void assertInvalid(String yaml, String message) {
        assertThatThrownBy(() -> read(yaml)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    private NetworkSettings read(String yaml) throws Exception {
        JsonNode network = mapper.readTree(yaml);
        return NetworkSettings.read(network, TOPOLOGY, 60);
    }
}
