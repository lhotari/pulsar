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
package org.apache.pulsar.tests.integration.lookup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Test cases for advertisedAddressAutoResolvedHostNameAsAbsoluteDnsName where the advertised address is absolute.
 */
public class AbsoluteFqdnTest extends PulsarTestSuite {
    private static final Logger log = LoggerFactory.getLogger(AbsoluteFqdnTest.class);

    @Override
    public void setupCluster() throws Exception {
        brokerEnvs.put("advertisedAddressAutoResolvedHostNameAsAbsoluteDnsName", "true");
        proxyEnvs.put("advertisedAddressAutoResolvedHostNameAsAbsoluteDnsName", "true");
        super.setupCluster();
    }

    @Test
    public void testPublishConsumeBinaryLookup() throws Exception {
        testPublishConsume(pulsarCluster.getPlainTextServiceUrl(), pulsarCluster.getHttpServiceUrl());
    }

    private void testPublishConsume(String serviceUrl, String serviceHttpUrl) throws Exception {
        final String tenant = "tenant-" + randomName(10);
        final String namespace = tenant + "/ns1";
        final String topic = "persistent://" + namespace + "/topic1";

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
            .serviceHttpUrl(serviceHttpUrl)
            .build();

        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Collections.emptySet(), Collections.singleton(pulsarCluster.getClusterName())));

        admin.namespaces().createNamespace(namespace, Collections.singleton(pulsarCluster.getClusterName()));

        @Cleanup
        PulsarClient client = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .build();

        @Cleanup
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub1")
                .subscribe();

        @Cleanup
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();
        producer.send("msg-0");
        producer.send("msg-1");

        assertThat(consumer.receive(5, TimeUnit.SECONDS)).isNotNull()
                .satisfies(msg -> assertThat(msg.getValue()).isEqualTo("msg-0"));
        assertThat(consumer.receive(5, TimeUnit.SECONDS)).isNotNull()
                .satisfies(msg -> assertThat(msg.getValue()).isEqualTo("msg-1"));

        for (int i = 0; i < 10; i++) {
            // Ensure we can get the stats for the topic irrespective of which broker the proxy decides to connect to
            TopicStats stats = admin.topics().getStats(topic);
            assertEquals(stats.getPublishers().size(), 1);
        }
    }

}
