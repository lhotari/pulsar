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
package org.apache.pulsar.broker.admin;

import static org.apache.pulsar.broker.service.TopicPoliciesService.GetType.GLOBAL_ONLY;
import static org.apache.pulsar.broker.service.TopicPoliciesService.GetType.LOCAL_ONLY;
import static org.apache.pulsar.common.naming.SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ConfigHelper;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.PublishRateLimiterImpl;
import org.apache.pulsar.broker.service.SystemTopicBasedTopicPoliciesService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicPoliciesService;
import org.apache.pulsar.broker.service.TopicPolicyTestUtils;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.SubscribeRateLimiter;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class TopicPoliciesTest extends MockedPulsarServiceBaseTest {

    private final String testTenant = "my-tenant";

    private final String testNamespace = "my-namespace";

    private final String myNamespace = testTenant + "/" + testNamespace;

    private final String myNamespaceV1 = testTenant + "/test/" + testNamespace;

    private final String testTopic = "persistent://" + myNamespace + "/test-set-backlog-quota";

    private final String persistenceTopic = "persistent://" + myNamespace + "/test-set-persistence";

    private final String topicPolicyEventsTopic = "persistent://" + myNamespace + "/__change_events";

    private final int testTopicPartitions = 2;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setDefaultNumberOfNamespaceBundles(1);
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("test"));
        admin.tenants().createTenant(this.testTenant, tenantInfo);
        admin.namespaces().createNamespace(testTenant + "/" + testNamespace, Set.of("test"));
        admin.namespaces().createNamespace(myNamespaceV1);
        admin.topics().createPartitionedTopic(testTopic, testTopicPartitions);
        Producer producer = pulsarClient.newProducer().topic(testTopic).create();
        producer.close();
        waitForZooKeeperWatchers();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void updatePropertiesForAutoCreatedTopicTest() throws Exception {
        TopicName topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(myNamespace),
                "test-" + UUID.randomUUID()
        );
        String testTopic = topicName.toString();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(testTopic).create();
        HashMap<String, String> properties = new HashMap<>();
        properties.put("backlogQuotaType", "message_age");
        admin.topics().updateProperties(testTopic, properties);
        admin.topics().delete(topicName.toString(), true);
    }

    @Test
    public void testTopicPolicyInitialValueWithNamespaceAlreadyLoaded() throws Exception{
        TopicName topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(myNamespace),
                "test-" + UUID.randomUUID()
        );
        String topic = topicName.toString();

        SystemTopicBasedTopicPoliciesService policyService =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();

        //set up topic with maxSubscriptionsPerTopic = 10
        admin.topics().createNonPartitionedTopic(topic);
        admin.topicPolicies().setMaxSubscriptionsPerTopicAsync(topic, 10).get();

        //wait until topic loaded with right policy value.
        Awaitility.await().untilAsserted(()-> {
            AbstractTopic topic1 = (AbstractTopic) pulsar.getBrokerService().getTopic(topic, true).get().get();
            assertEquals(topic1.getHierarchyTopicPolicies().getMaxSubscriptionsPerTopic().get(), Integer.valueOf(10));
        });
        //unload the topic
        pulsar.getNamespaceService().unloadNamespaceBundle(pulsar.getNamespaceService().getBundle(topicName)).get();
        assertFalse(pulsar.getBrokerService().getTopics().containsKey(topic));

        //load the nameserver, but topic is not init.
        log.info("lookup:{}", admin.lookups().lookupTopic(topic));
        assertTrue(pulsar.getBrokerService().isTopicNsOwnedByBrokerAsync(topicName).join());
        assertFalse(pulsar.getBrokerService().getTopics().containsKey(topic));
        //make sure namespace policy reader is fully started.
        Awaitility.await().untilAsserted(()-> {
            assertTrue(policyService.getPoliciesCacheInit(topicName.getNamespaceObject()).isDone());
        });

        //load the topic.
        AbstractTopic topic1 = (AbstractTopic) pulsar.getBrokerService().getTopic(topic, true).get().get();
        assertEquals(topic1.getHierarchyTopicPolicies().getMaxSubscriptionsPerTopic().get(), Integer.valueOf(10));
    }


    @Test
    public void testSetSizeBasedBacklogQuota() throws Exception {

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);

        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota, BacklogQuota
                .BacklogQuotaType.destination_storage);
        log.info("Backlog quota set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota));
        for (int i = 0; i < testTopicPartitions; i++) {
            String partition = TopicName.get(testTopic).getPartition(i).toString();
            Topic topic = pulsar.getBrokerService().getTopic(partition, false).get().get();
            BacklogQuota backlogQuotaInTopic = topic.getBacklogQuota(BacklogQuota
                    .BacklogQuotaType.destination_storage);
            log.info("Backlog quota {} in backlog quota manager on topic: {}", backlogQuotaInTopic, testTopic);
            Assert.assertEquals(backlogQuota, backlogQuotaInTopic);
        }
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetTimeBasedBacklogQuota() throws Exception {

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitTime(1000)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();

        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota,
                BacklogQuota.BacklogQuotaType.message_age);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.message_age), backlogQuota));
        for (int i = 0; i < testTopicPartitions; i++) {
            String partition = TopicName.get(testTopic).getPartition(i).toString();
            Topic topic = pulsar.getBrokerService().getTopic(partition, false).get().get();
            BacklogQuota backlogQuotaInTopic = topic.getBacklogQuota(
                    BacklogQuota.BacklogQuotaType.message_age);
            Assert.assertEquals(backlogQuota, backlogQuotaInTopic);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveSizeBasedBacklogQuota() throws Exception {
        List<Topic> partitions = new ArrayList<>();
        List<BacklogQuota> defaultBacklogQuotas = new ArrayList<>();
        for (int i = 0; i < testTopicPartitions; i++) {
            String partition = TopicName.get(testTopic).getPartition(i).toString();
            Topic topic = pulsar.getBrokerService().getOrCreateTopic(partition).get();
            partitions.add(topic);
            BacklogQuota defaultBacklogQuota = topic.getBacklogQuota(BacklogQuota
                    .BacklogQuotaType.destination_storage);
            defaultBacklogQuotas.add(defaultBacklogQuota);
        }

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);

        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota, BacklogQuota
                .BacklogQuotaType.destination_storage);
        log.info("Backlog quota set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota));

        partitions.forEach(topic -> {
            BacklogQuota backlogQuotaInTopic = topic.getBacklogQuota(
                    BacklogQuota.BacklogQuotaType.destination_storage);
            log.info("Backlog quota {} in backlog quota manager on topic: {}", backlogQuotaInTopic, testTopic);
            Assert.assertEquals(backlogQuota, backlogQuotaInTopic);
        });

        admin.topicPolicies().removeBacklogQuota(testTopic, BacklogQuota.BacklogQuotaType.destination_storage);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage)));
        for (int i = 0; i < partitions.size(); i++) {
            BacklogQuota backlogQuotaInTopic =
                    partitions.get(i).getBacklogQuota(BacklogQuota.BacklogQuotaType.destination_storage);
            log.info("Backlog quota {} in backlog quota manager on topic: {} after remove", backlogQuotaInTopic,
                    testTopic);
            Assert.assertEquals(defaultBacklogQuotas.get(i), backlogQuotaInTopic);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveTimeBasedBacklogQuota() throws Exception {
        List<Topic> partitions = new ArrayList<>();
        List<BacklogQuota> defaultBacklogQuotas = new ArrayList<>();
        for (int i = 0; i < testTopicPartitions; i++) {
            String partition = TopicName.get(testTopic).getPartition(i).toString();
            Topic topic = pulsar.getBrokerService().getOrCreateTopic(partition).get();
            partitions.add(topic);
            BacklogQuota defaultBacklogQuota = topic.getBacklogQuota(BacklogQuota.BacklogQuotaType.message_age);
            defaultBacklogQuotas.add(defaultBacklogQuota);
        }

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitTime(1000)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();

        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.message_age), backlogQuota));

        for (int i = 0; i < partitions.size(); i++) {
            Assert.assertEquals(partitions.get(i).getBacklogQuota(BacklogQuota.BacklogQuotaType.message_age),
                    backlogQuota);
            //destination_storage should keep the same.
            Assert.assertEquals(partitions.get(i).getBacklogQuota(
                    BacklogQuota.BacklogQuotaType.destination_storage),
                    defaultBacklogQuotas.get(i));
        }

        admin.topicPolicies().removeBacklogQuota(testTopic, BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.message_age)));

        for (int i = 0; i < partitions.size(); i++) {
            BacklogQuota backlogQuotaInTopic =
                    partitions.get(i).getBacklogQuota(BacklogQuota.BacklogQuotaType.message_age);
            log.info("Backlog quota {} in backlog quota manager on topic: {} after remove", backlogQuotaInTopic,
                    testTopic);
            Assert.assertEquals(defaultBacklogQuotas.get(i), backlogQuotaInTopic);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckSizeBasedBacklogQuota() throws Exception {
        RetentionPolicies retentionPolicies = new RetentionPolicies(10, 10);
        String namespace = TopicName.get(testTopic).getNamespace();
        admin.namespaces().setRetention(namespace, retentionPolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getRetention(namespace),
                        retentionPolicies));

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(10 * 1024 * 1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota,
                    BacklogQuota.BacklogQuotaType.destination_storage);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        backlogQuota = BacklogQuota.builder()
                .limitSize(10 * 1024 * 1024 + 1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota,
                    BacklogQuota.BacklogQuotaType.destination_storage);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        backlogQuota = BacklogQuota.builder()
                .limitSize(10 * 1024 * 1024 - 1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota,
                BacklogQuota.BacklogQuotaType.destination_storage);

        BacklogQuota finalBacklogQuota = backlogQuota;
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), finalBacklogQuota));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckTimeBasedBacklogQuota() throws Exception {
        RetentionPolicies retentionPolicies = new RetentionPolicies(10, 10);
        String namespace = TopicName.get(testTopic).getNamespace();
        admin.namespaces().setRetention(namespace, retentionPolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getRetention(namespace),
                        retentionPolicies));

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitTime(10 * 60)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota,
                    BacklogQuota.BacklogQuotaType.message_age);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        backlogQuota = BacklogQuota.builder()
                .limitTime(10 * 60 + 1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        try {
            admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota,
                    BacklogQuota.BacklogQuotaType.message_age);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        backlogQuota = BacklogQuota.builder()
                .limitTime(10 * 60 - 1)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota,
                BacklogQuota.BacklogQuotaType.message_age);

        BacklogQuota finalBacklogQuota = backlogQuota;
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.message_age), finalBacklogQuota));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @DataProvider(name = "clientRequestType")
    public Object[][] clientRequestType() {
        return new Object[][]{
            {"PULSAR_ADMIN"},
            {"HTTP"}
        };
    }

    @Test(dataProvider = "clientRequestType")
    public void testPriorityOfGlobalPolicies(String clientRequestType) throws Exception {
        final SystemTopicBasedTopicPoliciesService topicPoliciesService =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        final JerseyClient httpClient = JerseyClientBuilder.createClient();
        // create topic and load it up.
        final String namespace = myNamespace;
        final String topic = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/tp");
        final TopicName topicName = TopicName.get(topic);
        final String hostAndPort = pulsar.getWebServiceAddress();
        final String httpPath = "/admin/v2/persistent/" + namespace + "/" + TopicName.get(topic).getLocalName()
                + "/maxConsumers";
        admin.topics().createNonPartitionedTopic(topic);
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopics().get(topic).get().get();

        // Set non global policy.
        // Verify: it affects.
        if ("PULSAR_ADMIN".equals(clientRequestType)) {
            admin.topicPolicies(false).setMaxConsumers(topic, 10);
        } else {
            Response res = httpClient.target(hostAndPort).path(httpPath)
                    .queryParam("isGlobal", "false")
                    .request()
                    .header("Content-Type", "application/json")
                    .post(Entity.json(10));
            assertTrue(res.getStatus() == 200 || res.getStatus() == 204);
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, LOCAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 10);
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 10);
        });

        // Set global policy.
        // Verify: topic policies has higher priority than global policies.
        if ("PULSAR_ADMIN".equals(clientRequestType)) {
            admin.topicPolicies(true).setMaxConsumers(topic, 20);
        } else {
            Response globalRes = httpClient.target(hostAndPort).path(httpPath)
                    .queryParam("isGlobal", "true")
                    .request()
                    .header("Content-Type", "application/json")
                    .post(Entity.json(20));
            assertTrue(globalRes.getStatus() == 200 || globalRes.getStatus() == 204);
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, LOCAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 10);
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, GLOBAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 20);
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 10);
        });

        // Remove non-global policy.
        // Verify: global policy affects.
        if ("PULSAR_ADMIN".equals(clientRequestType)) {
            admin.topicPolicies(false).removeMaxConsumers(topic);
        } else {
            Response removeRes = httpClient.target(hostAndPort).path(httpPath)
                    .queryParam("isGlobal", "false")
                    .request()
                    .header("Content-Type", "application/json")
                    .delete();
            assertTrue(removeRes.getStatus() == 200 || removeRes.getStatus() == 204);
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, GLOBAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 20);
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 20);
        });

        // cleanup.
        producer.close();
        admin.topics().delete(topic, false);
    }

    @Test(dataProvider = "clientRequestType")
    public void testPriorityOfGlobalPolicies2(String clientRequestType) throws Exception {
        final SystemTopicBasedTopicPoliciesService topicPoliciesService =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();
        final JerseyClient httpClient = JerseyClientBuilder.createClient();
        // create topic and load it up.
        final String namespace = myNamespace;
        final String topic = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/tp");
        final TopicName topicName = TopicName.get(topic);
        final String hostAndPort = pulsar.getWebServiceAddress();
        final String httpPath = "/admin/v2/persistent/" + namespace + "/" + TopicName.get(topic).getLocalName()
                + "/maxConsumers";
        admin.topics().createNonPartitionedTopic(topic);
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopics().get(topic).get().get();

        // Set global policy.
        // Verify: it affects.
        if ("PULSAR_ADMIN".equals(clientRequestType)) {
            admin.topicPolicies(true).setMaxConsumers(topic, 20);
        } else {
            Response globalRes = httpClient.target(hostAndPort).path(httpPath)
                    .queryParam("isGlobal", "true")
                    .request()
                    .header("Content-Type", "application/json")
                    .post(Entity.json(20));
            assertTrue(globalRes.getStatus() == 200 || globalRes.getStatus() == 204);
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, GLOBAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 20);
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 20);
        });

        // Set non global policy.
        // Verify: topic policies has higher priority than global policies.
        if ("PULSAR_ADMIN".equals(clientRequestType)) {
            admin.topicPolicies(false).setMaxConsumers(topic, 10);
        } else {
            Response res = httpClient.target(hostAndPort).path(httpPath)
                    .queryParam("isGlobal", "false")
                    .request()
                    .header("Content-Type", "application/json")
                    .post(Entity.json(10));
            assertTrue(res.getStatus() == 200 || res.getStatus() == 204);
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, LOCAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 10);
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, GLOBAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 20);
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 10);
        });

        // Remove global policy.
        // Verify: non-global policy affects.
        if ("PULSAR_ADMIN".equals(clientRequestType)) {
            admin.topicPolicies(true).removeMaxConsumers(topic);
        } else {
            Response removeRes = httpClient.target(hostAndPort).path(httpPath)
                    .queryParam("isGlobal", "true")
                    .request()
                    .header("Content-Type", "application/json")
                    .delete();
            assertTrue(removeRes.getStatus() == 200 || removeRes.getStatus() == 204);
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, LOCAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 10);
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 10);
        });

        // cleanup.
        producer.close();
        admin.topics().delete(topic, false);
    }

    @Test
    public void testGlobalPolicyStillAffectsAfterUnloading() throws Exception {
        // create topic and load it up.
        final String namespace = myNamespace;
        final String topic = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/tp");
        final TopicName topicName = TopicName.get(topic);
        admin.topics().createNonPartitionedTopic(topic);
        pulsarClient.newProducer().topic(topic).create().close();
        final SystemTopicBasedTopicPoliciesService topicPoliciesService =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();

        // Set non-global policy of the limitation of max consumers.
        // Set global policy of the limitation of max producers.
        admin.topicPolicies(false).setMaxConsumers(topic, 10);
        admin.topicPolicies(true).setMaxProducers(topic, 20);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, LOCAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 10);
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, GLOBAL_ONLY).join().get()
                    .getMaxProducerPerTopic(), 20);
            PersistentTopic persistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopics().get(topic).get().get();
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 10);
            assertEquals(hierarchyTopicPolicies.getMaxProducersPerTopic().get(), 20);
        });

        // Reload topic and verify: both global policy and non-global policy affect.
        admin.topics().unload(topic);
        pulsarClient.newProducer().topic(topic).create().close();
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopics().get(topic).get().get();
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 10);
            assertEquals(hierarchyTopicPolicies.getMaxProducersPerTopic().get(), 20);
        });

        // cleanup.
        admin.topics().delete(topic, false);
    }

    @Test
    public void testRetentionGlobalPolicyAffects() throws Exception {
        // create topic and load it up.
        final String namespace = myNamespace;
        final String topic = BrokerTestUtil.newUniqueName("persistent://" + namespace + "/tp");
        final TopicName topicName = TopicName.get(topic);
        admin.topics().createNonPartitionedTopic(topic);
        pulsarClient.newProducer().topic(topic).create().close();
        final SystemTopicBasedTopicPoliciesService topicPoliciesService =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();

        // Set non-global policy of the limitation of max consumers.
        // Set global policy of the persistence policies.
        admin.topicPolicies(false).setMaxConsumers(topic, 10);
        RetentionPolicies retentionPolicies = new RetentionPolicies(100, 200);
        admin.topicPolicies(true).setRetention(topic, retentionPolicies);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, LOCAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 10);
            PersistentTopic persistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopics().get(topic).get().get();
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 10);
            ManagedLedgerConfig mlConfig = persistentTopic.getManagedLedger().getConfig();
            assertEquals(mlConfig.getRetentionTimeMillis(), TimeUnit.MINUTES.toMillis(100));
            assertEquals(mlConfig.getRetentionSizeInMB(), 200);
        });
        PersistencePolicies persistencePolicy = new PersistencePolicies(3, 2, 1, 4);
        admin.topicPolicies(true).setPersistence(topic, persistencePolicy);
        Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(topicPoliciesService.getTopicPoliciesAsync(topicName, LOCAL_ONLY).join().get()
                    .getMaxConsumerPerTopic(), 10);
            PersistentTopic persistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopics().get(topic).get().get();
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 10);
            ManagedLedgerConfig mlConfig = persistentTopic.getManagedLedger().getConfig();
            assertEquals(mlConfig.getRetentionTimeMillis(), TimeUnit.MINUTES.toMillis(100));
            assertEquals(mlConfig.getRetentionSizeInMB(), 200);
            assertEquals(mlConfig.getEnsembleSize(), 3);
            assertEquals(mlConfig.getWriteQuorumSize(), 2);
            assertEquals(mlConfig.getAckQuorumSize(), 1);
            assertEquals(mlConfig.getThrottleMarkDelete(), 4D);
        });

        // Reload topic and verify: retention policy of global policy affects.
        admin.topics().unload(topic);
        pulsarClient.newProducer().topic(topic).create().close();
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopics().get(topic).get().get();
            HierarchyTopicPolicies hierarchyTopicPolicies = persistentTopic.getHierarchyTopicPolicies();
            ManagedLedgerConfig mlConfig = persistentTopic.getManagedLedger().getConfig();
            assertEquals(hierarchyTopicPolicies.getMaxConsumerPerTopic().get(), 10);
            assertEquals(mlConfig.getRetentionTimeMillis(), TimeUnit.MINUTES.toMillis(100));
            assertEquals(mlConfig.getRetentionSizeInMB(), 200);
            assertEquals(mlConfig.getEnsembleSize(), 3);
            assertEquals(mlConfig.getWriteQuorumSize(), 2);
            assertEquals(mlConfig.getAckQuorumSize(), 1);
            assertEquals(mlConfig.getThrottleMarkDelete(), 4D);
        });

        // cleanup.
        admin.topics().delete(topic, false);
    }

    @Test(timeOut = 20000)
    public void testGetSizeBasedBacklogQuotaApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertEquals(admin.topicPolicies().getBacklogQuotaMap(topic), new HashMap<>());
        assertEquals(admin.namespaces().getBacklogQuotaMap(myNamespace), new HashMap<>());
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> brokerQuotaMap = ConfigHelper.backlogQuotaMap(conf);
        assertEquals(admin.topicPolicies().getBacklogQuotaMap(topic, true), brokerQuotaMap);
        BacklogQuota namespaceQuota = BacklogQuota.builder()
                .limitSize(30)
                .limitTime(10)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                .build();

        admin.namespaces().setBacklogQuota(myNamespace, namespaceQuota);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.namespaces()
                .getBacklogQuotaMap(myNamespace).isEmpty()));
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> namespaceQuotaMap = new HashMap<>();
        namespaceQuotaMap.put(BacklogQuota.BacklogQuotaType.destination_storage, namespaceQuota);
        namespaceQuotaMap.put(BacklogQuota.BacklogQuotaType.message_age, BacklogQuota.builder()
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold).build());
        assertEquals(admin.topicPolicies().getBacklogQuotaMap(topic, true), namespaceQuotaMap);

        BacklogQuota topicQuota = BacklogQuota.builder()
                .limitSize(40)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        admin.topicPolicies().setBacklogQuota(topic, topicQuota,
                BacklogQuota.BacklogQuotaType.destination_storage);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.topicPolicies()
                .getBacklogQuotaMap(topic).isEmpty()));
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> topicQuotaMap = new HashMap<>();
        topicQuotaMap.put(BacklogQuota.BacklogQuotaType.destination_storage, topicQuota);
        assertEquals(admin.topicPolicies().getBacklogQuotaMap(topic, true), topicQuotaMap);

        admin.namespaces().removeBacklogQuota(myNamespace);
        admin.topicPolicies().removeBacklogQuota(topic, BacklogQuota.BacklogQuotaType.destination_storage);
        Awaitility.await().untilAsserted(() -> assertTrue(admin.namespaces().getBacklogQuotaMap(myNamespace)
                .get(BacklogQuota.BacklogQuotaType.destination_storage) == null));
        Awaitility.await().untilAsserted(() -> assertTrue(admin.topicPolicies().getBacklogQuotaMap(topic).isEmpty()));
        assertTrue(admin.topicPolicies().getBacklogQuotaMap(topic, true)
                .get(BacklogQuota.BacklogQuotaType.destination_storage) == null);
    }

    @Test(timeOut = 20000)
    public void testGetTimeBasedBacklogQuotaApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertEquals(admin.topicPolicies().getBacklogQuotaMap(topic), new HashMap<>());
        assertEquals(admin.namespaces().getBacklogQuotaMap(myNamespace), new HashMap<>());
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> brokerQuotaMap = ConfigHelper.backlogQuotaMap(conf);
        assertEquals(admin.topicPolicies().getBacklogQuotaMap(topic, true), brokerQuotaMap);
        BacklogQuota namespaceQuota = BacklogQuota.builder()
                .limitTime(30)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception)
                .build();

        admin.namespaces().setBacklogQuota(myNamespace, namespaceQuota,
                BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.namespaces()
                .getBacklogQuotaMap(myNamespace).isEmpty()));
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> namespaceQuotaMap = new HashMap<>();
        namespaceQuotaMap.put(BacklogQuota.BacklogQuotaType.message_age, namespaceQuota);
        namespaceQuotaMap.put(BacklogQuota.BacklogQuotaType.destination_storage, BacklogQuota.builder()
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_request_hold).build());
        assertEquals(admin.topicPolicies().getBacklogQuotaMap(topic, true),
                namespaceQuotaMap);

        BacklogQuota topicQuota = BacklogQuota.builder()
                .limitTime(40)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        admin.topicPolicies().setBacklogQuota(topic, topicQuota,
                BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await().untilAsserted(() -> assertFalse(admin.topicPolicies()
                .getBacklogQuotaMap(topic).isEmpty()));
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> topicQuotaMap = new HashMap<>();
        topicQuotaMap.put(BacklogQuota.BacklogQuotaType.message_age, topicQuota);
        assertEquals(admin.topicPolicies().getBacklogQuotaMap(topic, true), topicQuotaMap);

        admin.namespaces().removeBacklogQuota(myNamespace, BacklogQuota.BacklogQuotaType.message_age);
        admin.topicPolicies().removeBacklogQuota(topic, BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await().untilAsserted(() -> assertTrue(admin.namespaces()
                .getBacklogQuotaMap(myNamespace)
                .get(BacklogQuota.BacklogQuotaType.message_age) == null));
        Awaitility.await().untilAsserted(() -> assertTrue(admin.topicPolicies()
                .getBacklogQuotaMap(topic).isEmpty()));
        assertTrue(admin.topicPolicies().getBacklogQuotaMap(topic, true)
                .get(BacklogQuota.BacklogQuotaType.message_age) == null);
    }

    @Test
    public void testCheckBacklogQuotaFailed() throws Exception {
        RetentionPolicies retentionPolicies = new RetentionPolicies(10, 10);
        String namespace = TopicName.get(testTopic).getNamespace();
        admin.namespaces().setRetention(namespace, retentionPolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getRetention(namespace),
                        retentionPolicies));

        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(10 * 1024 * 1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        try {
            admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota, BacklogQuota
                    .BacklogQuotaType.destination_storage);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }
        //Ensure that the cache has not been updated after a long time
        Awaitility.await().atLeast(1, TimeUnit.SECONDS);
        assertNull(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                .get(BacklogQuota.BacklogQuotaType.destination_storage));
    }

    @Test
    public void testCheckRetentionSizeBasedQuota() throws Exception {
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(10 * 1024 * 1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();

        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota, BacklogQuota
                .BacklogQuotaType.destination_storage);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota));

        RetentionPolicies retention = new RetentionPolicies(10, 10);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);
        try {
            admin.topicPolicies().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        retention = new RetentionPolicies(10, 9);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);
        try {
            admin.topicPolicies().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        retention = new RetentionPolicies(10, 12);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topicPolicies().setRetention(testTopic, retention);

        RetentionPolicies finalRetention = retention;
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getRetention(testTopic), finalRetention));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckRetentionTimeBasedQuota() throws Exception {
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitTime(10 * 60)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();

        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota, BacklogQuota.BacklogQuotaType.message_age);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.message_age), backlogQuota));

        RetentionPolicies retention = new RetentionPolicies(10, 10);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);
        try {
            admin.topicPolicies().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        retention = new RetentionPolicies(9, 10);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);
        try {
            admin.topicPolicies().setRetention(testTopic, retention);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        retention = new RetentionPolicies(12, 10);
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);
        admin.topicPolicies().setRetention(testTopic, retention);

        RetentionPolicies finalRetention = retention;
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getRetention(testTopic), finalRetention));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetRetention() throws Exception {
        RetentionPolicies retention = new RetentionPolicies(60, 1024);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);

        admin.topicPolicies().setRetention(testTopic, retention);
        log.info("Retention set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getRetention(testTopic), retention));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRetentionPolicyValidation() throws Exception {
        // should pass
        admin.topicPolicies().setRetention(testTopic, new RetentionPolicies());
        admin.topicPolicies().setRetention(testTopic, new RetentionPolicies(-1, -1));
        admin.topicPolicies().setRetention(testTopic, new RetentionPolicies(1, 1));

        // should not pass validation
        assertInvalidRetentionPolicy(testTopic, 1, 0);
        assertInvalidRetentionPolicy(testTopic, 0, 1);
        assertInvalidRetentionPolicy(testTopic, -1, 0);
        assertInvalidRetentionPolicy(testTopic, 0, -1);
        assertInvalidRetentionPolicy(testTopic, -2, 1);
        assertInvalidRetentionPolicy(testTopic, 1, -2);

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    private void assertInvalidRetentionPolicy(String topicName, int retentionTimeInMinutes, int retentionSizeInMB) {
        try {
            RetentionPolicies retention = new RetentionPolicies(retentionTimeInMinutes, retentionSizeInMB);
            admin.topicPolicies().setRetention(topicName, retention);
            fail("Validation should have failed for " + retention);
        } catch (PulsarAdminException e) {
            assertTrue(e.getCause() instanceof BadRequestException);
            assertTrue(e.getMessage().startsWith("Invalid retention policy"));
        }
    }

    @Test
    public void testRemoveRetention() throws Exception {

        RetentionPolicies retention = new RetentionPolicies(60, 1024);
        log.info("Retention: {} will set to the topic: {}", retention, testTopic);

        admin.topicPolicies().setRetention(testTopic, retention);
        log.info("Retention set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getRetention(testTopic),
                        retention));

        admin.topicPolicies().removeRetention(testTopic);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies().getRetention(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test(timeOut = 10000)
    public void testRetentionAppliedApi() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();

        RetentionPolicies brokerPolicies =
                new RetentionPolicies(conf.getDefaultRetentionTimeInMinutes(), conf.getDefaultRetentionSizeInMB());
        assertEquals(admin.topicPolicies().getRetention(topic, true), brokerPolicies);

        RetentionPolicies namespacePolicies = new RetentionPolicies(10, 20);
        admin.namespaces().setRetention(myNamespace, namespacePolicies);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topicPolicies().getRetention(topic, true), namespacePolicies));

        RetentionPolicies topicPolicies = new RetentionPolicies(20, 30);
        admin.topicPolicies().setRetention(topic, topicPolicies);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topicPolicies().getRetention(topic, true), topicPolicies));

        admin.topicPolicies().removeRetention(topic);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topicPolicies().getRetention(topic, true), namespacePolicies));

        admin.namespaces().removeRetention(myNamespace);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topicPolicies().getRetention(topic, true), brokerPolicies));
    }

    @Test(timeOut = 20000)
    public void testGetSubDispatchRateApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getSubscriptionDispatchRate(topic));
        assertNull(admin.namespaces().getSubscriptionDispatchRate(myNamespace));

        DispatchRate brokerDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(conf.getDispatchThrottlingRatePerSubscriptionInMsg())
                .dispatchThrottlingRateInByte(conf.getDispatchThrottlingRatePerSubscriptionInByte())
                .ratePeriodInSecond(1)
                .build();
        assertEquals(admin.topicPolicies().getSubscriptionDispatchRate(topic, true), brokerDispatchRate);
        DispatchRate namespaceDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(11)
                .ratePeriodInSecond(12)
                .build();

        admin.namespaces().setSubscriptionDispatchRate(myNamespace, namespaceDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces()
                .getSubscriptionDispatchRate(myNamespace)));
        assertEquals(admin.topicPolicies().getSubscriptionDispatchRate(topic, true),
                namespaceDispatchRate);

        DispatchRate topicDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(20)
                .dispatchThrottlingRateInByte(21)
                .ratePeriodInSecond(12)
                .build();
        admin.topicPolicies().setSubscriptionDispatchRate(topic, topicDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies()
                .getSubscriptionDispatchRate(topic)));
        assertEquals(admin.topicPolicies().getSubscriptionDispatchRate(topic, true), topicDispatchRate);

        admin.namespaces().removeSubscriptionDispatchRate(myNamespace);
        admin.topicPolicies().removeSubscriptionDispatchRate(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces()
                .getSubscriptionDispatchRate(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies()
                .getSubscriptionDispatchRate(topic)));
        assertEquals(admin.topicPolicies().getSubscriptionDispatchRate(topic, true),
                brokerDispatchRate);

        admin.namespaces().setSubscriptionDispatchRate(myNamespaceV1, namespaceDispatchRate);
        Awaitility.await().untilAsserted(() ->
                assertNotNull(admin.namespaces().getSubscriptionDispatchRate(myNamespaceV1)));
        admin.namespaces().removeSubscriptionDispatchRate(myNamespaceV1);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin.namespaces().getSubscriptionDispatchRate(myNamespaceV1)));

    }

    @Test(timeOut = 20000)
    public void testRetentionPriority() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getRetention(topic));
        assertNull(admin.namespaces().getRetention(myNamespace));

        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
        Method shouldTopicBeRetained = PersistentTopic.class.getDeclaredMethod("shouldTopicBeRetained");
        shouldTopicBeRetained.setAccessible(true);
        Field lastActive = PersistentTopic.class.getSuperclass().getDeclaredField("lastActive");
        lastActive.setAccessible(true);
        //set last active to 2 minutes ago
        lastActive.setLong(persistentTopic, System.nanoTime() - TimeUnit.MINUTES.toNanos(2));
        //the default value of the broker-level is 0, so it is not retained by default
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        //set namespace-level policy
        RetentionPolicies retentionPolicies = new RetentionPolicies(1, 1);
        admin.namespaces().setRetention(myNamespace, retentionPolicies);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getRetention(myNamespace)));
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        // set topic-level policy
        admin.topicPolicies().setRetention(topic, new RetentionPolicies(3, 1));
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topicPolicies().getRetention(topic)));
        assertTrue((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        //topic-level disabled
        admin.topicPolicies().setRetention(topic, new RetentionPolicies(0, 0));
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topicPolicies().getRetention(topic).getRetentionSizeInMB(), 0));
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        // remove topic-level policy
        admin.topicPolicies().removeRetention(topic);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topicPolicies().getRetention(topic)));
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        //namespace-level disabled
        admin.namespaces().setRetention(myNamespace, new RetentionPolicies(0, 0));
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getRetention(myNamespace)));
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        //change namespace-level policy
        admin.namespaces().setRetention(myNamespace, new RetentionPolicies(1, 1));
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getRetention(myNamespace)));
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
        // remove namespace-level policy
        admin.namespaces().removeRetention(myNamespace);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.namespaces().getRetention(myNamespace)));
        //the default value of the broker-level is 0, so it is not retained by default
        assertFalse((boolean) shouldTopicBeRetained.invoke(persistentTopic));
    }

    @Test(timeOut = 20000)
    public void testGetPersistenceApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getPersistence(topic));
        assertNull(admin.namespaces().getPersistence(myNamespace));
        PersistencePolicies brokerPolicy =
                new PersistencePolicies(pulsar.getConfiguration().getManagedLedgerDefaultEnsembleSize(),
                pulsar.getConfiguration().getManagedLedgerDefaultWriteQuorum(),
                pulsar.getConfiguration().getManagedLedgerDefaultAckQuorum(),
                pulsar.getConfiguration().getManagedLedgerDefaultMarkDeleteRateLimit());
        assertEquals(admin.topicPolicies().getPersistence(topic, true), brokerPolicy);
        PersistencePolicies namespacePolicy =
                new PersistencePolicies(5, 4, 3, 2);

        admin.namespaces().setPersistence(myNamespace, namespacePolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getPersistence(myNamespace)));
        assertEquals(admin.topicPolicies().getPersistence(topic, true), namespacePolicy);

        PersistencePolicies topicPolicy = new PersistencePolicies(4, 3, 2, 1);
        admin.topicPolicies().setPersistence(topic, topicPolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies().getPersistence(topic)));
        assertEquals(admin.topicPolicies().getPersistence(topic, true), topicPolicy);

        admin.namespaces().removePersistence(myNamespace);
        admin.topicPolicies().removePersistence(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getPersistence(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies().getPersistence(topic)));
        assertEquals(admin.topicPolicies().getPersistence(topic, true), brokerPolicy);
    }

    @Test
    public void testCheckPersistence() throws Exception {
        PersistencePolicies persistencePolicies = new PersistencePolicies(6, 2, 2, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topicPolicies().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        persistencePolicies = new PersistencePolicies(2, 6, 2, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topicPolicies().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        persistencePolicies = new PersistencePolicies(2, 2, 6, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topicPolicies().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        persistencePolicies = new PersistencePolicies(1, 2, 2, 0.0);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, testTopic);
        try {
            admin.topicPolicies().setPersistence(testTopic, persistencePolicies);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 400);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testSetPersistence() throws Exception {
        PersistencePolicies persistencePoliciesForNamespace = new PersistencePolicies(2, 2, 2, 0.3);
        admin.namespaces().setPersistence(myNamespace, persistencePoliciesForNamespace);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getPersistence(myNamespace),
                        persistencePoliciesForNamespace));

        PersistencePolicies persistencePolicies = new PersistencePolicies(3, 3, 3, 0.1);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);
        admin.topics().createNonPartitionedTopic(persistenceTopic);
        admin.topicPolicies().setPersistence(persistenceTopic, persistencePolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getPersistence(persistenceTopic), persistencePolicies));
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(persistenceTopic)
                .subscriptionName("test")
                .subscribe();
        admin.topics().unload(persistenceTopic);
        Topic t = pulsar.getBrokerService().getOrCreateTopic(persistenceTopic).get();
        PersistentTopic persistentTopic = (PersistentTopic) t;
        Awaitility.await().untilAsserted(() -> {
            ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
            assertEquals(managedLedgerConfig.getEnsembleSize(), 3);
            assertEquals(managedLedgerConfig.getWriteQuorumSize(), 3);
            assertEquals(managedLedgerConfig.getAckQuorumSize(), 3);
            assertEquals(managedLedgerConfig.getThrottleMarkDelete(), 0.1);
        });

        PersistencePolicies getPersistencePolicies = admin.topicPolicies().getPersistence(persistenceTopic);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);
        Assert.assertEquals(getPersistencePolicies, persistencePolicies);
        consumer.close();
    }

    @Test(dataProvider = "invalidPersistentPolicies")
    public void testSetIncorrectPersistentPolicies(int ensembleSize, int writeQuorum, int ackQuorum) throws Exception {
        admin.topics().createNonPartitionedTopic(persistenceTopic);
        PersistencePolicies persistence1 = new PersistencePolicies(ensembleSize, writeQuorum, ackQuorum, 0.0);

        boolean failed = false;
        try {
            admin.topicPolicies().setPersistence(persistenceTopic, persistence1);
        } catch (PulsarAdminException e) {
            failed = true;
            Assert.assertEquals(e.getStatusCode(), 400);
        }
        assertTrue(failed);
        admin.topics().delete(persistenceTopic);
    }

    @Test
    public void testGetDispatchRateApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getDispatchRate(topic));
        assertNull(admin.namespaces().getDispatchRate(myNamespace));
        DispatchRate brokerDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(conf.getDispatchThrottlingRatePerTopicInMsg())
                .dispatchThrottlingRateInByte(conf.getDispatchThrottlingRatePerTopicInByte())
                .ratePeriodInSecond(1)
                .build();
        assertEquals(admin.topicPolicies().getDispatchRate(topic, true), brokerDispatchRate);
        DispatchRate namespaceDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(11)
                .ratePeriodInSecond(12)
                .build();

        admin.namespaces().setDispatchRate(myNamespace, namespaceDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces().getDispatchRate(myNamespace)));
        assertEquals(admin.topicPolicies().getDispatchRate(topic, true), namespaceDispatchRate);

        DispatchRate topicDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(20)
                .dispatchThrottlingRateInByte(21)
                .ratePeriodInSecond(22)
                .build();
        admin.topicPolicies().setDispatchRate(topic, topicDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies().getDispatchRate(topic)));
        assertEquals(admin.topicPolicies().getDispatchRate(topic, true), topicDispatchRate);

        admin.namespaces().removeDispatchRate(myNamespace);
        admin.topicPolicies().removeDispatchRate(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getDispatchRate(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies().getDispatchRate(topic)));
        assertEquals(admin.topicPolicies().getDispatchRate(topic, true), brokerDispatchRate);
    }

    @Test
    public void testRemovePersistence() throws Exception {
        PersistencePolicies persistencePoliciesForNamespace = new PersistencePolicies(2, 2, 2, 0.3);
        admin.namespaces().setPersistence(myNamespace, persistencePoliciesForNamespace);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getPersistence(myNamespace),
                        persistencePoliciesForNamespace));

        PersistencePolicies persistencePolicies = new PersistencePolicies(3, 3, 3, 0.1);
        log.info("PersistencePolicies: {} will set to the topic: {}", persistencePolicies, persistenceTopic);
        admin.topics().createNonPartitionedTopic(persistenceTopic);
        admin.topicPolicies().setPersistence(persistenceTopic, persistencePolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getPersistence(persistenceTopic), persistencePolicies));

        admin.topicPolicies().removePersistence(persistenceTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies()
                        .getPersistence(persistenceTopic)));

        admin.lookups().lookupTopic(persistenceTopic);
        Topic t = pulsar.getBrokerService().getOrCreateTopic(persistenceTopic).get();
        PersistentTopic persistentTopic = (PersistentTopic) t;
        ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
        assertEquals(managedLedgerConfig.getEnsembleSize(), 2);
        assertEquals(managedLedgerConfig.getWriteQuorumSize(), 2);
        assertEquals(managedLedgerConfig.getAckQuorumSize(), 2);
        assertEquals(managedLedgerConfig.getThrottleMarkDelete(), 0.3);
    }

    @Test
    public void testCheckMaxProducers() throws Exception {
        int maxProducers = -1;
        log.info("MaxProducers: {} will set to the topic: {}", maxProducers, testTopic);
        try {
            admin.topicPolicies().setMaxProducers(testTopic, maxProducers);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetMaxProducerApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getMaxProducers(topic));
        assertNull(admin.namespaces().getMaxProducersPerTopic(myNamespace));
        assertEquals(admin.topicPolicies().getMaxProducers(topic, true).intValue(),
                conf.getMaxProducersPerTopic());

        admin.namespaces().setMaxProducersPerTopic(myNamespace, 7);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces()
                .getMaxProducersPerTopic(myNamespace)));
        assertEquals(admin.topicPolicies().getMaxProducers(topic, true).intValue(), 7);

        admin.topicPolicies().setMaxProducers(topic, 1000);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies().getMaxProducers(topic)));
        assertEquals(admin.topicPolicies().getMaxProducers(topic, true).intValue(), 1000);

        admin.namespaces().removeMaxProducersPerTopic(myNamespace);
        admin.topicPolicies().removeMaxProducers(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces()
                .getMaxProducersPerTopic(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies().getMaxProducers(topic)));
        assertEquals(admin.topicPolicies().getMaxProducers(topic, true).intValue(),
                conf.getMaxProducersPerTopic());
    }

    private void waitTopicPoliciesApplied(String topic, int partitions,
                                          java.util.function.Consumer<HierarchyTopicPolicies> condition) {
        TopicName topicName = TopicName.get(topic);
        if (partitions > 0) {
            for (int i = 0; i < partitions; i++) {
                String partition = topicName.getPartition(i).toString();
                Awaitility.await().untilAsserted(() -> {
                    Topic t = pulsar.getBrokerService().getTopicIfExists(partition).get().get();
                    assertTrue(t instanceof AbstractTopic);
                    condition.accept(((AbstractTopic) t).getHierarchyTopicPolicies());
                });
            }
        } else {
            Awaitility.await().untilAsserted(() -> {
                Topic t = pulsar.getBrokerService().getTopicIfExists(topic).get().get();
                assertTrue(t instanceof AbstractTopic);
                condition.accept(((AbstractTopic) t).getHierarchyTopicPolicies());
            });
        }
    }

    @Test
    public void testSetMaxProducers() throws Exception {
        Integer maxProducers = 2;
        log.info("MaxProducers: {} will set to the topic: {}", maxProducers, persistenceTopic);
        //broker level setting is 4
        conf.setMaxProducersPerTopic(4);
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        waitTopicPoliciesApplied(persistenceTopic, 2, hierarchyTopicPolicies -> {
            assertEquals((int) hierarchyTopicPolicies.getMaxProducersPerTopic().get(), 4);
        });
        //ns level setting is 3
        admin.namespaces().setMaxProducersPerTopic(myNamespace, 3);
        waitTopicPoliciesApplied(persistenceTopic, 2, hierarchyTopicPolicies -> {
            assertEquals((int) hierarchyTopicPolicies.getMaxProducersPerTopic().get(), 3);
        });
        //topic level setting is 2
        admin.topicPolicies().setMaxProducers(persistenceTopic, maxProducers);
        waitTopicPoliciesApplied(persistenceTopic, 2, hierarchyTopicPolicies -> {
            assertEquals((int) hierarchyTopicPolicies.getMaxProducersPerTopic().get(), 2);
        });

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                                .getMaxProducers(persistenceTopic),
                        maxProducers));

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Producer<byte[]> producer3 = null;

        try {
            producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit on topic level.");
        }
        Assert.assertNotNull(producer1);
        Assert.assertNotNull(producer2);
        Assert.assertNull(producer3);

        admin.topicPolicies().removeMaxProducers(persistenceTopic);
        waitTopicPoliciesApplied(persistenceTopic, 2, hierarchyTopicPolicies -> {
            assertEquals((int) hierarchyTopicPolicies.getMaxProducersPerTopic().get(), 3);
        });
        producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();

        Producer<byte[]> producer4;
        try {
            producer4 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit on topic level.");
        }

        admin.namespaces().removeMaxProducersPerTopic(myNamespace);
        waitTopicPoliciesApplied(persistenceTopic, 2, hierarchyTopicPolicies -> {
            assertEquals((int) hierarchyTopicPolicies.getMaxProducersPerTopic().get(), 4);
        });
        producer4 = pulsarClient.newProducer().topic(persistenceTopic).create();

        try {
            Producer<byte[]> producer5 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit on topic level.");
        }
        producer1.close();
        producer2.close();
        producer3.close();
        producer4.close();

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveMaxProducers() throws Exception {
        Integer maxProducers = 2;
        log.info("MaxProducers: {} will set to the topic: {}", maxProducers, persistenceTopic);
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        admin.topicPolicies().setMaxProducers(persistenceTopic, maxProducers);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getMaxProducers(persistenceTopic), maxProducers));

        Producer<byte[]> producer1 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Producer<byte[]> producer2 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Producer<byte[]> producer3 = null;
        Producer<byte[]> producer4 = null;

        try {
            producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit on topic level.");
        }
        Assert.assertNotNull(producer1);
        Assert.assertNotNull(producer2);
        Assert.assertNull(producer3);

        admin.topicPolicies().removeMaxProducers(persistenceTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies()
                        .getMaxProducers(persistenceTopic)));

        producer3 = pulsarClient.newProducer().topic(persistenceTopic).create();
        Assert.assertNotNull(producer3);
        admin.namespaces().setMaxProducersPerTopic(myNamespace, 3);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces()
                        .getMaxProducersPerTopic(myNamespace).intValue(), 3));

        log.info("MaxProducers: {} will set to the namespace: {}", 3, myNamespace);
        try {
            producer4 = pulsarClient.newProducer().topic(persistenceTopic).create();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max producers limit on namespace level.");
        }
        Assert.assertNull(producer4);

        producer1.close();
        producer2.close();
        producer3.close();
        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }


    @Test
    public void testGetSetDispatchRate() throws Exception {
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(1000)
                .ratePeriodInSecond(1)
                .relativeToPublishRate(true)
                .build();
        log.info("Dispatch Rate: {} will set to the topic: {}", dispatchRate, testTopic);

        admin.topicPolicies().setDispatchRate(testTopic, dispatchRate);
        log.info("Dispatch Rate set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getDispatchRate(testTopic), dispatchRate));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveDispatchRate() throws Exception {
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(1000)
                .ratePeriodInSecond(1)
                .relativeToPublishRate(true)
                .build();
        log.info("Dispatch Rate: {} will set to the topic: {}", dispatchRate, testTopic);

        admin.topicPolicies().setDispatchRate(testTopic, dispatchRate);
        log.info("Dispatch Rate set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getDispatchRate(testTopic), dispatchRate));

        admin.topicPolicies().removeDispatchRate(testTopic);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies()
                        .getDispatchRate(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test(timeOut = 20000)
    public void testPolicyOverwrittenByNamespaceLevel() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(200)
                .dispatchThrottlingRateInByte(20000)
                .ratePeriodInSecond(1)
                .relativeToPublishRate(true)
                .build();
        admin.namespaces().setDispatchRate(myNamespace, dispatchRate);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces()
                        .getDispatchRate(myNamespace).getDispatchThrottlingRateInMsg(), 200));

        dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(10000)
                .ratePeriodInSecond(1)
                .relativeToPublishRate(true)
                .build();
        admin.topicPolicies().setDispatchRate(topic, dispatchRate);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNotNull(admin.topicPolicies()
                        .getDispatchRate(topic)));

        //1 Set ns level policy, topic level should not be overwritten
        dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(300)
                .dispatchThrottlingRateInByte(30000)
                .ratePeriodInSecond(2)
                .relativeToPublishRate(true)
                .build();
        admin.namespaces().setDispatchRate(myNamespace, dispatchRate);

        Awaitility.await()
                .untilAsserted(() -> {
                    DispatchRateLimiter limiter = pulsar.getBrokerService().getTopicIfExists(topic)
                            .get().get().getDispatchRateLimiter().get();
                    Assert.assertEquals(limiter.getDispatchRateOnByte(), 10000);
                    Assert.assertEquals(limiter.getDispatchRateOnMsg(), 100);
                });

        admin.topicPolicies().removeDispatchRate(topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies().getDispatchRate(topic)));

        //2 Remove level policy, DispatchRateLimiter should us ns level policy
        Awaitility.await()
                .untilAsserted(() -> {
                    DispatchRateLimiter limiter = pulsar.getBrokerService().getTopicIfExists(topic)
                            .get().get().getDispatchRateLimiter().get();
                    Assert.assertEquals(limiter.getDispatchRateOnByte(), 30000);
                    Assert.assertEquals(limiter.getDispatchRateOnMsg(), 300);
                });
    }

    @Test(timeOut = 20000)
    public void testRestart() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        // set namespace level inactive topic policies
        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up,
                        100, true);
        admin.namespaces().setInactiveTopicPolicies(myNamespace, inactiveTopicPolicies);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces()
                                .getInactiveTopicPolicies(myNamespace).getInactiveTopicDeleteMode(),
                        InactiveTopicDeleteMode.delete_when_subscriptions_caught_up));

        // set namespace retention policies
        final RetentionPolicies retentionPolicies = new RetentionPolicies(10, -1);
        admin.namespaces().setRetention(myNamespace, retentionPolicies);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getRetention(myNamespace),
                        retentionPolicies));

        // set topic level inactive topic policies
        inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions,
                        200, false);
        admin.topicPolicies().setInactiveTopicPolicies(topic, inactiveTopicPolicies);

        InactiveTopicPolicies finalInactiveTopicPolicies = inactiveTopicPolicies;
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getInactiveTopicPolicies(topic),
                        finalInactiveTopicPolicies));

        // set topic level retention policies
        final RetentionPolicies finalRetentionPolicies = new RetentionPolicies(20, -1);
        admin.topicPolicies().setRetention(topic, finalRetentionPolicies);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getRetention(topic),
                        finalRetentionPolicies));

        // restart broker, policy should still take effect
        restartBroker();

        // Trigger the cache init.
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();

        // check inactive topic policies and retention policies.
        Awaitility.await()
                .untilAsserted(() -> {
                    PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                            .getTopicIfExists(topic).get().get();
                    ManagedLedgerConfig managedLedgerConfig = persistentTopic.getManagedLedger().getConfig();
                    Assert.assertEquals(persistentTopic.getInactiveTopicPolicies(), finalInactiveTopicPolicies);
                    Assert.assertEquals(managedLedgerConfig.getRetentionSizeInMB(),
                            finalRetentionPolicies.getRetentionSizeInMB());
                    Assert.assertEquals(managedLedgerConfig.getRetentionTimeMillis(),
                            TimeUnit.MINUTES.toMillis(finalRetentionPolicies.getRetentionTimeInMinutes()));
                });

        producer.close();
    }

    @Test
    public void testGetSetSubscriptionDispatchRate() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1000)
                .dispatchThrottlingRateInByte(1024 * 1024)
                .ratePeriodInSecond(1)
                .build();
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        admin.topicPolicies().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getSubscriptionDispatchRate(topic), dispatchRate));

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName)
                .topic(topic).subscribe();

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(),
                dispatchRate.getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(),
                dispatchRate.getDispatchThrottlingRateInMsg());

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testSubscriptionLevelDispatchRate() throws Exception{
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        String subscriptionName = "testSubscriptionLevelDispatchRate";
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName)
                .topic(topic).subscribe();
        AbstractTopic abstractTopic = (AbstractTopic) pulsar.getBrokerService()
                .getTopicIfExists(topic).get().get();

        DispatchRate topicLevelRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(1024)
                .ratePeriodInSecond(1)
                .build();
        admin.topicPolicies().setSubscriptionDispatchRate(topic, topicLevelRate);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(admin.topicPolicies().getSubscriptionDispatchRate(topic),
                        topicLevelRate));

        // topic level is set, applied value should be topic level setting.
        Assert.assertEquals(admin.topicPolicies().getSubscriptionDispatchRate(topic, subscriptionName,
                        true),
                topicLevelRate);
        // subscription level setting is not set.
        Assert.assertNull(admin.topicPolicies().getSubscriptionDispatchRate(topic, subscriptionName));

        DispatchRate subLevelRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1000)
                .dispatchThrottlingRateInByte(1024 * 1024)
                .ratePeriodInSecond(1)
                .build();

        admin.topicPolicies().setSubscriptionDispatchRate(topic, subscriptionName, subLevelRate);
        Awaitility.await().untilAsserted(() ->
                Assert.assertEquals(admin.topicPolicies().getSubscriptionDispatchRate(topic, subscriptionName),
                        subLevelRate));

        Awaitility.await().untilAsserted(() -> Assert.assertEquals(
                abstractTopic.getSubscriptionDispatchRate(subscriptionName), subLevelRate));

        DispatchRateLimiter dispatchRateLimiter = abstractTopic.getSubscription(subscriptionName)
                .getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), subLevelRate
                .getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), subLevelRate
                .getDispatchThrottlingRateInMsg());

        admin.topicPolicies().removeSubscriptionDispatchRate(topic, subscriptionName);
        Awaitility.await().untilAsserted(() ->
                Assert.assertNull(admin.topicPolicies().getSubscriptionDispatchRate(topic, subscriptionName)));

        admin.topics().delete(topic, true);
    }

    @Test
    public void testGetSetSubscriptionDispatchRateAfterTopicLoaded() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1000)
                .dispatchThrottlingRateInByte(1024 * 1024)
                .ratePeriodInSecond(1)
                .build();
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName)
                .topic(topic).subscribe();

        admin.topicPolicies().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getSubscriptionDispatchRate(topic), dispatchRate));

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate
                .getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate
                .getDispatchThrottlingRateInMsg());

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testRemoveSubscriptionDispatchRate() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(1000)
                .dispatchThrottlingRateInByte(1024 * 1024)
                .ratePeriodInSecond(1)
                .build();
        log.info("Subscription Dispatch Rate: {} will set to the topic: {}", dispatchRate, topic);

        admin.topicPolicies().setSubscriptionDispatchRate(topic, dispatchRate);
        log.info("Subscription dispatch rate set success on topic: {}", topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getSubscriptionDispatchRate(topic), dispatchRate));

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName)
                .topic(topic).subscribe();

        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertNotNull(dispatchRateLimiter);
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), dispatchRate
                .getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), dispatchRate
                .getDispatchThrottlingRateInMsg());

        // remove subscription dispatch rate
        admin.topicPolicies().removeSubscriptionDispatchRate(topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies()
                        .getSubscriptionDispatchRate(topic)));

        Awaitility.await().untilAsserted(() -> {
            DispatchRateLimiter drl = pulsar.getBrokerService().getTopicIfExists(topic)
                    .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
            Assert.assertNotEquals(drl.getDispatchRateOnMsg(),
                    dispatchRate.getDispatchThrottlingRateInMsg());
            Assert.assertNotEquals(drl.getDispatchRateOnByte(),
                    dispatchRate.getDispatchThrottlingRateInByte());
        });

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testSubscriptionDispatchRatePolicyOverwrittenNamespaceLevel() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        // set namespace level subscription dispatch rate
        DispatchRate namespaceDispatchRate =                     DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(1024 * 1024)
                .ratePeriodInSecond(1)
                .build();
        admin.namespaces().setSubscriptionDispatchRate(myNamespace, namespaceDispatchRate);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces()
                        .getSubscriptionDispatchRate(myNamespace), namespaceDispatchRate));

        String subscriptionName = "test_subscription_rate";
        Consumer<byte[]> consumer = pulsarClient.newConsumer().subscriptionName(subscriptionName)
                .topic(topic).subscribe();

        // get subscription dispatch Rate limiter
        DispatchRateLimiter dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get().getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), namespaceDispatchRate
                .getDispatchThrottlingRateInMsg());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), namespaceDispatchRate
                .getDispatchThrottlingRateInByte());

        // set topic level subscription dispatch rate
        DispatchRate topicDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(200)
                .dispatchThrottlingRateInByte(2 * 1024 * 1024)
                .ratePeriodInSecond(1)
                .build();
        admin.topicPolicies().setSubscriptionDispatchRate(topic, topicDispatchRate);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getSubscriptionDispatchRate(topic), topicDispatchRate));

        // get subscription dispatch rate limiter
        dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get()
                .getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(), topicDispatchRate
                .getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(), topicDispatchRate
                .getDispatchThrottlingRateInMsg());

        // remove topic level subscription dispatch rate limiter
        admin.topicPolicies().removeSubscriptionDispatchRate(topic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies()
                        .getSubscriptionDispatchRate(topic)));

        // get subscription dispatch rate limiter
        dispatchRateLimiter = pulsar.getBrokerService().getTopicIfExists(topic).get().get()
                .getSubscription(subscriptionName).getDispatcher().getRateLimiter().get();
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnByte(),
                namespaceDispatchRate.getDispatchThrottlingRateInByte());
        Assert.assertEquals(dispatchRateLimiter.getDispatchRateOnMsg(),
                namespaceDispatchRate.getDispatchThrottlingRateInMsg());

        consumer.close();
        admin.topics().delete(topic, true);
    }

    @Test
    public void testGetSetCompactionThreshold() throws Exception {
        Long compactionThreshold = 100000L;
        log.info("Compaction threshold: {} will set to the topic: {}", compactionThreshold, testTopic);

        admin.topicPolicies().setCompactionThreshold(testTopic, compactionThreshold);
        log.info("Compaction threshold set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getCompactionThreshold(testTopic), compactionThreshold));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveCompactionThreshold() throws Exception {
        Long compactionThreshold = 100000L;
        log.info("Compaction threshold: {} will set to the topic: {}", compactionThreshold, testTopic);

        admin.topicPolicies().setCompactionThreshold(testTopic, compactionThreshold);
        log.info("Compaction threshold set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getCompactionThreshold(testTopic), compactionThreshold));

        admin.topicPolicies().removeCompactionThreshold(testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies()
                        .getCompactionThreshold(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetSetMaxConsumersPerSubscription() throws Exception {
        Integer maxConsumersPerSubscription = 10;
        log.info("MaxConsumersPerSubscription: {} will set to the topic: {}", maxConsumersPerSubscription, testTopic);

        admin.topicPolicies().setMaxConsumersPerSubscription(testTopic, maxConsumersPerSubscription);
        log.info("MaxConsumersPerSubscription set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getMaxConsumersPerSubscription(testTopic), maxConsumersPerSubscription));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveMaxConsumersPerSubscription() throws Exception {
        Integer maxConsumersPerSubscription = 10;
        log.info("MaxConsumersPerSubscription: {} will set to the topic: {}", maxConsumersPerSubscription, testTopic);

        admin.topicPolicies().setMaxConsumersPerSubscription(testTopic, maxConsumersPerSubscription);
        log.info("MaxConsumersPerSubscription set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getMaxConsumersPerSubscription(testTopic), maxConsumersPerSubscription));

        admin.topicPolicies().removeMaxConsumersPerSubscription(testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies()
                        .getMaxConsumersPerSubscription(testTopic)));

        admin.namespaces().setMaxConsumersPerSubscription(myNamespaceV1, 20);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.namespaces().getMaxConsumersPerSubscription(myNamespaceV1).intValue(),
                        20));
        admin.namespaces().removeMaxConsumersPerSubscription(myNamespaceV1);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin.namespaces().getMaxConsumersPerSubscription(myNamespaceV1)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testGetSetPublishRate() throws Exception {
        PublishRate publishRate = new PublishRate(10000, 1024 * 1024 * 5);
        log.info("Publish Rate: {} will set to the topic: {}", publishRate, testTopic);

        admin.topicPolicies().setPublishRate(testTopic, publishRate);
        log.info("Publish Rate set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getPublishRate(testTopic),
                        publishRate));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemovePublishRate() throws Exception {
        PublishRate publishRate = new PublishRate(10000, 1024 * 1024 * 5);
        log.info("Publish Rate: {} will set to the topic: {}", publishRate, testTopic);

        admin.topicPolicies().setPublishRate(testTopic, publishRate);
        log.info("Publish Rate set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getPublishRate(testTopic),
                        publishRate));

        admin.topicPolicies().removePublishRate(testTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies().getPublishRate(testTopic)));

        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testCheckMaxConsumers() throws Exception {
        Integer maxProducers = -1;
        log.info("MaxConsumers: {} will set to the topic: {}", maxProducers, testTopic);
        try {
            admin.topicPolicies().setMaxConsumers(testTopic, maxProducers);
            Assert.fail();
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 412);
        }

        admin.topics().deletePartitionedTopic(testTopic, true);
    }
    @Test
    public void testGetMaxConsumersApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getMaxConsumers(topic));
        assertNull(admin.namespaces().getMaxConsumersPerTopic(myNamespace));
        assertEquals(admin.topicPolicies().getMaxConsumers(topic, true).intValue(),
                conf.getMaxConsumersPerTopic());

        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 7);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces()
                .getMaxConsumersPerTopic(myNamespace)));
        assertEquals(admin.topicPolicies().getMaxConsumers(topic, true).intValue(), 7);

        admin.topicPolicies().setMaxConsumers(topic, 1000);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies()
                .getMaxConsumers(topic)));
        assertEquals(admin.topicPolicies().getMaxConsumers(topic, true).intValue(), 1000);

        admin.namespaces().removeMaxConsumersPerTopic(myNamespace);
        admin.topicPolicies().removeMaxConsumers(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces()
                .getMaxConsumersPerTopic(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies()
                .getMaxConsumers(topic)));
        assertEquals(admin.topicPolicies().getMaxConsumers(topic, true).intValue(),
                conf.getMaxConsumersPerTopic());
    }

    @Test
    public void testSetMaxConsumers() throws Exception {
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 1);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces()
                        .getMaxConsumersPerTopic(myNamespace).intValue(), 1));
        log.info("MaxConsumers: {} will set to the namespace: {}", 1, myNamespace);
        Integer maxConsumers = 2;
        log.info("MaxConsumers: {} will set to the topic: {}", maxConsumers, persistenceTopic);
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        admin.topicPolicies().setMaxConsumers(persistenceTopic, maxConsumers);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getMaxConsumers(persistenceTopic), maxConsumers));

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().subscriptionName("sub1")
                .topic(persistenceTopic).subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().subscriptionName("sub2")
                .topic(persistenceTopic).subscribe();
        Consumer<byte[]> consumer3 = null;

        try {
            consumer3 = pulsarClient.newConsumer().subscriptionName("sub3")
                    .topic(persistenceTopic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max consumers limit");
        }
        Assert.assertNotNull(consumer1);
        Assert.assertNotNull(consumer2);
        Assert.assertNull(consumer3);
        consumer1.close();
        consumer2.close();

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testRemoveMaxConsumers() throws Exception {
        Integer maxConsumers = 2;
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        admin.topicPolicies().setMaxConsumers(persistenceTopic, maxConsumers);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getMaxConsumers(persistenceTopic), maxConsumers));

        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().subscriptionName("sub1")
                .topic(persistenceTopic).subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().subscriptionName("sub2")
                .topic(persistenceTopic).subscribe();
        Consumer<byte[]> consumer3 = null;

        try {
            consumer3 = pulsarClient.newConsumer().subscriptionName("sub3")
                    .topic(persistenceTopic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max consumers limit");
        }
        Assert.assertNotNull(consumer1);
        Assert.assertNotNull(consumer2);
        Assert.assertNull(consumer3);

        admin.topicPolicies().removeMaxConsumers(persistenceTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies()
                        .getMaxConsumers(persistenceTopic)));

        consumer3 = pulsarClient.newConsumer().subscriptionName("sub3")
                .topic(persistenceTopic).subscribe();
        Assert.assertNotNull(consumer3);
        admin.namespaces().setMaxConsumersPerTopic(myNamespace, 3);
        log.info("MaxConsumers: {} will set to the namespace: {}", 3, myNamespace);

        Consumer<byte[]> consumer4 = null;
        try {
            consumer4 = pulsarClient.newConsumer().subscriptionName("sub4")
                    .topic(persistenceTopic).subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("Topic reached max consumers limit on namespace level.");
        }
        Assert.assertNull(consumer4);

        consumer1.close();
        consumer2.close();
        consumer3.close();
        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testDisableSubscribeRate() throws Exception {
        assertEquals(pulsar.getConfiguration().getSubscribeThrottlingRatePerConsumer(), 0);
        admin.topics().createNonPartitionedTopic(persistenceTopic);
        admin.lookups().lookupTopic(persistenceTopic);
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(persistenceTopic)
                .get().get();
        Field field = PersistentTopic.class.getDeclaredField("subscribeRateLimiter");
        field.setAccessible(true);
        Optional<SubscribeRateLimiter> limiter = (Optional<SubscribeRateLimiter>) field.get(topic);
        // sub rate limiter should be null by default
        assertFalse(limiter.isPresent());

        // Enable / Disable subscribe rate in namespace-level
        final SubscribeRate subscribeRate = new SubscribeRate(1, 100);
        admin.namespaces().setSubscribeRate(myNamespace, subscribeRate);
        Awaitility.await().untilAsserted(()-> {
            Optional<SubscribeRateLimiter> limiter1 = (Optional<SubscribeRateLimiter>) field.get(topic);
            assertTrue(limiter1.isPresent());
        });
        admin.namespaces().removeSubscribeRate(myNamespace);
        Awaitility.await().untilAsserted(()-> {
            Optional<SubscribeRateLimiter> limiter2 = (Optional<SubscribeRateLimiter>) field.get(topic);
            assertFalse(limiter2.isPresent());
        });

        // Enable / Disable subscribe rate in topic-level
        admin.topicPolicies().setSubscribeRate(persistenceTopic, subscribeRate);
        Awaitility.await().untilAsserted(()-> {
            Optional<SubscribeRateLimiter> limiter1 = (Optional<SubscribeRateLimiter>) field.get(topic);
            assertTrue(limiter1.isPresent());
        });
        admin.topicPolicies().removeSubscribeRate(persistenceTopic);
        Awaitility.await().untilAsserted(()-> {
            Optional<SubscribeRateLimiter> limiter2 = (Optional<SubscribeRateLimiter>) field.get(topic);
            assertFalse(limiter2.isPresent());
        });
    }

    @Test
    public void testGetSetSubscribeRate() throws Exception {
        admin.topics().createPartitionedTopic(persistenceTopic, 2);

        SubscribeRate subscribeRate1 = new SubscribeRate(1, 30);
        log.info("Subscribe Rate: {} will be set to the namespace: {}", subscribeRate1, myNamespace);
        admin.namespaces().setSubscribeRate(myNamespace, subscribeRate1);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.namespaces().getSubscribeRate(myNamespace),
                        subscribeRate1));

        SubscribeRate subscribeRate2 =  new SubscribeRate(2, 30);
        log.info("Subscribe Rate: {} will set to the topic: {}", subscribeRate2, persistenceTopic);
        admin.topicPolicies().setSubscribeRate(persistenceTopic, subscribeRate2);
        log.info("Subscribe Rate set success on topic: {}", persistenceTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getSubscribeRate(persistenceTopic), subscribeRate2));

        PulsarClient pulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient3 = newPulsarClient(lookupUrl.toString(), 0);

        Consumer consumer1 = pulsarClient1.newConsumer().subscriptionName("sub1")
                .topic(persistenceTopic).consumerName("test").subscribe();
        Assert.assertNotNull(consumer1);
        consumer1.close();
        pulsarClient1.shutdown();

        Consumer consumer2 = pulsarClient2.newConsumer().subscriptionName("sub1")
                .topic(persistenceTopic).consumerName("test").subscribe();
        Assert.assertNotNull(consumer2);
        consumer2.close();
        pulsarClient2.shutdown();

        Consumer consumer3 = null;

        try {
            consumer3 = pulsarClient3.newConsumer().subscriptionName("sub1")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("subscribe rate reached max subscribe rate limit");
        }

        Assert.assertNull(consumer3);
        pulsarClient3.shutdown();

        admin.topics().deletePartitionedTopic(testTopic, true);
        admin.topics().deletePartitionedTopic(persistenceTopic, true);
    }

    @Test(timeOut = 20000)
    public void testGetSubscribeRateApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getSubscribeRate(topic));
        assertNull(admin.namespaces().getSubscribeRate(myNamespace));
        SubscribeRate brokerPolicy = new SubscribeRate(
                pulsar.getConfiguration().getSubscribeThrottlingRatePerConsumer(),
                pulsar.getConfiguration().getSubscribeRatePeriodPerConsumerInSecond()
        );
        assertEquals(admin.topicPolicies().getSubscribeRate(topic, true), brokerPolicy);
        SubscribeRate namespacePolicy = new SubscribeRate(10, 11);

        admin.namespaces().setSubscribeRate(myNamespace, namespacePolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces()
                .getSubscribeRate(myNamespace)));
        assertEquals(admin.topicPolicies().getSubscribeRate(topic, true), namespacePolicy);

        SubscribeRate topicPolicy = new SubscribeRate(20, 21);
        admin.topicPolicies().setSubscribeRate(topic, topicPolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies().getSubscribeRate(topic)));
        assertEquals(admin.topicPolicies().getSubscribeRate(topic, true), topicPolicy);

        admin.namespaces().removeSubscribeRate(myNamespace);
        admin.topicPolicies().removeSubscribeRate(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces().getSubscribeRate(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies().getSubscribeRate(topic)));
        assertEquals(admin.topicPolicies().getSubscribeRate(topic, true), brokerPolicy);
    }

    @Test(timeOut = 30000)
    public void testPriorityAndDisableMaxConsumersOnSub() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        int maxConsumerInBroker = 1;
        int maxConsumerInNs = 2;
        int maxConsumerInTopic = 4;
        String mySub = "my-sub";
        conf.setMaxConsumersPerSubscription(maxConsumerInBroker);
        pulsarClient.newProducer().topic(topic).create().close();
        List<Consumer<String>> consumerList = new ArrayList<>();
        ConsumerBuilder<String> builder = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionType(SubscriptionType.Shared)
                .topic(topic).subscriptionName(mySub);
        consumerList.add(builder.subscribe());
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException ignored) {
        }

        admin.namespaces().setMaxConsumersPerSubscription(myNamespace, maxConsumerInNs);
        Awaitility.await().untilAsserted(() ->
                assertNotNull(admin.namespaces().getMaxConsumersPerSubscription(myNamespace)));
        consumerList.add(builder.subscribe());
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException ignored) {
        }
        //disabled
        admin.namespaces().setMaxConsumersPerSubscription(myNamespace, 0);
        Awaitility.await().untilAsserted(() ->
                assertEquals(admin.namespaces().getMaxConsumersPerSubscription(myNamespace).intValue(), 0));
        consumerList.add(builder.subscribe());
        //set topic-level
        admin.topicPolicies().setMaxConsumersPerSubscription(topic, maxConsumerInTopic);
        Awaitility.await().untilAsserted(() ->
                assertNotNull(admin.topicPolicies().getMaxConsumersPerSubscription(topic)));
        consumerList.add(builder.subscribe());
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException ignored) {
        }
        //remove topic policies
        admin.topicPolicies().removeMaxConsumersPerSubscription(topic);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin.topicPolicies().getMaxConsumersPerSubscription(topic)));
        consumerList.add(builder.subscribe());
        //remove namespace policies, then use broker-level
        admin.namespaces().removeMaxConsumersPerSubscription(myNamespace);
        Awaitility.await().untilAsserted(() ->
                assertNull(admin.namespaces().getMaxConsumersPerSubscription(myNamespace)));
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException ignored) {
        }

        for (Consumer<String> consumer : consumerList) {
            consumer.close();
        }
    }

    @Test
    public void testRemoveSubscribeRate() throws Exception {
        admin.topics().createPartitionedTopic(persistenceTopic, 2);
        pulsarClient.newProducer().topic(persistenceTopic).create().close();
        SubscribeRate subscribeRate = new SubscribeRate(2, 30);
        log.info("Subscribe Rate: {} will set to the topic: {}", subscribeRate, persistenceTopic);
        admin.topicPolicies().setSubscribeRate(persistenceTopic, subscribeRate);
        log.info("Subscribe Rate set success on topic: {}", persistenceTopic);

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies()
                        .getSubscribeRate(persistenceTopic), subscribeRate));

        PulsarClient pulsarClient1 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient2 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient3 = newPulsarClient(lookupUrl.toString(), 0);

        Consumer<byte[]> consumer1 = pulsarClient1.newConsumer().subscriptionName("sub1")
                .topic(persistenceTopic).consumerName("test").subscribe();
        Assert.assertNotNull(consumer1);
        consumer1.close();
        pulsarClient1.shutdown();

        Consumer<byte[]> consumer2 = pulsarClient2.newConsumer().subscriptionName("sub1")
                .topic(persistenceTopic).consumerName("test").subscribe();
        Assert.assertNotNull(consumer2);
        consumer2.close();
        pulsarClient2.shutdown();

        Consumer<byte[]> consumer3 = null;

        try {
            consumer3 = pulsarClient3.newConsumer().subscriptionName("sub1")
                    .topic(persistenceTopic).consumerName("test").subscribe();
            Assert.fail();
        } catch (PulsarClientException e) {
            log.info("subscribe rate reached max subscribe rate limit");
        }
        Assert.assertNull(consumer3);

        admin.topicPolicies().removeSubscribeRate(persistenceTopic);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies()
                        .getSubscribeRate(persistenceTopic)));

        admin.topics().unload(persistenceTopic);

        PulsarClient pulsarClient4 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient5 = newPulsarClient(lookupUrl.toString(), 0);
        PulsarClient pulsarClient6 = newPulsarClient(lookupUrl.toString(), 0);

        consumer3 = pulsarClient3.newConsumer().subscriptionName("sub2")
                .topic(persistenceTopic).consumerName("test").subscribe();
        Assert.assertNotNull(consumer3);
        consumer3.close();
        pulsarClient3.shutdown();

        Consumer<byte[]> consumer4 = pulsarClient4.newConsumer().subscriptionName("sub2")
                .topic(persistenceTopic).consumerName("test").subscribe();
        Assert.assertNotNull(consumer4);
        consumer4.close();
        pulsarClient4.shutdown();

        Consumer<byte[]> consumer5 = pulsarClient5.newConsumer().subscriptionName("sub2")
                .topic(persistenceTopic).consumerName("test").subscribe();
        Assert.assertNotNull(consumer5);
        consumer5.close();
        pulsarClient5.shutdown();

        Consumer<byte[]> consumer6 = pulsarClient6.newConsumer().subscriptionName("sub2")
                .topic(persistenceTopic).consumerName("test").subscribe();
        Assert.assertNotNull(consumer6);
        consumer6.close();
        pulsarClient6.shutdown();

        admin.topics().deletePartitionedTopic(persistenceTopic, true);
        admin.topics().deletePartitionedTopic(testTopic, true);
    }

    @Test
    public void testPublishRateInDifferentLevelPolicy() throws Exception {
        cleanup();
        conf.setMaxPublishRatePerTopicInMessages(5);
        conf.setMaxPublishRatePerTopicInBytes(50L);
        setup();

        final String topicName = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        pulsarClient.newProducer().topic(topicName).create().close();

        //1 use broker-level policy by default
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicName).get().get();
        PublishRateLimiterImpl publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishRateLimiter.getTokenBucketOnMessage().getRate(), 5);
        Assert.assertEquals(publishRateLimiter.getTokenBucketOnByte().getRate(), 50L);

        //2 set namespace-level policy
        PublishRate publishMsgRate = new PublishRate(10, 100L);
        admin.namespaces().setPublishRate(myNamespace, publishMsgRate);

        Awaitility.await()
                .until(() -> {
                    PublishRateLimiterImpl limiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
                    return (int) limiter.getTokenBucketOnMessage().getRate() == 10;
                });

        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishRateLimiter.getTokenBucketOnMessage().getRate(), 10);
        Assert.assertEquals(publishRateLimiter.getTokenBucketOnByte().getRate(), 100L);

        //3 set topic-level policy, namespace-level policy should be overwritten
        PublishRate publishMsgRate2 = new PublishRate(11, 101L);
        admin.topicPolicies().setPublishRate(topicName, publishMsgRate2);

        Awaitility.await()
                .until(() -> admin.topicPolicies().getPublishRate(topicName) != null);

        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishRateLimiter.getTokenBucketOnMessage().getRate(), 11);
        Assert.assertEquals(publishRateLimiter.getTokenBucketOnByte().getRate(), 101L);

        //4 remove topic-level policy, namespace-level policy will take effect
        admin.topicPolicies().removePublishRate(topicName);

        Awaitility.await()
                .until(() -> admin.topicPolicies().getPublishRate(topicName) == null);

        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishRateLimiter.getTokenBucketOnMessage().getRate(), 10);
        Assert.assertEquals(publishRateLimiter.getTokenBucketOnByte().getRate(), 100L);

        //5 remove namespace-level policy, broker-level policy will take effect
        admin.namespaces().removePublishRate(myNamespace);

        Awaitility.await()
                .until(() -> {
                    PublishRateLimiterImpl limiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
                    return (int) limiter.getTokenBucketOnMessage().getRate() == 5;
                });

        publishRateLimiter = (PublishRateLimiterImpl) topic.getTopicPublishRateLimiter();
        Assert.assertEquals(publishRateLimiter.getTokenBucketOnMessage().getRate(), 5);
        Assert.assertEquals(publishRateLimiter.getTokenBucketOnByte().getRate(), 50L);
    }

    @Test(timeOut = 20000)
    public void testTopicMaxMessageSizeApi() throws Exception{
        admin.topics().createNonPartitionedTopic(persistenceTopic);
        assertNull(admin.topicPolicies().getMaxMessageSize(persistenceTopic));
        admin.topicPolicies().setMaxMessageSize(persistenceTopic, 10);
        Awaitility.await().until(()
                -> TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                TopicName.get(persistenceTopic)) != null);
        assertEquals(admin.topicPolicies().getMaxMessageSize(persistenceTopic).intValue(), 10);

        admin.topicPolicies().removeMaxMessageSize(persistenceTopic);
        assertNull(admin.topicPolicies().getMaxMessageSize(persistenceTopic));

        try {
            admin.topicPolicies().setMaxMessageSize(persistenceTopic, Integer.MAX_VALUE);
            fail("should fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
        }
        try {
            admin.topicPolicies().setMaxMessageSize(persistenceTopic, -1);
            fail("should fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
        }
    }

    @DataProvider(name = "persistentAndPartition")
    public Object[][] implementations() {
        return new Object[][]{
                {TopicDomain.persistent, true},
                {TopicDomain.persistent, false},
                {TopicDomain.non_persistent, true},
                {TopicDomain.non_persistent, false},
        };
    }

    @Test(dataProvider = "persistentAndPartition")
    public void testTopicMaxMessageSize(TopicDomain topicDomain, boolean isPartitioned) throws Exception {
        final String topic = TopicName.get(
                topicDomain.value(),
                NamespaceName.get(myNamespace),
                "test-" + UUID.randomUUID()
        ).toString();

        if (isPartitioned) {
            admin.topics().createPartitionedTopic(topic, 3);
        }
        // init cache
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        assertNull(admin.topicPolicies().getMaxMessageSize(topic));
        // set msg size
        admin.topicPolicies().setMaxMessageSize(topic, 10);
        Awaitility.await().until(() -> TopicPolicyTestUtils.getTopicPolicies(pulsar
                .getTopicPoliciesService(), TopicName.get(topic)) != null);
        if (isPartitioned) {
            for (int i = 0; i < 3; i++) {
                String partitionName = TopicName.get(topic).getPartition(i).toString();
                Awaitility.await().untilAsserted(() -> {
                    AbstractTopic partition =
                            (AbstractTopic) pulsar.getBrokerService().getTopicIfExists(partitionName)
                                    .get().get();
                    assertEquals(partition.getHierarchyTopicPolicies().getTopicMaxMessageSize().get(),
                            Integer.valueOf(10));
                });
            }
        } else {
            Awaitility.await().untilAsserted(() -> {
                AbstractTopic abstractTopic =
                        (AbstractTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
                assertEquals(abstractTopic.getHierarchyTopicPolicies().getTopicMaxMessageSize().get(),
                        Integer.valueOf(10));
            });
        }

        assertEquals(admin.topicPolicies().getMaxMessageSize(topic).intValue(), 10);

        try {
            producer.send(new byte[1024]);
        } catch (PulsarClientException e) {
            assertTrue(e instanceof PulsarClientException.NotAllowedException);
        }

        admin.topicPolicies().removeMaxMessageSize(topic);
        assertNull(admin.topicPolicies().getMaxMessageSize(topic));

        try {
            admin.topicPolicies().setMaxMessageSize(topic, Integer.MAX_VALUE);
            fail("should fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
        }
        try {
            admin.topicPolicies().setMaxMessageSize(topic, -1);
            fail("should fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
        }
        //make sure policy value take effect.
        if (isPartitioned) {
            for (int i = 0; i < 3; i++) {
                String partitionName = TopicName.get(topic).getPartition(i).toString();
                Awaitility.await().untilAsserted(() -> {
                    AbstractTopic partition =
                            (AbstractTopic) pulsar.getBrokerService().getTopicIfExists(partitionName).get().get();
                    assertNull(partition.getHierarchyTopicPolicies().getTopicMaxMessageSize().getTopicValue());
                });
            }
        } else {
            Awaitility.await().untilAsserted(() -> {
                AbstractTopic abstractTopic =
                        (AbstractTopic) pulsar.getBrokerService().getTopicIfExists(topic).get().get();
                assertNull(abstractTopic.getHierarchyTopicPolicies().getTopicMaxMessageSize().getTopicValue());
            });
        }

        Awaitility.await().untilAsserted(() -> {
            try {
                MessageId messageId = producer.send(new byte[1024]);
                assertNotNull(messageId);
            } catch (PulsarClientException e) {
                fail("failed to send message");
            }
        });
        producer.close();
    }

    @Test(timeOut = 20000)
    public void testMaxSubscriptionsFailFast() throws Exception {
        doTestMaxSubscriptionsFailFast(SubscriptionMode.Durable);
        doTestMaxSubscriptionsFailFast(SubscriptionMode.NonDurable);
    }

    private void doTestMaxSubscriptionsFailFast(SubscriptionMode subMode) throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();
        int maxSubInNamespace = 2;
        List<Consumer> consumers = new ArrayList<>();
        ConsumerBuilder consumerBuilder = pulsarClient.newConsumer().subscriptionMode(subMode)
                .subscriptionType(SubscriptionType.Shared).topic(topic);
        admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace, maxSubInNamespace);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.namespaces().getMaxSubscriptionsPerTopic(myNamespace)));
        for (int i = 0; i < maxSubInNamespace; i++) {
            consumers.add(consumerBuilder.subscriptionName("sub" + i).subscribe());
        }
        long start = System.currentTimeMillis();
        try {
            consumerBuilder.subscriptionName("sub").subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e instanceof PulsarClientException.NotAllowedException);
        }
        //fail fast
        assertTrue(System.currentTimeMillis() - start < 3000);
        //clean
        for (Consumer consumer : consumers) {
            consumer.close();
        }
    }

    @Test(timeOut = 20000)
    public void testMaxSubscriptionsPerTopicApi() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();

        assertNull(admin.topicPolicies().getMaxSubscriptionsPerTopic(topic));
        // set max subscriptions
        admin.topicPolicies().setMaxSubscriptionsPerTopic(topic, 10);
        Awaitility.await().until(()
                -> TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                TopicName.get(topic)) != null);
        assertEquals(admin.topicPolicies().getMaxSubscriptionsPerTopic(topic).intValue(), 10);
        // remove max subscriptions
        admin.topicPolicies().removeMaxSubscriptionsPerTopic(topic);
        assertNull(admin.topicPolicies().getMaxSubscriptionsPerTopic(topic));
        // set invalidate value
        try {
            admin.topicPolicies().setMaxMessageSize(topic, -1);
            fail("should fail");
        } catch (PulsarAdminException e) {
            assertEquals(e.getStatusCode(), 412);
        }
    }

    @Test(timeOut = 20000)
    public void testMaxSubscriptionsPerTopicWithExistingSubs() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();
        // Set topic-level max subscriptions
        final int topicLevelMaxSubNum = 2;
        admin.topicPolicies().setMaxSubscriptionsPerTopic(topic, topicLevelMaxSubNum);
        Awaitility.await().until(()
                -> TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                TopicName.get(topic)) != null);
        List<Consumer<String>> consumerList = new ArrayList<>();
        String subName = "my-sub-";
        for (int i = 0; i < topicLevelMaxSubNum; i++) {
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName(subName + i)
                    .topic(topic).subscribe();
            consumerList.add(consumer);
        }
        // should fail
        try (PulsarClient client = PulsarClient.builder().operationTimeout(2, TimeUnit.SECONDS)
                .serviceUrl(brokerUrl.toString()).build()) {
            consumerList.add(client.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe());
            fail("should fail");
        } catch (PulsarClientException ignore) {
            assertEquals(consumerList.size(), topicLevelMaxSubNum);
        }
        //create a consumer with the same subscription name, it should succeed
        pulsarClient.newConsumer(Schema.STRING)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName + "0")
                .topic(topic).subscribe().close();

        //Clean up
        for (Consumer<String> c : consumerList) {
            c.close();
        }
    }

    @Test
    public void testMaxUnackedMessagesOnSubscriptionPriority() throws Exception {
        cleanup();
        conf.setMaxUnackedMessagesPerSubscription(30);
        setup();
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        @Cleanup
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        //default value is null
        assertNull(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace));
        int msgNum = 100;
        int maxUnackedMsgOnTopic = 10;
        int maxUnackedMsgNumOnNamespace = 5;
        int defaultMaxUnackedMsgOnBroker = pulsar.getConfiguration().getMaxUnackedMessagesPerSubscription();
        produceMsg(producer, msgNum);
        //set namespace-level policy, the restriction should take effect
        admin.namespaces().setMaxUnackedMessagesPerSubscription(myNamespace,
                maxUnackedMsgNumOnNamespace);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace)
                .intValue(), maxUnackedMsgNumOnNamespace));
        List<Message<?>> messages;
        String subName = "sub-" + UUID.randomUUID();
        ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer().topic(topic)
                .subscriptionName(subName).receiverQueueSize(1)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionType(SubscriptionType.Shared);
        @Cleanup
        Consumer<byte[]> consumer1 = consumerBuilder.subscribe();
        messages = getMsgReceived(consumer1, msgNum);
        assertEquals(messages.size(), maxUnackedMsgNumOnNamespace);
        ackMessages(consumer1, messages);
        //disable namespace-level policy, should unlimited
        admin.namespaces().setMaxUnackedMessagesPerSubscription(myNamespace, 0);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace).intValue(),
                0));
        messages = getMsgReceived(consumer1, 40);
        assertEquals(messages.size(), 40);
        ackMessages(consumer1, messages);

        //set topic-level and namespace-level policy, topic-level should has higher priority
        admin.namespaces().setMaxUnackedMessagesPerSubscription(myNamespace,
                maxUnackedMsgNumOnNamespace);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace).intValue()
                , maxUnackedMsgNumOnNamespace));
        admin.topicPolicies().setMaxUnackedMessagesOnSubscription(topic, maxUnackedMsgOnTopic);
        Awaitility.await().untilAsserted(()
                -> assertNotNull(admin.topicPolicies().getMaxUnackedMessagesOnSubscription(topic)));
        //check the value applied
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get();
        assertEquals(persistentTopic.getMaxUnackedMessagesOnSubscription(), maxUnackedMsgOnTopic);
        messages = getMsgReceived(consumer1, Integer.MAX_VALUE);
        assertEquals(messages.size(), maxUnackedMsgOnTopic);
        ackMessages(consumer1, messages);

        //remove both namespace-level and topic-level policy, broker-level should take effect
        admin.namespaces().removeMaxUnackedMessagesPerSubscription(myNamespace);
        admin.topicPolicies().removeMaxUnackedMessagesOnSubscription(topic);
        Awaitility.await().until(()
                -> admin.namespaces().getMaxUnackedMessagesPerSubscription(myNamespace) == null
                && admin.topicPolicies().getMaxUnackedMessagesOnSubscription(topic) == null);
        messages = getMsgReceived(consumer1, Integer.MAX_VALUE);
        assertEquals(messages.size(), defaultMaxUnackedMsgOnBroker);
    }

    private void produceMsg(Producer producer, int msgNum) throws Exception{
        for (int i = 0; i < msgNum; i++) {
            producer.send("msg".getBytes());
        }
    }

    private List<Message<?>> getMsgReceived(Consumer<byte[]> consumer1, int msgNum) throws PulsarClientException {
        List<Message<?>> messages = new ArrayList<>();
        for (int i = 0; i < msgNum; i++) {
            Message<?> message = consumer1.receive(1000, TimeUnit.MILLISECONDS);
            if (message == null) {
                break;
            }
            messages.add(message);
        }
        return messages;
    }

    private void ackMessages(Consumer<?> consumer, List<Message<?>> messages) throws Exception{
        for (Message<?> message : messages) {
            consumer.acknowledge(message);
        }
    }

    @Test(timeOut = 20000)
    public void testMaxSubscriptionsPerTopic() throws Exception {
        int brokerLevelMaxSub = 4;
        conf.setMaxSubscriptionsPerTopic(4);
        restartBroker();
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();
        // Set topic-level max subscriptions
        final int topicLevelMaxSubNum = 2;
        admin.topicPolicies().setMaxSubscriptionsPerTopic(topic, topicLevelMaxSubNum);
        Awaitility.await().until(()
                -> TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                TopicName.get(topic)) != null);

        List<Consumer<String>> consumerList = new ArrayList<>();
        for (int i = 0; i < topicLevelMaxSubNum; i++) {
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe();
            consumerList.add(consumer);
        }
        try (PulsarClient client = PulsarClient.builder().operationTimeout(2, TimeUnit.SECONDS)
                .serviceUrl(brokerUrl.toString()).build()) {
            consumerList.add(client.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe());
            fail("should fail");
        } catch (PulsarClientException ignore) {
            assertEquals(consumerList.size(), topicLevelMaxSubNum);
        }
        // Set namespace-level policy, but will not take effect
        final int namespaceLevelMaxSub = 3;
        admin.namespaces().setMaxSubscriptionsPerTopic(myNamespace, namespaceLevelMaxSub);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topic)
                .get().get();
        Awaitility.await().until(() -> Integer.valueOf(namespaceLevelMaxSub)
                .equals(persistentTopic.getHierarchyTopicPolicies().getMaxSubscriptionsPerTopic().getNamespaceValue()));

        try (PulsarClient client = PulsarClient.builder().operationTimeout(1000, TimeUnit.MILLISECONDS)
                .serviceUrl(brokerUrl.toString()).build()) {
            client.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe();
            fail("should fail");
        } catch (PulsarClientException ignore) {
            assertEquals(consumerList.size(), topicLevelMaxSubNum);
        }
        //Removed topic-level policy, namespace-level should take effect
        admin.topicPolicies().removeMaxSubscriptionsPerTopic(topic);
        consumerList.add(pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe());
        assertEquals(consumerList.size(), namespaceLevelMaxSub);
        try (PulsarClient client = PulsarClient.builder().operationTimeout(1000, TimeUnit.MILLISECONDS)
                .serviceUrl(brokerUrl.toString()).build()) {
            consumerList.add(client.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe());
            fail("should fail");
        } catch (PulsarClientException ignore) {
            assertEquals(consumerList.size(), namespaceLevelMaxSub);
        }
        //Removed namespace-level policy, broker-level should take effect
        admin.namespaces().removeMaxSubscriptionsPerTopic(myNamespace);
        Awaitility.await().until(() -> persistentTopic
                .getHierarchyTopicPolicies()
                .getMaxSubscriptionsPerTopic()
                .getNamespaceValue() == null);
        consumerList.add(pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName(UUID.randomUUID().toString()).topic(topic).subscribe());
        assertEquals(consumerList.size(), brokerLevelMaxSub);
        try (PulsarClient client = PulsarClient.builder().operationTimeout(1000, TimeUnit.MILLISECONDS)
                .serviceUrl(brokerUrl.toString()).build()) {
            consumerList.add(client.newConsumer(Schema.STRING)
                    .subscriptionName(UUID.randomUUID().toString())
                    .topic(topic).subscribe());
            fail("should fail");
        } catch (PulsarClientException ignore) {
            assertEquals(consumerList.size(), brokerLevelMaxSub);
        }
        //Clean up
        for (Consumer<String> c : consumerList) {
            c.close();
        }
    }

    @Test(timeOut = 30000)
    public void testReplicatorRateApi() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();

        assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic));

        DispatchRate dispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(100)
                .dispatchThrottlingRateInByte(200)
                .ratePeriodInSecond(10)
                .build();
        admin.topicPolicies().setReplicatorDispatchRate(topic, dispatchRate);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic), dispatchRate));

        admin.topicPolicies().removeReplicatorDispatchRate(topic);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic)));
    }

    @Test(timeOut = 20000)
    public void testGetReplicatorRateApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getReplicatorDispatchRate(topic));
        assertNull(admin.namespaces().getReplicatorDispatchRate(myNamespace));
        DispatchRate brokerDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(pulsar.getConfiguration()
                        .getDispatchThrottlingRatePerReplicatorInMsg())
                .dispatchThrottlingRateInByte(pulsar.getConfiguration()
                        .getDispatchThrottlingRatePerReplicatorInByte())
                .ratePeriodInSecond(1)
                .build();
        assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true),
                brokerDispatchRate);
        DispatchRate namespaceDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(11)
                .ratePeriodInSecond(12)
                .build();

        admin.namespaces().setReplicatorDispatchRate(myNamespace, namespaceDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces()
                .getReplicatorDispatchRate(myNamespace)));
        assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true),
                namespaceDispatchRate);

        DispatchRate topicDispatchRate = DispatchRate.builder()
                .dispatchThrottlingRateInMsg(20)
                .dispatchThrottlingRateInByte(21)
                .ratePeriodInSecond(22)
                .build();
        admin.topicPolicies().setReplicatorDispatchRate(topic, topicDispatchRate);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies()
                .getReplicatorDispatchRate(topic)));
        assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true),
                topicDispatchRate);

        admin.namespaces().removeReplicatorDispatchRate(myNamespace);
        admin.topicPolicies().removeReplicatorDispatchRate(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces()
                .getReplicatorDispatchRate(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies()
                .getReplicatorDispatchRate(topic)));
        assertEquals(admin.topicPolicies().getReplicatorDispatchRate(topic, true),
                brokerDispatchRate);
    }

    @Test(timeOut = 30000)
    public void testAutoCreationDisabled() throws Exception {
        cleanup();
        conf.setAllowAutoTopicCreation(false);
        setup();
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        pulsarClient.newProducer().topic(topic).create().close();
        //should not fail
        assertNull(admin.topicPolicies().getMessageTTL(topic));
    }

    @Test
    public void testSubscriptionTypesWithPartitionedTopic() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 1);
        pulsarClient.newConsumer().topic(topic).subscriptionName("test")
                .subscribe().close();
        Set<SubscriptionType> subscriptionTypeSet = new HashSet<>();
        subscriptionTypeSet.add(SubscriptionType.Key_Shared);
        admin.topicPolicies().setSubscriptionTypesEnabled(topic, subscriptionTypeSet);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies()
                .getSubscriptionTypesEnabled(topic)));

        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(TopicName.get(topic).getPartition(0).toString()).get();
        Set<String> old = new HashSet<>(pulsar.getConfiguration().getSubscriptionTypesEnabled());
        try {
            pulsar.getConfiguration().getSubscriptionTypesEnabled().clear();
            assertTrue(persistentTopic.checkSubscriptionTypesEnable(CommandSubscribe.SubType.Key_Shared));
        } finally {
            //restore
            pulsar.getConfiguration().getSubscriptionTypesEnabled().addAll(old);
        }

        Awaitility.await().untilAsserted(() ->
                assertFalse(admin.topics().getSubscriptionTypesEnabled(topic).isEmpty()));
        admin.topics().removeSubscriptionTypesEnabled(topic);
        Awaitility.await().untilAsserted(() ->
                assertTrue(admin.topics().getSubscriptionTypesEnabled(topic).isEmpty()));
    }

    @Test(timeOut = 30000)
    public void testSubscriptionTypesEnabled() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // use broker.conf
        pulsar.getConfiguration().setSubscriptionTypesEnabled(Set.of("Exclusive"));
        admin.topics().createNonPartitionedTopic(topic);
        try {
            pulsarClient.newConsumer().topic(topic).subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("test").subscribe();
            fail();
        } catch (PulsarClientException pulsarClientException) {
            assertTrue(pulsarClientException instanceof PulsarClientException.NotAllowedException);
        }
        pulsarClient.newConsumer().topic(topic).subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("test")
                .subscribe().close();

        //update broker level dynamic update config
        admin.brokers().updateDynamicConfiguration("subscriptionTypesEnabled", "Shared");
        Awaitility.await().untilAsserted(()->{
            assertTrue(pulsar.getConfiguration().getSubscriptionTypesEnabled().contains("Shared"));
        });
        pulsarClient.newConsumer().topic(topic).subscriptionType(SubscriptionType.Shared)
                .subscriptionName("test").subscribe().close();

        assertNull(admin.topicPolicies().getSubscriptionTypesEnabled(topic));
        // set enable failover sub type
        Set<SubscriptionType> subscriptionTypeSet = new HashSet<>();
        subscriptionTypeSet.add(SubscriptionType.Failover);
        admin.topicPolicies().setSubscriptionTypesEnabled(topic, subscriptionTypeSet);

        Awaitility.await().until(()
                -> TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                TopicName.get(topic)) != null);
        waitTopicPoliciesApplied(topic, 0, hierarchyTopicPolicies -> {
            assertTrue(hierarchyTopicPolicies.getSubscriptionTypesEnabled().get()
                    .contains(CommandSubscribe.SubType.Failover));
        });
        subscriptionTypeSet = admin.topicPolicies().getSubscriptionTypesEnabled(topic);
        assertTrue(subscriptionTypeSet.contains(SubscriptionType.Failover));
        assertFalse(subscriptionTypeSet.contains(SubscriptionType.Shared));
        assertEquals(subscriptionTypeSet.size(), 1);
        try {
            pulsarClient.newConsumer().topic(topic)
                    .subscriptionType(SubscriptionType.Shared).subscriptionName("test")
                    .subscribe();
            fail();
        } catch (PulsarClientException pulsarClientException) {
            assertTrue(pulsarClientException instanceof PulsarClientException.NotAllowedException);
        }

        // add shared type
        subscriptionTypeSet.add(SubscriptionType.Shared);
        admin.topicPolicies().setSubscriptionTypesEnabled(topic, subscriptionTypeSet);
        waitTopicPoliciesApplied(topic, 0, hierarchyTopicPolicies -> {
            assertTrue(hierarchyTopicPolicies.getSubscriptionTypesEnabled().get()
                    .contains(CommandSubscribe.SubType.Shared));
        });
        pulsarClient.newConsumer().topic(topic)
                .subscriptionType(SubscriptionType.Shared).subscriptionName("test")
                .subscribe().close();

        // test namespace and topic policy
        subscriptionTypeSet.add(SubscriptionType.Shared);
        admin.namespaces().setSubscriptionTypesEnabled(myNamespace, subscriptionTypeSet);

        subscriptionTypeSet.clear();
        subscriptionTypeSet.add(SubscriptionType.Failover);
        admin.topicPolicies().setSubscriptionTypesEnabled(topic, subscriptionTypeSet);
        waitTopicPoliciesApplied(topic, 0, hierarchyTopicPolicies -> {
            assertTrue(hierarchyTopicPolicies.getSubscriptionTypesEnabled().getTopicValue()
                    .contains(CommandSubscribe.SubType.Failover));
        });

        try {
            pulsarClient.newConsumer().topic(topic)
                    .subscriptionType(SubscriptionType.Shared).subscriptionName("test")
                    .subscribe();
            fail();
        } catch (PulsarClientException pulsarClientException) {
            assertTrue(pulsarClientException instanceof PulsarClientException.NotAllowedException);
        }

        //clear topic level setting, use ns setting only, which only contains shared.
        admin.topicPolicies().setSubscriptionTypesEnabled(topic, Collections.emptySet());
        waitTopicPoliciesApplied(topic, 0, hierarchyTopicPolicies -> {
            assertNull(hierarchyTopicPolicies.getSubscriptionTypesEnabled().getTopicValue());
        });
        pulsarClient.newConsumer().topic(topic)
                .subscriptionType(SubscriptionType.Shared).subscriptionName("test")
                .subscribe().close();
    }

    @Test(timeOut = 20000)
    public void testNonPersistentMaxConsumerOnSub() throws Exception {
        int maxConsumerPerSubInBroker = 1;
        int maxConsumerPerSubInNs = 2;
        int maxConsumerPerSubInTopic = 3;
        conf.setMaxConsumersPerSubscription(maxConsumerPerSubInBroker);
        final String topic = "non-persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topic, 3);
        Producer producer = pulsarClient.newProducer().topic(topic).create();
        final String subName = "my-sub";
        ConsumerBuilder builder = pulsarClient.newConsumer()
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName).topic(topic);
        Consumer consumer = builder.subscribe();

        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("reached max consumers limit"));
        }
        // set namespace policy
        admin.namespaces().setMaxConsumersPerSubscription(myNamespace, maxConsumerPerSubInNs);
        Awaitility.await().untilAsserted(() -> {
            assertNotNull(admin.namespaces().getMaxConsumersPerSubscription(myNamespace));
            assertEquals(admin.namespaces().getMaxConsumersPerSubscription(myNamespace).intValue(),
                    maxConsumerPerSubInNs);
        });
        Consumer consumer2 = builder.subscribe();
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("reached max consumers limit"));
        }

        //set topic policy
        admin.topicPolicies().setMaxConsumersPerSubscription(topic, maxConsumerPerSubInTopic);
        Awaitility.await().untilAsserted(() -> {
            assertNotNull(admin.topicPolicies().getMaxConsumersPerSubscription(topic));
            assertEquals(admin.topicPolicies().getMaxConsumersPerSubscription(topic).intValue(),
                    maxConsumerPerSubInTopic);
        });
        Consumer consumer3 = builder.subscribe();
        try {
            builder.subscribe();
            fail("should fail");
        } catch (PulsarClientException e) {
            assertTrue(e.getMessage().contains("reached max consumers limit"));
        }
        consumer.close();
        consumer2.close();
        consumer3.close();
        producer.close();
    }

    @Test(timeOut = 20000)
    public void testGetCompactionThresholdApplied() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        assertNull(admin.topicPolicies().getCompactionThreshold(topic));
        assertNull(admin.namespaces().getCompactionThreshold(myNamespace));
        long brokerPolicy = pulsar.getConfiguration().getBrokerServiceCompactionThresholdInBytes();
        assertEquals(admin.topicPolicies().getCompactionThreshold(topic, true).longValue(),
                brokerPolicy);
        long namespacePolicy = 10L;

        admin.namespaces().setCompactionThreshold(myNamespace, namespacePolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.namespaces()
                .getCompactionThreshold(myNamespace)));
        assertEquals(admin.topicPolicies().getCompactionThreshold(topic, true).longValue(),
                namespacePolicy);

        long topicPolicy = 20L;
        admin.topicPolicies().setCompactionThreshold(topic, topicPolicy);
        Awaitility.await().untilAsserted(() -> assertNotNull(admin.topicPolicies()
                .getCompactionThreshold(topic)));
        assertEquals(admin.topicPolicies().getCompactionThreshold(topic, true).longValue(),
                topicPolicy);

        admin.namespaces().removeCompactionThreshold(myNamespace);
        admin.topicPolicies().removeCompactionThreshold(topic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.namespaces()
                .getCompactionThreshold(myNamespace)));
        Awaitility.await().untilAsserted(() -> assertNull(admin.topicPolicies()
                .getCompactionThreshold(topic)));
        assertEquals(admin.topicPolicies().getCompactionThreshold(topic, true).longValue(),
                brokerPolicy);
    }

    @Test(timeOut = 30000)
    public void testProduceConsumeOnTopicPolicy() {
        final String msg = "send message ";
        int numMsg = 10;
        try {
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(persistenceTopic)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionName("test").subscribe();

            Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(persistenceTopic)
                    .create();

            for (int i = 0; i < numMsg; ++i) {
                producer.newMessage().value(msg + i).send();
            }

            for (int i = 0; i < numMsg; ++i) {
                Message<String> message = consumer.receive(100, TimeUnit.MILLISECONDS);
                Assert.assertEquals(message.getValue(), msg + i);
            }
        } catch (PulsarClientException e) {
            log.error("Failed to send/produce message, ", e);
            Assert.fail();
        }
    }

    @Test(timeOut = 30000)
    public void testSystemTopicShouldBeCompacted() throws Exception {
        BacklogQuota backlogQuota = BacklogQuota.builder()
                .limitSize(1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.consumer_backlog_eviction)
                .build();
        log.info("Backlog quota: {} will set to the topic: {}", backlogQuota, testTopic);

        admin.topicPolicies().setBacklogQuota(testTopic, backlogQuota,
                BacklogQuota.BacklogQuotaType.destination_storage);
        log.info("Backlog quota set success on topic: {}", testTopic);

        Awaitility.await()
                .untilAsserted(() -> {
                    TopicStats stats = admin.topics().getStats(topicPolicyEventsTopic);
                    Assert.assertTrue(stats.getSubscriptions().containsKey("__compaction"));
                });

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topicPolicyEventsTopic);
        long previousCompactedLedgerId = internalStats.compactedLedger.ledgerId;

        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getBacklogQuotaMap(testTopic)
                        .get(BacklogQuota.BacklogQuotaType.destination_storage), backlogQuota));

        pulsar.getBrokerService().checkCompaction();

        Awaitility.await()
                .untilAsserted(() -> {
                    PersistentTopicInternalStats iStats = admin.topics().getInternalStats(topicPolicyEventsTopic);
                    Assert.assertTrue(iStats.compactedLedger.ledgerId != previousCompactedLedgerId);
                });
    }

    @Test(timeOut = 30000)
    public void testTopicRetentionPolicySetInManagedLedgerConfig() throws Exception {
        RetentionPolicies nsRetentionPolicies = new RetentionPolicies(1, -1);
        TopicName topicName = TopicName.get(testTopic);

        // set and check retention policy on namespace level
        admin.namespaces().setRetention(myNamespace, nsRetentionPolicies);
        ManagedLedgerConfig managedLedgerConfig = pulsar.getBrokerService().getManagedLedgerConfig(topicName).get();
        Assert.assertEquals(managedLedgerConfig.getRetentionTimeMillis(),
                TimeUnit.MINUTES.toMillis(nsRetentionPolicies.getRetentionTimeInMinutes()));
        Assert.assertEquals(managedLedgerConfig.getRetentionSizeInMB(), nsRetentionPolicies
                .getRetentionSizeInMB());

        // set and check retention policy on topic level
        RetentionPolicies topicRetentionPolicies = new RetentionPolicies(2, -1);
        admin.topicPolicies().setRetention(testTopic, topicRetentionPolicies);
        Awaitility.await().untilAsserted(() -> {
            ManagedLedgerConfig config = pulsar.getBrokerService().getManagedLedgerConfig(topicName).get();
            Assert.assertEquals(config.getRetentionTimeMillis(),
                    TimeUnit.MINUTES.toMillis(topicRetentionPolicies.getRetentionTimeInMinutes()));
            Assert.assertEquals(config.getRetentionSizeInMB(), topicRetentionPolicies.getRetentionSizeInMB());
        });
    }

    @Test
    public void testPolicyIsDeleteTogetherManually() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                                TopicName.get(topic)))
                        .isNull());

        int maxConsumersPerSubscription = 10;
        admin.topicPolicies().setMaxConsumersPerSubscription(topic, maxConsumersPerSubscription);

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getBrokerService().getTopic(topic, false)
                        .get().isPresent()).isTrue());
        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                                TopicName.get(topic)))
                        .isNotNull());

        admin.topics().delete(topic);

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getBrokerService().getTopic(topic, false)
                        .get().isPresent()).isFalse());
        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                                TopicName.get(topic)))
                        .isNull());
    }

    @Test
    public void testPoliciesCanBeDeletedWithTopic() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        final String topic2 = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();
        pulsarClient.newProducer().topic(topic2).create().close();

        Awaitility.await().untilAsserted(() -> {
            Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                    TopicName.get(topic))).isNull();
            Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                    TopicName.get(topic2))).isNull();
        });
        // Init Topic Policies. Send 4 messages in a row, there should be only 2 messages left after compression
        admin.topicPolicies().setMaxConsumersPerSubscription(topic, 1);
        admin.topicPolicies().setMaxConsumersPerSubscription(topic2, 2);
        admin.topicPolicies().setMaxConsumersPerSubscription(topic, 3);
        admin.topicPolicies().setMaxConsumersPerSubscription(topic2, 4);
        Awaitility.await().untilAsserted(() -> {
            Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                    TopicName.get(topic))).isNotNull();
            Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                    TopicName.get(topic2))).isNotNull();
        });
        String topicPoliciesTopic = "persistent://" + myNamespace + "/" + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(topicPoliciesTopic).get().get();
        // Trigger compaction and make sure it is finished.
        persistentTopic.triggerCompaction();
        Field field = PersistentTopic.class.getDeclaredField("currentCompaction");
        field.setAccessible(true);
        CompletableFuture<Long> future = (CompletableFuture<Long>) field.get(persistentTopic);
        Awaitility.await().untilAsserted(() -> assertTrue(future.isDone()));

        Consumer consumer = pulsarClient.newConsumer()
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .readCompacted(true)
                .topic(topicPoliciesTopic).subscriptionName("sub").subscribe();
        int count = 0;
        while (true) {
            Message message = consumer.receive(1, TimeUnit.SECONDS);
            if (message != null) {
                count++;
                consumer.acknowledge(message);
            } else {
                break;
            }
        }
        consumer.close();
        assertEquals(count, 2);

        // Delete topic, there should be only 1 message left after compression
        admin.topics().delete(topic, true);

        Awaitility.await().untilAsserted(() ->
                assertNull(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                        TopicName.get(topic))));
        persistentTopic.triggerCompaction();
        field = PersistentTopic.class.getDeclaredField("currentCompaction");
        field.setAccessible(true);
        CompletableFuture<Long> future2 = (CompletableFuture<Long>) field.get(persistentTopic);
        Awaitility.await().untilAsserted(() -> assertTrue(future2.isDone()));

        consumer = pulsarClient.newConsumer()
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .readCompacted(true)
                .topic(topicPoliciesTopic).subscriptionName("sub").subscribe();
        count = 0;
        while (true) {
            Message message = consumer.receive(1, TimeUnit.SECONDS);
            if (message != null) {
                count++;
                consumer.acknowledge(message);
            } else {
                break;
            }
        }
        consumer.close();
        assertEquals(count, 1);

    }

    @Test
    public void testPolicyIsDeleteTogetherAutomatically() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                                TopicName.get(topic)))
                        .isNull());

        int maxConsumersPerSubscription = 10;
        admin.topicPolicies().setMaxConsumersPerSubscription(topic, maxConsumersPerSubscription);

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getBrokerService().getTopic(topic, false)
                        .get().isPresent()).isTrue());
        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                                TopicName.get(topic)))
                        .isNotNull());

        InactiveTopicPolicies inactiveTopicPolicies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions,
                        3, true);
        admin.topicPolicies().setInactiveTopicPolicies(topic, inactiveTopicPolicies);

        Thread.sleep(4_000L);

        pulsar.getBrokerService().checkGC();

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(pulsar.getBrokerService().getTopic(topic, false)
                        .get().isPresent()).isFalse());
        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                                TopicName.get(topic)))
                        .isNull());
    }

    @Test
    public void testDoNotCreateSystemTopicForHeartbeatNamespace() {
        assertTrue(pulsar.getBrokerService().getTopics().size() > 0);
        pulsar.getBrokerService().getTopics().forEach((k, v) -> {
            TopicName topicName = TopicName.get(k);
            assertNull(NamespaceService.checkHeartbeatNamespace(topicName.getNamespaceObject()));
            assertNull(NamespaceService.checkHeartbeatNamespaceV2(topicName.getNamespaceObject()));
        });
    }

    @Test(timeOut = 30000)
    public void testReplicatorClusterApi() throws Exception {
        final String topic = "persistent://" + myNamespace + "/test-" + UUID.randomUUID();
        // init cache
        pulsarClient.newProducer().topic(topic).create().close();

        assertNull(admin.topics().getReplicationClusters(topic, false));

        List<String> clusters = new ArrayList<>();
        clusters.add("test");
        admin.topics().setReplicationClusters(topic, clusters);
        Awaitility.await().untilAsserted(()
                -> assertEquals(admin.topics().getReplicationClusters(topic, false), clusters));

        admin.topics().removeReplicationClusters(topic);
        Awaitility.await().untilAsserted(()
                -> assertNull(admin.topics().getReplicationClusters(topic, false)));

        assertThrows(PulsarAdminException.PreconditionFailedException.class, () -> admin.topics()
                .setReplicationClusters(topic, List.of()));
        assertThrows(PulsarAdminException.PreconditionFailedException.class, () -> admin.topics()
                .setReplicationClusters(topic, null));

    }

    @Test
    public void testLoopCreateAndDeleteTopicPolicies() throws Exception {
        final String topic = testTopic + UUID.randomUUID();

        int n = 0;
        while (n < 2) {
            n++;
            pulsarClient.newProducer().topic(topic).create().close();
            Awaitility.await().untilAsserted(() -> {
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                        TopicName.get(topic))).isNull();
            });

            admin.topicPolicies().setMaxConsumersPerSubscription(topic, 1);
            Awaitility.await().untilAsserted(() -> {
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                        TopicName.get(topic))).isNotNull();
            });

            admin.topics().delete(topic);
            Awaitility.await().untilAsserted(() -> {
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                        TopicName.get(topic))).isNull();
            });
        }
    }

    private void triggerAndWaitNewTopicCompaction(String topicName) throws Exception {
        PersistentTopic tp =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).join().get();
        // Wait for the old task finish.
        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<Long> compactionTask = WhiteboxImpl.getInternalState(tp, "currentCompaction");
            assertTrue(compactionTask == null || compactionTask.isDone());
        });
        // Trigger a new task.
        tp.triggerCompaction();
        // Wait for the new task finish.
        Awaitility.await().untilAsserted(() -> {
            CompletableFuture<Long> compactionTask = WhiteboxImpl.getInternalState(tp, "currentCompaction");
            assertTrue(compactionTask == null || compactionTask.isDone());
        });
    }

    /***
     * It is not a thread safety method, something will go to a wrong pointer if there is a task is trying to load a
     * topic policies.
     */
    private void clearTopicPoliciesCache() {
        TopicPoliciesService topicPoliciesService = pulsar.getTopicPoliciesService();
        if (topicPoliciesService instanceof TopicPoliciesService.TopicPoliciesServiceDisabled) {
            return;
        }
        assertTrue(topicPoliciesService instanceof SystemTopicBasedTopicPoliciesService);

        Map<NamespaceName, CompletableFuture<Void>> policyCacheInitMap =
                WhiteboxImpl.getInternalState(topicPoliciesService, "policyCacheInitMap");
        for (CompletableFuture<Void> future : policyCacheInitMap.values()) {
            future.join();
        }
        Map<TopicName, TopicPolicies> policiesCache =
                WhiteboxImpl.getInternalState(topicPoliciesService, "policiesCache");
        Map<TopicName, TopicPolicies> globalPoliciesCache =
                WhiteboxImpl.getInternalState(topicPoliciesService, "globalPoliciesCache");

        policyCacheInitMap.clear();
        policiesCache.clear();
        globalPoliciesCache.clear();
    }

    @DataProvider(name = "reloadPolicyTypes")
    public Object[][] reloadPolicyTypes() {
        return new Object[][]{
            {"Clean_Cache"},
            {"Recreate_Service"}
        };
    }

    @Test(dataProvider = "reloadPolicyTypes")
    public void testTopicPoliciesAfterCompaction(String reloadPolicyType) throws Exception {
        final String tpName = BrokerTestUtil.newUniqueName("persistent://" + myNamespace + "/tp");
        final String tpNameChangeEvents = "persistent://" + myNamespace + "/" + NAMESPACE_EVENTS_LOCAL_NAME;
        final String subscriptionName = "s1";
        final int rateMsgLocal = 2000;
        final int rateMsgGlobal = 1000;
        admin.topics().createNonPartitionedTopic(tpName);
        admin.topics().createSubscription(tpName, subscriptionName, MessageId.earliest);

        // Set global policy and local policy.
        // Trigger __change_events compaction.
        // Reload polices into memory.
        // Verify: policies was affected.
        DispatchRate dispatchRateLocal = new DispatchRateImpl(rateMsgLocal, 1, false, 1);
        DispatchRate dispatchRateGlobal = new DispatchRateImpl(rateMsgGlobal, 1, false, 1);
        admin.topicPolicies(false).setDispatchRate(tpName, dispatchRateLocal);
        admin.topicPolicies(true).setDispatchRate(tpName, dispatchRateGlobal);
        triggerAndWaitNewTopicCompaction(tpNameChangeEvents);
        Optional<TopicPolicies> topicPoliciesOptional1 = null;
        Optional<TopicPolicies> topicPoliciesOptionalGlobal1 = null;
        if ("Clean_Cache".equals(reloadPolicyType)) {
            clearTopicPoliciesCache();
            topicPoliciesOptional1 = pulsar.getTopicPoliciesService().getTopicPoliciesAsync(TopicName.get(tpName),
                            LOCAL_ONLY).join();
            topicPoliciesOptionalGlobal1 = pulsar.getTopicPoliciesService().getTopicPoliciesAsync(TopicName.get(tpName),
                    GLOBAL_ONLY).join();
        } else {
            SystemTopicBasedTopicPoliciesService newService = new SystemTopicBasedTopicPoliciesService(pulsar);
            topicPoliciesOptional1 = newService.getTopicPoliciesAsync(TopicName.get(tpName), LOCAL_ONLY).join();
            topicPoliciesOptionalGlobal1 = newService.getTopicPoliciesAsync(TopicName.get(tpName), GLOBAL_ONLY).join();
            newService.close();
        }
        assertTrue(topicPoliciesOptional1.isPresent());
        assertEquals(topicPoliciesOptional1.get().getDispatchRate().getDispatchThrottlingRateInMsg(), rateMsgLocal);
        assertEquals(topicPoliciesOptionalGlobal1.get().getDispatchRate().getDispatchThrottlingRateInMsg(),
                rateMsgGlobal);

        // Remove local policy.
        // Trigger __change_events compaction.
        // Reload polices into memory.
        // Verify: policies was affected.
        admin.topicPolicies(false).removeDispatchRate(tpName);
        triggerAndWaitNewTopicCompaction(tpNameChangeEvents);
        Optional<TopicPolicies> topicPoliciesOptional2 = null;
        Optional<TopicPolicies> topicPoliciesOptionalGlobal2 = null;
        if ("Clean_Cache".equals(reloadPolicyType)) {
            clearTopicPoliciesCache();
            topicPoliciesOptional2 = pulsar.getTopicPoliciesService().getTopicPoliciesAsync(TopicName.get(tpName),
                    LOCAL_ONLY).join();
            topicPoliciesOptionalGlobal2 = pulsar.getTopicPoliciesService().getTopicPoliciesAsync(TopicName.get(tpName),
                    GLOBAL_ONLY).join();
        } else {
            SystemTopicBasedTopicPoliciesService newService = new SystemTopicBasedTopicPoliciesService(pulsar);
            topicPoliciesOptional2 = newService.getTopicPoliciesAsync(TopicName.get(tpName), LOCAL_ONLY).join();
            topicPoliciesOptionalGlobal2 = newService.getTopicPoliciesAsync(TopicName.get(tpName), GLOBAL_ONLY).join();
            newService.close();
        }
        assertTrue(topicPoliciesOptional2.isEmpty() || topicPoliciesOptional2.get().getDispatchRate() == null);
        assertTrue(topicPoliciesOptionalGlobal2.isPresent());
        assertEquals(topicPoliciesOptionalGlobal2.get().getDispatchRate().getDispatchThrottlingRateInMsg(),
                rateMsgGlobal);

        // Delete topic.
        // Trigger __change_events compaction.
        // Reload polices into memory.
        // Verify: policies was deleted.
        admin.topics().delete(tpName, false);
        Awaitility.await().untilAsserted(() -> {
            // Reload polices into memory.
            // Verify: policies was affected.
            Optional<TopicPolicies> topicPoliciesOptional3 = null;
            Optional<TopicPolicies> topicPoliciesOptionalGlobal3 = null;
            if ("Clean_Cache".equals(reloadPolicyType)) {
                clearTopicPoliciesCache();
                topicPoliciesOptional3 = pulsar.getTopicPoliciesService().getTopicPoliciesAsync(TopicName.get(tpName),
                        LOCAL_ONLY).join();
                topicPoliciesOptionalGlobal3 = pulsar.getTopicPoliciesService()
                        .getTopicPoliciesAsync(TopicName.get(tpName), GLOBAL_ONLY).join();
            } else {
                SystemTopicBasedTopicPoliciesService newService = new SystemTopicBasedTopicPoliciesService(pulsar);
                topicPoliciesOptional3 = newService.getTopicPoliciesAsync(TopicName.get(tpName), LOCAL_ONLY).join();
                topicPoliciesOptionalGlobal3 = newService.getTopicPoliciesAsync(TopicName.get(tpName), GLOBAL_ONLY)
                        .join();
                newService.close();
            }
            assertTrue(topicPoliciesOptional3.isEmpty()
                    || topicPoliciesOptional3.get().getDispatchRate() == null);
            assertTrue(topicPoliciesOptionalGlobal3.isEmpty()
                    || topicPoliciesOptionalGlobal3.get().getDispatchRate() == null);
        });
    }

    @Test
    public void testDeleteGlobalPolicy() throws Exception {
        final String tpName = BrokerTestUtil.newUniqueName("persistent://" + myNamespace + "/tp");
        final String tpNameChangeEvents = "persistent://" + myNamespace + "/" + NAMESPACE_EVENTS_LOCAL_NAME;
        final String subscriptionName = "s1";
        final int rateMsgGlobal = 1000;
        admin.topics().createNonPartitionedTopic(tpName);
        admin.topics().createSubscription(tpName, subscriptionName, MessageId.earliest);
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(tpName).get().get();

        // Set global policy.
        // Verify: policies was affected.
        DispatchRate dispatchRateGlobal = new DispatchRateImpl(rateMsgGlobal, 1, false, 1);
        admin.topicPolicies(true).setDispatchRate(tpName, dispatchRateGlobal);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(persistentTopic.getHierarchyTopicPolicies().getDispatchRate().get(), dispatchRateGlobal);
        });

        // Delete global policy.
        // Verify: policies were deleted.
        triggerAndWaitNewTopicCompaction(tpNameChangeEvents);
        admin.topicPolicies(true).removeDispatchRate(tpName);

        Awaitility.await().untilAsserted(() -> {
            Optional<TopicPolicies> topicPoliciesOptional = pulsar.getTopicPoliciesService()
                    .getTopicPoliciesAsync(TopicName.get(tpName), LOCAL_ONLY).join();
            Optional<TopicPolicies> topicPoliciesOptionalGlobal = pulsar.getTopicPoliciesService()
                    .getTopicPoliciesAsync(TopicName.get(tpName), GLOBAL_ONLY).join();
            assertTrue(topicPoliciesOptional.isEmpty()
                    || topicPoliciesOptional.get().getDispatchRate() == null);
            assertTrue(topicPoliciesOptionalGlobal.isEmpty()
                    || topicPoliciesOptionalGlobal.get().getDispatchRate() == null);
        });

        // cleanup.
        admin.topics().delete(tpName, false);
    }

    @DataProvider
    public Object[][] partitionedTypes() {
        return new Object[][]{
            {TopicType.NON_PARTITIONED},
            {TopicType.PARTITIONED}
        };
    }

    @Test(dataProvider = "partitionedTypes")
    public void testCleanupPoliciesAfterDeletedTopic(TopicType topicType) throws Exception {
        final String tpName = BrokerTestUtil.newUniqueName("persistent://" + myNamespace + "/tp");
        final TopicName tpNameP0 = TopicName.get(tpName).getPartition(0);
        final String subscriptionName = "s1";
        final int rateMsgGlobal = 1000;
        final int rateMsgLocal = 1000;
        if (TopicType.PARTITIONED.equals(topicType)) {
            admin.topics().createPartitionedTopic(tpName, 2);
        } else {
            admin.topics().createNonPartitionedTopic(tpName);
        }

        admin.topics().createSubscription(tpName, subscriptionName, MessageId.earliest);
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
            .getTopicIfExists(TopicType.PARTITIONED.equals(topicType) ? tpNameP0.toString() : tpName).get().get();

        // Set global policy.
        // Verify: policies was affected.
        DispatchRate dispatchRateGlobal = new DispatchRateImpl(rateMsgGlobal, 1, false, 1);
        admin.topicPolicies(true).setDispatchRate(tpName, dispatchRateGlobal);
        DispatchRate dispatchRateLocal = new DispatchRateImpl(rateMsgLocal, 2, false, 2);
        admin.topicPolicies(true).setDispatchRate(tpName, dispatchRateLocal);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(persistentTopic.getHierarchyTopicPolicies().getDispatchRate().get(), dispatchRateLocal);
        });

        // cleanup.
        if (TopicType.PARTITIONED.equals(topicType)) {
            admin.topics().deletePartitionedTopic(tpName, false);
        } else {
            admin.topics().delete(tpName, false);
        }

        // Verify: the topic-level policies will be removed after the topic is deleted.
        Awaitility.await().untilAsserted(() -> {
            Optional<TopicPolicies> topicPoliciesOptional = pulsar.getTopicPoliciesService()
                    .getTopicPoliciesAsync(TopicName.get(tpName), LOCAL_ONLY).join();
            Optional<TopicPolicies> topicPoliciesOptionalGlobal = pulsar.getTopicPoliciesService()
                    .getTopicPoliciesAsync(TopicName.get(tpName), GLOBAL_ONLY).join();
            assertTrue(topicPoliciesOptional.isEmpty());
            assertTrue(topicPoliciesOptionalGlobal.isEmpty());
        });
    }

    @Test
    public void testGlobalTopicPolicies() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        pulsarClient.newProducer().topic(topic).create().close();

        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                                TopicName.get(topic)))
                        .isNull());
        admin.topicPolicies(true).setRetention(topic, new RetentionPolicies(1,
                2));
        SystemTopicBasedTopicPoliciesService topicPoliciesService =
                (SystemTopicBasedTopicPoliciesService) pulsar.getTopicPoliciesService();

        // check global topic policies can be added correctly.
        Awaitility.await().untilAsserted(() -> assertNotNull(
                TopicPolicyTestUtils.getGlobalTopicPolicies(topicPoliciesService, TopicName.get(topic))));
        TopicPolicies topicPolicies = TopicPolicyTestUtils.getGlobalTopicPolicies(topicPoliciesService,
                TopicName.get(topic));
        assertNull(TopicPolicyTestUtils.getLocalTopicPolicies(topicPoliciesService, TopicName.get(topic)));
        assertEquals(topicPolicies.getRetentionPolicies().getRetentionTimeInMinutes(), 1);
        assertEquals(topicPolicies.getRetentionPolicies().getRetentionSizeInMB(), 2);

        // check global topic policies can be updated correctly.
        admin.topicPolicies(true).setRetention(topic, new RetentionPolicies(3, 4));
        Awaitility.await().untilAsserted(() -> {
            TopicPolicies tempPolicies = TopicPolicyTestUtils.getGlobalTopicPolicies(topicPoliciesService,
                    TopicName.get(topic));
            assertNull(TopicPolicyTestUtils.getLocalTopicPolicies(topicPoliciesService,
                    (TopicName.get(topic))));
            assertEquals(tempPolicies.getRetentionPolicies().getRetentionTimeInMinutes(), 3);
            assertEquals(tempPolicies.getRetentionPolicies().getRetentionSizeInMB(), 4);
        });

        //Local topic policies and global topic policies can exist together.
        admin.topicPolicies().setRetention(topic, new RetentionPolicies(10, 20));
        Awaitility.await().untilAsserted(() -> assertNotNull(TopicPolicyTestUtils
                .getTopicPolicies(topicPoliciesService, (TopicName.get(topic)))));
        TopicPolicies tempPolicies = TopicPolicyTestUtils.getGlobalTopicPolicies(topicPoliciesService,
                TopicName.get(topic));
        assertEquals(tempPolicies.getRetentionPolicies().getRetentionTimeInMinutes(), 3);
        assertEquals(tempPolicies.getRetentionPolicies().getRetentionSizeInMB(), 4);
        tempPolicies = TopicPolicyTestUtils.getTopicPolicies(topicPoliciesService, (TopicName.get(topic)));
        assertEquals(tempPolicies.getRetentionPolicies().getRetentionTimeInMinutes(), 10);
        assertEquals(tempPolicies.getRetentionPolicies().getRetentionSizeInMB(), 20);

        // check remove global topic policies can be removed correctly.
        admin.topicPolicies(true).removeRetention(topic);
        Awaitility.await().untilAsserted(() -> assertNull(TopicPolicyTestUtils
                .getGlobalTopicPolicies(topicPoliciesService,
                TopicName.get(topic)).getRetentionPolicies()));

    }

    @Test
    public void testMaxMessageSizeWithChunking() throws Exception {
        this.conf.setMaxMessageSize(1000);

        @Cleanup
        PulsarClient pulsarClient = newPulsarClient(lookupUrl.toString(), 0);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(persistenceTopic)
                .enableChunking(true)
                .enableBatching(false)
                .create();

        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopic(persistenceTopic, false)
                .join().get();

        // send success when topic level maxMessage is not set.
        producer.send(new byte[2000]);

        admin.topicPolicies().setMaxMessageSize(persistenceTopic, 500);
        Awaitility.await().untilAsserted(() -> {
            Assert.assertEquals((int) topic.getHierarchyTopicPolicies().getTopicMaxMessageSize().get(), 500);
        });

        // non-chunk message send success
        producer.send(new byte[400]);

        // chunk message send success
        producer.send(new byte[2000]);
    }

    @Test(timeOut = 30000)
    public void testShadowTopics() throws Exception {
        final String sourceTopic = "persistent://" + myNamespace + "/source-test-" + UUID.randomUUID();
        final String shadowTopic1 = "persistent://" + myNamespace + "/shadow-test1-" + UUID.randomUUID();
        final String shadowTopic2 = "persistent://" + myNamespace + "/shadow-test2-" + UUID.randomUUID();

        pulsarClient.newProducer().topic(sourceTopic).create().close();

        Awaitility.await().untilAsserted(() ->
                Assert.assertNull(TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(),
                        TopicName.get(sourceTopic))));

        //shadow topic must exist
        Assert.expectThrows(PulsarAdminException.PreconditionFailedException.class, ()->
                admin.topics().setShadowTopics(sourceTopic, List.of(shadowTopic1)));

        //shadow topic must be persistent topic
        Assert.expectThrows(PulsarAdminException.PreconditionFailedException.class, ()->
                admin.topics().setShadowTopics(sourceTopic,
                        List.of("non-persistent://" + myNamespace + "/shadow-test1-" + UUID.randomUUID())));

        pulsarClient.newProducer().topic(shadowTopic1).create().close();
        pulsarClient.newProducer().topic(shadowTopic2).create().close();

        admin.topics().setShadowTopics(sourceTopic, List.of(shadowTopic1));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(admin.topics().getShadowTopics(sourceTopic),
                List.of(shadowTopic1)));
        admin.topics().setShadowTopics(sourceTopic, List.of(shadowTopic1, shadowTopic2));
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(admin.topics().getShadowTopics(sourceTopic),
                List.of(shadowTopic1, shadowTopic2)));

        admin.topics().removeShadowTopics(sourceTopic);
        Awaitility.await().untilAsserted(() -> assertNull(admin.topics().getShadowTopics(sourceTopic)));
    }

    @Test
    public void testGetTopicPoliciesWhenDeleteTopicPolicy() throws Exception {
        admin.topics().createNonPartitionedTopic(persistenceTopic);
        admin.topicPolicies().setMaxConsumers(persistenceTopic, 5);

        Integer maxConsumerPerTopic = TopicPolicyTestUtils.getTopicPoliciesBypassCache(pulsar.getTopicPoliciesService(),
                TopicName.get(persistenceTopic)).orElseThrow().getMaxConsumerPerTopic();

        assertEquals(maxConsumerPerTopic, 5);
        admin.topics().delete(persistenceTopic, true);
        assertTrue(TopicPolicyTestUtils.getTopicPoliciesBypassCache(pulsar.getTopicPoliciesService(),
                TopicName.get(persistenceTopic)).isEmpty());
    }

    @Test
    public void testProduceChangesWithEncryptionRequired() throws Exception {
        final String beforeLac = admin.topics().getInternalStats(topicPolicyEventsTopic).lastConfirmedEntry;
        admin.namespaces().setEncryptionRequiredStatus(myNamespace, true);
        // just an update to trigger writes on __change_events
        admin.topicPolicies().setMaxConsumers(testTopic, 5);
        Awaitility.await()
                .untilAsserted(() -> {
                    final PersistentTopicInternalStats newLac = admin.topics().getInternalStats(topicPolicyEventsTopic);
                    assertNotEquals(newLac, beforeLac);
                });
    }

    @Test
    public void testDelayedDeliveryPolicy() throws Exception {
        final String topic = testTopic + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);

        boolean isActive = true;
        long tickTime = 1000;
        long maxDeliveryDelayInMillis = 5000;
        DelayedDeliveryPolicies policy = DelayedDeliveryPolicies.builder()
                .active(isActive)
                .tickTime(tickTime)
                .maxDeliveryDelayInMillis(maxDeliveryDelayInMillis)
                .build();

        admin.topicPolicies().setDelayedDeliveryPolicy(topic, policy);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertEquals(admin.topicPolicies().getDelayedDeliveryPolicy(topic),
                        policy));

        admin.topicPolicies().removeDelayedDeliveryPolicy(topic);
        Awaitility.await()
                .untilAsserted(() -> Assert.assertNull(admin.topicPolicies().getDelayedDeliveryPolicy(topic)));

        admin.topics().delete(topic, true);
    }

    @Test
    public void testUpdateRetentionWithPartialFailure() throws Exception {
        String tpName = BrokerTestUtil.newUniqueName("persistent://" + myNamespace + "/tp");
        admin.topics().createNonPartitionedTopic(tpName);

        // Load topic up.
        admin.topics().getInternalStats(tpName);

        // Inject an error that makes dispatch rate update fail.
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        final var subscriptions = persistentTopic.getSubscriptions();
        PersistentSubscription mockedSubscription = Mockito.mock(PersistentSubscription.class);
        Mockito.when(mockedSubscription.getDispatcher()).thenThrow(
                new RuntimeException("Mocked error: getDispatcher"));
        subscriptions.put("mockedSubscription", mockedSubscription);

        // Update namespace-level retention policies.
        RetentionPolicies retentionPolicies1 = new RetentionPolicies(1, 1);
        admin.namespaces().setRetentionAsync(myNamespace, retentionPolicies1);

        // Verify: update retention will be success even if other component update throws exception.
        Awaitility.await().untilAsserted(() -> {
            ManagedLedgerImpl mL = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            assertEquals(mL.getConfig().getRetentionSizeInMB(), 1);
            assertEquals(mL.getConfig().getRetentionTimeMillis(), 1 * 60 * 1000);
        });

        // Update topic-level retention policies.
        RetentionPolicies retentionPolicies2 = new RetentionPolicies(2, 2);
        admin.topics().setRetentionAsync(tpName, retentionPolicies2);

        // Verify: update retention will be success even if other component update throws exception.
        Awaitility.await().untilAsserted(() -> {
            ManagedLedgerImpl mL = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            assertEquals(mL.getConfig().getRetentionSizeInMB(), 2);
            assertEquals(mL.getConfig().getRetentionTimeMillis(), 2 * 60 * 1000);
        });

        // Cleanup.
        subscriptions.clear();
        admin.namespaces().removeRetention(myNamespace);
        admin.topics().delete(tpName, false);
    }

    @Test
    public void testTopicPoliciesGetSubscriptionPolicies() throws Exception {
        TopicPolicies topicPolicies = TopicPolicies.builder()
                .maxProducerPerTopic(10).subscriptionPolicies(null).build();
        Assert.assertNotNull(topicPolicies.getSubscriptionPolicies());
        Assert.assertEquals(topicPolicies.getMaxProducerPerTopic(), 10);
        Assert.assertTrue(topicPolicies.getSubscriptionPolicies().isEmpty());
        topicPolicies.getSubscriptionPolicies().computeIfAbsent("sub", k ->
                new SubscriptionPolicies()).setDispatchRate(new DispatchRateImpl());
        Assert.assertEquals(topicPolicies.getSubscriptionPolicies().get("sub").getDispatchRate()
                .getDispatchThrottlingRateInByte(), 0);
    }

    @Test
    public void testSetSubRateWithNoSub() throws Exception {
        String topic = "persistent://" + myNamespace + "/testSetSubRateWithNoSub";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topicPolicies().setSubscriptionDispatchRate(topic, DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(10)
                .ratePeriodInSecond(10)
                .build());
    }

    /**
     * Verify: {@link BacklogQuota#getPolicy()} can not be null.
     */
    @Test
    public void testSetNonBacklogQuotType() throws Exception {
        final NamespaceName ns = NamespaceName.get(myNamespace);
        final String hostAndPort = pulsar.getWebServiceAddress();
        final String nsPath = "/admin/v2/namespaces/" + ns + "/backlogQuota";
        final String topicPath = "/admin/v2/persistent/" + ns + "/test-set-backlog-quota/backlogQuota";
        admin.namespaces().setBacklogQuota(ns.toString(),
                BacklogQuota.builder().limitTime(1).limitSize(1).retentionPolicy(
                BacklogQuota.RetentionPolicy.consumer_backlog_eviction).build());
        BacklogQuota backlogQuotaWithNonPolicy = BacklogQuota.builder().limitTime(1).limitSize(1).build();
        JerseyClient httpClient = JerseyClientBuilder.createClient();
        // Namespace level.
        Response response1 = httpClient.target(hostAndPort).path(nsPath).request()
                .header("Content-Type", "application/json")
                .post(Entity.json(backlogQuotaWithNonPolicy));
        assertEquals(response1.getStatus(), 400);
        assertTrue(response1.getStatusInfo().getReasonPhrase().contains("policy cannot be null"));
        // Topic level.
        Response response2 = httpClient.target(hostAndPort).path(topicPath).request()
                .header("Content-Type", "application/json")
                .post(Entity.json(backlogQuotaWithNonPolicy));
        assertEquals(response2.getStatus(), 400);
        assertTrue(response2.getStatusInfo().getReasonPhrase().contains("policy cannot be null"));
        // cleanup.
        httpClient.close();

    }

    @Test
    public void testSetSubRateWithSub() throws Exception {
        String topic = "persistent://" + myNamespace + "/testSetSubRateWithSub";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().createSubscription(topic, "sub1", MessageId.earliest);
        admin.topicPolicies().setSubscriptionDispatchRate(topic, "sub1", DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(10)
                .ratePeriodInSecond(10)
                .build());
    }

    @DataProvider
    public Object[][] topicTypes() {
        return new Object[][]{
            {TopicType.PARTITIONED},
            {TopicType.NON_PARTITIONED}
        };
    }

    @Test(dataProvider = "topicTypes")
    public void testRemoveLocalCluster(TopicType topicType) throws Exception {
        String topic = "persistent://" + myNamespace + "/testSetSubRateWithSub";
        if (TopicType.PARTITIONED.equals(topicType)) {
            admin.topics().createNonPartitionedTopic(topic);
        } else {
            admin.topics().createPartitionedTopic(topic, 2);
        }
        try {
            admin.topics().setReplicationClusters(topic, Arrays.asList("not-local-cluster"));
            fail("Can not remove local cluster from the topic-level replication clusters policy");
        } catch (PulsarAdminException.PreconditionFailedException e) {
            assertTrue(e.getMessage().contains("Can not remove local cluster from the topic-level replication clusters"
                + " policy"));
        }
        // cleanup.
        if (TopicType.PARTITIONED.equals(topicType)) {
            admin.topics().delete(topic, false);
        } else {
            admin.topics().deletePartitionedTopic(topic, false);
        }
    }

    /**
     * Test topic level persistence policies and verify that the internal managed ledger
     * has applied the correct managedLedgerMaxMarkDeleteRate.
     *
     * This test verifies:
     * 1. Setting topic persistence policies with managedLedgerMaxMarkDeleteRate
     * 2. Creating a topic and verifying the managed ledger config is applied correctly
     * 3. Verifying that cursors have the correct throttle mark delete rate
     * 4. Testing policy updates and verifying they are applied to existing topics
     * 5. Testing policy removal and fallback to broker defaults
     * 6. Testing managedLedgerMaxMarkDeleteRate=-1 which should fall back to broker defaults
     *
     * @throws Exception
     */
    @Test
    public void testTopicPersistencePoliciesWithManagedLedgerMaxMarkDeleteRate() throws Exception {
        final String topicName = "persistent://" + myNamespace + "/test-topic-persistence-policies";

        // Create topic
        admin.topics().createNonPartitionedTopic(topicName);

        // Test 1: Set initial topic persistence policies with managedLedgerMaxMarkDeleteRate
        final double initialMarkDeleteRate = 25;
        final int initialEnsembleSize = 4;
        final int initialWriteQuorum = 3;
        final int initialAckQuorum = 2;

        PersistencePolicies initialPolicies = new PersistencePolicies(
            initialEnsembleSize, initialWriteQuorum, initialAckQuorum, initialMarkDeleteRate);

        admin.topicPolicies().setPersistence(topicName, initialPolicies);

        // Verify the policies are set correctly
        Awaitility.await().untilAsserted(() -> {
            PersistencePolicies retrievedPolicies = admin.topicPolicies().getPersistence(topicName);
            assertEquals(retrievedPolicies, initialPolicies);
            assertEquals(retrievedPolicies.getManagedLedgerMaxMarkDeleteRate(), initialMarkDeleteRate);
        });

        // Test 2: Create producer and consumer to load the topic and verify managed ledger config
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(topicName)
            .subscriptionName("test-sub")
            .subscribe();

        // Get the topic and managed ledger
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getOrCreateTopic(topicName).get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) topic.getManagedLedger();

        // Verify managed ledger config has the correct settings
        assertEquals(managedLedger.getConfig().getEnsembleSize(), initialEnsembleSize);
        assertEquals(managedLedger.getConfig().getWriteQuorumSize(), initialWriteQuorum);
        assertEquals(managedLedger.getConfig().getAckQuorumSize(), initialAckQuorum);
        assertEquals(managedLedger.getConfig().getThrottleMarkDelete(), initialMarkDeleteRate, 0.0001);

        // Test 3: Verify cursor has the correct throttle mark delete rate
        ManagedCursorImpl cursor = (ManagedCursorImpl) managedLedger.getCursors().iterator().next();
        assertEquals(cursor.getThrottleMarkDelete(), initialMarkDeleteRate, 0.0001);

        // Test 4: Update topic persistence policies and verify they are applied
        final double updatedMarkDeleteRate = 75;
        final int updatedEnsembleSize = 3;
        final int updatedWriteQuorum = 2;
        final int updatedAckQuorum = 1;

        PersistencePolicies updatedPolicies = new PersistencePolicies(
            updatedEnsembleSize, updatedWriteQuorum, updatedAckQuorum, updatedMarkDeleteRate);

        admin.topicPolicies().setPersistence(topicName, updatedPolicies);

        // Verify the policies are updated correctly
        Awaitility.await().untilAsserted(() -> {
            PersistencePolicies retrievedPolicies = admin.topicPolicies().getPersistence(topicName);
            assertEquals(retrievedPolicies, updatedPolicies);
            assertEquals(retrievedPolicies.getManagedLedgerMaxMarkDeleteRate(), updatedMarkDeleteRate);
        });

        // Wait for the managed ledger config to be updated
        Awaitility.await().untilAsserted(() -> {
            assertEquals(managedLedger.getConfig().getEnsembleSize(), updatedEnsembleSize);
            assertEquals(managedLedger.getConfig().getWriteQuorumSize(), updatedWriteQuorum);
            assertEquals(managedLedger.getConfig().getAckQuorumSize(), updatedAckQuorum);
            assertEquals(managedLedger.getConfig().getThrottleMarkDelete(), updatedMarkDeleteRate, 0.0001);
            assertEquals(cursor.getThrottleMarkDelete(), updatedMarkDeleteRate, 0.0001);
        });

        // Test 5: Test managedLedgerMaxMarkDeleteRate=-1 which should fall back to namespace/broker defaults
        final double fallbackMarkDeleteRate = -1.0;
        final int fallbackEnsembleSize = 5;
        final int fallbackWriteQuorum = 3;
        final int fallbackAckQuorum = 2;

        PersistencePolicies fallbackPolicies = new PersistencePolicies(
            fallbackEnsembleSize, fallbackWriteQuorum, fallbackAckQuorum, fallbackMarkDeleteRate);

        admin.topicPolicies().setPersistence(topicName, fallbackPolicies);

        // Verify the policies are set correctly
        Awaitility.await().untilAsserted(() -> {
            PersistencePolicies retrievedPolicies = admin.topicPolicies().getPersistence(topicName);
            assertEquals(retrievedPolicies, fallbackPolicies);
            assertEquals(retrievedPolicies.getManagedLedgerMaxMarkDeleteRate(), fallbackMarkDeleteRate);
        });

        // Wait for the managed ledger config to be updated with fallback values
        Awaitility.await().untilAsserted(() -> {
            assertEquals(managedLedger.getConfig().getEnsembleSize(), fallbackEnsembleSize);
            assertEquals(managedLedger.getConfig().getWriteQuorumSize(), fallbackWriteQuorum);
            assertEquals(managedLedger.getConfig().getAckQuorumSize(), fallbackAckQuorum);
            // Should fall back to broker default for mark delete rate
            assertEquals(managedLedger.getConfig().getThrottleMarkDelete(),
                pulsar.getConfiguration().getManagedLedgerDefaultMarkDeleteRateLimit(), 0.0001);
            assertEquals(cursor.getThrottleMarkDelete(),
                pulsar.getConfiguration().getManagedLedgerDefaultMarkDeleteRateLimit(), 0.0001);
        });

        // Test 6: Set namespace level persistence policies and verify topic level takes precedence
        final double namespaceMarkDeleteRate = 55;
        final int namespaceEnsembleSize = 3;
        final int namespaceWriteQuorum = 2;
        final int namespaceAckQuorum = 1;

        PersistencePolicies namespacePolicies = new PersistencePolicies(
            namespaceEnsembleSize, namespaceWriteQuorum, namespaceAckQuorum, namespaceMarkDeleteRate);

        admin.namespaces().setPersistence(myNamespace, namespacePolicies);

        // Set topic level policies that should override namespace level
        final double topicMarkDeleteRate = 95;
        final int topicEnsembleSize = 3;
        final int topicWriteQuorum = 2;
        final int topicAckQuorum = 1;

        PersistencePolicies topicPolicies = new PersistencePolicies(
            topicEnsembleSize, topicWriteQuorum, topicAckQuorum, topicMarkDeleteRate);

        admin.topicPolicies().setPersistence(topicName, topicPolicies);

        // Verify topic level policies take precedence over namespace level
        Awaitility.await().untilAsserted(() -> {
            assertEquals(managedLedger.getConfig().getEnsembleSize(), topicEnsembleSize);
            assertEquals(managedLedger.getConfig().getWriteQuorumSize(), topicWriteQuorum);
            assertEquals(managedLedger.getConfig().getAckQuorumSize(), topicAckQuorum);
            assertEquals(managedLedger.getConfig().getThrottleMarkDelete(), topicMarkDeleteRate, 0.0001);
            assertEquals(cursor.getThrottleMarkDelete(), topicMarkDeleteRate, 0.0001);
        });

        // Test 7: Remove topic persistence policies and verify fallback to namespace level
        admin.topicPolicies().removePersistence(topicName);

        // Verify topic policies are removed
        Awaitility.await().untilAsserted(() ->
            assertNull(admin.topicPolicies().getPersistence(topicName)));

        // Verify managed ledger config falls back to namespace level policies
        Awaitility.await().untilAsserted(() -> {
            assertEquals(managedLedger.getConfig().getEnsembleSize(), namespaceEnsembleSize);
            assertEquals(managedLedger.getConfig().getWriteQuorumSize(), namespaceWriteQuorum);
            assertEquals(managedLedger.getConfig().getAckQuorumSize(), namespaceAckQuorum);
            assertEquals(managedLedger.getConfig().getThrottleMarkDelete(), namespaceMarkDeleteRate, 0.0001);
            assertEquals(cursor.getThrottleMarkDelete(), namespaceMarkDeleteRate, 0.0001);
        });

        // Test 8: Remove namespace persistence policies and verify fallback to broker defaults
        admin.namespaces().removePersistence(myNamespace);

        // Verify namespace policies are removed
        Awaitility.await().untilAsserted(() ->
            assertNull(admin.namespaces().getPersistence(myNamespace)));

        // Verify managed ledger config falls back to broker defaults
        Awaitility.await().untilAsserted(() -> {
            assertEquals(managedLedger.getConfig().getEnsembleSize(),
                pulsar.getConfiguration().getManagedLedgerDefaultEnsembleSize());
            assertEquals(managedLedger.getConfig().getWriteQuorumSize(),
                pulsar.getConfiguration().getManagedLedgerDefaultWriteQuorum());
            assertEquals(managedLedger.getConfig().getAckQuorumSize(),
                pulsar.getConfiguration().getManagedLedgerDefaultAckQuorum());
            assertEquals(managedLedger.getConfig().getThrottleMarkDelete(),
                pulsar.getConfiguration().getManagedLedgerDefaultMarkDeleteRateLimit(), 0.0001);
            assertEquals(cursor.getThrottleMarkDelete(),
                pulsar.getConfiguration().getManagedLedgerDefaultMarkDeleteRateLimit(), 0.0001);
        });
    }
}
