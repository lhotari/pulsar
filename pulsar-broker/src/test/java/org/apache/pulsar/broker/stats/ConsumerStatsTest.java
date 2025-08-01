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
package org.apache.pulsar.broker.stats;

import static org.apache.pulsar.broker.BrokerTestUtil.newUniqueName;
import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.INTEGER;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertEquals;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.PendingAcksMap;
import org.apache.pulsar.broker.service.StickyKeyConsumerSelector;
import org.apache.pulsar.broker.service.StickyKeyDispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilterProducerTest;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.DrainingHash;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.assertj.core.groups.Tuple;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ConsumerStatsTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        conf.setMaxUnackedMessagesPerConsumer(0);
        // wait for shutdown of the broker, this prevents flakiness which could be caused by metrics being
        // unregistered asynchronously. This impacts the execution of the next test method if this would be happening.
        conf.setBrokerShutdownTimeoutMs(5000L);
        return conf;
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConsumerStatsOnZeroMaxUnackedMessagesPerConsumer() throws PulsarClientException,
            InterruptedException, PulsarAdminException {
        Assert.assertEquals(pulsar.getConfiguration().getMaxUnackedMessagesPerConsumer(), 0);
        final String topicName = "persistent://my-property/my-ns/testConsumerStatsOnZeroMaxUnackedMessagesPerConsumer";

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName("sub")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();

        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer.send(("message-" + i).getBytes());
        }

        int received = 0;
        for (int i = 0; i < messages; i++) {
            // don't ack messages here
            consumer.receive();
            received++;
        }

        Assert.assertEquals(received, messages);
        received = 0;

        TopicStats stats = admin.topics().getStats(topicName);
        Assert.assertEquals(stats.getSubscriptions().size(), 1);
        Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next()
                .getValue().getConsumers().size(), 1);
        Assert.assertFalse(stats.getSubscriptions().entrySet().iterator().next()
                .getValue().getConsumers().get(0).isBlockedConsumerOnUnackedMsgs());
        Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next()
                .getValue().getConsumers().get(0).getUnackedMessages(), messages);

        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
            received++;
        }

        Assert.assertEquals(received, messages);

        // wait acknowledge send
        Thread.sleep(2000);

        stats = admin.topics().getStats(topicName);

        Assert.assertFalse(stats.getSubscriptions().entrySet().iterator().next()
                .getValue().getConsumers().get(0).isBlockedConsumerOnUnackedMsgs());
        Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next()
                .getValue().getConsumers().get(0).getUnackedMessages(), 0);
    }

    @Test
    public void testAckStatsOnPartitionedTopicForExclusiveSubscription() throws PulsarAdminException,
            PulsarClientException, InterruptedException {
        final String topic = "persistent://my-property/my-ns/testAckStatsOnPartitionedTopicForExclusiveSubscription";
        admin.topics().createPartitionedTopic(topic, 3);
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionName("sub")
                .subscribe();

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        final int messages = 10;
        for (int i = 0; i < messages; i++) {
            producer.send(("message-" + i).getBytes());
        }

        int received = 0;
        for (int i = 0; i < messages; i++) {
            consumer.acknowledge(consumer.receive());
            received++;
        }
        Assert.assertEquals(messages, received);

        // wait acknowledge send
        Thread.sleep(2000);

        for (int i = 0; i < 3; i++) {
            TopicStats stats = admin.topics().getStats(topic + "-partition-" + i);
            Assert.assertEquals(stats.getSubscriptions().size(), 1);
            Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next()
                    .getValue().getConsumers().size(), 1);
            Assert.assertEquals(stats.getSubscriptions().entrySet().iterator().next()
                    .getValue().getConsumers().get(0).getUnackedMessages(), 0);
        }
    }

    @Test
    public void testUpdateStatsForActiveConsumerAndSubscription() throws Exception {
        final String topicName = "persistent://public/default/testUpdateStatsForActiveConsumerAndSubscription";
        pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("my-subscription")
                .subscribe();

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService().getTopicReference(topicName).get();
        Assert.assertNotNull(topicRef);
        Assert.assertEquals(topicRef.getSubscriptions().size(), 1);
        List<org.apache.pulsar.broker.service.Consumer> consumers = topicRef.getSubscriptions()
                .get("my-subscription").getConsumers();
        Assert.assertEquals(consumers.size(), 1);
        ConsumerStatsImpl consumerStats = new ConsumerStatsImpl();
        consumerStats.msgOutCounter = 10;
        consumerStats.bytesOutCounter = 1280;
        consumers.get(0).updateStats(consumerStats);
        ConsumerStats updatedStats = consumers.get(0).getStats();

        Assert.assertEquals(updatedStats.getMsgOutCounter(), 10);
        Assert.assertEquals(updatedStats.getBytesOutCounter(), 1280);
    }

    @DataProvider(name = "classicAndSubscriptionType")
    public Object[][] classicAndSubscriptionType() {
        return new Object[][]{
                {false, SubscriptionType.Shared},
                {true, SubscriptionType.Key_Shared},
                {false, SubscriptionType.Key_Shared}
        };
    }

    @Test(dataProvider = "classicAndSubscriptionType")
    public void testConsumerStatsOutput(boolean classicDispatchers, SubscriptionType subscriptionType)
            throws Exception {
        if (this instanceof AuthenticatedConsumerStatsTest) {
            throw new SkipException("Skip test for AuthenticatedConsumerStatsTest");
        }
        conf.setSubscriptionSharedUseClassicPersistentImplementation(classicDispatchers);
        conf.setSubscriptionKeySharedUseClassicPersistentImplementation(classicDispatchers);
        Set<String> expectedFields = Sets.newHashSet(
                "msgRateOut",
                "msgThroughputOut",
                "bytesOutCounter",
                "msgOutCounter",
                "messageAckRate",
                "msgRateRedeliver",
                "chunkedMessageRate",
                "consumerName",
                "availablePermits",
                "unackedMessages",
                "avgMessagesPerEntry",
                "blockedConsumerOnUnackedMsgs",
                "lastAckedTime",
                "lastAckedTimestamp",
                "lastConsumedTime",
                "lastConsumedTimestamp",
                "firstMessagesSentTimestamp",
                "firstConsumedFlowTimestamp",
                "lastConsumedFlowTimestamp",
                "metadata",
                "address",
                "connectedSince",
                "clientVersion",
                "drainingHashesCount",
                "drainingHashesClearedTotal",
                "drainingHashesUnackedMessages"
        );
        if (subscriptionType == SubscriptionType.Key_Shared) {
            if (classicDispatchers) {
                expectedFields.addAll(List.of(
                        "readPositionWhenJoining",
                        "keyHashRanges"
                ));
            } else {
                expectedFields.addAll(List.of(
                        "drainingHashes",
                        "keyHashRangeArrays"
                ));
            }
        }
        final String topicName = newUniqueName("persistent://my-property/my-ns/testConsumerStatsOutput");
        final String subName = "my-subscription";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(subscriptionType)
                .subscriptionName(subName)
                .subscribe();

        String topicStatsUri =
                String.format("%s/admin/v2/%s/stats", pulsar.getWebServiceAddress(), topicName.replace("://", "/"));
        String topicStatsJson = BrokerTestUtil.getJsonResourceAsString(topicStatsUri);
        ObjectMapper mapper = ObjectMapperFactory.create();
        JsonNode node = mapper.readTree(topicStatsJson).get("subscriptions").get(subName).get("consumers").get(0);
        assertThat(node.fieldNames()).toIterable().containsExactlyInAnyOrderElementsOf(expectedFields);
    }

    @Test
    public void testLastConsumerFlowTimestamp() throws PulsarClientException, PulsarAdminException {
        final String topicName = newUniqueName("persistent://my-property/my-ns/testLastConsumerFlowTimestamp");
        final String subName = "my-subscription";

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName(subName)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topicName);
        ConsumerStats consumerStats = stats.getSubscriptions()
                .get(subName).getConsumers().get(0);
        Assert.assertTrue(consumerStats.getLastConsumedFlowTimestamp() > 0);
    }


    @Test
    public void testPersistentTopicMessageAckRateMetricTopicLevel() throws Exception {
        String topicName = "persistent://public/default/msg_ack_rate" + UUID.randomUUID();
        testMessageAckRateMetric(topicName, true);
    }

    @Test
    public void testPersistentTopicMessageAckRateMetricNamespaceLevel() throws Exception {
        String topicName = "persistent://public/default/msg_ack_rate" + UUID.randomUUID();
        testMessageAckRateMetric(topicName, false);
    }

    private void testMessageAckRateMetric(String topicName, boolean exposeTopicLevelMetrics)
            throws Exception {
        final int messages = 1000;
        String subName = "test_sub";
        CountDownLatch latch = new CountDownLatch(messages);

        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .enableBatching(true).batchingMaxMessages(10).create();

        MessageListener<String> listener = (consumer, msg) -> {
            try {
                consumer.acknowledge(msg);
                latch.countDown();
            } catch (PulsarClientException e) {
                //ignore
            }
        };
        @Cleanup
        Consumer<String> c1 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .messageListener(listener)
                .subscribe();
        @Cleanup
        Consumer<String> c2 = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .messageListener(listener)
                .subscribe();

        String namespace = TopicName.get(topicName).getNamespace();

        for (int i = 0; i < messages; i++) {
            producer.sendAsync(UUID.randomUUID().toString());
        }
        producer.flush();

        latch.await(20, TimeUnit.SECONDS);
        TimeUnit.SECONDS.sleep(1);

        Topic topic = pulsar.getBrokerService().getTopic(topicName, false).get().get();
        Subscription subscription = topic.getSubscription(subName);
        List<org.apache.pulsar.broker.service.Consumer> consumers = subscription.getConsumers();
        Assert.assertEquals(consumers.size(), 2);
        org.apache.pulsar.broker.service.Consumer consumer1 = consumers.get(0);
        org.apache.pulsar.broker.service.Consumer consumer2 = consumers.get(1);
        consumer1.updateRates();
        consumer2.updateRates();

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsTestUtil.generate(pulsar, exposeTopicLevelMetrics, true, true, output);
        String metricStr = output.toString(StandardCharsets.UTF_8);

        Multimap<String, Metric> metricsMap = parseMetrics(metricStr);
        Collection<Metric> ackRateMetric = metricsMap.get("pulsar_consumer_msg_ack_rate");

        String rateOutMetricName = exposeTopicLevelMetrics ? "pulsar_consumer_msg_rate_out" : "pulsar_rate_out";
        Collection<Metric> rateOutMetric = metricsMap.get(rateOutMetricName);
        Assert.assertTrue(ackRateMetric.size() > 0);
        Assert.assertTrue(rateOutMetric.size() > 0);

        if (exposeTopicLevelMetrics) {
            String consumer1Name = consumer1.consumerName();
            String consumer2Name = consumer2.consumerName();
            double totalAckRate = ackRateMetric.stream()
                    .filter(metric -> metric.tags.get("consumer_name").equals(consumer1Name)
                            || metric.tags.get("consumer_name").equals(consumer2Name))
                    .mapToDouble(metric -> metric.value).sum();
            double totalRateOut = rateOutMetric.stream()
                    .filter(metric -> metric.tags.get("consumer_name").equals(consumer1Name)
                            || metric.tags.get("consumer_name").equals(consumer2Name))
                    .mapToDouble(metric -> metric.value).sum();

            Assert.assertTrue(totalAckRate > 0D);
            Assert.assertTrue(totalRateOut > 0D);
            Assert.assertEquals(totalAckRate, totalRateOut, totalRateOut * 0.1D);
        } else {
            double totalAckRate = ackRateMetric.stream()
                    .filter(metric -> namespace.equals(metric.tags.get("namespace")))
                    .mapToDouble(metric -> metric.value).sum();
            double totalRateOut = rateOutMetric.stream()
                    .filter(metric -> namespace.equals(metric.tags.get("namespace")))
                    .mapToDouble(metric -> metric.value).sum();

            Assert.assertTrue(totalAckRate > 0D);
            Assert.assertTrue(totalRateOut > 0D);
            Assert.assertEquals(totalAckRate, totalRateOut, totalRateOut * 0.1D);
        }
    }

    @Test
    public void testAvgMessagesPerEntry() throws Exception {
        conf.setAllowOverrideEntryFilters(true);
        final String topic = "persistent://public/default/testFilterState";
        String subName = "sub";

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .producerName("producer1")
                .enableBatching(true).topic(topic)
                .batchingMaxMessages(20)
                .batchingMaxPublishDelay(5, TimeUnit.SECONDS)
                .batchingMaxBytes(Integer.MAX_VALUE)
                .create();
        // The first messages deliver: 20 msgs.
        // Average of "messages per batch" is "1".
        for (int i = 0; i < 20; i++) {
            producer.send("first-message");
        }
        // The second messages deliver: 20 msgs.
        // Average of "messages per batch" is "Math.round(1 * 0.9 + 20 * 0.1) = 2.9 ～ 3".
        List<CompletableFuture<MessageId>> futures = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            futures.add(producer.sendAsync("message"));
        }
        FutureUtil.waitForAll(futures);
        producer.close();

        Producer<String> producer2 = pulsarClient.newProducer(Schema.STRING)
                .producerName("producer2")
                .enableBatching(false).topic(topic)
                .create();
        producer2.newMessage().value("producer2-message").send();
        producer2.close();

        // mock entry filters
        NarClassLoader narClassLoader = mock(NarClassLoader.class);
        EntryFilter filter = new EntryFilterProducerTest();
        EntryFilterWithClassLoader
                loader = spyWithClassAndConstructorArgs(EntryFilterWithClassLoader.class, filter,
                narClassLoader, false);
        Pair<String, List<EntryFilter>> entryFilters = Pair.of("filter", List.of(loader));

        PersistentTopic topicRef = (PersistentTopic) pulsar.getBrokerService()
                .getTopicReference(topic).get();
        Field field1 = topicRef.getClass().getSuperclass().getDeclaredField("entryFilters");
        field1.setAccessible(true);
        field1.set(topicRef, entryFilters);

        Map<String, String> metadataConsumer = new HashMap<>();
        metadataConsumer.put("matchValueAccept", "producer1");
        metadataConsumer.put("matchValueReschedule", "producer2");
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic)
                .properties(metadataConsumer)
                .receiverQueueSize(20)
                .subscriptionName(subName).subscriptionInitialPosition(
                        SubscriptionInitialPosition.Earliest).subscribe();

        int counter = 0;
        while (true) {
            Message<String> message = consumer.receive(10, TimeUnit.SECONDS);
            if (message != null) {
                counter++;
                assertNotEquals(message.getValue(), "producer2-message");
                consumer.acknowledge(message);
            } else {
                break;
            }
        }

        assertEquals(40, counter);

        ConsumerStats consumerStats =
                admin.topics().getStats(topic).getSubscriptions().get(subName).getConsumers().get(0);

        assertEquals(40, consumerStats.getMsgOutCounter());

        // The first messages deliver: 20 msgs.
        // Average of "messages per batch" is "1".
        // The second messages deliver: 20 msgs.
        // Average of "messages per batch" is "Math.round(1 * 0.9 + 20 * 0.1) = 2.9 ～ 3".
        int avgMessagesPerEntry = consumerStats.getAvgMessagesPerEntry();
        assertEquals(3, avgMessagesPerEntry);
    }

    @Test()
    public void testNonPersistentTopicSharedSubscriptionUnackedMessages() throws Exception {
        final String topicName = "non-persistent://my-property/my-ns/my-topic" + UUID.randomUUID();
        final String subName = "my-sub";

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        for (int i = 0; i < 5; i++) {
            producer.send(("message-" + i).getBytes());
        }
        for (int i = 0; i < 5; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            consumer.acknowledge(msg);
        }
        TimeUnit.SECONDS.sleep(1);

        TopicStats topicStats = admin.topics().getStats(topicName);
        assertEquals(1, topicStats.getSubscriptions().size());
        List<? extends ConsumerStats> consumers = topicStats.getSubscriptions().get(subName).getConsumers();
        assertEquals(1, consumers.size());
        assertEquals(0, consumers.get(0).getUnackedMessages());
    }

    @Test
    public void testKeySharedDrainingHashesConsumerStats() throws Exception {
        String topic = newUniqueName("testKeySharedDrainingHashesConsumerStats");
        String subscriptionName = "sub";
        int numberOfKeys = 10;

        // Create a producer for the topic
        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topic)
                .enableBatching(false)
                .create();

        // Create the first consumer (c1) for the topic
        @Cleanup
        Consumer<Integer> c1 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c1")
                .receiverQueueSize(100)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        // Get the dispatcher and selector for the topic
        StickyKeyDispatcher dispatcher = getDispatcher(topic, subscriptionName);
        StickyKeyConsumerSelector selector = dispatcher.getSelector();

        // Send 20 messages with keys cycling from 0 to numberOfKeys-1
        for (int i = 0; i < 20; i++) {
            String key = String.valueOf(i % numberOfKeys);
            int stickyKeyHash = selector.makeStickyKeyHash(key.getBytes(StandardCharsets.UTF_8));
            log.info("Sending message with value {} key {} hash {}", key, i, stickyKeyHash);
            producer.newMessage()
                    .key(key)
                    .value(i)
                    .send();
        }

        // Wait until all the already published messages have been pre-fetched by c1
        PendingAcksMap c1PendingAcks = dispatcher.getConsumers().get(0).getPendingAcks();
        Awaitility.await().ignoreExceptions().until(() -> c1PendingAcks.size() == 20);

        // Add a new consumer (c2) for the topic
        @Cleanup
        Consumer<Integer> c2 = pulsarClient.newConsumer(Schema.INT32)
                .topic(topic)
                .consumerName("c2")
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        // Get the subscription stats and consumer stats
        SubscriptionStats subscriptionStats = admin.topics().getStats(topic).getSubscriptions().get(subscriptionName);
        ConsumerStats c1Stats = subscriptionStats.getConsumers().get(0);
        ConsumerStats c2Stats = subscriptionStats.getConsumers().get(1);

        Set<Integer> c2HashesByStats = new HashSet<>();
        Set<Integer> c2HashesByDispatcher = new HashSet<>();
        Map<Integer, Integer> c1DrainingHashesExpected = new HashMap<>();

        int expectedDrainingHashesUnackMessages = 0;
        // Determine which hashes are assigned to c2 and which are draining from c1
        // run for the same keys as the sent messages
        for (int i = 0; i < 20; i++) {
            // use the same key as in the sent messages
            String key = String.valueOf(i % numberOfKeys);
            int hash = selector.makeStickyKeyHash(key.getBytes(StandardCharsets.UTF_8));
            // Validate that the hash is assigned to c2 in stats
            if ("c2".equals(findConsumerNameForHash(subscriptionStats, hash))) {
                c2HashesByStats.add(hash);
            }
            // use the selector to determine the expected draining hashes for c1
            org.apache.pulsar.broker.service.Consumer selected = selector.select(hash);
            if ("c2".equals(selected.consumerName())) {
                c2HashesByDispatcher.add(hash);
                c1DrainingHashesExpected.compute(hash, (k, v) -> v == null ? 1 : v + 1);
                expectedDrainingHashesUnackMessages++;
            }
        }

        // Validate that the hashes assigned to c2 match between stats and dispatcher
        assertThat(c2HashesByStats).containsExactlyInAnyOrderElementsOf(c2HashesByDispatcher);

        // Validate the draining hashes for c1
        assertThat(c1Stats.getDrainingHashes()).extracting(DrainingHash::getHash)
                .containsExactlyInAnyOrderElementsOf(c2HashesByStats);
        assertThat(c1Stats.getDrainingHashes()).extracting(DrainingHash::getHash, DrainingHash::getUnackMsgs)
                .containsExactlyInAnyOrderElementsOf(c1DrainingHashesExpected.entrySet().stream()
                        .map(e -> Tuple.tuple(e.getKey(), e.getValue())).toList());

        // Validate that c2 has no draining hashes
        assertThat(c2Stats.getDrainingHashes()).isEmpty();

        // Validate counters
        assertThat(c1Stats.getDrainingHashesCount()).isEqualTo(c2HashesByStats.size());
        assertThat(c1Stats.getDrainingHashesClearedTotal()).isEqualTo(0);
        assertThat(c1Stats.getDrainingHashesUnackedMessages()).isEqualTo(expectedDrainingHashesUnackMessages);
        assertThat(c2Stats.getDrainingHashesCount()).isEqualTo(0);
        assertThat(c2Stats.getDrainingHashesClearedTotal()).isEqualTo(0);
        assertThat(c2Stats.getDrainingHashesUnackedMessages()).isEqualTo(0);

        // Send another 20 messages
        for (int i = 0; i < 20; i++) {
            producer.newMessage()
                    .key(String.valueOf(i % numberOfKeys))
                    .value(i)
                    .send();
        }

        // Validate blocked attempts for c1
        Awaitility.await().ignoreExceptions().untilAsserted(() -> {
            SubscriptionStats stats = admin.topics().getStats(topic).getSubscriptions().get(subscriptionName);
            assertThat(stats.getConsumers().get(0).getDrainingHashes()).isNotEmpty().allSatisfy(dh -> {
                assertThat(dh).extracting(DrainingHash::getBlockedAttempts)
                        .asInstanceOf(INTEGER)
                        .isGreaterThan(0);
            });
        });

        // Acknowledge messages that were sent before c2 joined, to clear all draining hashes
        for (int i = 0; i < 20; i++) {
            Message<Integer> message = c1.receive(1, TimeUnit.SECONDS);
            log.info("Acking message with value {} key {}", message.getValue(), message.getKey());
            c1.acknowledge(message);

            if (i == 18) {
                // Validate that there is one draining hash left
                Awaitility.await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
                        .untilAsserted(() -> {
                            SubscriptionStats stats =
                                    admin.topics().getStats(topic).getSubscriptions().get(subscriptionName);
                            assertThat(stats.getConsumers().get(0)).satisfies(consumerStats -> {
                                assertThat(consumerStats)
                                        .describedAs("Consumer stats should have one draining hash %s", consumerStats)
                                        .extracting(ConsumerStats::getDrainingHashes)
                                        .asList().hasSize(1);
                            });
                        });
            }

            if (i == 19) {
                // Validate that there are no draining hashes left
                Awaitility.await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
                        .untilAsserted(() -> {
                            SubscriptionStats stats =
                                    admin.topics().getStats(topic).getSubscriptions().get(subscriptionName);
                            assertThat(stats.getConsumers().get(0)).satisfies(consumerStats -> {
                                assertThat(consumerStats).extracting(ConsumerStats::getDrainingHashes)
                                        .asList().isEmpty();
                            });
                        });
            }
        }

        // Get the subscription stats and consumer stats
        subscriptionStats = admin.topics().getStats(topic).getSubscriptions().get(subscriptionName);
        c1Stats = subscriptionStats.getConsumers().get(0);
        c2Stats = subscriptionStats.getConsumers().get(1);

        // Validate counters
        assertThat(c1Stats.getDrainingHashesCount()).isEqualTo(0);
        assertThat(c1Stats.getDrainingHashesClearedTotal()).isEqualTo(c2HashesByStats.size());
        assertThat(c1Stats.getDrainingHashesUnackedMessages()).isEqualTo(0);
        assertThat(c2Stats.getDrainingHashesCount()).isEqualTo(0);
        assertThat(c2Stats.getDrainingHashesClearedTotal()).isEqualTo(0);
        assertThat(c2Stats.getDrainingHashesUnackedMessages()).isEqualTo(0);

    }

    private String findConsumerNameForHash(SubscriptionStats subscriptionStats, int hash) {
        return findConsumerForHash(subscriptionStats, hash).map(ConsumerStats::getConsumerName).orElse(null);
    }

    private Optional<? extends ConsumerStats> findConsumerForHash(SubscriptionStats subscriptionStats, int hash) {
        return subscriptionStats.getConsumers().stream()
                .filter(consumerStats -> consumerStats.getKeyHashRangeArrays().stream()
                        .anyMatch(hashRanges -> hashRanges[0] <= hash && hashRanges[1] >= hash))
                .findFirst();
    }

    @SneakyThrows
    private StickyKeyDispatcher getDispatcher(String topic, String subscription) {
        return (StickyKeyDispatcher) pulsar.getBrokerService().getTopicIfExists(topic).get()
                .get().getSubscription(subscription).getDispatcher();
    }
}
