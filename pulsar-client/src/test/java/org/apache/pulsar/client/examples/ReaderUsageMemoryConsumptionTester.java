/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.examples;

import org.apache.pulsar.client.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Reproduces the memory leak described in https://github.com/apache/pulsar/issues/8138#issuecomment-700413950
 * <p>
 * Start Pulsar locally to run against it:
 * docker run --rm -e 'PULSAR_MEM=-Xms512m -Xmx2g -XX:MaxDirectMemorySize=2g' -e 'PULSAR_GC=-XX:+UseG1GC' -p6650:6650 apachepulsar/pulsar:2.6.1 bin/pulsar standalone
 */
public class ReaderUsageMemoryConsumptionTester implements AutoCloseable {
    int numberOfTopics = 100;
    int concurrencyLevel = 16;
    PulsarClient client;
    List<String> topicNames;
    List<String> words = Arrays.asList("Hello", "beautiful", "world");
    String lastWord = words.get(words.size() - 1);
    Random random = new Random();
    ExecutorService executorService = Executors.newFixedThreadPool(concurrencyLevel);

    public void setUp() throws PulsarClientException {
        String serviceUrl = "pulsar://localhost:6650";

        client = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .connectionsPerBroker(16)
                .build();

        topicNames = IntStream.rangeClosed(1, numberOfTopics).boxed().map(n -> UUID.randomUUID().toString())
                .collect(Collectors.toList());

        for (String topicName : topicNames) {
            createTopicWithMessagePayloads(topicName, words);
        }
    }

    public void close() throws Exception {
        executorService.shutdownNow();
        client.close();
    }

    public static void main(String[] args) throws Exception {
        try (ReaderUsageMemoryConsumptionTester readerUsageMemoryLeak = new ReaderUsageMemoryConsumptionTester()) {
            readerUsageMemoryLeak.setUp();
            readerUsageMemoryLeak.reproduceLeak(1000, 60);
            HeapDumper.INSTANCE.dumpHeap();
        }
    }

    public boolean reproduceLeak(int loopCount, int timeoutSeconds) throws InterruptedException {
        AtomicInteger failureCount = new AtomicInteger();
        CountDownLatch completionLatch = new CountDownLatch(loopCount);
        for (int i = 0; i < loopCount; i++) {
            executorService.submit(() ->
                    client.newReader(Schema.STRING)
                            .topic(randomTopic())
                            .startMessageId(MessageId.latest)
                            .startMessageIdInclusive()
                            .createAsync()
                            .thenCompose(reader -> reader.hasMessageAvailableAsync()
                                    .thenCompose(hasMessage -> {
                                        if (!hasMessage) {
                                            throw new IllegalStateException("No message available on reader for topic " + reader.getTopic());
                                        }
                                        return reader.readNextAsync();
                                    })
                                    .thenCompose(message -> {
                                                if (!message.getValue().equals(lastWord)) {
                                                    throw new IllegalStateException("Payload of last message isn't expected. payload is '" + message.getValue() + "'");
                                                }
                                                return reader.closeAsync();
                                            }
                                    ))
                            .whenComplete((__, t) -> {
                                if (t != null) {
                                    failureCount.incrementAndGet();
                                    System.err.println("Failed with exception " + t.getMessage());
                                    t.printStackTrace(System.err);
                                }
                                completionLatch.countDown();
                            })
            );
        }
        boolean hasErrors = false;
        if (!completionLatch.await(timeoutSeconds, TimeUnit.SECONDS)) {
            System.err.println("Execution didn't complete before timeout. Latch count = " + completionLatch.getCount());
            hasErrors = true;
        }
        if (failureCount.get() > 0) {
            System.err.println("There were " + failureCount.get() + " errors.");
            hasErrors = true;
        }
        return hasErrors;
    }

    private String randomTopic() {
        return topicNames.get(random.nextInt(topicNames.size()));
    }

    private void createTopicWithMessagePayloads(String topicName, List<String> payloads) throws PulsarClientException {
        // create subscription so that messages don't get removed
        client.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("subscription:" + topicName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionMode(SubscriptionMode.Durable)
                .subscribe()
                .close();
        try (Producer<String> producer = client.newProducer(Schema.STRING).enableBatching(false).topic(topicName).create()) {
            for (String payload : payloads) {
                producer.newMessage().value(payload).send();
            }
        }
    }
}
