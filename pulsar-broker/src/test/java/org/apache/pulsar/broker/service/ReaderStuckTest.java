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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ReaderStuckTest extends BrokerTestBase {
    private static final Logger log = LoggerFactory.getLogger(ReaderStuckTest.class);

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        this.conf.setManagedLedgerCursorBackloggedThreshold(1);
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testReaderStuck() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testReaderStuck");
        final String subName = "testReaderStuck";
        int numMessages = 5000;

        admin.topics().createNonPartitionedTopic(topicName);

        AtomicInteger lastMessageNumberSent = new AtomicInteger(0);
        AtomicInteger lastMessageNumberReceived = new AtomicInteger(0);

        @Cleanup
        Producer<Integer> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .enableBatching(false)
                .create();

        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).get().get();
        ScheduledFuture<?> backloggedCheck = pulsar.getExecutor()
                .scheduleAtFixedRate(persistentTopic::checkBackloggedCursors, 0, 10, TimeUnit.MILLISECONDS);
        try {
            Thread producerThread = new Thread(() -> {
                int messageNumber = 0;
                while (messageNumber < numMessages) {
                    messageNumber = lastMessageNumberSent.incrementAndGet();
                    if (messageNumber % 100 == 0) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    try {
                        producer.send(messageNumber);
                    } catch (Exception e) {
                        log.error("Failed to send message", e);
                        break;
                    }
                }
            });
            producerThread.start();

            @Cleanup
            Reader<Integer> reader = pulsarClient.newReader(Schema.INT32).topic(topicName)
                    .startMessageId(MessageId.earliest)
                    .create();
            while (true) {
                Message<Integer> message = reader.readNext(5, TimeUnit.SECONDS);
                if (message == null) {
                    fail("No message received in 5 seconds. lastMessageNumberReceived: "
                            + lastMessageNumberReceived.get() + ", lastMessageNumberSent: "
                            + lastMessageNumberSent.get() + ", numMessages: " + numMessages);
                }
                int received = message.getValue();
                int expected = lastMessageNumberReceived.incrementAndGet();
                if (received != expected) {
                    assertEquals(received, expected,
                            "Received message number " + received + " but expected " + expected);
                }
                if (received == numMessages) {
                    break;
                }
            }
        } finally {
            backloggedCheck.cancel(true);
        }
    }
}
