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
package org.apache.pulsar.broker.service.persistent;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.SucceededFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.PendingAcksMap;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.service.plugin.EntryFilterProvider;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentDispatcherMultipleConsumersMockTest {

    private PulsarService pulsarMock;
    private BrokerService brokerMock;
    private ManagedLedgerImpl ledgerMock;
    private ManagedCursorImpl cursorMock;
    private PersistentTopic topicMock;
    private PersistentSubscription subscriptionMock;
    private ServiceConfiguration configMock;
    private Future<Void> succeededFuture;
    private OrderedExecutor orderedExecutor;

    private PersistentDispatcherMultipleConsumers dispatcher;

    final String topicName = "persistent://public/default/testTopic";
    final String subscriptionName = "testSubscription";
    private AtomicInteger consumerMockAvailablePermits;

    @BeforeMethod
    public void setup() throws Exception {
        configMock = mock(ServiceConfiguration.class);
        doReturn(true).when(configMock).isSubscriptionRedeliveryTrackerEnabled();
        doReturn(100).when(configMock).getDispatcherMaxReadBatchSize();
        doReturn(false).when(configMock).isDispatcherDispatchMessagesInSubscriptionThread();
        doReturn(false).when(configMock).isAllowOverrideEntryFilters();
        doReturn(false).when(configMock).isDispatchThrottlingOnNonBacklogConsumerEnabled();
        pulsarMock = mock(PulsarService.class);
        doReturn(configMock).when(pulsarMock).getConfiguration();

        EntryFilterProvider mockEntryFilterProvider = mock(EntryFilterProvider.class);
        when(mockEntryFilterProvider.getBrokerEntryFilters()).thenReturn(Collections.emptyList());

        brokerMock = mock(BrokerService.class);
        doReturn(pulsarMock).when(brokerMock).pulsar();
        when(brokerMock.getEntryFilterProvider()).thenReturn(mockEntryFilterProvider);

        HierarchyTopicPolicies topicPolicies = new HierarchyTopicPolicies();
        topicPolicies.getMaxConsumersPerSubscription().updateBrokerValue(0);

        orderedExecutor = OrderedExecutor.newBuilder().build();
        doReturn(orderedExecutor).when(brokerMock).getTopicOrderedExecutor();

        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        doReturn(eventLoopGroup).when(brokerMock).executor();
        doAnswer(invocation -> {
            orderedExecutor.execute(invocation.getArgument(0, Runnable.class));
            return null;
        }).when(eventLoopGroup).execute(any(Runnable.class));

        topicMock = mock(PersistentTopic.class);
        doReturn(brokerMock).when(topicMock).getBrokerService();
        doReturn(topicName).when(topicMock).getName();
        doReturn(topicPolicies).when(topicMock).getHierarchyTopicPolicies();

        ledgerMock = mock(ManagedLedgerImpl.class);
        doAnswer((invocationOnMock -> {
            final Position position = invocationOnMock.getArgument(0);
            if (position.getEntryId() > 0) {
                return PositionFactory.create(position.getLedgerId(), position.getEntryId() - 1);
            } else {
                fail("Undefined behavior on mock");
                return PositionFactory.EARLIEST;
            }
        })).when(ledgerMock).getPreviousPosition(any(Position.class));
        doAnswer((invocationOnMock -> {
            final Position position = invocationOnMock.getArgument(0);
            return PositionFactory.create(position.getLedgerId(),
                    position.getEntryId() < 0 ? 0 : position.getEntryId() + 1);
        })).when(ledgerMock).getNextValidPosition(any(Position.class));
        doAnswer((invocationOnMock -> {
            final Range<Position> range = invocationOnMock.getArgument(0);
            Position fromPosition = range.lowerEndpoint();
            boolean fromIncluded = range.lowerBoundType() == BoundType.CLOSED;
            Position toPosition = range.upperEndpoint();
            boolean toIncluded = range.upperBoundType() == BoundType.CLOSED;

            long count = 0;

            if (fromPosition.getLedgerId() == toPosition.getLedgerId()) {
                // If the 2 positions are in the same ledger
                count = toPosition.getEntryId() - fromPosition.getEntryId() - 1;
                count += fromIncluded ? 1 : 0;
                count += toIncluded ? 1 : 0;
            } else {
                fail("Undefined behavior on mock");
            }
            return count;
        })).when(ledgerMock).getNumberOfEntries(any());

        cursorMock = mock(ManagedCursorImpl.class);
        doReturn(null).when(cursorMock).getLastIndividualDeletedRange();
        doReturn(subscriptionName).when(cursorMock).getName();
        doReturn(ledgerMock).when(cursorMock).getManagedLedger();
        doAnswer(invocation -> {
            int max = invocation.getArgument(0);
            return max;
        }).when(cursorMock).applyMaxSizeCap(anyInt(), anyLong());

        EventExecutor eventExecutor = mock(EventExecutor.class);
        doAnswer(invocation -> {
            orderedExecutor.execute(invocation.getArgument(0, Runnable.class));
            return null;
        }).when(eventExecutor).execute(any(Runnable.class));
        doReturn(false).when(eventExecutor).inEventLoop();
        succeededFuture = new SucceededFuture<>(eventExecutor, null);

        subscriptionMock = mock(PersistentSubscription.class);
        when(subscriptionMock.getTopic()).thenReturn(topicMock);
    }

    private void mockSendMessages(Consumer consumerMock, java.util.function.Consumer<List<Entry>> entryConsumer) {
        doAnswer(invocation -> {
            List<Entry> entries = invocation.getArgument(0);
            if (entryConsumer != null) {
                entryConsumer.accept(entries);
            }
            entries.stream().filter(Objects::nonNull).forEach(Entry::release);
            return succeededFuture;
        }).when(consumerMock).sendMessages(
                anyList(),
                any(EntryBatchSizes.class),
                any(EntryBatchIndexesAcks.class),
                anyInt(),
                anyLong(),
                anyLong(),
                any(RedeliveryTracker.class)
        );
    }

    protected static Consumer createMockConsumer() {
        Consumer consumerMock = mock(Consumer.class);
        TransportCnx transportCnx = mock(TransportCnx.class);
        doReturn(transportCnx).when(consumerMock).cnx();
        doReturn(true).when(transportCnx).isActive();
        doReturn(100).when(consumerMock).getMaxUnackedMessages();
        doReturn(1).when(consumerMock).getAvgMessagesPerEntry();
        PendingAcksMap pendingAcksMap = mock(PendingAcksMap.class);
        doReturn(pendingAcksMap).when(consumerMock).getPendingAcks();
        return consumerMock;
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (dispatcher != null && !dispatcher.isClosed()) {
            dispatcher.close();
        }
        if (orderedExecutor != null) {
            orderedExecutor.shutdownNow();
            orderedExecutor = null;
        }
    }

    @Test(timeOut = 10000)
    public void testReadMoreEntriesWhileSendInProgress() throws Exception {
        // This test case is to verify the fix for race condition between readMoreEntries and sendInProgress
        // where a call to readMoreEntries could be skipped if a send is in progress.
        // See https://github.com/apache/pulsar/pull/24700 for more details.

        // (1) Setup dispatcher to use a separate thread for dispatching messages
        doReturn(true).when(configMock).isDispatcherDispatchMessagesInSubscriptionThread();
        dispatcher = new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock);

        // (2) Add a consumer and grant some permits
        final Consumer consumer = createMockConsumer();
        consumerMockAvailablePermits = new AtomicInteger(10);
        doAnswer(invocation -> consumerMockAvailablePermits.get()).when(consumer).getAvailablePermits();
        doReturn(true).when(consumer).isWritable();
        dispatcher.addConsumer(consumer).join();

        // (3) Setup latches to control message sending flow
        CountDownLatch sendStartedLatch = new CountDownLatch(1);
        CountDownLatch sendInProgressLatch = new CountDownLatch(1);

        // (4) Mock sendMessages to block until sendInProgressLatch is released
        doAnswer(invocation -> {
            sendStartedLatch.countDown();
            sendInProgressLatch.await();
            List<Entry> entries = invocation.getArgument(0);
            entries.forEach(Entry::release);
            return succeededFuture;
        }).when(consumer).sendMessages(
                anyList(),
                any(EntryBatchSizes.class),
                any(EntryBatchIndexesAcks.class),
                anyInt(),
                anyLong(),
                anyLong(),
                any(RedeliveryTracker.class)
        );

        // (5) Mock cursor read to provide one entry
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(2);
            Object ctx = invocation.getArgument(3);
            List<Entry> entries = List.of(createEntry(1, 1, "message1", 1));
            callback.readEntriesComplete(new ArrayList<>(entries), ctx);
            return null;
        }).when(cursorMock).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());

        // (6) Trigger a read by sending consumer flow. This will call asyncReadEntries, which will call
        // readEntriesComplete, which will call sendMessages.
        dispatcher.consumerFlow(consumer, 10);

        // (7) Wait until send operation is in progress
        assertTrue(sendStartedLatch.await(5, TimeUnit.SECONDS));
        //Awaitility.await().until(dispatcher::isSendInProgress);

        // (8) While send is in progress, trigger another consumer flow, which calls readMoreEntries
        dispatcher.consumerFlow(consumer, 10);

        // (9) Unblock the send operation
        sendInProgressLatch.countDown();

        // (10) Verify that a new read is scheduled.
        // `consumerFlow` in step 6 triggers first read.
        // `consumerFlow` in step 8 calls `readMoreEntries`, which will set the flag and return.
        // After `sendMessages` is unblocked, `handleSendingMessagesAndReadingMore` will call `readMoreEntries`
        // which will trigger the second read.
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
            verify(cursorMock, times(2)).asyncReadEntriesWithSkipOrWait(
                anyInt(), anyLong(), any(), any(), any(), any())
        );
    }

    private EntryImpl createEntry(long ledgerId, long entryId, String message, long sequenceId) {
        return createEntry(ledgerId, entryId, message, sequenceId, "testKey");
    }

    private EntryImpl createEntry(long ledgerId, long entryId, String message, long sequenceId, String key) {
        ByteBuf data = createMessage(message, sequenceId, key);
        EntryImpl entry = EntryImpl.create(ledgerId, entryId, data);
        data.release();
        return entry;
    }

    private ByteBuf createMessage(String message, long sequenceId, String key) {
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKey(key)
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis());
        ByteBuf payload = Unpooled.copiedBuffer(message.getBytes(UTF_8));
        ByteBuf byteBuf = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c,
                messageMetadata, payload);
        payload.release();
        return byteBuf;
    }
}
