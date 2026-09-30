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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PersistentDispatcherMultipleConsumersReadFailureTest extends SharedPulsarBaseTest {

    private final List<Object> ownedMocks = new ArrayList<>();
    private final List<Position> deliveries = Collections.synchronizedList(new ArrayList<>());
    private ManagedLedgerFactory ledgerFactory;
    private ManagedLedgerImpl ledger;
    private ManagedCursorImpl cursor;
    private PreparationFailureDispatcher dispatcher;

    @DataProvider(name = "subscriptionThread")
    public Object[][] subscriptionThread() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "subscriptionThread", timeOut = 30_000)
    public void testPreparationFailureRetriesWithoutAnotherFlow(boolean subscriptionThread) throws Exception {
        List<Position> published = buildFixture(subscriptionThread, true);
        ReadState stateAfterFailure;
        synchronized (dispatcher) {
            assertThatThrownBy(dispatcher::readMoreEntries)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("injected read preparation failure");
            stateAfterFailure = readState();
            assertThat(cursor.getPendingReadOpsCount()).isZero();
            assertThat(cursor.hasPendingReadRequest()).isFalse();
            assertThat(deliveredPositions()).isEmpty();
        }

        // The injected exception is a verified preparation failure, not a claim about the incident's cause.
        // No further Flow or read trigger is supplied: the dispatcher must arrange its own retry.
        Awaitility.await("delivery after preparation failure; initial state: " + stateAfterFailure)
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(deliveredPositions()).containsExactlyElementsOf(published));
    }

    @Test(dataProvider = "subscriptionThread", timeOut = 30_000)
    public void testHealthyCallbackCanOutliveCursorReadAccounting(boolean subscriptionThread) throws Exception {
        List<Position> published = buildFixture(subscriptionThread, false);
        synchronized (dispatcher) {
            dispatcher.readMoreEntries();
            // Hold the dispatcher monitor until the real read has finished and its callback is waiting
            // to enter readEntriesComplete. Neither the cursor operation nor its callback is replaced.
            Awaitility.await("cursor completes while its dispatcher callback waits for the monitor")
                    .atMost(Duration.ofSeconds(5))
                    .untilAsserted(() -> {
                        assertThat(cursor.getPendingReadOpsCount()).isZero();
                        assertThat(cursor.getReadPosition().compareTo(published.get(0)))
                                .isPositive();
                    });
            assertThat(readState()).isEqualTo(new ReadState(true, 0, false));
            assertThat(deliveredPositions()).isEmpty();
            // Even repeated samples cannot distinguish this healthy queued callback from an orphan.
            for (int check = 0; check < 3; check++) {
                assertThat(dispatcher.checkAndUnblockIfStuck()).isFalse();
                assertThat(dispatcher.isHavePendingRead()).isTrue();
            }
        }
        Awaitility.await("the original callback delivers its entries after the monitor is released")
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(deliveredPositions()).containsExactlyElementsOf(published));
    }

    @Test(dataProvider = "subscriptionThread", timeOut = 30_000)
    public void testRejectedTailWaitRegistrationRetriesWithoutAnotherFlow(boolean subscriptionThread) throws Exception {
        int delayMillis = 10;
        buildFixture(subscriptionThread, false, 0, delayMillis);
        OrderedScheduler realScheduler = ledger.getScheduledExecutor();
        OrderedScheduler scheduler = ownDelegatingMock(OrderedScheduler.class, realScheduler);
        Thread callingThread = Thread.currentThread();
        AtomicBoolean rejectNextSchedule = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (Thread.currentThread() == callingThread && rejectNextSchedule.compareAndSet(true, false)) {
                // The real cursor has already accepted this read into its waiting slot.
                assertThat(cursor.hasPendingReadRequest()).isTrue();
                assertThat(cursor.getPendingReadOpsCount()).isZero();
                throw new RejectedExecutionException("injected tail-wait registration rejection");
            }
            return realScheduler.schedule(invocation.<Runnable>getArgument(0), invocation.<Long>getArgument(1),
                    invocation.getArgument(2));
        }).when(scheduler).schedule(any(Runnable.class), eq((long) delayMillis), eq(TimeUnit.MILLISECONDS));
        doReturn(scheduler).when(ledger).getScheduledExecutor();

        List<Position> published;
        synchronized (dispatcher) {
            dispatcher.readMoreEntries();
            assertThat(rejectNextSchedule).isFalse();
            assertThat(readState()).isEqualTo(new ReadState(false, 0, false));
            assertThat(cursor.cancelPendingReadRequest()).isFalse();
            // Publish before the scheduled retry can enter the dispatcher, so its progress requires
            // neither another Flow nor a leftover waiting operation from the rejected registration.
            published = publishMessages(5);
        }
        Awaitility.await("delivery after the real tail-wait scheduler rejects registration")
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(deliveredPositions()).containsExactlyElementsOf(published));
    }

    private List<Position> buildFixture(boolean subscriptionThread, boolean failPreparation) throws Exception {
        return buildFixture(subscriptionThread, failPreparation, 5, 0);
    }

    private List<Position> buildFixture(boolean subscriptionThread, boolean failPreparation, int initialEntries,
                                        int newEntriesCheckDelayMillis) throws Exception {
        deliveries.clear();
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName(getConfig().getClusterName());
        config.setDispatcherDispatchMessagesInSubscriptionThread(subscriptionThread);
        config.setDispatcherMaxReadBatchSize(failPreparation ? 5 : 2);
        config.setDispatcherRetryBackoffInitialTimeInMs(10);
        config.setDispatcherRetryBackoffMaxTimeInMs(20);
        config.setDispatcherReadFailureBackoffInitialTimeInMs(10);

        // Configuration belongs to this fixture; the shared broker and its executors remain shared.
        PulsarService pulsar = ownDelegatingMock(PulsarService.class, getPulsar());
        doReturn(config).when(pulsar).getConfiguration();
        doReturn(config).when(pulsar).getConfig();
        BrokerService broker = ownDelegatingMock(BrokerService.class, getPulsar().getBrokerService());
        doReturn(pulsar).when(broker).pulsar();
        doReturn(pulsar).when(broker).getPulsar();

        String topicName = newTopicName();
        ledgerFactory = getPulsar().getDefaultManagedLedgerFactory();
        ManagedLedgerConfig ledgerConfig = new ManagedLedgerConfig().setReadEntriesCallbackInline(false);
        ledgerConfig.setEnsembleSize(getConfig().getManagedLedgerDefaultEnsembleSize());
        ledgerConfig.setWriteQuorumSize(getConfig().getManagedLedgerDefaultWriteQuorum());
        ledgerConfig.setAckQuorumSize(getConfig().getManagedLedgerDefaultAckQuorum());
        ledgerConfig.setNewEntriesCheckDelayInMillis(newEntriesCheckDelayMillis);
        ledger = (ManagedLedgerImpl) ledgerFactory.open(TopicName.get(topicName).getPersistenceNamingEncoding(),
                ledgerConfig);
        if (newEntriesCheckDelayMillis > 0) {
            // Cursor creation must use this ledger instance so its scheduler seam reaches the actual cursor.
            ledger = Mockito.spy(ledger);
            ownedMocks.add(ledger);
        }
        PersistentTopic topic = new PersistentTopic(topicName, ledger, broker);
        cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        PersistentSubscription subscription = new PersistentSubscription(topic, "sub", cursor, false);
        dispatcher = new PreparationFailureDispatcher(topic, cursor, subscription, failPreparation);
        List<Position> published = publishMessages(initialEntries);
        dispatcher.addConsumer(newConsumer()).get(5, TimeUnit.SECONDS);
        return published;
    }

    private List<Position> publishMessages(int numberOfEntries) throws Exception {
        List<Position> published = new ArrayList<>();
        for (int entry = 0; entry < numberOfEntries; entry++) {
            MessageMetadata metadata = new MessageMetadata().setProducerName("testProducer")
                    .setSequenceId(entry).setPublishTime(System.currentTimeMillis());
            ByteBuf payload = Unpooled.copiedBuffer("message-" + entry, UTF_8);
            ByteBuf serialized = null;
            try {
                serialized = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
                published.add(ledger.addEntry(ByteBufUtil.getBytes(serialized)));
            } finally {
                if (serialized != null) {
                    serialized.release();
                }
                payload.release();
            }
        }
        return published;
    }

    private Consumer newConsumer() {
        TransportCnx cnx = ownMock(TransportCnx.class);
        doReturn(true).when(cnx).isActive();
        Consumer consumer = ownMock(Consumer.class);
        doReturn(cnx).when(consumer).cnx();
        doReturn(1000).when(consumer).getAvailablePermits();
        doReturn(1).when(consumer).getAvgMessagesPerEntry();
        doReturn(true).when(consumer).isWritable();
        doReturn("consumer").when(consumer).consumerName();
        doReturn(SubType.Shared).when(consumer).subType();
        doAnswer(invocation -> {
            List<Entry> entries = invocation.getArgument(0);
            entries.forEach(entry -> {
                deliveries.add(entry.getPosition());
                entry.release();
            });
            EntryBatchSizes batchSizes = invocation.getArgument(1);
            if (batchSizes != null) {
                batchSizes.recyle();
            }
            EntryBatchIndexesAcks batchIndexesAcks = invocation.getArgument(2);
            if (batchIndexesAcks != null) {
                batchIndexesAcks.recycle();
            }
            return ImmediateEventExecutor.INSTANCE.newSucceededFuture(null);
        }).when(consumer).sendMessages(any(), any(), any(), anyInt(), anyLong(), anyLong(), any());
        return consumer;
    }

    private <T> T ownMock(Class<T> type) {
        T mock = mock(type);
        ownedMocks.add(mock);
        return mock;
    }

    private <T> T ownDelegatingMock(Class<T> type, T delegate) {
        T mock = mock(type, withSettings().defaultAnswer(AdditionalAnswers.delegatesTo(delegate)));
        ownedMocks.add(mock);
        return mock;
    }

    private ReadState readState() {
        return new ReadState(dispatcher.isHavePendingRead(), cursor.getPendingReadOpsCount(),
                cursor.hasPendingReadRequest());
    }

    private List<Position> deliveredPositions() {
        synchronized (deliveries) {
            return List.copyOf(deliveries);
        }
    }

    private record ReadState(boolean dispatcherPending, int cursorPending, boolean cursorWaiting) {
    }

    private static final class PreparationFailureDispatcher extends PersistentDispatcherMultipleConsumers {
        private final AtomicBoolean failPreparation;

        private PreparationFailureDispatcher(PersistentTopic topic, ManagedCursor cursor, Subscription subscription,
                                             boolean failPreparation) {
            super(topic, cursor, subscription);
            this.failPreparation = new AtomicBoolean(failPreparation);
        }

        @Override
        protected synchronized Predicate<Position> createReadEntriesSkipConditionForNormalRead() {
            if (failPreparation.compareAndSet(true, false)) {
                throw new IllegalStateException("injected read preparation failure");
            }
            return super.createReadEntriesSkipConditionForNormalRead();
        }
    }

    @AfterMethod(alwaysRun = true)
    public void closeFixture() throws Exception {
        try {
            if (dispatcher != null) {
                dispatcher.close(false, Optional.empty()).get(5, TimeUnit.SECONDS);
            }
            if (cursor != null) {
                cursor.cancelPendingReadRequest();
                cursor.close();
            }
            if (ledger != null) {
                ledger.close();
                ledgerFactory.delete(ledger.getName());
            }
        } finally {
            dispatcher = null;
            cursor = null;
            ledger = null;
            Mockito.reset(ownedMocks.toArray());
            ownedMocks.forEach(mock -> Mockito.framework().clearInlineMock(mock));
            ownedMocks.clear();
        }
    }
}
