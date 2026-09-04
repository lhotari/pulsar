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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers.ReadContext;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers.ReadType;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.mockito.AdditionalAnswers;
import org.mockito.invocation.InvocationOnMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Covers the read-slot model that replaced the {@code havePendingRead} / {@code havePendingReplayRead}
 * booleans, and the stall it exists to prevent.
 *
 * <p><b>The stall</b> (apache/pulsar#26454). A Shared subscription with a 600k-entry backlog and a connected
 * consumer holding free permits stopped dispatching for 26 hours. {@code topics stats-internal} showed
 * {@code subscriptionHavePendingRead=true} with {@code pendingReadOps=0} and {@code waitingReadOp=false}: the
 * dispatcher believed a read was outstanding while the cursor held neither a parked nor an in-flight read.
 * A boolean can only say "someone owns this", never "who", so nothing below could tell that no completion
 * was coming, and {@code readMoreEntries()} refused every later read.
 *
 * <p><b>The model.</b> Each read now reserves a slot and carries a {@link ReadContext} identifying it, so a
 * callback can be matched against the slot it reserved: only the read still holding a slot may release it or
 * act on its own completion. Shared keeps a slot per read type, so a redelivery can be fetched while a
 * sequential read runs; {@link PersistentStickyKeyDispatcherMultipleConsumers} maps both types onto one slot,
 * because ordered delivery cannot tolerate two cursor operations at once.
 *
 * <p><b>Recovery.</b> The periodic subscription check notices a reserved slot the cursor knows nothing
 * about, rewinds the cursor and arms a replacement. The rewind is what makes it safe: a completion that
 * turns up afterwards -- because it had merely been queued behind a stalled managed-ledger executor rather
 * than lost -- can have its entries released, since the rewind guarantees they are read again.
 *
 * <p><b>Fidelity.</b> The managed ledger, cursor, topic, subscription, dispatcher and the whole dispatch tail
 * are real, and the published payloads are properly serialized Pulsar messages. The only seam is the
 * dispatcher's {@code cursor}: a Mockito proxy that delegates every call to the real {@link ManagedCursorImpl}
 * so a single call can be made to throw or to be swallowed.
 */
public class PersistentDispatcherReadSlotTest extends MockedBookKeeperTestCase {

    private static final String TOPIC = "persistent://prop/ns/read-slots";
    private static final int DELIVERY_TIMEOUT_SECONDS = 15;

    private PulsarTestContext pulsarTestContext;
    private BrokerService brokerService;

    private ManagedLedgerImpl ledger;
    private ManagedCursorImpl realCursor;
    /** Delegating proxy handed to the dispatcher; every call reaches {@link #realCursor} unless stubbed. */
    private ManagedCursor cursorProxy;

    private PersistentTopic topic;
    private PersistentSubscription subscription;
    private PersistentDispatcherMultipleConsumers dispatcher;
    private Consumer consumer;

    /** Positions the dispatcher handed to {@code Consumer#sendMessages}, in delivery order. */
    private final List<Position> deliveries = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong msgCounter = new AtomicLong();
    /** The context the dispatcher handed to the cursor for the read that was swallowed. */
    private final AtomicReference<ReadContext> swallowedRead = new AtomicReference<>();

    @Override
    protected ManagedLedgerConfig initManagedLedgerConfig(ManagedLedgerConfig config) {
        super.initManagedLedgerConfig(config);
        config.setMaxEntriesPerLedger(1_000_000);
        config.setRetentionTime(1, TimeUnit.HOURS);
        config.setRetentionSizeInMB(-1);
        return config;
    }

    @BeforeMethod(alwaysRun = true)
    public void buildFixture() throws Exception {
        ServiceConfiguration svcConfig = new ServiceConfiguration();
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setClusterName("test");
        svcConfig.setSystemTopicEnabled(false);
        svcConfig.setTopicLevelPoliciesEnabled(false);
        // Keep the retry cadence short so "the read is retried" assertions don't wait on the production
        // backoff.
        svcConfig.setDispatcherRetryBackoffInitialTimeInMs(1);
        svcConfig.setDispatcherRetryBackoffMaxTimeInMs(10);

        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .config(svcConfig)
                .spyByDefault()
                .managedLedgerClients(bkc, factory)
                .build();
        brokerService = pulsarTestContext.getBrokerService();

        ledger = (ManagedLedgerImpl) factory.open("read-slots-" + System.nanoTime(),
                initManagedLedgerConfig(new ManagedLedgerConfig()));
        topic = new PersistentTopic(TOPIC, ledger, brokerService);
        // Open the cursor after topic construction so the topic does not auto-create a second subscription
        // sharing this cursor.
        realCursor = (ManagedCursorImpl) ledger.openCursor("sub");
        cursorProxy = mock(ManagedCursor.class,
                withSettings().defaultAnswer(AdditionalAnswers.delegatesTo(realCursor)));

        subscription = new PersistentSubscription(topic, "sub", realCursor, false);
        dispatcher = new PersistentDispatcherMultipleConsumers(topic, cursorProxy, subscription);

        deliveries.clear();
        swallowedRead.set(null);
        consumer = newMockConsumer("c1");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDownFixture() {
        try {
            if (realCursor != null && !realCursor.isClosed()) {
                realCursor.close();
            }
        } catch (Exception ignore) {
            // best-effort cleanup
        }
        try {
            if (ledger != null) {
                ledger.close();
            }
        } catch (Exception ignore) {
            // best-effort cleanup
        }
        try {
            if (pulsarTestContext != null) {
                pulsarTestContext.close();
            }
        } catch (Exception ignore) {
            // best-effort cleanup
        }
        realCursor = null;
        cursorProxy = null;
        ledger = null;
        brokerService = null;
        pulsarTestContext = null;
    }

    private Consumer newMockConsumer(String consumerName) {
        TransportCnx cnx = mock(TransportCnx.class);
        doReturn(true).when(cnx).isActive();
        Consumer mockConsumer = mock(Consumer.class);
        doReturn(cnx).when(mockConsumer).cnx();
        doReturn(1000).when(mockConsumer).getAvailablePermits();
        doReturn(1).when(mockConsumer).getAvgMessagesPerEntry();
        doReturn(true).when(mockConsumer).isWritable();
        doReturn(false).when(mockConsumer).readCompacted();
        doReturn(false).when(mockConsumer).isPreciseDispatcherFlowControl();
        doReturn(false).when(mockConsumer).isBlocked();
        doReturn(consumerName).when(mockConsumer).consumerName();
        doReturn(0).when(mockConsumer).getPriorityLevel();
        doReturn(0L).when(mockConsumer).getConsumerEpoch();
        doReturn(SubType.Shared).when(mockConsumer).subType();
        doReturn(new ConsumerStatsImpl()).when(mockConsumer).getStats();
        doAnswer(inv -> {
            List<Entry> entries = inv.getArgument(0);
            for (Entry entry : entries) {
                deliveries.add(entry.getPosition());
                entry.release();
            }
            EntryBatchSizes batchSizes = inv.getArgument(1);
            if (batchSizes != null) {
                batchSizes.recyle();
            }
            EntryBatchIndexesAcks batchIndexesAcks = inv.getArgument(2);
            if (batchIndexesAcks != null) {
                batchIndexesAcks.recycle();
            }
            return ImmediateEventExecutor.INSTANCE.newSucceededFuture(null);
        }).when(mockConsumer).sendMessages(any(), any(), any(), anyInt(), anyLong(), anyLong(), any());
        return mockConsumer;
    }

    /** Serializes a Pulsar message, so the real dispatch tail can parse its metadata. */
    private ByteBuf serializeMessage(String value) {
        MessageMetadata metadata = new MessageMetadata()
                .setSequenceId(msgCounter.incrementAndGet())
                .setProducerName("testProducer")
                .setPublishTime(System.currentTimeMillis());
        ByteBuf payload = Unpooled.copiedBuffer(value.getBytes(UTF_8));
        try {
            return Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        } finally {
            payload.release();
        }
    }

    private Position publish() throws Exception {
        ByteBuf message = serializeMessage("m" + (msgCounter.get() + 1));
        try {
            return ledger.addEntry(ByteBufUtil.getBytes(message));
        } finally {
            message.release();
        }
    }

    private void awaitDelivered(int expected) {
        Awaitility.await("dispatcher should deliver " + expected + " entries")
                .atMost(Duration.ofSeconds(DELIVERY_TIMEOUT_SECONDS))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> deliveries.size() >= expected);
    }

    /**
     * Swallows the next Normal read before it reaches the cursor, reproducing the #26454 state: the slot is
     * reserved, the cursor owns nothing, and no completion will ever arrive.
     */
    private void swallowNextNormalRead() {
        AtomicBoolean swallowed = new AtomicBoolean();
        doAnswer(inv -> {
            if (swallowed.compareAndSet(false, true)) {
                swallowedRead.set(inv.getArgument(3));
                return null;
            }
            return delegateReadToRealCursor(inv);
        }).when(cursorProxy).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());
    }

    /**
     * Forwards an {@code asyncReadEntriesWithSkipOrWait} invocation to the real cursor. The proxy is an
     * interface mock, so {@code callRealMethod()} is not available on it.
     */
    private Object delegateReadToRealCursor(InvocationOnMock inv) {
        realCursor.asyncReadEntriesWithSkipOrWait(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2),
                inv.getArgument(3), inv.getArgument(4), inv.getArgument(5));
        return null;
    }

    /** Runs the periodic check the way {@code PersistentTopic.updateRates} does with the heuristic on. */
    private boolean stuckCheck() {
        return dispatcher.checkAndUnblockIfStuck();
    }

    /** Runs the periodic check the way {@code PersistentTopic.updateRates} does by default. */
    private boolean repairCheck() {
        return dispatcher.checkAndRepairInconsistentReadState();
    }

    // -----------------------------------------------------------------------------------------------
    // The slot model
    // -----------------------------------------------------------------------------------------------

    @Test(groups = "broker")
    public void testSharedKeepsASlotPerReadType() throws Exception {
        ReadContext normal = dispatcher.reserveRead(ReadType.Normal);
        assertThat(normal).isNotNull();
        assertThat(dispatcher.isHavePendingRead()).isTrue();
        assertThat(dispatcher.isHavePendingReplayRead()).isFalse();

        // A redelivery can be fetched while a sequential read is outstanding.
        ReadContext replay = dispatcher.reserveRead(ReadType.Replay);
        assertThat(replay).as("Shared keeps the two read types in separate slots").isNotNull();
        assertThat(dispatcher.isHavePendingReplayRead()).isTrue();

        assertThat(dispatcher.reserveRead(ReadType.Normal)).as("a slot holds only one read").isNull();
        assertThat(dispatcher.reserveRead(ReadType.Replay)).isNull();

        // Each completion releases only its own slot.
        assertThat(dispatcher.releaseIfCurrent(normal)).isTrue();
        assertThat(dispatcher.isHavePendingRead()).isFalse();
        assertThat(dispatcher.isHavePendingReplayRead()).isTrue();
        assertThat(dispatcher.releaseIfCurrent(replay)).isTrue();
        assertThat(dispatcher.isHavePendingReplayRead()).isFalse();

        // A read that no longer owns its slot releases nothing.
        assertThat(dispatcher.releaseIfCurrent(normal)).isFalse();
    }

    @Test(groups = "broker")
    public void testReadIdsAreUnique() {
        ReadContext first = dispatcher.reserveRead(ReadType.Normal);
        dispatcher.releaseIfCurrent(first);
        ReadContext second = dispatcher.reserveRead(ReadType.Normal);
        assertThat(second.id()).as("ids are never reused").isNotEqualTo(first.id());
        assertThat(dispatcher.isCurrentRead(second)).isTrue();
        assertThat(dispatcher.isCurrentRead(first))
                .as("a released read never matches the slot again").isFalse();
    }

    @Test(groups = "broker")
    public void testKeySharedSharesOneSlotBetweenBothReadTypes() throws Exception {
        PersistentStickyKeyDispatcherMultipleConsumers keyShared = keySharedDispatcher();
        try {
            ReadContext normal = keyShared.reserveRead(ReadType.Normal);
            assertThat(normal).isNotNull();
            assertThat(keyShared.reserveRead(ReadType.Replay))
                    .as("ordered delivery cannot tolerate two cursor operations at once").isNull();
            assertThat(keyShared.isHavePendingRead()).isTrue();
            assertThat(keyShared.isHavePendingReplayRead())
                    .as("the slot holds a Normal read, so the stats must not claim a replay read").isFalse();

            keyShared.releaseIfCurrent(normal);
            ReadContext replay = keyShared.reserveRead(ReadType.Replay);
            assertThat(replay).isNotNull();
            assertThat(keyShared.reserveRead(ReadType.Normal))
                    .as("a replay read blocks a normal read too").isNull();
            assertThat(keyShared.isHavePendingReplayRead()).isTrue();
            assertThat(keyShared.isHavePendingRead()).isFalse();
        } finally {
            keyShared.close();
        }
    }

    private PersistentStickyKeyDispatcherMultipleConsumers keySharedDispatcher() {
        return new PersistentStickyKeyDispatcherMultipleConsumers(topic, cursorProxy, subscription,
                brokerService.pulsar().getConfiguration(),
                new org.apache.pulsar.common.api.proto.KeySharedMeta()
                        .setKeySharedMode(org.apache.pulsar.common.api.proto.KeySharedMode.AUTO_SPLIT));
    }

    // -----------------------------------------------------------------------------------------------
    // Rollback when arming throws
    // -----------------------------------------------------------------------------------------------

    @Test(groups = "broker")
    public void testNormalSlotIsReleasedWhenArmingTheReadThrows() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        AtomicBoolean thrown = new AtomicBoolean();
        doAnswer(inv -> {
            if (thrown.compareAndSet(false, true)) {
                throw new IllegalStateException("simulated failure while arming the read");
            }
            return delegateReadToRealCursor(inv);
        }).when(cursorProxy).asyncReadEntriesWithSkipOrWait(anyInt(), anyLong(), any(), any(), any(), any());

        dispatcher.addConsumer(consumer).get();
        assertThatThrownBy(() -> dispatcher.readMoreEntries())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("simulated failure while arming the read");

        assertThat(dispatcher.isHavePendingRead())
                .as("the slot must be released when the read never reached the cursor").isFalse();
        // The rescheduled read drains the backlog without any further nudging.
        awaitDelivered(5);
    }

    @Test(groups = "broker")
    public void testReplaySlotIsReleasedWhenArmingTheReplayThrows() throws Exception {
        List<Position> published = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            published.add(publish());
        }
        AtomicBoolean thrown = new AtomicBoolean();
        // The dispatcher picks the ordered overload when delayed delivery is enabled on the topic, so both
        // are wired to the same injection.
        doAnswer(inv -> {
            if (thrown.compareAndSet(false, true)) {
                throw new IllegalStateException("simulated failure while arming the replay read");
            }
            return realCursor.asyncReplayEntries(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2));
        }).when(cursorProxy).asyncReplayEntries(any(), any(), any());
        doAnswer(inv -> {
            if (thrown.compareAndSet(false, true)) {
                throw new IllegalStateException("simulated failure while arming the replay read");
            }
            return realCursor.asyncReplayEntries(inv.getArgument(0), inv.getArgument(1), inv.getArgument(2),
                    inv.getArgument(3));
        }).when(cursorProxy).asyncReplayEntries(any(), any(), any(), anyBoolean());

        dispatcher.addConsumer(consumer).get();
        // A negative acknowledgement is what puts a position into the replay queue; the dispatcher then
        // triggers the replay read asynchronously, which is where the injected failure fires.
        dispatcher.redeliverUnacknowledgedMessages(consumer, List.of(published.get(0)));

        Awaitility.await("the failure injection should fire")
                .atMost(Duration.ofSeconds(DELIVERY_TIMEOUT_SECONDS))
                .pollInterval(Duration.ofMillis(10))
                .untilTrue(thrown);
        // Without the rollback the replay slot would stay reserved and calculateToRead() would refuse every
        // later read.
        awaitDelivered(5);
        assertThat(dispatcher.isHavePendingReplayRead()).isFalse();
    }

    @Test(groups = "broker")
    public void testNormalSlotIsReleasedWhenFailureHandlingThrows() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        dispatcher.addConsumer(consumer).get();
        dispatcher.readMoreEntries();
        Awaitility.await("the first read should complete")
                .atMost(Duration.ofSeconds(DELIVERY_TIMEOUT_SECONDS))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> !deliveries.isEmpty());

        ReadContext read = reserveNormalRead();
        assertThat(dispatcher.isHavePendingRead()).isTrue();
        // Make the failure-handling body blow up, then deliver a read failure the way the cursor does.
        doAnswer(inv -> {
            throw new IllegalStateException("simulated failure while handling a failed read");
        }).when(cursorProxy).hasBacklog(anyBoolean());

        assertThatThrownBy(() -> dispatcher.readEntriesFailed(
                new ManagedLedgerException.NoMoreEntriesToReadException("no more entries"), read))
                .isInstanceOf(IllegalStateException.class);

        assertThat(dispatcher.isHavePendingRead())
                .as("a failed read releases its slot even when failure handling throws").isFalse();
    }

    /** Takes over the Normal slot, standing in for the read that is currently outstanding. */
    private ReadContext reserveNormalRead() {
        synchronized (dispatcher) {
            dispatcher.releaseIfCurrent(dispatcher.readSlot(ReadType.Normal));
            return dispatcher.reserveRead(ReadType.Normal);
        }
    }

    // -----------------------------------------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------------------------------------

    @Test(groups = "broker")
    public void testStuckCheckRecoversASlotTheCursorKnowsNothingAbout() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        swallowNextNormalRead();
        dispatcher.addConsumer(consumer).get();

        dispatcher.readMoreEntries();
        assertThat(dispatcher.isHavePendingRead())
                .as("the swallowed read leaves its slot reserved").isTrue();
        assertThat(realCursor.hasOutstandingReadOperation())
                .as("the cursor owns no read: this is the state reported in #26454").isFalse();
        assertThat(realCursor.hasPendingReadRequest()).isFalse();
        assertThat(realCursor.getPendingReadOpsCount()).isZero();

        // Every later trigger is a no-op while the slot is held.
        dispatcher.readMoreEntries();
        assertThat(deliveries).as("a dispatcher holding a dead slot delivers nothing").isEmpty();

        // First call only primes the cursor's read-position sample.
        assertThat(stuckCheck()).as("first check only samples the read position").isFalse();
        // One observation is not enough: hasOutstandingReadOperation() dips to false while the cursor moves
        // a read between its internal stages.
        assertThat(stuckCheck()).as("a single observation must not be acted on").isFalse();
        assertThat(dispatcher.isHavePendingRead()).isTrue();
        assertThat(stuckCheck()).as("the dead slot should be recovered").isTrue();

        awaitDelivered(5);
    }

    @Test(groups = "broker")
    public void testRecoveryRunsWithStuckSubscriptionUnblockingDisabled() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        swallowNextNormalRead();
        dispatcher.addConsumer(consumer).get();

        dispatcher.readMoreEntries();
        assertThat(dispatcher.isHavePendingRead()).isTrue();

        assertThat(repairCheck()).isFalse();
        assertThat(repairCheck()).isFalse();
        assertThat(repairCheck()).as("the dead slot should be recovered").isTrue();

        awaitDelivered(5);
    }

    @Test(groups = "broker")
    public void testRecoveryDoesNotApplyTheUnblockHeuristic() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        dispatcher.addConsumer(consumer).get();
        // No slot is held, and there is a backlog with permits: exactly what checkAndUnblockIfStuck acts on,
        // and what checkAndRepairInconsistentReadState must not.
        assertThat(dispatcher.isHavePendingRead()).isFalse();

        for (int i = 0; i < 10; i++) {
            assertThat(repairCheck()).as("the repair must not force-issue reads").isFalse();
        }
    }

    @Test(groups = "broker")
    public void testRecoveryDoesNotFireWhileTheCursorOwnsARead() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        swallowNextNormalRead();
        // Report a read the cursor still owns, the way an in-flight or parked read does.
        doReturn(true).when(cursorProxy).hasOutstandingReadOperation();
        dispatcher.addConsumer(consumer).get();

        dispatcher.readMoreEntries();
        assertThat(dispatcher.isHavePendingRead()).isTrue();

        for (int i = 0; i < 10; i++) {
            assertThat(stuckCheck()).as("no recovery while the cursor owns a read").isFalse();
        }
        assertThat(dispatcher.isHavePendingRead()).isTrue();
        assertThat(deliveries).isEmpty();
    }

    @Test(groups = "broker")
    public void testRecoveryRewindsTheCursorSoDiscardedEntriesAreReadAgain() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        dispatcher.addConsumer(consumer).get();
        dispatcher.readMoreEntries();
        awaitDelivered(5);
        // Nothing was acknowledged, so the mark-delete position is still behind everything delivered.
        Position firstUnacked = ledger.getNextValidPosition(realCursor.getMarkDeletedPosition());
        assertThat(realCursor.getReadPosition()).isGreaterThan(firstUnacked);

        // Park the subscription with no read outstanding, then give it a backlog nothing is driving.
        Awaitility.await("a tail-wait read should be armed")
                .atMost(Duration.ofSeconds(DELIVERY_TIMEOUT_SECONDS))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> dispatcher.isHavePendingRead() && realCursor.hasPendingReadRequest());
        dispatcher.cancelPendingRead();
        assertThat(dispatcher.isHavePendingRead()).isFalse();
        for (int i = 0; i < 5; i++) {
            publish();
        }

        // Strand a slot with the cursor idle.
        swallowNextNormalRead();
        dispatcher.readMoreEntries();
        assertThat(dispatcher.isHavePendingRead()).isTrue();
        assertThat(realCursor.hasOutstandingReadOperation()).isFalse();

        deliveries.clear();
        assertThat(stuckCheck()).isFalse();
        assertThat(stuckCheck()).isFalse();
        assertThat(stuckCheck()).as("the dead slot should be recovered").isTrue();

        // The rewind is what lets a superseded completion release its entries: everything still unacked is
        // read again, starting from just after the mark-delete position.
        awaitDelivered(10);
        assertThat(deliveries.get(0))
                .as("delivery resumes from just after the mark-delete position")
                .isEqualTo(firstUnacked);
    }

    @Test(groups = "broker")
    public void testASupersededCompletionIsDiscardedInsteadOfDispatched() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        swallowNextNormalRead();
        dispatcher.addConsumer(consumer).get();

        dispatcher.readMoreEntries();
        ReadContext supersededRead = swallowedRead.get();
        assertThat(supersededRead).as("the swallowed read's context").isNotNull();

        assertThat(stuckCheck()).isFalse();
        assertThat(stuckCheck()).isFalse();
        assertThat(stuckCheck()).as("the dead slot should be recovered").isTrue();
        awaitDelivered(5);

        Awaitility.await("a replacement read should be outstanding")
                .atMost(Duration.ofSeconds(DELIVERY_TIMEOUT_SECONDS))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> dispatcher.isHavePendingRead() && realCursor.hasOutstandingReadOperation());

        // The superseded read turns up after all. Its entries were re-read by the replacement after the
        // rewind, so dispatching them here would duplicate the delivery.
        int deliveredBefore = deliveries.size();
        ByteBuf lateMessage = serializeMessage("late");
        Entry lateEntry;
        try {
            lateEntry = EntryImpl.create(realCursor.getMarkDeletedPosition().getLedgerId(), 9999, lateMessage);
        } finally {
            lateMessage.release();
        }
        dispatcher.readEntriesComplete(new ArrayList<>(List.of(lateEntry)), supersededRead);

        assertThat(dispatcher.isHavePendingRead())
                .as("a superseded completion must not release the replacement read").isTrue();
        assertThat(deliveries.size())
                .as("a superseded completion must not dispatch").isEqualTo(deliveredBefore);
    }

    @Test(groups = "broker")
    public void testASupersededFailureDoesNotReleaseTheReplacementRead() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        swallowNextNormalRead();
        dispatcher.addConsumer(consumer).get();

        dispatcher.readMoreEntries();
        ReadContext supersededRead = swallowedRead.get();
        assertThat(supersededRead).isNotNull();

        assertThat(stuckCheck()).isFalse();
        assertThat(stuckCheck()).isFalse();
        assertThat(stuckCheck()).as("the dead slot should be recovered").isTrue();
        awaitDelivered(5);

        Awaitility.await("a replacement read should be outstanding")
                .atMost(Duration.ofSeconds(DELIVERY_TIMEOUT_SECONDS))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> dispatcher.isHavePendingRead() && realCursor.hasOutstandingReadOperation());

        dispatcher.readEntriesFailed(new ManagedLedgerException("simulated late read failure"), supersededRead);
        assertThat(dispatcher.isHavePendingRead())
                .as("a superseded failure must not release the replacement read").isTrue();
    }
}
