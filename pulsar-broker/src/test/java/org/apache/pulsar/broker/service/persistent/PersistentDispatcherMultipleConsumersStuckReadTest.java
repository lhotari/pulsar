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
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.TransportCnx;
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
 * Regression tests for a Shared subscription that stops dispatching forever because
 * {@code PersistentDispatcherMultipleConsumers.havePendingRead} is left set while the cursor owns no read
 * operation at all.
 *
 * <p><b>The reported state</b> (apache/pulsar#26454): a Shared subscription with a 600k-entry backlog and a
 * connected consumer holding 1676 free permits stopped dispatching for 26 hours, until the topic partition
 * was unloaded. {@code topics stats-internal} showed the inconsistent triple
 * {@code subscriptionHavePendingRead=true}, {@code pendingReadOps=0}, {@code waitingReadOp=false}: the
 * dispatcher believed a Normal read was in flight while {@link ManagedCursorImpl} held neither a read parked
 * waiting for entries nor a read in flight. Nothing below the dispatcher can clear the flag in that state,
 * so {@link PersistentDispatcherMultipleConsumers#readMoreEntries()} rejected every subsequent read at its
 * {@code doesntHavePendingRead()} guard, and {@code checkAndUnblockIfStuck()} could not help either since it
 * required {@code !havePendingRead} — the one variant that never self-heals was the one it excluded.
 *
 * <p><b>How the flag gets stranded.</b> {@code havePendingRead} is assigned {@code true} before the read is
 * handed to the cursor, and the calls in between ({@code updateMinReplayedPosition()},
 * {@code topic.getMaxReadPosition()}, {@code createReadEntriesSkipConditionForNormalRead()} and the
 * cursor's own pre-registration work, which includes a {@code checkArgument} and an {@code OpReadEntry}
 * allocation) are all allowed to throw synchronously. {@code readMoreEntries()} has no rollback, so a
 * throwable escaping that window unwinds into the broker executor with the flag set, the cursor holding
 * nothing, and {@code pendingReadOps} still zero — exactly the captured tuple. The throwable is logged by
 * the executor ("A task raised an exception" / "Error while running task"), which carries no topic name, so
 * a topic-scoped log search finds nothing — matching the report of no WARN/ERROR for the topic.
 *
 * <p><b>What is fixed.</b> Two independent things:
 * <ol>
 *   <li>{@code readMoreEntries()} rolls the flag back and reschedules when arming a Normal or a Replay read
 *       throws, so that window can no longer strand the dispatcher; and</li>
 *   <li>{@code checkAndUnblockIfStuck()} now also recognises "the dispatcher believes a read is pending but
 *       the cursor owns none" and repairs it, so the state is recoverable however it is reached. Because
 *       {@link ManagedCursor#hasOutstandingReadOperation()} is a sampled signal, the repair only fires after
 *       the inconsistency has been observed on consecutive checks with the cursor's read position frozen.</li>
 * </ol>
 *
 * <p><b>Fidelity.</b> The managed ledger, cursor, topic, subscription and dispatcher are real. The
 * dispatcher's {@code cursor} is a delegating proxy around the real {@link ManagedCursorImpl} so a single
 * call can be made to throw or to be swallowed, reproducing "the read never reached the cursor" without
 * touching the cursor's own state — every other call, including the {@code hasOutstandingReadOperation()}
 * that the repair consults, goes to the real cursor. Only the metadata-parsing filter step is stubbed,
 * because the published payloads are raw bytes rather than serialized Pulsar messages.
 */
@CustomLog
public class PersistentDispatcherMultipleConsumersStuckReadTest extends MockedBookKeeperTestCase {

    private static final String TOPIC = "persistent://prop/ns/shared-stuck-read";
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
        // Keep the retry cadence short so the "the read is retried after a failure" assertions don't have to
        // wait on the production backoff.
        svcConfig.setDispatcherRetryBackoffInitialTimeInMs(1);
        svcConfig.setDispatcherRetryBackoffMaxTimeInMs(10);

        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .config(svcConfig)
                .spyByDefault()
                .managedLedgerClients(bkc, factory)
                .build();
        brokerService = pulsarTestContext.getBrokerService();

        ledger = (ManagedLedgerImpl) factory.open("shared-stuck-read-" + System.nanoTime(),
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

    /** Appends a properly serialized Pulsar message, so the real dispatch tail can parse its metadata. */
    private Position publish() throws Exception {
        MessageMetadata metadata = new MessageMetadata()
                .setSequenceId(msgCounter.incrementAndGet())
                .setProducerName("testProducer")
                .setPublishTime(System.currentTimeMillis());
        ByteBuf payload = Unpooled.copiedBuffer(("m" + msgCounter.get()).getBytes(UTF_8));
        ByteBuf message = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        try {
            return ledger.addEntry(ByteBufUtil.getBytes(message));
        } finally {
            message.release();
            payload.release();
        }
    }

    private void awaitDelivered(int expected) {
        Awaitility.await("dispatcher should deliver " + expected + " entries")
                .atMost(Duration.ofSeconds(DELIVERY_TIMEOUT_SECONDS))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> deliveries.size() >= expected);
    }

    /**
     * Strands {@code havePendingRead} exactly the way the incident did: the next Normal read is swallowed
     * before it reaches the cursor, so the dispatcher believes a read is outstanding while the cursor owns
     * none and no completion will ever arrive.
     */
    private void strandNextNormalRead() {
        AtomicBoolean swallowed = new AtomicBoolean();
        doAnswer(inv -> {
            if (swallowed.compareAndSet(false, true)) {
                // Drop the read on the floor: no op registered at the cursor, no callback, ever.
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

    /**
     * Runs the periodic check the way {@code PersistentTopic.updateRates} does when
     * {@code unblockStuckSubscriptionEnabled} is on.
     */
    private boolean stuckCheck() {
        return dispatcher.checkAndUnblockIfStuck();
    }

    /**
     * Runs the periodic check the way {@code PersistentTopic.updateRates} does when
     * {@code unblockStuckSubscriptionEnabled} is off (the default).
     */
    private boolean repairCheck() {
        return dispatcher.checkAndRepairInconsistentReadState();
    }

    // -----------------------------------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------------------------------

    /**
     * The headline regression test for #26454: once {@code havePendingRead} is stranded, the subscription is
     * dead — and the periodic stuck check must bring it back.
     */
    @Test
    public void testStuckCheckRecoversStrandedPendingRead() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        strandNextNormalRead();
        dispatcher.addConsumer(consumer).get();

        // This is the read that gets swallowed: the dispatcher now believes it is outstanding, the cursor
        // never saw it, and no completion will ever arrive.
        dispatcher.readMoreEntries();
        assertThat(dispatcher.isHavePendingRead())
                .as("the swallowed read should strand havePendingRead").isTrue();
        assertThat(realCursor.hasOutstandingReadOperation())
                .as("the cursor owns no read: this is the inconsistent triple from the report")
                .isFalse();
        assertThat(realCursor.hasPendingReadRequest()).isFalse();
        assertThat(realCursor.getPendingReadOpsCount()).isZero();

        // Every later trigger is a no-op while the flag is set.
        dispatcher.readMoreEntries();
        assertThat(deliveries).as("a stranded dispatcher delivers nothing").isEmpty();

        // First call only primes the cursor's read-position sample, so it cannot conclude anything yet.
        assertThat(stuckCheck()).as("first check only samples the read position").isFalse();
        // Second call observes the inconsistency for the first time; one sample is not enough because
        // hasOutstandingReadOperation() dips to false while the cursor hands a read between its stages.
        assertThat(stuckCheck()).as("a single observation must not be acted on").isFalse();
        assertThat(dispatcher.isHavePendingRead()).isTrue();
        // Second consecutive observation with a frozen read position: repair.
        assertThat(stuckCheck()).as("the stale flag should be repaired").isTrue();

        awaitDelivered(5);
        // The invariant is restored: after the repair the dispatcher believes a read is pending only while
        // the cursor actually owns one (it re-arms a tail-wait read once the backlog is drained).
        Awaitility.await("dispatcher and cursor read state should agree again")
                .atMost(Duration.ofSeconds(DELIVERY_TIMEOUT_SECONDS))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> !dispatcher.isHavePendingRead() || realCursor.hasOutstandingReadOperation());
    }

    /**
     * The repair must also run with {@code unblockStuckSubscriptionEnabled} left at its default of
     * {@code false}: unlike force-issuing a read on a dispatcher that believes it is idle, this state cannot
     * recover on its own, so gating it behind an opt-in flag would leave most brokers permanently stuck.
     */
    @Test
    public void testStrandedPendingReadIsRepairedWithStuckSubscriptionUnblockingDisabled() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        strandNextNormalRead();
        dispatcher.addConsumer(consumer).get();

        dispatcher.readMoreEntries();
        assertThat(dispatcher.isHavePendingRead()).isTrue();
        assertThat(realCursor.hasOutstandingReadOperation()).isFalse();

        assertThat(repairCheck()).as("first check only samples the read position").isFalse();
        assertThat(repairCheck()).as("a single observation must not be acted on").isFalse();
        assertThat(repairCheck()).as("the stale flag should be repaired").isTrue();

        awaitDelivered(5);
    }

    /**
     * With the heuristic disabled, a dispatcher that merely believes it has no read outstanding must be left
     * alone: that is the opt-in {@code unblockStuckSubscriptionEnabled} behaviour, not an invariant repair.
     */
    @Test
    public void testRepairCheckDoesNotApplyTheUnblockHeuristic() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        dispatcher.addConsumer(consumer).get();
        // havePendingRead is false and there is a backlog with permits: exactly what checkAndUnblockIfStuck
        // acts on, and what checkAndRepairInconsistentReadState must not.
        assertThat(dispatcher.isHavePendingRead()).isFalse();

        for (int i = 0; i < 10; i++) {
            assertThat(repairCheck()).as("the repair must not force-issue reads").isFalse();
        }
    }

    /**
     * {@code readEntriesFailed} is the failed read's only completion, so it must release the flag before
     * anything that can throw. It previously cleared it near the end, after {@code cursor.hasBacklog},
     * {@code checkAndApplyReachedEndOfTopicOrTopicMigration} and {@code cursor.rewind} -- a throwable from any
     * of those left the dispatcher believing the failed read was still outstanding, i.e. the same permanent
     * stall by a different route.
     */
    @Test
    public void testPendingReadFlagIsClearedWhenFailureHandlingThrows() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        dispatcher.addConsumer(consumer).get();
        dispatcher.readMoreEntries();
        Awaitility.await("the first read should complete")
                .atMost(Duration.ofSeconds(DELIVERY_TIMEOUT_SECONDS))
                .pollInterval(Duration.ofMillis(10))
                .until(() -> !deliveries.isEmpty());

        // Make the failure-handling body blow up, then deliver a read failure the way the managed ledger does.
        doAnswer(inv -> {
            throw new IllegalStateException("simulated failure while handling a failed read");
        }).when(cursorProxy).hasBacklog(anyBoolean());

        assertThatThrownBy(() -> dispatcher.readEntriesFailed(
                new ManagedLedgerException.NoMoreEntriesToReadException("no more entries"),
                PersistentDispatcherMultipleConsumers.ReadType.Normal))
                .isInstanceOf(IllegalStateException.class);

        assertThat(dispatcher.isHavePendingRead())
                .as("a failed read must release the flag even when failure handling throws").isFalse();
    }

    /**
     * The repair must never fire while a read really is outstanding, or it would let the dispatcher run two
     * concurrent Normal reads.
     */
    @Test
    public void testStuckCheckDoesNotClearAGenuinelyPendingRead() throws Exception {
        for (int i = 0; i < 5; i++) {
            publish();
        }
        strandNextNormalRead();
        // Report a read that the cursor still owns, the way an in-flight or parked read does.
        doReturn(true).when(cursorProxy).hasOutstandingReadOperation();
        dispatcher.addConsumer(consumer).get();

        dispatcher.readMoreEntries();
        assertThat(dispatcher.isHavePendingRead()).isTrue();

        for (int i = 0; i < 10; i++) {
            assertThat(stuckCheck()).as("no repair while the cursor owns a read").isFalse();
        }
        assertThat(dispatcher.isHavePendingRead())
                .as("a genuinely pending read must keep its flag").isTrue();
        assertThat(deliveries).isEmpty();
    }

    /**
     * A throwable escaping between {@code havePendingRead = true} and the cursor accepting the read must not
     * strand the dispatcher, and the read must be retried.
     */
    @Test
    public void testPendingReadFlagIsRolledBackWhenArmingTheReadThrows() throws Exception {
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
                .as("the flag must be rolled back when the read never reached the cursor").isFalse();
        assertThat(thrown).as("the failure injection must have fired").isTrue();

        // The rescheduled read drains the backlog without any further nudging.
        awaitDelivered(5);
    }

    /**
     * The same rollback for the replay read: a stranded {@code havePendingReplayRead} blocks every later read
     * as well, because {@code calculateToRead} refuses to read while a replay is believed to be in flight.
     */
    @Test
    public void testPendingReplayReadFlagIsRolledBackWhenArmingTheReplayThrows() throws Exception {
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
        // The rescheduled read replays the nacked position and drains the rest of the backlog. Without the
        // rollback, havePendingReplayRead would stay set and calculateToRead() would refuse every later read.
        awaitDelivered(5);
        assertThat(dispatcher.isHavePendingReplayRead())
                .as("the replay flag must be rolled back when the replay never reached the cursor").isFalse();
    }
}
