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

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerTest;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.AbstractReplicator;
import org.apache.pulsar.broker.service.BrokerServiceInternalMethodInvoker;
import org.apache.pulsar.broker.service.OneWayReplicatorTestBase;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.InFlightTask;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.ProducerSendCallback;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.ReasonOfWaitForCursorRewinding;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.awaitility.Awaitility;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-replication")
public class PersistentReplicatorInflightTaskTest extends OneWayReplicatorTestBase {

    private final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
    private final String subscriptionName = "s1";

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
        createTopics();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    private void createTopics() throws Exception {
        admin2.topics().createNonPartitionedTopic(topicName);
        admin2.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
        admin1.topics().createNonPartitionedTopic(topicName);
        admin1.topics().createSubscription(topicName, subscriptionName, MessageId.earliest);
    }

    @Test
    public void testReplicationTaskStoppedAfterTopicClosed() throws Exception {
        // Close a topic, which has enabled replication.
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicName);
        waitReplicatorStarted(topicName, pulsar2);
        PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false)
                .join().get();
        PersistentReplicator replicator = (PersistentReplicator) topic.getReplicators().get(cluster2);
        admin1.topics().unload(topicName);

        // Inject a task into the "inFlightTasks" to calculate how many times the method "replicator.readMoreEntries"
        // has been called.
        AtomicInteger counter = new AtomicInteger();
        InFlightTask injectedTask = new InFlightTask(PositionFactory.create(1, 1), 1, replicator.getReplicatorId());
        injectedTask.setEntries(Collections.emptyList());
        InFlightTask spyTask = spy(injectedTask);
        replicator.inFlightTasks.add(spyTask);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                counter.incrementAndGet();
                return invocationOnMock.callRealMethod();
            }
        }).when(spyTask).getReadPos();

        // Verify: there is no scheduled task to retry to read entries to replicate.
        // Call "readMoreEntries" to make the issue happen.
        replicator.readMoreEntries();
        Thread.sleep(PersistentTopic.MESSAGE_RATE_BACKOFF_MS * 10);
        assertEquals(replicator.getState(), AbstractReplicator.State.Terminated);
        assertTrue(counter.get() <= 1);
    }

    @Test
    public void testReadEntriesFailedCompletesInFlightTaskAfterReplicatorTerminated() throws Exception {
        String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        CountDownLatch readStarted = new CountDownLatch(1);
        CountDownLatch failRead = new CountDownLatch(1);
        Producer<String> producer = null;
        try {
            admin1.topics().createNonPartitionedTopic(topicName);
            admin2.topics().createNonPartitionedTopic(topicName);
            producer = client1.newProducer(Schema.STRING).topic(topicName).create();
            producer.send("msg");

            PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false)
                    .join().get();
            ManagedLedgerImpl ml = (ManagedLedgerImpl) topic.getManagedLedger();
            ManagedLedgerTest.makeReadEntryProbFail(ml, () -> {
                readStarted.countDown();
                try {
                    if (!failRead.await(30, TimeUnit.SECONDS)) {
                        return new ManagedLedgerException("Timed out waiting to fail read entries");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return new ManagedLedgerException(e);
                }
                return new ManagedLedgerException.TooManyRequestsException("mocked read failure");
            });

            pulsar1.getConfig().setReplicationStartAt("earliest");
            admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
            assertTrue(readStarted.await(30, TimeUnit.SECONDS));

            PersistentReplicator replicator = (PersistentReplicator) topic.getReplicators().get(cluster2);
            Assert.assertNotNull(replicator, "Replicator should not be null");
            Assert.assertTrue(replicator.hasPendingRead());

            replicator.terminate();
            Assert.assertTrue(replicator.getState() != AbstractReplicator.State.Started);
            failRead.countDown();

            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                    Assert.assertFalse(replicator.hasPendingRead()));
        } finally {
            failRead.countDown();
            if (producer != null) {
                producer.close();
            }
            admin1.topics().delete(topicName, true);
            admin2.topics().delete(topicName, true);
        }
    }

    @Test
    public void testFailedPublishCompletesInFlightTask() throws Exception {
        PersistentReplicator replicator = spy(getReplicator(topicName));
        doNothing().when(replicator).beforeTerminateOrCursorRewinding(ReasonOfWaitForCursorRewinding.Failed_Publishing);
        doNothing().when(replicator).doRewindCursor(false);
        doNothing().when(replicator).readMoreEntries();

        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        try {
            InFlightTask task = new InFlightTask(PositionFactory.create(1, 1), 1, replicator.getReplicatorId());
            task.setEntries(Collections.singletonList(mock(Entry.class)));
            inFlightTasks.add(task);
            assertEquals(replicator.getPermitsIfNoPendingRead(), 999);

            ProducerSendCallback callback = ProducerSendCallback.create(replicator, mock(Entry.class), null, task);
            callback.sendComplete(new PulsarClientException.ProducerBlockedQuotaExceededException("mocked"), null);

            assertTrue(task.isDone());
            assertEquals(replicator.getPermitsIfNoPendingRead(), 1000);
        } finally {
            inFlightTasks.clear();
            inFlightTasks.addAll(originalTasks);
        }
    }

    /**
     * Reproduces a geo-replication stall on the cursor-rewind path.
     *
     * <p>When a cursor rewind happens while a cursor read has already been dispatched to bookies,
     * {@code cursor.cancelPendingReadRequest()} returns {@code false} (there is no registered waiting
     * read op to cancel), so {@code cancelPendingReadTasks()} only flags the in-flight task with
     * {@code skipReadResultDueToCursorRewind=true} without completing it. When that dispatched read
     * later completes, {@link PersistentReplicator#readEntriesComplete} hits the skip branch and must
     * still complete the task; otherwise the task stays {@code entries == null} forever,
     * {@link PersistentReplicator#hasPendingRead()} stays {@code true}, all permits remain occupied,
     * and reads never resume — replication stalls with backlog.
     */
    @Test
    public void testCursorRewindSkippedReadCompletesInFlightTask() throws Exception {
        PersistentReplicator replicator = spy(getReplicator(topicName));
        // Isolate the unit: don't issue a real cursor read, only verify reads are resumed.
        doNothing().when(replicator).readMoreEntries();

        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        try {
            int fullPermits = replicator.getPermitsIfNoPendingRead();
            assertTrue(fullPermits > 0, "precondition: replicator should have free permits");

            // A pending cursor read (entries == null) flagged to be skipped because of a cursor rewind
            // whose pending read could not be cancelled (cancelPendingReadRequest() returned false).
            InFlightTask task =
                    new InFlightTask(PositionFactory.create(1, 1), 1, replicator.getReplicatorId());
            task.setSkipReadResultDueToCursorRewind(true);
            inFlightTasks.add(task);

            // Precondition: the uncompleted task blocks all reads.
            assertTrue(replicator.hasPendingRead(), "precondition: task must look like a pending read");
            assertEquals(replicator.getPermitsIfNoPendingRead(), 0,
                    "precondition: pending read must occupy all permits");

            // The dispatched read finally completes; its result is discarded because of the rewind.
            replicator.readEntriesComplete(Collections.singletonList(mock(Entry.class)), task);

            // The task must be completed so it no longer blocks replication.
            assertTrue(task.isDone(), "skipped read must complete the in-flight task");
            assertFalse(replicator.hasPendingRead(),
                    "replication must not stay stuck on an uncompleted pending read");
            assertEquals(replicator.getPermitsIfNoPendingRead(), fullPermits,
                    "permits must be released after the skipped read completes");
            // Reads must be resumed once the slot is freed (dispatched on the broker executor to
            // avoid recursing in the read-completion thread).
            Awaitility.await().untilAsserted(() -> verify(replicator, atLeastOnce()).readMoreEntries());
        } finally {
            inFlightTasks.clear();
            inFlightTasks.addAll(originalTasks);
        }
    }

    /**
     * End-to-end reproduction of the cursor-rewind stall over a real two-cluster replication setup.
     *
     * <p>A real cursor read is held in flight (entries == null) while the cursor is rewound the same way
     * {@link ProducerSendCallback#sendComplete} does on a failed publish to the remote cluster:
     * {@code beforeTerminateOrCursorRewinding(Failed_Publishing)} followed by {@code doRewindCursor(false)}.
     * Because the read was already dispatched, {@code cursor.cancelPendingReadRequest()} returns false, so
     * {@code cancelPendingReadTasks()} only flags the task and does not complete it. When the dispatched
     * read then completes through the skip branch, the task must still be completed and reads resumed —
     * otherwise the replicator stalls and the backlog is never delivered to the remote cluster.
     *
     * <p>Before the fix this test times out: the task stays {@code entries == null}, {@code hasPendingRead()}
     * stays true, and the backlog never drains.
     */
    @Test
    public void testReplicationRecoversAfterPublishFailureRewindWithInflightRead() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + nonReplicatedNamespace + "/tp_");
        final int messageCount = 5;
        final CountDownLatch readBlocked = new CountDownLatch(1);
        final CountDownLatch releaseRead = new CountDownLatch(1);
        final AtomicBoolean blockNextRead = new AtomicBoolean(true);
        Producer<String> producer = null;
        Consumer<String> remoteConsumer = null;
        try {
            admin1.topics().createNonPartitionedTopic(topicName);
            admin2.topics().createNonPartitionedTopic(topicName);
            // Create the verifier subscription on the remote topic up front so replicated messages are
            // retained for the end-to-end assertion regardless of retention policy.
            admin2.topics().createSubscription(topicName, "e2e-verify", MessageId.earliest);

            // Produce a backlog before replication starts so the replicator must read it from the ledger.
            producer = client1.newProducer(Schema.STRING).topic(topicName).enableBatching(false).create();
            for (int i = 0; i < messageCount; i++) {
                producer.send("msg-" + i);
            }

            PersistentTopic topic = (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false)
                    .join().get();
            ManagedLedgerImpl ml = (ManagedLedgerImpl) topic.getManagedLedger();
            // Clear the entry cache and intercept ledger reads: block the first read so it stays in flight
            // (the in-flight task keeps entries == null), then let it and all later reads succeed.
            ManagedLedgerTest.makeReadEntryProbFail(ml, () -> {
                if (blockNextRead.compareAndSet(true, false)) {
                    readBlocked.countDown();
                    try {
                        releaseRead.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                // Never fail the read; it must complete successfully to reach the skip branch.
                return null;
            });

            // Start replication from earliest; the first read blocks inside the ledger read (in flight).
            pulsar1.getConfig().setReplicationStartAt("earliest");
            admin1.topics().setReplicationClusters(topicName, Arrays.asList(cluster1, cluster2));
            assertTrue(readBlocked.await(30, TimeUnit.SECONDS), "the replicator's cursor read should start");

            PersistentReplicator replicator = (PersistentReplicator) topic.getReplicators().get(cluster2);
            Assert.assertNotNull(replicator, "Replicator should not be null");
            // The dispatched read is in flight and occupies the only read slot.
            assertTrue(replicator.hasPendingRead());

            // Rewind the cursor exactly as a failed publish to the remote cluster does. The in-flight read
            // was already dispatched, so cancelPendingReadRequest() returns false and the task is only
            // flagged for skipping (not completed).
            replicator.beforeTerminateOrCursorRewinding(ReasonOfWaitForCursorRewinding.Failed_Publishing);
            replicator.doRewindCursor(false);

            // The in-flight read now completes successfully and is discarded by the skip branch.
            releaseRead.countDown();

            // Recovery: the freed slot lets reading resume, so the whole backlog is replicated and the
            // cursor mark-delete advances, draining the backlog to 0. Before the fix the leaked task keeps
            // getPermitsIfNoPendingRead() == 0, so no read is ever issued and the backlog stays at
            // messageCount (this await times out). Note: hasPendingRead() is intentionally NOT used as the
            // recovery signal because a healthy idle replicator also holds a pending "wait for new entries"
            // read, so it stays true even after a successful recovery.
            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                    assertEquals(replicator.getNumberOfEntriesInBacklog(), 0,
                            "replication backlog must drain after recovery"));

            // End-to-end: every message is delivered to the remote cluster.
            remoteConsumer = client2.newConsumer(Schema.STRING).topic(topicName)
                    .subscriptionName("e2e-verify")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            for (int i = 0; i < messageCount; i++) {
                Message<String> received = remoteConsumer.receive(30, TimeUnit.SECONDS);
                Assert.assertNotNull(received, "remote cluster should receive replicated message " + i);
            }
        } finally {
            releaseRead.countDown();
            if (producer != null) {
                producer.close();
            }
            if (remoteConsumer != null) {
                remoteConsumer.close();
            }
            admin1.topics().delete(topicName, true);
            admin2.topics().delete(topicName, true);
        }
    }

    @Test
    public void testCreateOrRecycleInFlightTaskIntoQueue() throws Exception {
        log.info("Starting testCreateOrRecycleInFlightTaskIntoQueue");

        // Get the replicator for the test topic
        PersistentReplicator replicator = getReplicator(topicName);
        Assert.assertNotNull(replicator, "Replicator should not be null");

        // Get access to the inFlightTasks list for verification
        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        Assert.assertNotNull(inFlightTasks, "InFlightTasks list should not be null");

        // Clear any existing tasks to start with a clean state
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        // Test Case 1: Create a new task when the queue is empty
        Position position1 = PositionFactory.create(1, 1);
        Assert.assertNotNull(position1, "Position should not be null");
        InFlightTask task1 = replicator.createOrRecycleInFlightTaskIntoQueue(position1, 10);
        // Verify a new task was created and added to the queue
        Assert.assertNotNull(task1, "Task should not be null");
        Assert.assertEquals(inFlightTasks.size(), 1, "Queue should have one task");
        Assert.assertEquals(task1.getReadPos(), position1, "Task should have the correct position");
        Assert.assertEquals(task1.getReadingEntries(), 10, "Task should have the correct reading entries count");
        // Mark the task as done to test recycling
        task1.setEntries(Collections.emptyList());

        // Test Case 2: Recycle an existing task
        Position position2 = PositionFactory.create(2, 2);
        Assert.assertNotNull(position2, "Position should not be null");
        InFlightTask task2 = replicator.createOrRecycleInFlightTaskIntoQueue(position2, 20);
        // Verify the task was recycled
        Assert.assertNotNull(task2, "Task should not be null");
        Assert.assertEquals(inFlightTasks.size(), 1, "Queue should still have one task");
        Assert.assertEquals(task2.getReadPos(), position2, "Task should have the updated position");
        Assert.assertEquals(task2.getReadingEntries(), 20, "Task should have the updated reading entries count");

        // Test Case 3: Create a new task when no tasks can be recycled
        task2.setEntries(null); // Make the task not done
        Position position3 = PositionFactory.create(3, 3);
        Assert.assertNotNull(position3, "Position should not be null");
        InFlightTask task3 = replicator.createOrRecycleInFlightTaskIntoQueue(position3, 30);
        // Verify a new task was created
        Assert.assertNotNull(task3, "Task should not be null");
        Assert.assertEquals(inFlightTasks.size(), 2, "Queue should have two tasks");
        Assert.assertEquals(task3.getReadPos(), position3, "Task should have the correct position");
        Assert.assertEquals(task3.getReadingEntries(), 30, "Task should have the correct reading entries count");

        // cleanup.
        log.info("Completed testCreateOrRecycleInFlightTaskIntoQueue");
        inFlightTasks.clear();
        inFlightTasks.addAll(originalTasks);
    }

    @Test
    public void testGetInflightMessagesCount() throws Exception {
        log.info("Starting testGetInflightMessagesCount");

        // Get the replicator for the test topic
        PersistentReplicator replicator = getReplicator(topicName);
        Assert.assertNotNull(replicator, "Replicator should not be null");

        // Get access to the inFlightTasks list for setup
        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        Assert.assertNotNull(inFlightTasks, "InFlightTasks list should not be null");

        // Save original tasks and clear for testing
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        try {
            // Test Case 1: no task.
            Assert.assertEquals(replicator.getInflightMessagesCount(), 0);

            // Test Case 2: cursor reading.
            Position position1 = PositionFactory.create(1, 1);
            InFlightTask task1 = new InFlightTask(position1, 3, "");
            inFlightTasks.add(task1);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 3);

            // Test Case 3: read completed.
            inFlightTasks.clear();
            Position position2 = PositionFactory.create(2, 2);
            InFlightTask task2 = new InFlightTask(position2, 3, "");
            task2.setEntries(Arrays.asList(mock(Entry.class), mock(Entry.class)));
            inFlightTasks.add(task2);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 2);

            // Test Case 4: Task with some completed entries
            task2.setCompletedEntries(1);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 1);

            // Test Case 5: Task with all entries completed
            task2.setCompletedEntries(2);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 0);

            // Test Case 6: Multiple tasks with different states
            // task2 has 0 in-flight (2 completed out of 2)
            // task3 has 2 in-flight (1 completed out of 3)
            Position position3 = PositionFactory.create(3, 3);
            InFlightTask task3 = new InFlightTask(position3, 4, "");
            task3.setEntries(Arrays.asList(mock(Entry.class), mock(Entry.class), mock(Entry.class)));
            task3.setCompletedEntries(1);
            inFlightTasks.add(task3);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 2);

            // Test Case 7: Multiple tasks with different states
            // task2 has 0 in-flight (2 completed out of 2)
            // task3 has 2 in-flight (1 completed out of 3)
            // task4 has 0 in-flight (empty readoutEntries)
            Position position4 = PositionFactory.create(4, 4);
            InFlightTask task4 = new InFlightTask(position4, 2, "");
            task4.setEntries(Collections.emptyList());
            inFlightTasks.add(task4);
            Assert.assertEquals(replicator.getInflightMessagesCount(), 2);

            log.info("Completed testGetInflightMessagesCount");
        } finally {
            // Restore original tasks
            inFlightTasks.clear();
            inFlightTasks.addAll(originalTasks);
        }
    }

    @Test
    public void testGetPermitsIfNoPendingRead() throws Exception {
        log.info("Starting testGetPermitsIfNoPendingRead");

        // Get the replicator for the test topic
        PersistentReplicator replicator = getReplicator(topicName);
        Assert.assertNotNull(replicator, "Replicator should not be null");

        // Get access to the inFlightTasks list for setup
        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        Assert.assertNotNull(inFlightTasks, "InFlightTasks list should not be null");

        // Save original tasks and clear for testing
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        try {
            // Test Case 1: Empty queue - should return producerQueueSize (1000)
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 1000,
                    "With empty queue, should return full producerQueueSize");

            // Test Case 2: Task with pending read (readPos != null && readoutEntries == null)
            Position position1 = PositionFactory.create(1, 1);
            InFlightTask pendingReadTask = new InFlightTask(position1, 5, "");
            // Don't set readoutEntries to simulate pending read
            inFlightTasks.add(pendingReadTask);
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 0,
                    "With pending read task, should return 0");

            // Test Case 3: Task with completed read but in-flight messages
            inFlightTasks.clear();
            Position position2 = PositionFactory.create(2, 2);
            InFlightTask completedReadTask = new InFlightTask(position2, 5, "");
            completedReadTask.setEntries(Arrays.asList(
                    mock(Entry.class), mock(Entry.class), mock(Entry.class)));
            inFlightTasks.add(completedReadTask);
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 1000 - 3,
                    "With completed read task, should return producerQueueSize - inflightMessages");

            // Test Case 4: Multiple tasks with no pending reads
            Position position3 = PositionFactory.create(3, 3);
            InFlightTask task2 = new InFlightTask(position3, 5, "");
            task2.setEntries(Arrays.asList(mock(Entry.class), mock(Entry.class)));
            task2.setCompletedEntries(1); // 1 in-flight message
            inFlightTasks.add(task2);
            // Now we have 3 + 1 = 4 in-flight messages
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 1000 - 4,
                    "With multiple tasks, should return producerQueueSize - total inflightMessages");

            // Test Case 5: Multiple tasks including one with pending read
            Position position4 = PositionFactory.create(4, 4);
            InFlightTask pendingReadTask2 = new InFlightTask(position4, 5, "");
            // Don't set readoutEntries to simulate pending read
            inFlightTasks.add(pendingReadTask2);
            Assert.assertEquals(replicator.getPermitsIfNoPendingRead(), 0,
                    "With any pending read task, should return 0 regardless of other tasks");

            log.info("Completed testGetPermitsIfNoPendingRead");
        } finally {
            // Restore original tasks
            inFlightTasks.clear();
            inFlightTasks.addAll(originalTasks);
        }
    }

    @Test
    public void testAcquirePermitsIfNotFetchingSchema() throws Exception {
        log.info("Starting testAcquirePermitsIfNotFetchingSchema");
        // Get the replicator for the test topic
        PersistentReplicator replicator = getReplicator(topicName);
        Assert.assertNotNull(replicator, "Replicator should not be null");

        // Get access to the inFlightTasks list for setup
        LinkedList<InFlightTask> inFlightTasks = replicator.inFlightTasks;
        Assert.assertNotNull(inFlightTasks, "InFlightTasks list should not be null");

        // Save original tasks and clear for testing
        List<InFlightTask> originalTasks = new ArrayList<>(inFlightTasks);
        inFlightTasks.clear();

        // Save original state
        int originalWaitForCursorRewinding = replicator.waitForCursorRewindingRefCnf;
        AbstractReplicator.State originalState = replicator.getState();

        try {
            // Test Case 1: Normal case - no pending read, not waiting for cursor rewinding, state is Started
            // Should return a new InFlightTask
            // First, check the current permits available
            int expectedPermits = replicator.getPermitsIfNoPendingRead();
            Assert.assertTrue(expectedPermits > 0, "Should have available permits for the test");
            InFlightTask task1 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNotNull(task1, "Should return a new InFlightTask in normal case");
            Assert.assertNotNull(task1.getReadPos(), "Task should have a read position");
            Assert.assertEquals(task1.getReadingEntries(), expectedPermits,
                    "Task readingEntries should equal the number of permits available");
            Assert.assertTrue(inFlightTasks.contains(task1),
                    "Task should be added to the inFlightTasks list");

            // Test Case 2: With pending read - should return null
            inFlightTasks.clear();
            Position position1 = PositionFactory.create(1, 1);
            InFlightTask pendingReadTask = new InFlightTask(position1, 5, "");
            // Don't set readoutEntries to simulate pending read
            inFlightTasks.add(pendingReadTask);
            InFlightTask task2 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNull(task2, "Should return null when there is a pending read");

            // Test Case 3: With waitForCursorRewinding=true - should return null
            inFlightTasks.clear();
            replicator.waitForCursorRewindingRefCnf = 1;
            InFlightTask task3 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNull(task3, "Should return null when waiting for cursor rewinding");
            // Reset for next test
            replicator.waitForCursorRewindingRefCnf = 0;

            // Test Case 4: With state != Started - should return null
            // We need to use reflection to modify the state since it's protected by AtomicReferenceFieldUpdater
            BrokerServiceInternalMethodInvoker.replicatorSetState(replicator, AbstractReplicator.State.Starting);
            InFlightTask task4 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNull(task4, "Should return null when state is not Started");
            // Reset state for next test
            BrokerServiceInternalMethodInvoker.replicatorSetState(replicator, AbstractReplicator.State.Started);

            // Test Case 5: With limited permits - verify readingEntries is set correctly
            inFlightTasks.clear();
            // Add a task with some in-flight messages to reduce available permits
            Position positionLimited = PositionFactory.create(10, 10);
            InFlightTask limitedTask = new InFlightTask(positionLimited, 5, "");
            // Add enough entries to leave just a small number of permits (e.g., 10)
            List<Entry> limitedEntries = new ArrayList<>();
            int entriesCount = 990;
            for (int j = 0; j < entriesCount; j++) {
                limitedEntries.add(mock(Entry.class));
            }
            limitedTask.setEntries(limitedEntries);
            inFlightTasks.add(limitedTask);
            // Check that we have limited permits available
            int limitedPermits = replicator.getPermitsIfNoPendingRead();
            Assert.assertTrue(limitedPermits > 0 && limitedPermits < 20,
                    "Should have a small number of permits available for testing");
            // Now acquire permits and verify readingEntries matches the limited permits
            InFlightTask task5 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNotNull(task5, "Should return a task with limited permits");
            Assert.assertEquals(task5.getReadingEntries(), limitedPermits,
                    "Task readingEntries should equal the limited number of permits available");

            // Test Case 6: With permits=0 - should return null
            inFlightTasks.clear();
            // Add tasks that will make getPermitsIfNoPendingRead() return 0
            // We need enough in-flight messages to equal producerQueueSize
            for (int i = 0; i < 10; i++) {
                Position position = PositionFactory.create(i, i);
                InFlightTask task = new InFlightTask(position, 5, "");
                List<Entry> entries = new ArrayList<>();
                for (int j = 0; j < 100; j++) {
                    entries.add(mock(Entry.class));
                }
                task.setEntries(entries);
                inFlightTasks.add(task);
            }
            InFlightTask task6 = replicator.acquirePermitsIfNotFetchingSchema();
            Assert.assertNull(task6, "Should return null when permits is 0");
            log.info("Completed testAcquirePermitsIfNotFetchingSchema");
        } finally {
            // Restore original state
            replicator.waitForCursorRewindingRefCnf = originalWaitForCursorRewinding;
            BrokerServiceInternalMethodInvoker.replicatorSetState(replicator, originalState);
            // Restore original tasks
            inFlightTasks.clear();
            inFlightTasks.addAll(originalTasks);
        }
    }

    public static Runnable pauseReplicator(PersistentReplicator replicator) {
        Awaitility.await().untilAsserted(() -> {
            assertTrue(replicator.isConnected());
        });
        replicator.beforeTerminateOrCursorRewinding(PersistentReplicator.ReasonOfWaitForCursorRewinding.Disconnecting);
        replicator.doRewindCursor(false);
        InFlightTask inFlightTask = replicator.createOrRecycleInFlightTaskIntoQueue(PositionFactory.create(1, 1), 1);
        return () -> {
            inFlightTask.setEntries(Collections.emptyList());
            replicator.readMoreEntries();
        };
    }
}
