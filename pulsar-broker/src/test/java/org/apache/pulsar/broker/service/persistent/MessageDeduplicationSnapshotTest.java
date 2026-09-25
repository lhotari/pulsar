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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.logging.log4j.Level;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.utils.TestLogAppender;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker", timeOut = 10000)
public class MessageDeduplicationSnapshotTest {
    private ScheduledExecutorService executor;
    private MessageDeduplication deduplication;
    private final AtomicBoolean enabled = new AtomicBoolean();
    private final AtomicReference<ManagedCursor> cursor = new AtomicReference<>();
    private final List<Snapshot> snapshots = new CopyOnWriteArrayList<>();

    @BeforeMethod
    public void setUp() throws Exception {
        enabled.set(true);
        snapshots.clear();
        executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("dedup-snapshot-test"));
        ServiceConfiguration config = new ServiceConfiguration();
        config.setBrokerDeduplicationEntriesInterval(2);
        PulsarService pulsar = mock(PulsarService.class);
        when(pulsar.getConfiguration()).thenReturn(config);
        when(pulsar.getExecutor()).thenReturn(executor);
        PersistentTopic topic = mock(PersistentTopic.class);
        when(topic.getName()).thenReturn("persistent://public/default/snapshot-test");
        when(topic.isDeduplicationEnabled()).thenAnswer(__ -> enabled.get());
        ManagedLedger ledger = mock(ManagedLedger.class);
        cursor.set(newCursor());
        doAnswer(invocation -> {
            invocation.<OpenCursorCallback>getArgument(1).openCursorComplete(cursor.get(), null);
            return null;
        }).when(ledger).asyncOpenCursor(any(), any(), any());
        doAnswer(invocation -> {
            invocation.<DeleteCursorCallback>getArgument(1).deleteCursorComplete(null);
            return null;
        }).when(ledger).asyncDeleteCursor(any(), any(), any());
        deduplication = new MessageDeduplication(pulsar, topic, ledger);
        deduplication.checkStatus().get(5, TimeUnit.SECONDS);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void pendingSnapshotSkipsRepeatedThresholdsWithoutWarning() throws Exception {
        try (TestLogAppender appender = TestLogAppender.create(MessageDeduplication.class)) {
            publish(0);
            publish(1);
            assertThat(snapshots).hasSize(1);
            Snapshot first = snapshots.get(0);
            assertThat(first.properties()).containsExactly(Map.entry("producer", 1L));
            for (long sequence = 2; sequence < 8; sequence++) {
                publish(sequence);
            }
            assertThat(snapshots).hasSize(1);
            assertThat(appender.getEvents()).noneMatch(event ->
                    event.getLoggerName().equals(MessageDeduplication.class.getName())
                            && event.getLevel().isMoreSpecificThan(Level.WARN));

            first.callback().markDeleteComplete(null);
            assertThat(snapshots).hasSize(1);
            publish(8);
            assertThat(snapshots).hasSize(1);
            publish(9);
            assertThat(snapshots).hasSize(2);
            assertThat(snapshots.get(1).position()).isEqualTo(PositionFactory.create(1, 9));
            assertThat(snapshots.get(1).properties()).containsExactly(Map.entry("producer", 9L));
            assertThat(first.properties()).containsExactly(Map.entry("producer", 1L));
            snapshots.get(1).callback().markDeleteComplete(null);
        }
    }

    @Test
    public void asynchronousFailureAllowsLaterSnapshot() {
        publish(0);
        publish(1);
        snapshots.get(0).callback().markDeleteFailed(new ManagedLedgerException("snapshot failed"), null);
        publish(2);
        publish(3);
        assertThat(snapshots).hasSize(2);
        snapshots.get(1).callback().markDeleteComplete(null);
    }

    @Test
    public void synchronousFailureDoesNotEscapePublishOrPreventLaterSnapshot() {
        AtomicBoolean fail = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (fail.getAndSet(false)) {
                throw new IllegalStateException("snapshot failed synchronously");
            }
            snapshots.add(new Snapshot(cursor.get(), invocation.getArgument(0), invocation.getArgument(1),
                    invocation.getArgument(2)));
            return null;
        }).when(cursor.get()).asyncMarkDelete(any(), any(), any(), any());
        publish(0);
        assertThatCode(() -> publish(1)).doesNotThrowAnyException();
        publish(2);
        publish(3);
        assertThat(snapshots).hasSize(1);
        snapshots.get(0).callback().markDeleteComplete(null);
    }

    @Test
    public void disableDuringPublishDoesNotPreventSnapshotsAfterReenable() throws Exception {
        publish(0);
        CountDownLatch enteredPublish = new CountDownLatch(1);
        CountDownLatch finishPublish = new CountDownLatch(1);
        PublishContext context = publishContext(1);
        AtomicBoolean firstCall = new AtomicBoolean(true);
        when(context.getProducerName()).thenAnswer(__ -> {
            if (firstCall.getAndSet(false)) {
                enteredPublish.countDown();
                assertThat(finishPublish.await(5, TimeUnit.SECONDS)).isTrue();
            }
            return "producer";
        });
        CompletableFuture<Void> publishing = CompletableFuture.runAsync(() ->
                deduplication.recordMessagePersisted(context, PositionFactory.create(1, 1)), executor);
        try {
            assertThat(enteredPublish.await(5, TimeUnit.SECONDS)).isTrue();
            enabled.set(false);
            deduplication.checkStatus().get(5, TimeUnit.SECONDS);
            assertThat(deduplication.getManagedCursor()).isNull();
        } finally {
            finishPublish.countDown();
        }
        publishing.get(5, TimeUnit.SECONDS);
        assertThat(snapshots).isEmpty();

        reenable();
        publish(2);
        publish(3);
        assertThat(snapshots).hasSize(1);
        assertThat(snapshots.get(0).cursor()).isSameAs(cursor.get());
        snapshots.get(0).callback().markDeleteComplete(null);
    }

    @Test
    public void oldCursorFailureAllowsSnapshotOnReopenedCursor() throws Exception {
        publish(0);
        publish(1);
        Snapshot oldSnapshot = snapshots.get(0);
        enabled.set(false);
        deduplication.checkStatus().get(5, TimeUnit.SECONDS);
        reenable();
        publish(2);
        publish(3);
        assertThat(snapshots).hasSize(1);
        oldSnapshot.callback().markDeleteFailed(
                new ManagedLedgerException.CursorAlreadyClosedException("cursor deleted"), null);
        publish(4);
        publish(5);
        assertThat(snapshots).hasSize(2);
        assertThat(snapshots.get(1).cursor()).isSameAs(cursor.get()).isNotSameAs(oldSnapshot.cursor());
        snapshots.get(1).callback().markDeleteComplete(null);
    }

    @DataProvider
    public Object[][] snapshotFailures() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "snapshotFailures")
    public void replayPropagatesSnapshotFailureAndCanRetry(boolean synchronous) throws Exception {
        enabled.set(false);
        deduplication.checkStatus().get(5, TimeUnit.SECONDS);
        ManagedCursor replayCursor = newCursor();
        cursor.set(replayCursor);
        when(replayCursor.hasMoreEntries()).thenReturn(true, false);
        doAnswer(invocation -> {
            invocation.<ReadEntriesCallback>getArgument(2).readEntriesComplete(
                    List.of(entry(0), entry(1)), null);
            return null;
        }).when(replayCursor).asyncReadEntries(anyInt(), anyLong(), any(), any(), any());
        Throwable failure = synchronous ? new IllegalStateException("snapshot failed synchronously")
                : new ManagedLedgerException("snapshot failed asynchronously");
        AtomicBoolean fail = new AtomicBoolean(true);
        doAnswer(invocation -> {
            MarkDeleteCallback callback = invocation.getArgument(2);
            if (fail.getAndSet(false)) {
                if (synchronous) {
                    throw failure;
                }
                callback.markDeleteFailed((ManagedLedgerException) failure, null);
            } else {
                callback.markDeleteComplete(null);
            }
            return null;
        }).when(replayCursor).asyncMarkDelete(any(), any(), any(), any());
        enabled.set(true);
        assertThatThrownBy(() -> deduplication.checkStatus().get(5, TimeUnit.SECONDS)).hasCause(failure);
        assertThat(deduplication.getStatus()).isEqualTo(MessageDeduplication.Status.Failed);
        when(replayCursor.hasMoreEntries()).thenReturn(true, false);
        deduplication.checkStatus().get(5, TimeUnit.SECONDS);
        assertThat(deduplication.isEnabled()).isTrue();
        verify(replayCursor, times(2)).asyncMarkDelete(any(), any(), any(), any());
    }

    private void reenable() throws Exception {
        cursor.set(newCursor());
        enabled.set(true);
        deduplication.checkStatus().get(5, TimeUnit.SECONDS);
    }

    private ManagedCursor newCursor() {
        ManagedCursor result = mock(ManagedCursor.class);
        when(result.getProperties()).thenReturn(Map.of());
        doAnswer(invocation -> {
            snapshots.add(new Snapshot(result, invocation.getArgument(0), invocation.getArgument(1),
                    invocation.getArgument(2)));
            return null;
        }).when(result).asyncMarkDelete(any(), any(), any(), any());
        return result;
    }

    private void publish(long sequence) {
        deduplication.recordMessagePersisted(publishContext(sequence), PositionFactory.create(1, sequence));
    }

    private PublishContext publishContext(long sequence) {
        PublishContext context = mock(PublishContext.class);
        when(context.getProducerName()).thenReturn("producer");
        when(context.getSequenceId()).thenReturn(sequence);
        when(context.getHighestSequenceId()).thenReturn(sequence);
        return context;
    }

    private EntryImpl entry(long sequence) {
        MessageMetadata metadata = new MessageMetadata().setProducerName("producer")
                .setSequenceId(sequence).setPublishTime(0);
        ByteBuf buffer = Commands.serializeMetadataAndPayload(Commands.ChecksumType.None, metadata,
                Unpooled.EMPTY_BUFFER);
        try {
            return EntryImpl.create(1, sequence, buffer);
        } finally {
            buffer.release();
        }
    }

    private record Snapshot(ManagedCursor cursor, Position position, Map<String, Long> properties,
                            MarkDeleteCallback callback) {
    }
}
