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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.service.TransportCnx.CloseNotification;
import org.testng.annotations.Test;

public class ShutdownClientNotificationsTest {
    private static final class QueuedExecutor implements Executor {
        final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable task) {
            tasks.add(task);
        }

        void tick() {
            tasks.remove().run();
        }

        void drain() {
            while (!tasks.isEmpty()) {
                tick();
            }
        }
    }

    private static final class Fixture {
        final Topic topic = mock(Topic.class);
        final TransportCnx cnx = mock(TransportCnx.class);
        final Map<String, Producer> producers = new LinkedHashMap<>();
        final CompletableFuture<Void> disposed = new CompletableFuture<>();

        Fixture(int count, CompletableFuture<CloseNotification> write) {
            when(topic.getProducers()).thenReturn(producers);
            when(topic.getSubscriptions()).thenReturn(Map.of());
            when(topic.disposeAfterTransfer()).thenReturn(disposed);
            doAnswer(invocation -> {
                ((Runnable) invocation.getArgument(0)).run();
                return null;
            }).when(cnx).execute(any());
            when(cnx.closeProducerAsync(any(), any(), any())).thenReturn(write);
            for (int i = 0; i < count; i++) {
                String name = Integer.toString(i);
                Producer producer = mock(Producer.class);
                when(producer.getCnx()).thenReturn(cnx);
                doAnswer(invocation -> {
                    producers.remove(name);
                    return null;
                }).when(producer).closeNow(true);
                producers.put(name, producer);
            }
        }
    }

    @Test
    public void chunksYieldWithoutWaitingForEarlierWritesOrTopics() {
        QueuedExecutor executor = new QueuedExecutor();
        CompletableFuture<CloseNotification> held = new CompletableFuture<>();
        Fixture slow = new Fixture(1, held);
        Fixture fast = new Fixture(256, CompletableFuture.completedFuture(CloseNotification.FLUSHED));
        fast.disposed.complete(null);
        ShutdownClientNotifications drain = new ShutdownClientNotifications(List.of(slow.topic, fast.topic), executor,
                () -> Long.MAX_VALUE, new CompletableFuture<>());
        CompletableFuture<Void> canceled = drain.start();
        assertThat(canceled.cancel(false)).isTrue();
        CompletableFuture<Void> result = drain.start();
        assertThat(executor.tasks).hasSize(1);
        executor.tick();
        verify(slow.cnx).closeProducerAsync(any(), any(), any());
        verify(fast.cnx, times(127)).closeProducerAsync(any(), any(), any());
        assertThat(executor.tasks).hasSize(1);
        executor.tick();
        verify(fast.cnx, times(255)).closeProducerAsync(any(), any(), any());
        assertThat(executor.tasks).hasSize(1);
        executor.drain();
        verify(fast.cnx, times(256)).closeProducerAsync(any(), any(), any());
        verify(fast.topic).disposeAfterTransfer();
        verify(slow.topic, never()).disposeAfterTransfer();
        assertThat(result).isNotDone();
        assertThat(drain.snapshot()).isEqualTo(new ShutdownClientNotifications.Snapshot(256, 0, 0, 0));
        held.complete(CloseNotification.FLUSHED);
        executor.drain();
        verify(slow.topic).disposeAfterTransfer();
        assertThat(result).as("source disposal is also part of completion").isNotDone();
        slow.disposed.complete(null);
        assertThat(result).isCompleted();
        assertThat(drain.snapshot()).isEqualTo(new ShutdownClientNotifications.Snapshot(257, 0, 0, 0));
    }

    @Test
    public void cutoffAbandonsUnstartedEntitiesWithoutDisposalOrReleasingPendingWrites() {
        QueuedExecutor executor = new QueuedExecutor();
        AtomicLong remaining = new AtomicLong(100);
        CompletableFuture<CloseNotification> held = new CompletableFuture<>();
        Fixture fixture = new Fixture(257, held);
        ShutdownClientNotifications drain = new ShutdownClientNotifications(List.of(fixture.topic), executor,
                remaining::get, CompletableFuture.completedFuture(Optional.empty()));
        CompletableFuture<Void> result = drain.start();
        executor.tick();
        remaining.set(0);
        executor.drain();
        verify(fixture.cnx, times(128)).closeProducerAsync(any(), any(), any());
        assertThat(fixture.producers).hasSize(129);
        assertThat(result).isNotDone();
        held.complete(CloseNotification.FLUSHED);
        executor.drain();
        assertThat(result).isCompletedExceptionally();
        verify(fixture.topic, never()).disposeAfterTransfer();
        assertThat(drain.snapshot()).isEqualTo(new ShutdownClientNotifications.Snapshot(128, 0, 0, 129));
    }

    @Test
    public void cutoffIsRecheckedOnTheConnectionExecutor() {
        QueuedExecutor executor = new QueuedExecutor();
        QueuedExecutor connection = new QueuedExecutor();
        AtomicLong remaining = new AtomicLong(100);
        Fixture fixture = new Fixture(1, CompletableFuture.completedFuture(CloseNotification.FLUSHED));
        doAnswer(invocation -> {
            connection.execute(invocation.getArgument(0));
            return null;
        }).when(fixture.cnx).execute(any());
        ShutdownClientNotifications drain = new ShutdownClientNotifications(List.of(fixture.topic), executor,
                remaining::get, CompletableFuture.completedFuture(Optional.empty()));
        CompletableFuture<Void> result = drain.start();
        executor.drain();
        remaining.set(0);
        connection.drain();
        executor.drain();
        assertThat(result).isCompletedExceptionally();
        verify(fixture.cnx, never()).closeProducerAsync(any(), any(), any());
        verify(fixture.topic, never()).disposeAfterTransfer();
        assertThat(fixture.producers).hasSize(1);
    }

    @Test
    public void knownWriteFailureStillWaitsForOtherEntitiesAndDisposesRemovedSources() throws Exception {
        QueuedExecutor executor = new QueuedExecutor();
        Fixture fixture = new Fixture(1, CompletableFuture.failedFuture(new IllegalStateException("write failed")));
        Subscription subscription = mock(Subscription.class);
        Consumer consumer = mock(Consumer.class);
        List<Consumer> consumers = new ArrayList<>(List.of(consumer));
        when(consumer.cnx()).thenReturn(fixture.cnx);
        when(subscription.getConsumers()).thenReturn(consumers);
        when(fixture.topic.getSubscriptions()).thenAnswer(__ -> Map.of("durable", subscription));
        CompletableFuture<CloseNotification> consumerWrite = new CompletableFuture<>();
        when(fixture.cnx.closeConsumerAsync(any(), any(), any())).thenReturn(consumerWrite);
        doAnswer(__ -> {
            consumers.clear();
            return null;
        }).when(consumer).close(false);
        fixture.disposed.complete(null);
        ShutdownClientNotifications drain = new ShutdownClientNotifications(List.of(fixture.topic), executor,
                () -> Long.MAX_VALUE, CompletableFuture.completedFuture(Optional.empty()));
        CompletableFuture<Void> result = drain.start();
        executor.drain();
        assertThat(result).isNotDone();
        verify(fixture.topic, never()).disposeAfterTransfer();
        consumerWrite.complete(CloseNotification.UNTRACKED);
        executor.drain();
        assertThat(result).isCompletedExceptionally();
        verify(fixture.topic).disposeAfterTransfer();
        verify(subscription, never()).delete();
        assertThat(drain.snapshot()).isEqualTo(new ShutdownClientNotifications.Snapshot(0, 1, 1, 0));
    }

    @Test
    public void executorRejectionSettlesWithoutNotificationsOrDisposal() {
        Fixture fixture = new Fixture(1, CompletableFuture.completedFuture(CloseNotification.FLUSHED));
        ShutdownClientNotifications drain = new ShutdownClientNotifications(List.of(fixture.topic),
                task -> {
                    throw new RejectedExecutionException("stopped");
                }, () -> Long.MAX_VALUE,
                CompletableFuture.completedFuture(Optional.empty()));
        assertThat(drain.start()).isCompletedExceptionally();
        verify(fixture.cnx, never()).closeProducerAsync(any(), any(), any());
        verify(fixture.topic, never()).disposeAfterTransfer();
        assertThat(drain.snapshot()).isEqualTo(new ShutdownClientNotifications.Snapshot(0, 0, 0, 1));
    }

    @Test
    public void sourceRemovalFailurePreventsDisposalEvenWhenWriteSucceeds() {
        QueuedExecutor executor = new QueuedExecutor();
        Fixture fixture = new Fixture(1, CompletableFuture.completedFuture(CloseNotification.FLUSHED));
        Producer producer = fixture.producers.values().iterator().next();
        doAnswer(__ -> {
            throw new IllegalStateException("source removal failed");
        })
                .when(producer).closeNow(anyBoolean());
        ShutdownClientNotifications drain = new ShutdownClientNotifications(List.of(fixture.topic), executor,
                () -> Long.MAX_VALUE, CompletableFuture.completedFuture(Optional.empty()));
        CompletableFuture<Void> result = drain.start();
        executor.drain();
        assertThat(result).isCompletedExceptionally();
        verify(fixture.topic, never()).disposeAfterTransfer();
        assertThat(fixture.producers).hasSize(1);
    }
}
