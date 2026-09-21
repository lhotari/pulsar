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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.service.TransportCnx.CloseNotification;
import org.apache.pulsar.common.util.FutureUtil;

/** One bundle's notification phase, after its storage and ownership barriers have completed. */
final class ShutdownClientNotifications {
    static final int BATCH_SIZE = 128;

    record Snapshot(int flushed, int untracked, int failed, int notStarted) { }

    private final class TopicWork {
        final Topic topic;
        final List<CompletableFuture<CloseNotification>> notifications = new ArrayList<>();
        final CompletableFuture<Void> finished = new CompletableFuture<>();
        final AtomicInteger remainingRemovals = new AtomicInteger();
        volatile boolean abandoned;

        TopicWork(Topic topic) {
            this.topic = topic;
        }

        void start() {
            CompletableFuture<Void> notified = FutureUtil.waitForAll(notifications);
            // A known transport error still permits local disposal, provided source entity removal succeeded.
            CompletableFuture<Void> disposed = notified.handle((__, error) -> null).thenComposeAsync(__ -> {
                if (abandoned || remainingRemovals.get() != 0) {
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Source client removal incomplete"));
                }
                return FutureUtil.supplySafely(topic::disposeAfterTransfer);
            }, executor);
            FutureUtil.completeAfter(finished, CompletableFuture.allOf(notified, disposed));
        }
    }

    private final class EntityWork {
        final TopicWork owner;
        final Function<Runnable, CompletableFuture<CloseNotification>> operation;
        final CompletableFuture<CloseNotification> result = new CompletableFuture<>();

        EntityWork(TopicWork owner, Function<Runnable, CompletableFuture<CloseNotification>> operation) {
            this.owner = owner;
            this.operation = operation;
            owner.notifications.add(result);
            owner.remainingRemovals.incrementAndGet();
        }

        void start() {
            FutureUtil.supplySafely(() -> operation.apply(owner.remainingRemovals::decrementAndGet))
                    .whenComplete((notification, error) -> {
                        if (error != null) {
                            failed.incrementAndGet();
                            result.completeExceptionally(error);
                        } else if (notification == null) {
                            failed.incrementAndGet();
                            result.completeExceptionally(
                                    new IllegalStateException("Transport returned no close outcome"));
                        } else {
                            if (notification == CloseNotification.FLUSHED) {
                                flushed.incrementAndGet();
                            } else {
                                untracked.incrementAndGet();
                            }
                            result.complete(notification);
                        }
                    });
        }
    }

    private final Executor executor;
    private final LongSupplier remaining;
    private final CompletableFuture<Optional<BrokerLookupData>> destination;
    private final List<TopicWork> topics = new ArrayList<>();
    private final List<EntityWork> entities = new ArrayList<>();
    private final AtomicInteger flushed = new AtomicInteger();
    private final AtomicInteger untracked = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private final AtomicInteger notStarted = new AtomicInteger();
    private final AtomicBoolean started = new AtomicBoolean();
    private final CompletableFuture<Void> result = new CompletableFuture<>();
    // Only the dispatch continuation accesses this cursor.
    private int next;

    ShutdownClientNotifications(Collection<Topic> capturedTopics, Executor executor, LongSupplier remaining,
                                CompletableFuture<Optional<BrokerLookupData>> destination) {
        this.executor = executor;
        this.remaining = remaining;
        this.destination = destination;
        for (Topic topic : capturedTopics) {
            TopicWork work = new TopicWork(topic);
            topics.add(work);
            // Admission and storage are fenced. Only removals can race this finite snapshot.
            for (Producer producer : List.copyOf(topic.getProducers().values())) {
                entities.add(new EntityWork(work, removed -> disconnect(producer, removed)));
            }
            for (Subscription subscription : List.copyOf(topic.getSubscriptions().values())) {
                for (Consumer consumer : List.copyOf(subscription.getConsumers())) {
                    entities.add(new EntityWork(work, removed -> disconnect(consumer, removed)));
                }
            }
        }
    }

    CompletableFuture<Void> start() {
        if (started.compareAndSet(false, true)) {
            topics.forEach(TopicWork::start);
            FutureUtil.completeAfter(result,
                    FutureUtil.waitForAll(topics.stream().map(work -> work.finished).toList()));
            schedule();
        }
        return result.copy();
    }

    Snapshot snapshot() {
        return new Snapshot(flushed.get(), untracked.get(), failed.get(), notStarted.get());
    }

    private void schedule() {
        try {
            executor.execute(this::dispatch);
        } catch (Throwable error) {
            abandon(error);
        }
    }

    private void dispatch() {
        try {
            int end = next + Math.min(BATCH_SIZE, entities.size() - next);
            while (next < end) {
                if (remaining.getAsLong() <= 0) {
                    abandon(new TimeoutException("Bundle client notification deadline expired"));
                    return;
                }
                entities.get(next++).start();
            }
            if (next < entities.size()) {
                // Do not wait for this chunk's writes: a slow connection must not block unrelated notifications.
                schedule();
            }
        } catch (Throwable error) {
            abandon(error);
        }
    }

    private void abandon(Throwable error) {
        // Mark every affected topic before completing any placeholder that could trigger disposal.
        for (int index = next; index < entities.size(); index++) {
            entities.get(index).owner.abandoned = true;
        }
        while (next < entities.size()) {
            notStarted.incrementAndGet();
            entities.get(next++).result.completeExceptionally(error);
        }
    }

    private Optional<BrokerLookupData> destinationNow() {
        // The ownership boundary has already passed, so ordinary lookup is safe. A hint is opportunistic and must
        // never hold all clients behind a registry lookup or consume a fresh timeout budget.
        return destination.isCompletedExceptionally() ? Optional.empty() : destination.getNow(Optional.empty());
    }

    private CompletableFuture<CloseNotification> disconnect(Producer producer, Runnable sourceRemoved) {
        TransportCnx cnx = producer.getCnx();
        return FutureUtil.composeAsync(() -> {
            if (remaining.getAsLong() <= 0) {
                return CompletableFuture.failedFuture(new TimeoutException("Producer notification deadline expired"));
            }
            CompletableFuture<CloseNotification> notification = FutureUtil.supplySafely(
                    () -> cnx.closeProducerAsync(producer, destinationNow(), remaining));
            // Native close queues its tombstone before this removal queues the connection-map cleanup.
            CompletableFuture<Void> removed = FutureUtil.supplySafely(() -> {
                producer.closeNow(true);
                sourceRemoved.run();
                return CompletableFuture.completedFuture(null);
            });
            return removed.thenCombine(notification, (__, outcome) -> outcome);
        }, cnx::execute);
    }

    private CompletableFuture<CloseNotification> disconnect(Consumer consumer, Runnable sourceRemoved) {
        TransportCnx cnx = consumer.cnx();
        return FutureUtil.composeAsync(() -> {
            if (remaining.getAsLong() <= 0) {
                return CompletableFuture.failedFuture(new TimeoutException("Consumer notification deadline expired"));
            }
            CompletableFuture<CloseNotification> notification = FutureUtil.supplySafely(
                    () -> cnx.closeConsumerAsync(consumer, destinationNow(), remaining));
            CompletableFuture<Void> removed = FutureUtil.supplySafely(() -> {
                try {
                    consumer.close(false);
                } catch (BrokerServiceException error) {
                    // Client-initiated close can win the snapshot race. Other removal failures remain failures.
                    if (consumer.getSubscription().getConsumers().contains(consumer)) {
                        return CompletableFuture.failedFuture(error);
                    }
                }
                sourceRemoved.run();
                return CompletableFuture.completedFuture(null);
            });
            return removed.thenCombine(notification, (__, outcome) -> outcome);
        }, cnx::execute);
    }
}
