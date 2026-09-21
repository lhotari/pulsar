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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.stream.Stream;
import org.apache.pulsar.broker.service.BrokerServiceException.BrokerDrainingException;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Linearizes new connection/entity admission with shutdown acceptance. No user code runs under its monitor.
 * Existing admitted entities are independent of this gate and keep serving until their topic is unloaded.
 */
public final class BrokerAdmission {
    private Set<Ticket> pending = new HashSet<>();
    private volatile boolean closed;
    private final Set<TopicLoad> topicLoads = new HashSet<>();
    // Keep one failed physical cleanup per name. A later logical retry does not prove the old resources closed.
    private final Map<TopicName, TopicLoad> failedTopicLoads = new HashMap<>();

    public boolean isClosed() {
        return closed;
    }

    /** Register a connection or entity request. A null ticket means that shutdown has already won. */
    public synchronized Ticket register(Executor executor, Runnable rejected) {
        if (closed) {
            return null;
        }
        Ticket ticket = new Ticket(executor, rejected);
        pending.add(ticket);
        return ticket;
    }

    /** Publish the irreversible cutoff, returning cancellation dispatches to run outside the acceptance lock. */
    public synchronized Iterable<Runnable> close() {
        if (closed) {
            return List.of();
        }
        closed = true;
        Set<Ticket> rejected = pending;
        pending = new HashSet<>();
        // Iteration and executor dispatch happen after releasing the gate. Even a large pending set must
        // not delay installing the watchdog or hold the admission monitor while callbacks are scheduled.
        return () -> Stream.concat(rejected.stream()
                        .<Runnable>map(ticket -> () -> ticket.executor.execute(ticket.rejected)),
                topicLoads.stream().<Runnable>map(load -> load::rejectOnShutdown)).iterator();
    }

    /** Register before any asynchronous cache-miss work, including metadata reads before cache insertion. */
    TopicLoad registerTopicLoad(TopicName name, CompletableFuture<Optional<Topic>> result) {
        TopicLoad load;
        synchronized (this) {
            if (closed) {
                return null;
            }
            load = new TopicLoad(name, result);
            topicLoads.add(load);
        }
        result.whenComplete((ignored, error) -> {
            if (error != null) {
                load.finishWithoutMaterialization();
            }
        });
        return load;
    }

    /**
     * After cutoff, membership is stable: completions settle their own records instead of removing them.
     * The potentially large copy runs outside the shutdown acceptance monitor.
     */
    List<TopicLoad> getShutdownTopicLoads() {
        synchronized (this) {
            if (!closed) {
                throw new IllegalStateException("Topic loads can only be captured after admission closes");
            }
        }
        List<TopicLoad> result = new ArrayList<>(topicLoads);
        result.addAll(failedTopicLoads.values());
        return result;
    }

    /** Physical materialization completion, independent of the public request's timeout or cancellation. */
    final class TopicLoad {
        private final TopicName name;
        private final CompletableFuture<Optional<Topic>> result;
        private final CompletableFuture<Optional<Topic>> completion = new CompletableFuture<>();
        // Guarded by BrokerAdmission.this.
        private boolean materializing;
        private boolean finished;

        private TopicLoad(TopicName name, CompletableFuture<Optional<Topic>> result) {
            this.name = name;
            this.result = result;
        }

        TopicName name() {
            return name;
        }

        CompletableFuture<Optional<Topic>> request() {
            return result;
        }

        CompletableFuture<Optional<Topic>> completion() {
            return completion.copy();
        }

        private void rejectOnShutdown() {
            finishWithoutMaterialization();
            result.completeExceptionally(new BrokerDrainingException());
        }

        boolean beginMaterialization() {
            synchronized (BrokerAdmission.this) {
                if (closed || finished || materializing || result.isDone()) {
                    return false;
                }
                materializing = true;
                return true;
            }
        }

        boolean canPublish() {
            synchronized (BrokerAdmission.this) {
                return materializing && !closed && !finished && !result.isDone();
            }
        }

        /** Call only after successful publication into the installed cache future. */
        void published(Topic topic) {
            finish(Optional.of(topic), null, false);
        }

        /** An alias owns no storage: its installed cache winner has its own record. */
        void finishWithoutMaterialization() {
            finish(Optional.empty(), null, true);
        }

        void cleaned(CompletionStage<Void> cleanup) {
            cleanup.whenComplete((ignored, error) -> finish(Optional.empty(), error, false));
        }

        void cleanupUntracked(Throwable error) {
            finish(Optional.empty(), error, false);
        }

        private void finish(Optional<Topic> topic, Throwable error, boolean onlyBeforeMaterialization) {
            synchronized (BrokerAdmission.this) {
                if (finished || (onlyBeforeMaterialization && materializing)) {
                    return;
                }
                finished = true;
                if (!closed) {
                    topicLoads.remove(this);
                    if (error != null) {
                        failedTopicLoads.putIfAbsent(name, this);
                    }
                }
            }
            // Completion may run a bundle transition or user test callback; never do it under admission.
            if (error == null) {
                completion.complete(topic);
            } else {
                completion.completeExceptionally(error);
            }
        }
    }

    public final class Ticket implements AutoCloseable {
        private final Executor executor;
        private final Runnable rejected;

        private Ticket(Executor executor, Runnable rejected) {
            this.executor = executor;
            this.rejected = rejected;
        }

        /** Commit before sending success, completing futures, or invoking entity callbacks. */
        public boolean commit() {
            synchronized (BrokerAdmission.this) {
                return !closed && pending.remove(this);
            }
        }

        /** Forget a failed or canceled request without running its rejection callback. */
        @Override
        public void close() {
            synchronized (BrokerAdmission.this) {
                pending.remove(this);
            }
        }
    }
}
