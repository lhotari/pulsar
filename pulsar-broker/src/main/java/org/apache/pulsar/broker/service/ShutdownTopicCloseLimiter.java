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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.pulsar.common.util.FutureUtil;

/** Bounds physical storage work, independently of timeouts on callers observing that work. */
final class ShutdownTopicCloseLimiter implements AutoCloseable {
    private static final int DISPATCH_BATCH_SIZE = 128;
    private final int limit;
    private final Executor executor;
    private final LongSupplier remainingNanos;
    private final Queue<CompletableFuture<Permit>> waiting = new ArrayDeque<>();
    private int active;
    private boolean dispatching;
    private volatile boolean closed;

    ShutdownTopicCloseLimiter(int limit, Executor executor, LongSupplier remainingNanos) {
        this.limit = Math.max(1, limit);
        this.executor = executor;
        this.remainingNanos = remainingNanos;
    }

    CompletableFuture<Void> run(Supplier<CompletableFuture<Void>> operation) {
        // The returned copy is an observer. Cancellation or orTimeout() cannot release the physical slot.
        return reserve().thenCompose(permit -> permit.run(operation)).copy();
    }

    /** Factory cleanup can already be running when reported. Account for it before admitting more work. */
    void observeExisting(CompletionStage<Void> operation) {
        synchronized (this) {
            active++;
        }
        Permit permit = new Permit();
        try {
            operation.whenComplete((ignored, error) -> permit.close());
        } catch (RuntimeException | Error error) {
            permit.close();
            throw error;
        }
    }

    /** Reserve without fencing a topic or starting I/O. The owning bundle must use or release the permit. */
    CompletableFuture<Permit> reserve() {
        CompletableFuture<Permit> result = new CompletableFuture<>();
        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(new TimeoutException("Topic close admission ended"));
            }
            waiting.add(result);
        }
        requestDispatch();
        return result;
    }

    private void requestDispatch() {
        synchronized (this) {
            if (closed || dispatching || active >= limit || waiting.isEmpty()) {
                return;
            }
            dispatching = true;
        }
        try {
            executor.execute(this::dispatch);
        } catch (RuntimeException error) {
            failWaiting(error);
        }
    }

    private void dispatch() {
        for (int i = 0; i < DISPATCH_BATCH_SIZE; i++) {
            CompletableFuture<Permit> request;
            synchronized (this) {
                if (closed || active >= limit || waiting.isEmpty()) {
                    dispatching = false;
                    return;
                }
                request = waiting.remove();
                active++;
            }
            Permit permit = new Permit();
            try {
                if (remainingNanos.getAsLong() <= 0) {
                    request.completeExceptionally(new TimeoutException("Topic close admission ended"));
                    permit.close();
                } else if (!request.complete(permit)) {
                    permit.close();
                }
            } catch (Throwable error) {
                request.completeExceptionally(error);
                permit.close();
            }
        }
        synchronized (this) {
            dispatching = false;
        }
        requestDispatch();
    }

    @Override
    public void close() {
        failWaiting(new TimeoutException("Topic close admission ended"));
    }

    private void failWaiting(Throwable error) {
        List<CompletableFuture<Permit>> rejected;
        synchronized (this) {
            closed = true;
            dispatching = false;
            rejected = new ArrayList<>(waiting);
            waiting.clear();
        }
        rejected.forEach(request -> request.completeExceptionally(error));
    }

    @VisibleForTesting
    synchronized int activeCount() {
        return active;
    }

    final class Permit implements AutoCloseable {
        private final AtomicBoolean released = new AtomicBoolean();
        private final AtomicBoolean started = new AtomicBoolean();

        CompletableFuture<Void> run(Supplier<CompletableFuture<Void>> operation) {
            if (!started.compareAndSet(false, true)) {
                return CompletableFuture.failedFuture(new IllegalStateException("Topic close permit was already used"));
            }
            return FutureUtil.composeAsync(() -> {
                if (closed || released.get() || remainingNanos.getAsLong() <= 0) {
                    return CompletableFuture.failedFuture(new TimeoutException("Topic close admission ended"));
                }
                return FutureUtil.supplySafely(operation);
            }, executor).whenComplete((ignored, error) -> close()).copy();
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                synchronized (ShutdownTopicCloseLimiter.this) {
                    active--;
                }
                requestDispatch();
            }
        }
    }
}
