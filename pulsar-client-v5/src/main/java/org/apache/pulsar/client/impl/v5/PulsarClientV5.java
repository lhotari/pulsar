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
package org.apache.pulsar.client.impl.v5;

import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.v5.CheckpointConsumerBuilder;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.StreamConsumerBuilder;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.MemoryLimitController;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.util.ExecutorProvider;

/**
 * V5 PulsarClient implementation that wraps the v4 PulsarClientImpl for
 * connection management and transport. Adds scalable topic routing on top.
 */
final class PulsarClientV5 implements PulsarClient {

    private final PulsarClientImpl v4Client;
    private final String description;
    private final Duration transactionTimeout;

    /**
     * Threads that run a producer's dispatch chain when it cannot run on the caller's thread.
     * Handing a message to a per-segment producer waits once that producer's queue is full, so
     * these have to be threads where waiting costs nothing but this client's own dispatch. The
     * client's IO threads are not — they deliver the acknowledgements that drain the queue — and
     * neither is the shared internal pool, which consumers and the transaction coordinator pin
     * work to. See {@code ScalableTopicProducer#newDispatchChainHead}.
     *
     * <p>The pool is a set of single-threaded executors, each of which starts its thread only when
     * a producer is first routed to it, so a client that never backs up never spends one.
     */
    private final ExecutorProvider producerDispatchExecutors;

    /** Thread-name prefix netty's factory gives the dispatch pool, used to recognise its threads. */
    private static final String PRODUCER_DISPATCH_POOL_NAME = "pulsar-v5-producer-dispatch";

    /**
     * Whether the calling thread is one this client's own send path runs on. Worked out once per
     * thread, since a thread never changes which it is.
     */
    private final ThreadLocal<Boolean> clientOwnedThread =
            ThreadLocal.withInitial(this::computeClientOwnedThread);

    /**
     * How many sends this client's producers may have outstanding at once, as a byte budget spent
     * at {@code ScalableTopicProducer#PENDING_SEND_MEMORY_OVERHEAD_BYTES} per send. Client-wide,
     * because the memory limit it is sized from is documented as a budget across all producers.
     *
     * <p>It is a counter of its own rather than a second claim on the v4 client's memory limiter:
     * that one is spent on message payloads as sends reach the per-segment producers, and a single
     * counter would let the producers' bookkeeping consume the whole budget before the first
     * payload could be charged against it — leaving nothing in flight to free it again.
     *
     * <p>{@code null} when the application disabled the memory limit; producers then fall back to
     * a budget of their own. See {@code ScalableTopicProducer#NO_MEMORY_LIMIT_MAX_PENDING_SENDS}.
     */
    private final MemoryLimitController producerSendBudget;

    PulsarClientV5(PulsarClientImpl v4Client, String description, Duration transactionTimeout) {
        this.v4Client = v4Client;
        this.description = description;
        this.transactionTimeout = transactionTimeout;
        MemoryLimitController clientMemoryLimit = v4Client.getMemoryLimitController();
        this.producerSendBudget = clientMemoryLimit.isMemoryLimited()
                ? new MemoryLimitController(clientMemoryLimit.memoryLimit())
                : null;
        this.producerDispatchExecutors = new ExecutorProvider(
                Math.max(1, v4Client.getConfiguration().getNumIoThreads()),
                PRODUCER_DISPATCH_POOL_NAME, true);
    }

    /**
     * The client-wide outstanding-send budget, or {@code null} when the client has no memory limit
     * to size one from.
     */
    MemoryLimitController producerSendBudget() {
        return producerSendBudget;
    }

    /**
     * Get the underlying v4 client. Package-private for use by internal components.
     */
    PulsarClientImpl v4Client() {
        return v4Client;
    }

    /**
     * Whether the calling thread is one the client's own send path runs on: an IO thread, which
     * delivers the acknowledgements that free both the send budget and the per-segment producers'
     * queues, or a producer-dispatch thread, which drains the chain those acknowledgements unblock.
     *
     * <p>Neither may be made to wait for room to send. An IO thread that waits is waiting on itself.
     * A dispatch thread that waits stops draining the chain, so the sends whose completion would
     * free the room never run. Callers on such a thread are failed instead, and a dispatch that
     * would land on an IO thread is handed to a dispatch thread first.
     */
    boolean onClientOwnedThread() {
        return clientOwnedThread.get();
    }

    private boolean computeClientOwnedThread() {
        if (Thread.currentThread().getName().startsWith(PRODUCER_DISPATCH_POOL_NAME)) {
            return true;
        }
        for (EventExecutor eventLoop : v4Client.eventLoopGroup()) {
            if (eventLoop.inEventLoop()) {
                return true;
            }
        }
        return false;
    }

    /**
     * An executor for the dispatch chain of the producer identified by {@code key}. Same key, same
     * single-threaded executor, so a producer's dispatch is never spread across threads.
     */
    ExecutorService producerDispatchExecutor(Object key) {
        return producerDispatchExecutors.getExecutor(key);
    }

    @Override
    public <T> ProducerBuilder<T> newProducer(Schema<T> schema) {
        return new ProducerBuilderV5<>(this, schema);
    }

    @Override
    public <T> StreamConsumerBuilder<T> newStreamConsumer(Schema<T> schema) {
        return new StreamConsumerBuilderV5<>(this, schema);
    }

    @Override
    public <T> QueueConsumerBuilder<T> newQueueConsumer(Schema<T> schema) {
        return new QueueConsumerBuilderV5<>(this, schema);
    }

    @Override
    public <T> CheckpointConsumerBuilder<T> newCheckpointConsumer(Schema<T> schema) {
        return new CheckpointConsumerBuilderV5<>(this, schema);
    }

    @Override
    public Transaction newTransaction() throws PulsarClientException {
        try {
            return newTransactionAsync().get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new PulsarClientException(cause.getMessage(), cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Interrupted while creating transaction", e);
        }
    }

    @Override
    public CompletableFuture<Transaction> newTransactionAsync() {
        var builder = v4Client.newTransaction();
        if (transactionTimeout != null) {
            builder.withTransactionTimeout(transactionTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        return builder.build().thenApply(v4Txn -> (Transaction) new TransactionV5(v4Txn));
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            v4Client.close();
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new PulsarClientException(e.getMessage(), e);
        } finally {
            producerDispatchExecutors.shutdownNow();
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return v4Client.closeAsync()
                .whenComplete((__, ___) -> producerDispatchExecutors.shutdownNow())
                .exceptionally(ex -> {
                    throw new CompletionException(new PulsarClientException(ex.getMessage(), ex));
                });
    }

    @Override
    public void shutdown() {
        try {
            v4Client.shutdown();
        } catch (org.apache.pulsar.client.api.PulsarClientException e) {
            throw new RuntimeException(e);
        } finally {
            producerDispatchExecutors.shutdownNow();
        }
    }
}
