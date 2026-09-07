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
package org.apache.pulsar.client.api.v5.async;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.Producer;

/**
 * Asynchronous view of a {@link Producer}.
 *
 * <p>All operations return {@link CompletableFuture}. Obtained via {@link Producer#async()}.
 *
 * <p>The one thing that can hold up a caller is a full send queue. A producer left at the default
 * {@link org.apache.pulsar.client.api.v5.ProducerBuilder#blockIfQueueFull(boolean)} waits for room
 * before accepting a send, which is the only backpressure an application that never looks at the
 * returned futures would feel. Build the producer with {@code blockIfQueueFull(false)} for a send
 * that fails with {@link org.apache.pulsar.client.api.v5.PulsarClientException.MemoryBufferIsFullException}
 * instead of waiting. A send issued from one of the client's own IO threads — a send chained onto
 * the future of a previous one, say — never waits either way, since waiting there would stall the
 * acknowledgements that drain the queue.
 *
 * @param <T> the type of message values this producer sends
 */
public interface AsyncProducer<T> {

    /**
     * Create a message builder for advanced message construction.
     * Use {@link AsyncMessageBuilder#send()} as the terminal operation.
     *
     * @return a new {@link AsyncMessageBuilder} for configuring and sending a message
     */
    AsyncMessageBuilder<T> newMessage();

    /**
     * Flush all pending messages asynchronously.
     *
     * @return a {@link CompletableFuture} that completes when all pending messages have been
     *         flushed to the broker
     */
    CompletableFuture<Void> flush();

    /**
     * Close this producer asynchronously.
     *
     * @return a {@link CompletableFuture} that completes when the producer has been closed
     *         and all resources have been released
     */
    CompletableFuture<Void> close();
}
