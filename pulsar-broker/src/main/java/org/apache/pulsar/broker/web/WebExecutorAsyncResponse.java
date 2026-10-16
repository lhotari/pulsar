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
package org.apache.pulsar.broker.web;

import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.TimeoutHandler;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import lombok.CustomLog;

/**
 * An {@link AsyncResponse} that completes the request on the web service executor.
 *
 * <p>Jersey writes the response on the thread that calls {@code resume} or {@code cancel}, and the servlet output
 * stream blocks that thread while a slow client reads the response. Async REST endpoints usually resume in a
 * {@code CompletableFuture} continuation, which runs on the thread that completed the future, such as a metadata
 * store callback thread. This wrapper hands the completion to the web service executor, the pool meant for blocking
 * response writing, unless the calling thread already belongs to it. When the executor rejects the task, the
 * request is completed on the calling thread so that the response isn't lost.
 */
@CustomLog
final class WebExecutorAsyncResponse implements AsyncResponse {

    private final AsyncResponse delegate;
    private final WebExecutorThreadPool executor;
    // Set by the first resume or cancel call, so that later calls fail like they would on a completed response
    // while the first completion is still waiting in the executor's queue.
    private final AtomicBoolean completionRequested = new AtomicBoolean();

    WebExecutorAsyncResponse(AsyncResponse delegate, WebExecutorThreadPool executor) {
        this.delegate = delegate;
        this.executor = executor;
    }

    @Override
    public boolean resume(Object response) {
        return complete(() -> delegate.resume(response));
    }

    @Override
    public boolean resume(Throwable response) {
        return complete(() -> delegate.resume(response));
    }

    @Override
    public boolean cancel() {
        return complete(delegate::cancel);
    }

    @Override
    public boolean cancel(int retryAfter) {
        return complete(() -> delegate.cancel(retryAfter));
    }

    @Override
    public boolean cancel(Date retryAfter) {
        return complete(() -> delegate.cancel(retryAfter));
    }

    private boolean complete(BooleanSupplier completion) {
        if (delegate.isDone() || !completionRequested.compareAndSet(false, true)) {
            return false;
        }
        if (executor.isPoolThread()) {
            return completion.getAsBoolean();
        }
        try {
            executor.execute(() -> {
                try {
                    completion.getAsBoolean();
                } catch (Throwable t) {
                    log.warn().exception(t).log("Failed to complete the async HTTP response");
                }
            });
            return true;
        } catch (RejectedExecutionException e) {
            log.debug()
                    .exceptionMessage(e)
                    .log("The web service executor rejected the response, completing it on the calling thread");
            return completion.getAsBoolean();
        }
    }

    @Override
    public boolean isSuspended() {
        return !completionRequested.get() && delegate.isSuspended();
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public boolean isDone() {
        return completionRequested.get() || delegate.isDone();
    }

    @Override
    public boolean setTimeout(long time, TimeUnit unit) {
        return delegate.setTimeout(time, unit);
    }

    @Override
    public void setTimeoutHandler(TimeoutHandler handler) {
        delegate.setTimeoutHandler(handler);
    }

    @Override
    public Collection<Class<?>> register(Class<?> callback) {
        return delegate.register(callback);
    }

    @Override
    public Map<Class<?>, Collection<Class<?>>> register(Class<?> callback, Class<?>... callbacks) {
        return delegate.register(callback, callbacks);
    }

    @Override
    public Collection<Class<?>> register(Object callback) {
        return delegate.register(callback);
    }

    @Override
    public Map<Class<?>, Collection<Class<?>>> register(Object callback, Object... callbacks) {
        return delegate.register(callback, callbacks);
    }
}
