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
package org.apache.pulsar.common.util.netty;

import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

/**
 * A {@link ForkJoinWorkerThread} that Netty treats like a {@link FastThreadLocalThread}: it runs with
 * {@link FastThreadLocalThread#runWithFastThreadLocal(Runnable)}, which removes the worker's {@link FastThreadLocal}
 * values when it terminates.
 *
 * <p>A worker can't extend {@link FastThreadLocalThread}. Netty uses its thread-local recycler pools and allocator
 * caches only on threads that remove their {@link FastThreadLocal} values when they end
 * ({@link FastThreadLocalThread#currentThreadWillCleanupFastThreadLocals()}); on other threads, such as the default
 * workers of the common pool, which runs {@code CompletableFuture}'s {@code *Async} methods by default,
 * {@code Recycler.get()} creates a new object every time. With this worker, those pools and caches are used, and the
 * pool can stop the worker as its load drops without leaving their contents to garbage collection: the values'
 * {@code onRemoval} callbacks release them when the worker terminates.
 *
 * <p>This cleanup matters most for the pooled allocators: {@link FastThreadLocal#removeAll()} removes each value
 * ({@code FastThreadLocal.removeAndGet}), whose {@code onRemoval} returns the thread's cached buffers to the arenas
 * ({@code PooledByteBufAllocator.PoolThreadLocalCache.onRemoval} calls {@code PoolThreadCache.free(false)}) or frees
 * the thread-local magazines of {@code AdaptivePoolingAllocator}. A {@link FastThreadLocalThread} runs
 * {@link FastThreadLocal#removeAll()} only when its {@link Runnable} is passed to its constructor, which wraps it, and
 * {@link Thread#run()} isn't overridden. This class overrides {@code run()} because it isn't a
 * {@link FastThreadLocalThread}: {@link FastThreadLocalThread#runWithFastThreadLocal(Runnable)} does the cleanup.
 */
public final class FastThreadLocalForkJoinWorkerThread extends ForkJoinWorkerThread {

    FastThreadLocalForkJoinWorkerThread(ForkJoinPool pool) {
        super(pool);
    }

    @Override
    public void run() {
        FastThreadLocalThread.runWithFastThreadLocal(super::run);
    }
}
