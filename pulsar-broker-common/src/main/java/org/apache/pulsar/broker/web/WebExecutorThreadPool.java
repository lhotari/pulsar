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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;

/**
 * The thread pool of the Jetty web servers.
 *
 * <p>The pool keeps {@code numThreads} threads. Unless {@code maxThreads} is larger, it is a fixed size pool that
 * queues tasks when all of its threads are busy. When {@code maxThreads} is larger, the pool starts additional
 * threads, up to {@code maxThreads}, when a task is submitted and no thread is idle, and only queues tasks once
 * it has {@code maxThreads} threads. Threads beyond {@code numThreads} stop after being idle for the idle timeout,
 * see {@link #setIdleTimeout(int)}, which is {@link #DEFAULT_IDLE_TIMEOUT_MS} by default. A plain
 * {@link ThreadPoolExecutor} would start threads beyond its core size only once its queue is full.
 */
public class WebExecutorThreadPool extends ExecutorThreadPool {

    public static final int DEFAULT_IDLE_TIMEOUT_MS = 60_000;

    private static final ThreadLocal<WebExecutorThreadPool> CURRENT_POOL = new ThreadLocal<>();

    private final ThreadFactory threadFactory;
    private volatile boolean startAttempted;

    public WebExecutorThreadPool(int numThreads, String namePrefix) {
        this(numThreads, namePrefix, 8192);
    }

    public WebExecutorThreadPool(int numThreads, String namePrefix, int queueCapacity) {
        this(numThreads, 0, namePrefix, queueCapacity);
    }

    /**
     * @param numThreads the number of threads that the pool keeps
     * @param maxThreads the maximum number of threads; a value smaller than {@code numThreads}, such as 0, means
     *                   that the pool doesn't grow beyond {@code numThreads}
     * @param namePrefix the name prefix of the threads
     * @param queueCapacity the capacity of the queue for tasks that wait for a thread
     */
    public WebExecutorThreadPool(int numThreads, int maxThreads, String namePrefix, int queueCapacity) {
        super(createExecutor(numThreads, maxThreads, queueCapacity), -1, null);
        // the number of threads that are started when the pool starts
        setMinThreads(Math.min(8, numThreads));
        this.threadFactory = new DefaultThreadFactory(namePrefix);
    }

    private static ThreadPoolExecutor createExecutor(int numThreads, int maxThreads, int queueCapacity) {
        if (maxThreads <= numThreads) {
            // the same executor as ExecutorThreadPool(maxThreads, minThreads, queue) creates
            return new ThreadPoolExecutor(numThreads, numThreads, DEFAULT_IDLE_TIMEOUT_MS, TimeUnit.MILLISECONDS,
                    new BlockingArrayQueue<>(queueCapacity));
        }
        return new GrowingThreadPoolExecutor(numThreads, maxThreads, queueCapacity);
    }

    @Override
    protected Thread newThread(Runnable job) {
        return threadFactory.newThread(() -> {
            CURRENT_POOL.set(this);
            job.run();
        });
    }

    /**
     * Returns whether the calling thread is one of this pool's threads.
     */
    public boolean isPoolThread() {
        return CURRENT_POOL.get() == this;
    }

    @Override
    protected void doStart() throws Exception {
        startAttempted = true;
        super.doStart();
    }

    /**
     * {@link ExecutorThreadPool#join()} waits without a timeout for the executor to terminate, but the executor
     * is shut down only in {@link #doStop()}, which Jetty skips for a pool that was never started. That happens
     * when {@code Server.start()} fails to open a connector (e.g. the port is already in use), since connectors
     * are opened before the server's beans are started. Joining such a pool would block forever.
     */
    @Override
    public void join() throws InterruptedException {
        if (startAttempted) {
            super.join();
        }
    }

    /**
     * A {@link ThreadPoolExecutor} that starts a new thread, up to its maximum pool size, instead of queueing a
     * task when no thread is idle.
     */
    private static final class GrowingThreadPoolExecutor extends ThreadPoolExecutor {
        // tasks that have been submitted and haven't completed yet, both queued and running
        private final AtomicInteger submittedTasks = new AtomicInteger();

        GrowingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, int queueCapacity) {
            this(corePoolSize, maximumPoolSize, new TaskQueue(queueCapacity));
        }

        private GrowingThreadPoolExecutor(int corePoolSize, int maximumPoolSize, TaskQueue queue) {
            super(corePoolSize, maximumPoolSize, DEFAULT_IDLE_TIMEOUT_MS, TimeUnit.MILLISECONDS, queue,
                    (task, executor) -> {
                        // the queue declined the task to start a new thread, but the pool reached its maximum
                        // size concurrently, so queue the task after all
                        if (executor.isShutdown() || !queue.forceOffer(task)) {
                            throw new RejectedExecutionException("Task " + task + " rejected from " + executor);
                        }
                    });
            queue.executor = this;
        }

        @Override
        public void execute(Runnable command) {
            submittedTasks.incrementAndGet();
            try {
                super.execute(command);
            } catch (RuntimeException | Error e) {
                submittedTasks.decrementAndGet();
                throw e;
            }
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            submittedTasks.decrementAndGet();
        }
    }

    private static final class TaskQueue extends BlockingArrayQueue<Runnable> {
        private volatile GrowingThreadPoolExecutor executor;

        TaskQueue(int capacity) {
            super(capacity);
        }

        @Override
        public boolean offer(Runnable task) {
            GrowingThreadPoolExecutor executor = this.executor;
            if (executor != null) {
                int poolSize = executor.getPoolSize();
                // returning false makes the executor start a new thread when no thread is idle
                if (poolSize < executor.getMaximumPoolSize() && executor.submittedTasks.get() > poolSize) {
                    return false;
                }
            }
            return super.offer(task);
        }

        boolean forceOffer(Runnable task) {
            return super.offer(task);
        }
    }
}
