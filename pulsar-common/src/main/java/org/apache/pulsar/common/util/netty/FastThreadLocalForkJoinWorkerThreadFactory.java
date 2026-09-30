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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

/**
 * Creates {@link FastThreadLocalForkJoinWorkerThread}s. The common pool, which runs the {@code *Async} methods of
 * {@code CompletableFuture} by default, uses it with
 * {@code -Djava.util.concurrent.ForkJoinPool.common.threadFactory=}{@value #CLASS_NAME}; the JDK loads the class
 * with the system class loader and needs its public no-argument constructor.
 *
 * <p>{@code conf/pulsar_env.sh} and {@code conf/bkenv.sh} set it for the common pool up to Java 23 only, where its
 * workers behave like the JDK's default ones apart from the {@code FastThreadLocal} cleanup. From Java 24 on, the
 * JDK's default common-pool workers are {@code InnocuousForkJoinWorkerThread}s: they run in their own thread group,
 * instead of the group of the thread that caused them to be created, and clear their {@code ThreadLocal}s when they
 * go idle. These workers can't do that: clearing the {@code ThreadLocal}s would also drop the map that holds their
 * {@code FastThreadLocal} values, without running the values' {@code onRemoval} callbacks.
 */
public final class FastThreadLocalForkJoinWorkerThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
    static final String CLASS_NAME = "org.apache.pulsar.common.util.netty.FastThreadLocalForkJoinWorkerThreadFactory";

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        return new FastThreadLocalForkJoinWorkerThread(pool);
    }
}
