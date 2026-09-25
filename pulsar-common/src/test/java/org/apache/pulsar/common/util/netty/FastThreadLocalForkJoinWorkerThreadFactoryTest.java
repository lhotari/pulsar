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

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.util.Recycler;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

public class FastThreadLocalForkJoinWorkerThreadFactoryTest {

    private static final class Pooled {
        private final Recycler.Handle<Pooled> handle;

        Pooled(Recycler.Handle<Pooled> handle) {
            this.handle = handle;
        }
    }

    private static final Recycler<Pooled> RECYCLER = new Recycler<>() {
        @Override
        protected Pooled newObject(Handle<Pooled> handle) {
            return new Pooled(handle);
        }
    };

    private static boolean reusesRecycledObject() {
        Pooled pooled = RECYCLER.get();
        pooled.handle.recycle(pooled);
        return RECYCLER.get() == pooled;
    }

    @Test
    public void testWorkerUsesNettyThreadLocalPools() throws Exception {
        ForkJoinPool pool = new ForkJoinPool(1, new FastThreadLocalForkJoinWorkerThreadFactory(), null, false);
        try {
            assertThat(pool.submit(FastThreadLocalThread::currentThreadWillCleanupFastThreadLocals)
                    .get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(pool.submit(FastThreadLocalForkJoinWorkerThreadFactoryTest::reusesRecycledObject)
                    .get(10, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdown();
        }
    }

    @Test
    public void testDefaultWorkerDoesNotUseNettyThreadLocalPools() throws Exception {
        // The behavior that the factory changes: without it, Netty's recycler doesn't pool objects on the workers
        ForkJoinPool pool = new ForkJoinPool(1);
        try {
            assertThat(pool.submit(FastThreadLocalForkJoinWorkerThreadFactoryTest::reusesRecycledObject)
                    .get(10, TimeUnit.SECONDS)).isFalse();
        } finally {
            pool.shutdown();
        }
    }

    @Test
    public void testWorkerRemovesFastThreadLocalsWhenItTerminates() throws Exception {
        CountDownLatch removed = new CountDownLatch(1);
        AtomicReference<String> removedValue = new AtomicReference<>();
        FastThreadLocal<String> threadLocal = new FastThreadLocal<>() {
            @Override
            protected void onRemoval(String value) {
                removedValue.set(value);
                removed.countDown();
            }
        };
        ForkJoinPool pool = new ForkJoinPool(1, new FastThreadLocalForkJoinWorkerThreadFactory(), null, false);
        try {
            Thread worker = pool.submit(() -> {
                threadLocal.set("value");
                return Thread.currentThread();
            }).get(10, TimeUnit.SECONDS);
            assertThat(worker).isInstanceOf(FastThreadLocalForkJoinWorkerThread.class);
        } finally {
            pool.shutdown();
        }
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(removed.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(removedValue.get()).isEqualTo("value");
    }

    @Test
    public void testClassNameMatchesTheFactory() {
        assertThat(FastThreadLocalForkJoinWorkerThreadFactory.CLASS_NAME)
                .isEqualTo(FastThreadLocalForkJoinWorkerThreadFactory.class.getName());
    }
}
