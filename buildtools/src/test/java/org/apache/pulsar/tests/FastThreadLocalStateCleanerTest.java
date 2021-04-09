/**
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
package org.apache.pulsar.tests;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

public class FastThreadLocalStateCleanerTest {
    final FastThreadLocal<Integer> magicNumberThreadLocal = new FastThreadLocal<Integer>() {
        @Override
        protected Integer initialValue() throws Exception {
            return 42;
        }
    };

    @Test
    public void testThreadLocalStateCleanupInCurrentThread() {
        magicNumberThreadLocal.set(44);
        assertEquals(magicNumberThreadLocal.get().intValue(), 44);
        FastThreadLocalStateCleaner.INSTANCE.cleanupAllFastThreadLocals(Thread.currentThread(), null);
        assertEquals(magicNumberThreadLocal.get().intValue(), 42);
    }

    @Test
    public void testThreadLocalStateCleanupInCurrentAndOtherThread() throws InterruptedException, ExecutionException {
        magicNumberThreadLocal.set(44);
        assertEquals(magicNumberThreadLocal.get().intValue(), 44);

        CountDownLatch numberHasBeenSet = new CountDownLatch(1);
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        CompletableFuture<Integer> valueAfterReset = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                magicNumberThreadLocal.set(45);
                assertEquals(magicNumberThreadLocal.get().intValue(), 45);
                numberHasBeenSet.countDown();
                shutdownLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                valueAfterReset.complete(magicNumberThreadLocal.get());
            }
        });
        thread.start();
        numberHasBeenSet.await();
        Set<Thread> cleanedThreads = new HashSet<>();
        FastThreadLocalStateCleaner.INSTANCE.cleanupAllFastThreadLocals((t, currentValue) -> {
            cleanedThreads.add(t);
        });
        shutdownLatch.countDown();
        assertEquals(magicNumberThreadLocal.get().intValue(), 42);
        assertEquals(valueAfterReset.get().intValue(), 42);
        assertEquals(cleanedThreads.size(), 2);
        assertTrue(cleanedThreads.contains(thread));
        assertTrue(cleanedThreads.contains(Thread.currentThread()));
    }

    @Test
    public void testThreadLocalStateCleanupInFastThreadLocalThread() throws InterruptedException, ExecutionException {
        CountDownLatch numberHasBeenSet = new CountDownLatch(1);
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        CompletableFuture<Integer> valueAfterReset = new CompletableFuture<>();
        Thread thread = new FastThreadLocalThread(() -> {
            try {
                magicNumberThreadLocal.set(45);
                assertEquals(magicNumberThreadLocal.get().intValue(), 45);
                numberHasBeenSet.countDown();
                shutdownLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                valueAfterReset.complete(magicNumberThreadLocal.get());
            }
        });
        thread.start();
        numberHasBeenSet.await();
        Set<Thread> cleanedThreads = new HashSet<>();
        FastThreadLocalStateCleaner.INSTANCE.cleanupAllFastThreadLocals((t, currentValue) -> {
            cleanedThreads.add(t);
        });
        shutdownLatch.countDown();
        assertEquals(valueAfterReset.get().intValue(), 42);
        assertTrue(cleanedThreads.contains(thread));
    }

}