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
package org.apache.pulsar.metadata.coordination.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ResourceLockReleaseTest {
    @DataProvider
    public Object[][] releaseCases() {
        return new Object[][] {{0, false}, {0, true}, {1, false}, {1, true}, {2, false}, {2, true}};
    }

    @Test(dataProvider = "releaseCases")
    public void testBundleReleaseAndManagerCloseSharePhysicalResult(int failure, boolean cancelCaller)
            throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLock<String> lock = fixture.manager.acquireLock("/bundle", "owner").get(10, TimeUnit.SECONDS);
            CompletableFuture<Void> deletion = new CompletableFuture<>();
            when(fixture.store.delete("/bundle", Optional.of(7L))).thenReturn(deletion);
            CompletableFuture<Void> first = lock.release();
            CompletableFuture<Void> managerClose = fixture.manager.asyncClose();
            CompletableFuture<Void> repeated = lock.release();
            verify(fixture.store).delete("/bundle", Optional.of(7L));
            assertThat(first).isNotDone();
            assertThat(managerClose).isNotDone();
            assertThat(repeated).isNotDone();
            if (cancelCaller) {
                assertThat(first.cancel(false)).isTrue();
                assertThat(managerClose).isNotDone();
                assertThat(repeated).isNotDone();
            }
            // Completing the metadata call must not dispatch expiry listeners while holding the lock monitor.
            CompletableFuture<Void> listener = lock.getLockExpiredFuture().thenRun(() -> {
                assertThat(Thread.holdsLock(lock)).isFalse();
                assertThat(lock.release()).isNotDone();
            });
            if (failure == 1) {
                deletion.completeExceptionally(new MetadataStoreException("ambiguous delete failure"));
                assertThatThrownBy(() -> repeated.get(10, TimeUnit.SECONDS))
                        .hasRootCauseMessage("ambiguous delete failure");
                assertThatThrownBy(() -> managerClose.get(10, TimeUnit.SECONDS))
                        .hasRootCauseMessage("ambiguous delete failure");
                assertThatThrownBy(() -> lock.release().get(10, TimeUnit.SECONDS))
                        .hasRootCauseMessage("ambiguous delete failure");
                assertThat(listener).isNotDone();
            } else {
                if (failure == 2) {
                    deletion.completeExceptionally(new MetadataStoreException.NotFoundException("gone"));
                } else {
                    deletion.complete(null);
                }
                repeated.get(10, TimeUnit.SECONDS);
                managerClose.get(10, TimeUnit.SECONDS);
                listener.get(10, TimeUnit.SECONDS);
                lock.release().get(10, TimeUnit.SECONDS);
            }
            verify(fixture.store).delete("/bundle", Optional.of(7L));
        }
    }

    @Test
    public void testSynchronousDeleteFailureIsRetained() throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLock<String> lock = fixture.manager.acquireLock("/bundle", "owner").get(10, TimeUnit.SECONDS);
            when(fixture.store.delete("/bundle", Optional.of(7L)))
                    .thenThrow(new IllegalStateException("store closed"));
            CompletableFuture<Void> first = lock.release();
            assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("store closed");
            assertThatThrownBy(() -> fixture.manager.asyncClose().get(10, TimeUnit.SECONDS))
                    .hasRootCauseMessage("store closed");
            assertThatThrownBy(() -> lock.release().get(10, TimeUnit.SECONDS)).hasRootCauseMessage("store closed");
            verify(fixture.store).delete("/bundle", Optional.of(7L));
        }
    }

    @Test
    public void testReleaseDoesNotInvokeStoreOrListenersUnderLifecycleMonitor() throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLock<String> lock = fixture.manager.acquireLock("/bundle", "owner").get(10, TimeUnit.SECONDS);
            when(fixture.store.delete("/bundle", Optional.of(7L))).thenAnswer(invocation -> {
                assertThat(Thread.holdsLock(lock)).isFalse();
                return CompletableFuture.completedFuture(null);
            });
            CompletableFuture<Void> listener = lock.getLockExpiredFuture().thenRun(() ->
                    assertThat(Thread.holdsLock(lock)).isFalse());
            lock.release().get(10, TimeUnit.SECONDS);
            listener.get(10, TimeUnit.SECONDS);
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        private final LockManagerImpl<String> manager;

        @SuppressWarnings("unchecked")
        private Fixture() throws Exception {
            MetadataSerde<String> serde = mock(MetadataSerde.class);
            when(serde.serialize(anyString(), anyString())).thenReturn(new byte[0]);
            Stat stat = mock(Stat.class);
            when(stat.getVersion()).thenReturn(7L);
            when(store.put(eq("/bundle"), any(), eq(Optional.of(-1L)), eq(EnumSet.of(CreateOption.Ephemeral))))
                    .thenReturn(CompletableFuture.completedFuture(stat));
            manager = new LockManagerImpl<>(store, serde, executor);
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
