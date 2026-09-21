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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.metadata.api.GetResult;
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

    @DataProvider
    public Object[][] cancellationCases() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "cancellationCases")
    public void testReleaseJoinsQueuedUpdatesAndUsesFinalVersion(boolean cancelCallers) throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLockImpl<String> lock = fixture.acquire();
            CompletableFuture<Stat> firstWrite = new CompletableFuture<>();
            CompletableFuture<Stat> secondWrite = new CompletableFuture<>();
            when(fixture.store.put(eq("/bundle"), any(), eq(Optional.of(7L)),
                    eq(EnumSet.of(CreateOption.Ephemeral)))).thenAnswer(invocation -> {
                assertThat(Thread.holdsLock(lock)).isFalse();
                return firstWrite;
            });
            when(fixture.store.put(eq("/bundle"), any(), eq(Optional.of(8L)),
                    eq(EnumSet.of(CreateOption.Ephemeral)))).thenAnswer(invocation -> {
                assertThat(Thread.holdsLock(lock)).isFalse();
                return secondWrite;
            });
            CompletableFuture<Void> deleted = new CompletableFuture<>();
            when(fixture.store.delete("/bundle", Optional.of(9L))).thenReturn(deleted);
            CompletableFuture<Void> first = lock.updateValue("first");
            CompletableFuture<Void> second = lock.updateValue("second");
            CompletableFuture<Void> released = lock.release();
            if (cancelCallers) {
                assertThat(first.cancel(false)).isTrue();
                assertThat(second.cancel(false)).isTrue();
            }
            verify(fixture.store, never()).delete(anyString(), any());
            assertThat(lock.updateValue("too-late")).isCompletedExceptionally();
            lock.lockWasInvalidated();
            lock.silentRevalidateOnce().get(10, TimeUnit.SECONDS);
            verify(fixture.store, never()).get(anyString());
            firstWrite.complete(stat(8, true));
            assertThat(lock.updateValue("still-too-late")).isCompletedExceptionally();
            verify(fixture.store).put(eq("/bundle"), any(), eq(Optional.of(8L)),
                    eq(EnumSet.of(CreateOption.Ephemeral)));
            verify(fixture.store, never()).delete(anyString(), any());
            secondWrite.complete(stat(9, true));
            assertThat(lock.getValue()).isEqualTo("second");
            assertThat(lock.updateValue("never-revived")).isCompletedExceptionally();
            verify(fixture.store).delete("/bundle", Optional.of(9L));
            assertThat(released).isNotDone();
            deleted.complete(null);
            released.get(10, TimeUnit.SECONDS);
            verify(fixture.store, times(1)).delete(anyString(), any());
            if (!cancelCallers) {
                first.get(10, TimeUnit.SECONDS);
                second.get(10, TimeUnit.SECONDS);
            }
        }
    }

    @DataProvider
    public Object[][] revalidationCases() {
        return new Object[][] {{0}, {1}, {2}};
    }

    @Test(dataProvider = "revalidationCases")
    public void testReleaseJoinsReadAndRecreationWithoutRevivingAdmission(int recovery) throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLockImpl<String> lock = fixture.acquire();
            CompletableFuture<Optional<GetResult>> read = new CompletableFuture<>();
            when(fixture.store.get("/bundle")).thenAnswer(invocation -> {
                assertThat(Thread.holdsLock(lock)).isFalse();
                return read;
            });
            when(fixture.serde.deserialize(eq("/bundle"), any(), any())).thenReturn("owner");
            CompletableFuture<Stat> recreated = new CompletableFuture<>();
            when(fixture.store.put(eq("/bundle"), any(), eq(Optional.of(-1L)),
                    eq(EnumSet.of(CreateOption.Ephemeral)))).thenAnswer(invocation -> {
                assertThat(Thread.holdsLock(lock)).isFalse();
                return recreated;
            });
            CompletableFuture<Void> staleDeleted = new CompletableFuture<>();
            when(fixture.store.delete("/bundle", Optional.of(7L))).thenAnswer(invocation -> {
                assertThat(Thread.holdsLock(lock)).isFalse();
                return staleDeleted;
            });
            long finalVersion = recovery == 0 ? 9 : 0;
            CompletableFuture<Void> deleted = new CompletableFuture<>();
            when(fixture.store.delete("/bundle", Optional.of(finalVersion))).thenAnswer(invocation -> {
                assertThat(Thread.holdsLock(lock)).isFalse();
                return deleted;
            });
            CompletableFuture<Void> revalidation = lock.silentRevalidateOnce();
            assertThat(revalidation.cancel(false)).isTrue();
            CompletableFuture<Void> released = lock.release();
            verify(fixture.store, never()).delete(anyString(), any());
            if (recovery == 0) {
                read.complete(Optional.of(new GetResult(new byte[0], stat(9, true))));
            } else {
                read.complete(recovery == 1 ? Optional.empty()
                        : Optional.of(new GetResult(new byte[0], stat(7, false))));
                if (recovery == 2) {
                    verify(fixture.store).delete("/bundle", Optional.of(7L));
                    verify(fixture.store, times(1)).put(anyString(), any(), any(),
                    eq(EnumSet.of(CreateOption.Ephemeral)));
                    assertThat(released).isNotDone();
                    staleDeleted.complete(null);
                }
                verify(fixture.store, times(2)).put(eq("/bundle"), any(), eq(Optional.of(-1L)),
                    eq(EnumSet.of(CreateOption.Ephemeral)));
                verify(fixture.store, never()).delete("/bundle", Optional.of(finalVersion));
                assertThat(released).isNotDone();
                recreated.complete(stat(0, true));
            }
            assertThat(lock.updateValue("too-late")).isCompletedExceptionally();
            verify(fixture.store).delete("/bundle", Optional.of(finalVersion));
            assertThat(released).isNotDone();
            deleted.complete(null);
            released.get(10, TimeUnit.SECONDS);
            assertThat(lock.updateValue("released")).isCompletedExceptionally();
        }
    }

    @Test
    public void testObservedReplacementExpiresOutsideMonitorWithoutDeletingIt() throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLockImpl<String> lock = fixture.acquire();
            CompletableFuture<Optional<GetResult>> read = new CompletableFuture<>();
            when(fixture.store.get("/bundle")).thenReturn(read);
            when(fixture.serde.deserialize(eq("/bundle"), any(), any())).thenReturn("another-owner");
            lock.silentRevalidateOnce();
            CompletableFuture<Void> released = lock.release();
            CompletableFuture<Void> notified = lock.getLockExpiredFuture().thenRun(() -> {
                assertThat(Thread.holdsLock(lock)).isFalse();
                assertThat(lock.release()).isNotDone();
            });
            read.complete(Optional.of(new GetResult(new byte[0], stat(7, false))));
            released.get(10, TimeUnit.SECONDS);
            notified.get(10, TimeUnit.SECONDS);
            verify(fixture.store, never()).delete(anyString(), any());
            assertThat(lock.updateValue("must-not-overwrite")).isCompletedExceptionally();
        }
    }

    @Test(dataProvider = "cancellationCases")
    public void testAmbiguousUpdateFailureCannotTriggerDelete(boolean notFound) throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLockImpl<String> lock = fixture.acquire();
            CompletableFuture<Stat> write = new CompletableFuture<>();
            when(fixture.store.put(eq("/bundle"), any(), eq(Optional.of(7L)),
                    eq(EnumSet.of(CreateOption.Ephemeral)))).thenReturn(write);
            CompletableFuture<Void> update = lock.updateValue("new-value");
            CompletableFuture<Void> released = lock.release();
            MetadataStoreException error = notFound ? new MetadataStoreException.NotFoundException("uncertain update")
                    : new MetadataStoreException("uncertain update");
            write.completeExceptionally(error);
            verify(fixture.store, never()).delete(anyString(), any());
            assertThat(update).isCompletedExceptionally();
            assertThatThrownBy(() -> released.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("uncertain update");
            assertThatThrownBy(() -> lock.release().get(10, TimeUnit.SECONDS)).hasRootCauseMessage("uncertain update");
            assertThat(lock.getLockExpiredFuture()).isNotDone();
            verify(fixture.store, never()).delete(anyString(), any());
        }
    }

    @Test
    public void testReadFailureDuringReleaseIsRetainedAndDoesNotRetry() throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLockImpl<String> lock = fixture.acquire();
            CompletableFuture<Optional<GetResult>> read = new CompletableFuture<>();
            when(fixture.store.get("/bundle")).thenReturn(read);
            CompletableFuture<Void> revalidation = lock.silentRevalidateOnce();
            CompletableFuture<Void> released = lock.release();
            read.completeExceptionally(new MetadataStoreException("disconnected"));
            revalidation.get(10, TimeUnit.SECONDS);
            assertThatThrownBy(() -> released.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("disconnected");
            lock.lockWasInvalidated();
            lock.revalidateIfNeededAfterReconnection().get(10, TimeUnit.SECONDS);
            verify(fixture.store).get("/bundle");
            verify(fixture.store, never()).delete(anyString(), any());
        }
    }

    @Test
    public void testLocalSerializationFailureDoesNotPreventReleasingKnownOwnership() throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLockImpl<String> lock = fixture.acquire();
            when(fixture.serde.serialize("/bundle", "invalid"))
                    .thenThrow(new IllegalArgumentException("invalid value"));
            assertThat(lock.updateValue("invalid")).isCompletedExceptionally();
            when(fixture.store.delete("/bundle", Optional.of(7L))).thenReturn(CompletableFuture.completedFuture(null));
            lock.release().get(10, TimeUnit.SECONDS);
            verify(fixture.store, times(1)).put(anyString(), any(), any(), eq(EnumSet.of(CreateOption.Ephemeral)));
            verify(fixture.store).delete("/bundle", Optional.of(7L));
        }
    }

    private static Stat stat(long version, boolean self) {
        return new Stat("/bundle", version, 0, 0, true, self);
    }

    @Test
    public void testReleaseBeforeAcquisitionDoesNotDeleteAnUnownedPath() throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLockImpl<String> lock = new ResourceLockImpl<>(fixture.store, fixture.serde,
                    "/bundle", fixture.executor);
            lock.release().get(10, TimeUnit.SECONDS);
            assertThat(lock.acquire("late")).isCompletedExceptionally();
            verify(fixture.store, never()).delete(anyString(), any());
            verify(fixture.store, never()).put(anyString(), any(), any(), eq(EnumSet.of(CreateOption.Ephemeral)));
        }
    }

    @Test
    public void testReleaseAfterInitialSerializationFailureDoesNotDeleteAnUnownedPath() throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLockImpl<String> lock = new ResourceLockImpl<>(fixture.store, fixture.serde,
                    "/bundle", fixture.executor);
            when(fixture.serde.serialize("/bundle", "invalid"))
                    .thenThrow(new IllegalArgumentException("invalid value"));
            assertThat(lock.acquire("invalid")).isCompletedExceptionally();
            lock.release().get(10, TimeUnit.SECONDS);
            verify(fixture.store, never()).delete(anyString(), any());
            verify(fixture.store, never()).put(anyString(), any(), any(), eq(EnumSet.of(CreateOption.Ephemeral)));
        }
    }

    @Test
    public void testReleaseDuringInitialAcquisitionWaitsForThePhysicalWrite() throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLockImpl<String> lock = new ResourceLockImpl<>(fixture.store, fixture.serde,
                    "/bundle", fixture.executor);
            CompletableFuture<Stat> created = new CompletableFuture<>();
            when(fixture.store.put(eq("/bundle"), any(), eq(Optional.of(-1L)),
                    eq(EnumSet.of(CreateOption.Ephemeral)))).thenAnswer(invocation -> {
                        assertThat(Thread.holdsLock(lock)).isFalse();
                        return created;
                    });
            CompletableFuture<Void> deleted = new CompletableFuture<>();
            when(fixture.store.delete("/bundle", Optional.of(7L))).thenReturn(deleted);
            CompletableFuture<Void> acquisition = lock.acquire("owner");
            CompletableFuture<Void> released = lock.release();
            assertThat(acquisition.cancel(false)).isTrue();
            verify(fixture.store, never()).delete(anyString(), any());
            created.complete(stat(7, true));
            assertThat(lock.updateValue("late")).isCompletedExceptionally();
            assertThat(released).isNotDone();
            verify(fixture.store).delete("/bundle", Optional.of(7L));
            deleted.complete(null);
            released.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDeepAdmittedUpdateQueueDrainsWithoutCallerCancellationSkippingWrites() throws Exception {
        try (Fixture fixture = new Fixture()) {
            ResourceLockImpl<String> lock = fixture.acquire();
            int updates = 1000;
            CompletableFuture<Stat> firstWrite = new CompletableFuture<>();
            AtomicInteger calls = new AtomicInteger();
            when(fixture.store.put(eq("/bundle"), any(), any(), eq(EnumSet.of(CreateOption.Ephemeral))))
                    .thenAnswer(invocation -> {
                        if (calls.getAndIncrement() == 0) {
                            return firstWrite;
                        }
                        Optional<Long> expected = invocation.getArgument(2);
                        return CompletableFuture.completedFuture(stat(expected.orElseThrow() + 1, true));
                    });
            when(fixture.store.delete("/bundle", Optional.of(7L + updates)))
                    .thenReturn(CompletableFuture.completedFuture(null));
            for (int index = 0; index < updates; index++) {
                CompletableFuture<Void> update = lock.updateValue("value-" + index);
                if (index % 2 == 0) {
                    assertThat(update.cancel(false)).isTrue();
                }
            }
            CompletableFuture<Void> released = lock.release();
            assertThat(calls).hasValue(1);
            firstWrite.complete(stat(8, true));
            released.get(10, TimeUnit.SECONDS);
            assertThat(calls).hasValue(updates);
            assertThat(lock.getValue()).isEqualTo("value-999");
            verify(fixture.store).delete("/bundle", Optional.of(7L + updates));
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        private final LockManagerImpl<String> manager;
        private final MetadataSerde<String> serde;

        @SuppressWarnings("unchecked")
        private Fixture() throws Exception {
            serde = mock(MetadataSerde.class);
            when(serde.serialize(anyString(), anyString())).thenReturn(new byte[0]);
            Stat stat = mock(Stat.class);
            when(stat.getVersion()).thenReturn(7L);
            when(store.put(eq("/bundle"), any(), eq(Optional.of(-1L)), eq(EnumSet.of(CreateOption.Ephemeral))))
                    .thenReturn(CompletableFuture.completedFuture(stat));
            manager = new LockManagerImpl<>(store, serde, executor);
        }

        private ResourceLockImpl<String> acquire() throws Exception {
            return (ResourceLockImpl<String>) manager.acquireLock("/bundle", "owner").get(10, TimeUnit.SECONDS);
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}
