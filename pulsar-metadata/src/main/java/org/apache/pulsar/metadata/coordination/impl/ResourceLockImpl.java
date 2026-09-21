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

import java.util.EnumSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.LockBusyException;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@CustomLog
public class ResourceLockImpl<T> implements ResourceLock<T> {

    private final MetadataStoreExtended store;
    private final MetadataSerde<T> serde;
    private final String path;

    private volatile T value;
    private long version;
    private final CompletableFuture<Void> expiredFuture;
    private CompletableFuture<Void> releaseFuture;
    private boolean revalidateAfterReconnection = false;
    private final Backoff backoff;
    // Admission and tail replacement are guarded by this monitor; work and callbacks run outside it.
    private CompletableFuture<Void> operationTail = CompletableFuture.completedFuture(null);
    private Throwable ownershipFailure;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> revalidateTask;

    private enum State {
        Init,
        Valid,
        Releasing,
        Released,
    }

    private State state;

    ResourceLockImpl(MetadataStoreExtended store, MetadataSerde<T> serde, String path,
                     ScheduledExecutorService executor) {
        this.store = store;
        this.serde = serde;
        this.path = path;
        this.version = -1;
        this.expiredFuture = new CompletableFuture<>();
        this.state = State.Init;
        this.executor = executor;
        this.backoff = Backoff.create();
    }

    @Override
    public synchronized T getValue() {
        return value;
    }

    @Override
    public CompletableFuture<Void> updateValue(T newValue) {
        return submit(State.Valid, () -> acquireValue(newValue), false);
    }

    private CompletableFuture<Void> submit(State required, Supplier<CompletableFuture<Void>> operation,
                                           boolean ignoreClosed) {
        CompletableFuture<Void> admitted = new CompletableFuture<>();
        CompletableFuture<Void> result;
        synchronized (this) {
            if (state != required) {
                return ignoreClosed ? CompletableFuture.completedFuture(null) : CompletableFuture.failedFuture(
                        new IllegalStateException("Lock was not in valid state: " + state));
            }
            // The incomplete admission gate prevents even an immediately completed predecessor from invoking
            // store code while this monitor is held. Keep the actual operation in the private tail.
            result = operationTail.handle((ignored, error) -> null).thenCompose(ignored -> admitted)
                    .thenCompose(ignored -> {
                        synchronized (this) {
                            if (state == State.Released) {
                                return CompletableFuture.failedFuture(
                                        new LockBusyException("Lock has expired: " + path));
                            }
                        }
                        return FutureUtil.supplySafely(operation);
                    });
            operationTail = result;
        }
        // Already admitted work may finish while release waits; new work cannot pass its state check.
        admitted.complete(null);
        return result.copy();
    }

    @Override
    public CompletableFuture<Void> release() {
        CompletableFuture<Void> result;
        CompletableFuture<Void> previous;
        ScheduledFuture<?> pendingRevalidation;
        synchronized (this) {
            if (releaseFuture != null) {
                return releaseFuture.copy();
            }
            if (state == State.Released) {
                return CompletableFuture.completedFuture(null);
            }
            result = new CompletableFuture<>();
            releaseFuture = result;
            state = State.Releasing;
            previous = operationTail;
            pendingRevalidation = revalidateTask;
            revalidateTask = null;
            revalidateAfterReconnection = false;
        }
        if (pendingRevalidation != null) {
            pendingRevalidation.cancel(false);
        }

        // One delete, using the version after every previously admitted operation settled. An ambiguous failed
        // operation is not permission to delete a path that might now belong to another session or owner.
        previous.handle((ignored, error) -> null).thenCompose(ignored -> deleteAfterOperations())
                .whenComplete((ignored, error) -> {
                    if (error == null) {
                        synchronized (ResourceLockImpl.this) {
                            state = State.Released;
                        }
                        expiredFuture.complete(null);
                        result.complete(null);
                    } else {
                        result.completeExceptionally(error);
                    }
                });
        return result.copy();
    }

    private CompletableFuture<Void> deleteAfterOperations() {
        long expectedVersion;
        synchronized (this) {
            if (state == State.Released) {
                // Revalidation proved that this generation was lost. Never delete the observed replacement.
                return CompletableFuture.completedFuture(null);
            }
            if (ownershipFailure != null) {
                return CompletableFuture.failedFuture(ownershipFailure);
            }
            expectedVersion = version;
        }
        if (expectedVersion < 0) {
            // No write was attempted successfully. Never turn the initial version into an unconditional delete.
            return CompletableFuture.completedFuture(null);
        }
        return FutureUtil.supplySafely(() -> store.delete(path, Optional.of(expectedVersion)))
                .exceptionallyCompose(error -> FutureUtil.unwrapCompletionException(error)
                        instanceof MetadataStoreException.NotFoundException ? CompletableFuture.completedFuture(null)
                        : CompletableFuture.failedFuture(error));
    }

    @Override
    public CompletableFuture<Void> getLockExpiredFuture() {
        return expiredFuture.copy();
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    CompletableFuture<Void> acquire(T newValue) {
        return submit(State.Init, () -> acquireValue(newValue), false);
    }

    private CompletableFuture<Void> acquireValue(T newValue) {
        return acquireWithNoRevalidation(newValue).exceptionallyCompose(error -> {
            Throwable cause = FutureUtil.unwrapCompletionException(error);
            return cause instanceof LockBusyException ? revalidate(newValue) : CompletableFuture.failedFuture(cause);
        });
    }

    // Simple operation of acquiring the lock with no retries, or checking for the lock content
    private CompletableFuture<Void> acquireWithNoRevalidation(T newValue) {
        long expectedVersion;
        synchronized (this) {
            expectedVersion = version;
        }
        log.debug().attr("newValue", newValue).attr("version", expectedVersion).log("acquireWithNoRevalidation");
        byte[] payload;
        try {
            payload = serde.serialize(path, newValue);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return FutureUtil.supplySafely(() -> store.put(path, payload, Optional.of(expectedVersion),
                EnumSet.of(CreateOption.Ephemeral))).thenAccept(stat -> {
            synchronized (this) {
                // Release waits for this write's version but admission must remain sealed.
                if (state == State.Init) {
                    state = State.Valid;
                }
                version = stat.getVersion();
                value = newValue;
                ownershipFailure = null;
            }
            log.info().attr("path", path).log("Acquired resource lock");
        }).exceptionallyCompose(error -> {
            Throwable cause = FutureUtil.unwrapCompletionException(error);
            synchronized (this) {
                ownershipFailure = cause;
            }
            return CompletableFuture.failedFuture(cause instanceof BadVersionException
                    ? new LockBusyException("Resource at " + path + " is already locked") : cause);
        });
    }

    void lockWasInvalidated() {
        log.info().attr("path", path).log("Lock on resource was invalidated");
        silentRevalidateOnce();
    }

    CompletableFuture<Void> revalidateIfNeededAfterReconnection() {
        synchronized (this) {
            if (!revalidateAfterReconnection) {
                return CompletableFuture.completedFuture(null);
            }
            revalidateAfterReconnection = false;
        }
        log.warn().attr("path", path).log("Revalidate lock after reconnection");
        return silentRevalidateOnce();
    }

    /** Revalidations admitted before release join its barrier; later notifications cannot start new work. */
    CompletableFuture<Void> silentRevalidateOnce() {
        return submit(State.Valid, () -> revalidate(value).whenComplete((ignored, error) ->
                revalidationCompleted(error)), true).exceptionally(error -> null);
    }

    private void revalidationCompleted(Throwable error) {
        boolean expired = false;
        long retryDelayMillis = -1;
        synchronized (this) {
            if (error == null) {
                backoff.reset();
            } else {
                Throwable cause = FutureUtil.unwrapCompletionException(error);
                if (cause instanceof BadVersionException || cause instanceof LockBusyException) {
                    state = State.Released;
                    expired = true;
                } else if (state == State.Valid) {
                    revalidateAfterReconnection = true;
                    retryDelayMillis = backoff.next().toMillis();
                }
            }
        }
        if (expired) {
            log.warn().attr("path", path).exceptionMessage(error).log("Failed to revalidate lock. Marked as expired.");
            expiredFuture.complete(null);
        } else if (retryDelayMillis >= 0) {
            log.warn().attr("path", path).exceptionMessage(error).attr("retryInMillis", retryDelayMillis)
                    .log("Failed to revalidate lock. Retrying.");
            ScheduledFuture<?> retry = executor.schedule(this::silentRevalidateOnce, retryDelayMillis,
                    TimeUnit.MILLISECONDS);
            boolean cancel;
            synchronized (this) {
                cancel = state != State.Valid;
                if (!cancel) {
                    revalidateTask = retry;
                }
            }
            if (cancel) {
                retry.cancel(false);
            }
        }
    }

    private CompletableFuture<Void> revalidate(T newValue) {
        // This is part of an admitted operation and may finish while release is waiting for it.
        synchronized (this) {
            if (state == State.Released) {
                return CompletableFuture.failedFuture(new LockBusyException("Lock has expired: " + path));
            }
        }
        log.debug().attr("newValue", newValue).log("doRevalidate");
        return FutureUtil.supplySafely(() -> store.get(path))
                .thenCompose(optGetResult -> {
                    if (!optGetResult.isPresent()) {
                        // The lock just disappeared, try to acquire it again
                        // Reset the expectation on the version
                        setVersion(-1L);
                        return acquireWithNoRevalidation(newValue)
                                .thenRun(() -> log.info().attr("path", path)
                                        .log("Successfully re-acquired missing lock"));
                    }

                    GetResult res = optGetResult.get();
                    if (!res.getStat().isEphemeral()) {
                        return CompletableFuture.failedFuture(
                                new LockBusyException(
                                        "Path " + path + " is already created as non-ephemeral"));
                    }

                    T existingValue;
                    try {
                        existingValue = serde.deserialize(path, res.getValue(), res.getStat());
                    } catch (Throwable t) {
                        return CompletableFuture.failedFuture(t);
                    }

                    if (newValue.equals(existingValue) && res.getStat().isCreatedBySelf()) {
                        synchronized (this) {
                            version = res.getStat().getVersion();
                            value = newValue;
                            if (state == State.Init) {
                                state = State.Valid;
                            }
                        }
                        return CompletableFuture.completedFuture(null);
                    }
                    if (!newValue.equals(existingValue) && !res.getStat().isCreatedBySelf()) {
                        return CompletableFuture.failedFuture(
                                new LockBusyException("Resource at " + path + " is already locked"));
                    }
                    // Preserve the existing same-value stale-session recovery and same-session value update.
                    // The whole delete/recreate chain belongs to this operation and precedes final release.
                    return FutureUtil.supplySafely(() -> store.delete(path,
                                    Optional.of(res.getStat().getVersion())))
                            .thenRun(() -> setVersion(-1L))
                            .thenCompose(ignored -> acquireWithNoRevalidation(newValue));
                }).whenComplete((ignored, error) -> {
                    synchronized (this) {
                        // Revalidation follows invalidation/session events. Even a failed read leaves ownership
                        // unproven: a recreated ZooKeeper path can have the previous generation's version again.
                        ownershipFailure = error == null ? null : FutureUtil.unwrapCompletionException(error);
                    }
                });
    }

    private synchronized void setVersion(long version) {
        this.version = version;
    }
}
