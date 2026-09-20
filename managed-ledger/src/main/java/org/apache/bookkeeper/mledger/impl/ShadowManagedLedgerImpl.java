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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.util.Errors.isNoSuchLedgerExistsException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerAlreadyClosedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerInterceptException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.NestedPositionInfo;
import org.apache.bookkeeper.mledger.util.Futures.CloseFuture;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.Stat;

/**
 * Detailed design can be found in <a href="https://github.com/apache/pulsar/issues/16153">PIP-180</a>.
 */
@CustomLog
public class ShadowManagedLedgerImpl extends ManagedLedgerImpl {
    private final String sourceMLName;
    private volatile Stat sourceLedgersStat;
    // Guarded by this; unwatch does not cancel already-issued source ledger opens.
    private final Set<CompletableFuture<Void>> pendingSourceOpens = new HashSet<>();
    private CompletableFuture<Void> shadowCloseFuture;
    private Throwable sourceCleanupFailure;
    private boolean initializingSource = true;
    private SourceInfo deferredSourceInfo;

    private record SourceInfo(ManagedLedgerInfo info, Stat stat) { }

    public ShadowManagedLedgerImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper,
                                   MetaStore store, ManagedLedgerConfig config,
                                   OrderedScheduler scheduledExecutor,
                                   String name, final Supplier<CompletableFuture<Boolean>> mlOwnershipChecker) {
        super(factory, bookKeeper, store, config, scheduledExecutor, name, mlOwnershipChecker);
        this.sourceMLName = config.getShadowSourceName();
        // ShadowManagedLedgerImpl does not implement add entry timeout yet, so this variable will always be false.
        this.currentLedgerTimeoutTriggered = new AtomicBoolean(false);
    }

    /**
     * ShadowManagedLedger init steps:
     * 1. this.initializeMetadata : read source managedLedgerInfo
     * 2. super.initializeMetadata : read its own managedLedgerInfo
     * 3. this.initializeBookKeeper
     * 4. super.initializeCursors
     */
    @Override
    void initializeMetadata(ManagedLedgerInitializeLedgerCallback callback, Object ctx) {
        log.info().attr("name", name).attr("source", sourceMLName).log("Opening shadow managed ledger");
        executor.execute(() -> {
            try {
                doInitialize(callback, ctx);
            } catch (Throwable error) {
                callback.initializeFailed(createManagedLedgerException(error));
            }
        });
    }

    private synchronized void doInitialize(ManagedLedgerInitializeLedgerCallback callback, Object ctx) {
        if (sourceIsClosing()) {
            callback.initializeFailed(new ManagedLedgerAlreadyClosedException("Shadow ledger is closed"));
            return;
        }
        // Fetch the list of existing ledgers in the source managed ledger
        store.watchManagedLedgerInfo(sourceMLName, (managedLedgerInfo, stat) -> {
            synchronized (ShadowManagedLedgerImpl.this) {
                if (sourceIsClosing()) {
                    return;
                }
            }
            try {
                executor.execute(() -> processSourceManagedLedgerInfo(managedLedgerInfo, stat));
            } catch (Throwable error) {
                log.debug().exception(error).log("Could not dispatch source metadata update");
            }
        });
        store.getManagedLedgerInfo(sourceMLName, false, null, new MetaStore.MetaStoreCallback<>() {
            @Override
            public void operationComplete(ManagedLedgerInfo mlInfo, Stat stat) {
                try {
                    initializeSource(callback, ctx, mlInfo, stat);
                } catch (Throwable error) {
                    callback.initializeFailed(createManagedLedgerException(error));
                }
            }

            @Override
            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                if (e instanceof ManagedLedgerException.MetadataNotFoundException) {
                    callback.initializeFailed(new ManagedLedgerException.ManagedLedgerNotFoundException(e));
                } else {
                    callback.initializeFailed(new ManagedLedgerException(e));
                }
            }
        });
    }

    private synchronized boolean sourceIsClosing() {
        return shadowCloseFuture != null || isClosing();
    }

    private synchronized void initializeSource(ManagedLedgerInitializeLedgerCallback callback, Object ctx,
                                               ManagedLedgerInfo info, Stat stat) {
        if (sourceIsClosing()) {
            callback.initializeFailed(new ManagedLedgerAlreadyClosedException("Shadow ledger is closed"));
            return;
        }
        sourceLedgersStat = stat;
        for (int i = 0; i < info.getLedgerInfosCount(); i++) {
            LedgerInfo ledgerInfo = info.getLedgerInfoAt(i);
            ledgers.put(ledgerInfo.getLedgerId(), ledgerInfo);
        }
        if (info.hasTerminatedPosition()) {
            NestedPositionInfo position = info.getTerminatedPosition();
            lastConfirmedEntry = PositionFactory.create(position.getLedgerId(), position.getEntryId());
        }
        if (ledgers.isEmpty()) {
            super.initializeMetadata(callback, ctx);
            return;
        }
        long ledgerId = ledgers.lastKey();
        openSourceLedger(ledgerId, (handle, error) -> {
            synchronized (ShadowManagedLedgerImpl.this) {
                if (sourceIsClosing()) {
                    callback.initializeFailed(new ManagedLedgerAlreadyClosedException("Shadow ledger is closed"));
                    return closeSourceHandle(handle);
                }
                if (error != null) {
                    if (isNoSuchLedgerExistsException(BKException.getExceptionCode(error))) {
                        ledgers.remove(ledgerId);
                        ShadowManagedLedgerImpl.super.initializeMetadata(callback, ctx);
                    } else {
                        callback.initializeFailed(createManagedLedgerException(error));
                    }
                    return CompletableFuture.completedFuture(null);
                }
                ledgers.put(ledgerId, new LedgerInfo().setLedgerId(ledgerId)
                        .setEntries(handle.getLastAddConfirmed() + 1).setSize(handle.getLength())
                        .setTimestamp(clock.millis()));
                STATE_UPDATER.set(ShadowManagedLedgerImpl.this, State.LedgerOpened);
                currentLedger = handle;
            }
            CompletableFuture<Void> intercepted = managedLedgerInterceptor == null
                    ? CompletableFuture.completedFuture(null)
                    : FutureUtil.supplySafely(() -> managedLedgerInterceptor
                            .onManagedLedgerLastLedgerInitialize(name, createLastEntryHandle(handle)));
            intercepted.whenComplete((__, failure) -> {
                if (failure == null) {
                    ShadowManagedLedgerImpl.super.initializeMetadata(callback, ctx);
                } else {
                    callback.initializeFailed(new ManagedLedgerInterceptException(failure));
                }
            });
            return intercepted.handle((__, failure) -> null);
        }, error -> callback.initializeFailed(createManagedLedgerException(error)));
    }

    // Called under the lifecycle monitor after the source admission check.
    private void openSourceLedger(long ledgerId,
                                  BiFunction<LedgerHandle, Throwable, CompletableFuture<Void>> process,
                                  Consumer<Throwable> failed) {
        CompletableFuture<Void> operation = new CompletableFuture<>();
        synchronized (this) {
            pendingSourceOpens.add(operation);
        }
        mbean.startDataLedgerOpenOp();
        FutureUtil.supplySafely(() -> bookKeeper.newOpenLedgerOp().withRecovery(false).withLedgerId(ledgerId)
                .withDigestType(config.getDigestType()).withPassword(config.getPassword()).withOrderingKey(name)
                .execute()).whenComplete((readHandle, error) -> {
                    mbean.endDataLedgerOpenOp();
                    LedgerHandle handle = (LedgerHandle) readHandle;
                    Runnable complete = () -> {
                        CompletableFuture<Void> processing;
                        try {
                            processing = process.apply(handle, error);
                        } catch (Throwable failure) {
                            processing = closeSourceHandle(handle).thenCompose(__ ->
                                    CompletableFuture.failedFuture(failure));
                        }
                        processing.whenComplete((__, failure) -> finishSourceOpen(operation, failure, failed));
                    };
                    try {
                        executor.execute(complete);
                    } catch (Throwable failure) {
                        closeSourceHandle(handle).whenComplete((__, closeError) -> {
                            finishSourceOpen(operation, closeError, failed);
                            if (closeError == null) {
                                failed.accept(failure);
                            }
                        });
                    }
                });
    }

    private void finishSourceOpen(CompletableFuture<Void> operation, Throwable error, Consumer<Throwable> failed) {
        synchronized (this) {
            if (error != null && sourceCleanupFailure == null) {
                sourceCleanupFailure = FutureUtil.unwrapCompletionException(error);
            }
            pendingSourceOpens.remove(operation);
        }
        if (error == null) {
            operation.complete(null);
        } else {
            operation.completeExceptionally(error);
            failed.accept(error);
        }
    }

    private static CompletableFuture<Void> closeSourceHandle(LedgerHandle handle) {
        return handle == null ? CompletableFuture.completedFuture(null) : FutureUtil.supplySafely(handle::closeAsync);
    }

    @Override
    protected synchronized void initializeBookKeeper(ManagedLedgerInitializeLedgerCallback callback) {
        if (sourceIsClosing()) {
            callback.initializeFailed(new ManagedLedgerAlreadyClosedException("Shadow ledger is closed"));
            return;
        }
        log.debug().attr("name", name).attr("ledgers", ledgers).log("Initializing bookkeeper for shadowManagedLedger");

        // Calculate total entries and size
        Iterator<LedgerInfo> iterator = ledgers.values().iterator();
        while (iterator.hasNext()) {
            LedgerInfo li = iterator.next();
            if (li.getEntries() > 0) {
                NUMBER_OF_ENTRIES_UPDATER.addAndGet(this, li.getEntries());
                TOTAL_SIZE_UPDATER.addAndGet(this, li.getSize());
            } else if (currentLedger == null || li.getLedgerId() != currentLedger.getId()) {
                //do not remove the last empty ledger.
                iterator.remove();
            }
        }

        initLastConfirmedEntry();
        // Save it back to ensure all nodes exist and properties are persisted.
        store.asyncUpdateLedgerIds(name, getManagedLedgerInfo(), ledgersStat, new MetaStore.MetaStoreCallback<>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                synchronized (ShadowManagedLedgerImpl.this) {
                    ledgersStat = stat;
                    initializingSource = false;
                    SourceInfo deferred = deferredSourceInfo;
                    deferredSourceInfo = null;
                    if (deferred != null) {
                        processSourceManagedLedgerInfo(deferred.info(), deferred.stat());
                    }
                }
                initializeCursors(callback);
            }

            @Override
            public void operationFailed(ManagedLedgerException.MetaStoreException e) {
                handleBadVersion(e);
                callback.initializeFailed(new ManagedLedgerException(e));
            }
        });
    }

    private void initLastConfirmedEntry() {
        if (currentLedger == null) {
            return;
        }
        lastConfirmedEntry = PositionFactory.create(currentLedger.getId(), currentLedger.getLastAddConfirmed());
        // bypass empty ledgers, find last ledger with Message if possible.
        while (lastConfirmedEntry.getEntryId() == -1) {
            Map.Entry<Long, LedgerInfo> formerLedger = ledgers.lowerEntry(lastConfirmedEntry.getLedgerId());
            if (formerLedger != null) {
                LedgerInfo ledgerInfo = formerLedger.getValue();
                lastConfirmedEntry = PositionFactory.create(ledgerInfo.getLedgerId(), ledgerInfo.getEntries() - 1);
            } else {
                break;
            }
        }
    }

    @Override
    protected synchronized void internalAsyncAddEntry(OpAddEntry addOperation) {
        if (!beforeAddEntry(addOperation)) {
            return;
        }
        if (state != State.LedgerOpened) {
            addOperation.failed(new ManagedLedgerException("Managed ledger is not opened"));
            return;
        }

        if (addOperation.getCtx() == null || !(addOperation.getCtx() instanceof Position position)) {
            addOperation.failed(new ManagedLedgerException("Illegal addOperation context object."));
            return;
        }

        log.debug().attr("name", name)
                .attr("ledgerId", currentLedger.getId())
                .attr("entries", currentLedgerEntries)
                .attr("posLedgerId", position.getLedgerId())
                .attr("posEntryId", position.getEntryId())
                .log("Add entry into shadow ledger");
        pendingAddEntries.add(addOperation);
        if (position.getLedgerId() <= currentLedger.getId()) {
            // Write into lastLedger
            if (position.getLedgerId() == currentLedger.getId()) {
                addOperation.setLedger(currentLedger);
            }
            currentLedgerEntries = position.getEntryId();
            currentLedgerSize += addOperation.data.readableBytes();
            addOperation.initiateShadowWrite();
        } // for addOperation with ledgerId > currentLedger, will be processed in `updateLedgersIdsComplete`
        lastAddEntryTimeMs = System.currentTimeMillis();
    }

    /**
     * terminate is not allowed on shadow topic.
     * @param callback
     * @param ctx
     */
    @Override
    public synchronized void asyncTerminate(AsyncCallbacks.TerminateCallback callback, Object ctx) {
        callback.terminateFailed(new ManagedLedgerException("Terminate is not allowed on shadow topic."), ctx);
    }

    /**
     * Handle source ManagedLedgerInfo updates.
     * Update types:
     * 1. new ledgers.
     * 2. old ledgers deleted.
     * 3. old ledger offload info updated (including ledger deleted from bookie by offloader)
     */
    private synchronized void processSourceManagedLedgerInfo(ManagedLedgerInfo mlInfo, Stat stat) {
        if (sourceIsClosing()) {
            return;
        }
        if (initializingSource) {
            if (deferredSourceInfo == null || deferredSourceInfo.stat().getVersion() < stat.getVersion()) {
                deferredSourceInfo = new SourceInfo(mlInfo, stat);
            }
            return;
        }

        log.debug().attr("name", name)
                .attr("source", sourceMLName)
                .attr("mlInfo", mlInfo)
                .attr("previousStat", sourceLedgersStat)
                .attr("stat", stat)
                .log("New SourceManagedLedgerInfo");
        if (sourceLedgersStat != null && sourceLedgersStat.getVersion() >= stat.getVersion()) {
            log.warn().attr("previousStat", sourceLedgersStat)
                    .attr("currentStat", stat)
                    .log("Newer version of mlInfo is already processed");
            return;
        }
        sourceLedgersStat = stat;

        if (mlInfo.hasTerminatedPosition()) {
            NestedPositionInfo terminatedPosition = mlInfo.getTerminatedPosition();
            lastConfirmedEntry =
                    PositionFactory.create(terminatedPosition.getLedgerId(), terminatedPosition.getEntryId());
            log.info().attr("name", name)
                    .attr("source", sourceMLName)
                    .attr("lastConfirmedEntry", lastConfirmedEntry)
                    .log("Process managed ledger terminated");
        }

        TreeMap<Long, LedgerInfo> newLedgerInfos = new TreeMap<>();
        for (int i = 0; i < mlInfo.getLedgerInfosCount(); i++) {
            LedgerInfo ls = mlInfo.getLedgerInfoAt(i);
            newLedgerInfos.put(ls.getLedgerId(), ls);
        }

        for (Map.Entry<Long, LedgerInfo> ledgerInfoEntry : newLedgerInfos.entrySet()) {
            Long ledgerId = ledgerInfoEntry.getKey();
            LedgerInfo ledgerInfo = ledgerInfoEntry.getValue();
            if (ledgerInfo.getEntries() > 0) {
                LedgerInfo oldLedgerInfo = ledgers.put(ledgerId, ledgerInfo);
                if (oldLedgerInfo == null) {
                    log.info().attr("name", name).attr("ledgerId", ledgerId).log("Read new ledger info from source");
                } else {
                    if (!oldLedgerInfo.equals(ledgerInfo)) {
                        log.info().attr("name", name)
                                .attr("ledgerId", ledgerId)
                                .log("Old ledger info updated in source");
                        // ledger deleted from bookkeeper by offloader.
                        if (ledgerInfo.hasOffloadContext()
                                && ledgerInfo.getOffloadContext().isBookkeeperDeleted()
                                && (!oldLedgerInfo.hasOffloadContext() || !oldLedgerInfo.getOffloadContext()
                                .isBookkeeperDeleted())) {
                            log.info().attr("name", name)
                                    .attr("ledgerId", ledgerId)
                                    .log("Old ledger removed from bookkeeper"
                                            + " by offloader in source");
                            invalidateReadHandle(ledgerId);
                        }
                    }
                }
            }
        }
        Long lastLedgerId = newLedgerInfos.lastKey();
        // open the last ledger.
        if (lastLedgerId != null && !(currentLedger != null && currentLedger.getId() == lastLedgerId)) {
            ledgers.put(lastLedgerId, newLedgerInfos.get(lastLedgerId));
            openSourceLedger(lastLedgerId, (handle, error) -> {
                LedgerHandle previous;
                synchronized (ShadowManagedLedgerImpl.this) {
                    if (sourceIsClosing() || sourceLedgersStat.getVersion() != stat.getVersion()) {
                        return closeSourceHandle(handle);
                    }
                    if (error != null) {
                        if (isNoSuchLedgerExistsException(BKException.getExceptionCode(error))) {
                            ledgers.remove(lastLedgerId);
                        }
                        return CompletableFuture.completedFuture(null);
                    }
                    ledgers.put(lastLedgerId, new LedgerInfo().setLedgerId(lastLedgerId)
                            .setEntries(handle.getLastAddConfirmed() + 1).setSize(handle.getLength())
                            .setTimestamp(clock.millis()));
                    previous = currentLedger;
                    currentLedger = handle;
                    currentLedgerEntries = 0;
                    currentLedgerSize = 0;
                    initLastConfirmedEntry();
                    updateLedgersIdsComplete(null);
                }
                return FutureUtil.waitForAll(List.of(closeSourceHandle(previous),
                        maybeUpdateCursorBeforeTrimmingConsumedLedger()));
            }, error -> log.debug().exception(error).log("Source ledger update failed"));
        }

        //handle old ledgers deleted.
        List<LedgerInfo> ledgersToDelete = new ArrayList<>(ledgers.headMap(newLedgerInfos.firstKey(), false).values());
        if (!ledgersToDelete.isEmpty()) {
            log.info().attr("name", name).attr("size", ledgersToDelete.size()).log("Ledgers deleted in source");
            try {
                advanceCursorsIfNecessary(ledgersToDelete);
            } catch (ManagedLedgerException.LedgerNotExistException e) {
                log.info().attr("name", name).log("First non deleted Ledger is not found, advanceCursors fails");
            }
            doDeleteLedgers(ledgersToDelete);
        }
    }


    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        final CompletableFuture<Void> closing;
        final List<CompletableFuture<Void>> pending;
        final boolean initiate;
        synchronized (this) {
            initiate = shadowCloseFuture == null;
            if (initiate) {
                shadowCloseFuture = new CompletableFuture<>();
            }
            closing = shadowCloseFuture;
            pending = new ArrayList<>(pendingSourceOpens);
            if (sourceCleanupFailure != null) {
                pending.add(CompletableFuture.failedFuture(sourceCleanupFailure));
            }
            deferredSourceInfo = null;
        }
        closing.whenComplete((__, error) -> {
            if (error == null) {
                callback.closeComplete(ctx);
            } else {
                callback.closeFailed(createManagedLedgerException(error), ctx);
            }
        });
        if (!initiate) {
            return;
        }
        CloseFuture baseClosed = new CloseFuture();
        pending.add(baseClosed);
        try {
            store.unwatchManagedLedgerInfo(sourceMLName);
        } catch (Throwable error) {
            pending.add(CompletableFuture.failedFuture(error));
        }
        try {
            super.asyncClose(baseClosed, null);
        } catch (Throwable error) {
            baseClosed.completeExceptionally(error);
        }
        FutureUtil.waitForAll(pending).whenComplete((__, error) -> {
            if (error == null) {
                closing.complete(null);
            } else {
                closing.completeExceptionally(error);
            }
        });
    }

    @Override
    protected synchronized void updateLedgersIdsComplete(LedgerHandle originalCurrentLedger) {
        if (sourceIsClosing()) {
            return;
        }
        STATE_UPDATER.set(this, State.LedgerOpened);
        updateLastLedgerCreatedTimeAndScheduleRolloverTask();

        log.debug().attr("name", name)
                .attr("pendingMessages", pendingAddEntries.size())
                .log("Resending pending messages");

        createNewOpAddEntryForNewLedger();

        // Process all the pending addEntry requests
        for (OpAddEntry op : pendingAddEntries) {
            Position position = (Position) op.getCtx();
            if (position.getLedgerId() <= currentLedger.getId()) {
                if (position.getLedgerId() == currentLedger.getId()) {
                    op.setLedger(currentLedger);
                } else {
                    op.setLedger(null);
                }
                currentLedgerEntries = position.getEntryId();
                currentLedgerSize += op.data.readableBytes();
                op.initiateShadowWrite();
            } else {
                break;
            }
        }
    }

    @Override
    protected void updateLastLedgerCreatedTimeAndScheduleRolloverTask() {
        this.lastLedgerCreatedTimestamp = clock.millis();
    }

    @Override
    boolean shouldCacheAddedEntry() {
        return false;
    }
}
