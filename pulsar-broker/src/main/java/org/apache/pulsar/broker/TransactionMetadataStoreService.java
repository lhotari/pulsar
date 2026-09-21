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
package org.apache.pulsar.broker;

import static org.apache.pulsar.transaction.coordinator.impl.MLTransactionLogImpl.getMLTransactionLogName;
import static org.apache.pulsar.transaction.coordinator.proto.TxnStatus.ABORTING;
import static org.apache.pulsar.transaction.coordinator.proto.TxnStatus.COMMITTING;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.transaction.exception.coordinator.TransactionCoordinatorException;
import org.apache.pulsar.broker.transaction.recover.TransactionRecoverTrackerImpl;
import org.apache.pulsar.broker.transaction.timeout.TransactionTimeoutTrackerFactoryImpl;
import org.apache.pulsar.client.api.PulsarClientException.BrokerPersistenceException;
import org.apache.pulsar.client.api.PulsarClientException.ConnectException;
import org.apache.pulsar.client.api.PulsarClientException.LookupException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClient;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException.ReachMaxPendingOpsException;
import org.apache.pulsar.client.api.transaction.TransactionBufferClientException.RequestTimeoutException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreOpening;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreOpening.UnreportedFailedOpenCleanupException;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionSubscription;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTrackerFactory;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.CoordinatorNotFoundException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.InvalidTxnStatusException;
import org.apache.pulsar.transaction.coordinator.exceptions.CoordinatorException.TransactionMetadataStoreStateException;
import org.apache.pulsar.transaction.coordinator.impl.TxnLogBufferedWriterConfig;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;

@CustomLog
public class TransactionMetadataStoreService {
    private final Map<TransactionCoordinatorID, TransactionMetadataStore> stores;
    private final TransactionMetadataStoreProvider transactionMetadataStoreProvider;
    private final PulsarService pulsarService;
    private final TransactionBufferClient tbClient;
    private final TransactionTimeoutTrackerFactory timeoutTrackerFactory;
    private static final long endTransactionRetryIntervalTime = 1000;
    private final Timer transactionOpRetryTimer;
    private final Object lifecycle = new Object();
    // A failed physical close remains registered even after the public store has been removed.
    private final Map<TransactionCoordinatorID, Generation> generations = new HashMap<>();
    // Legacy providers may retry failed opens, but cannot prove earlier storage was disposed. Keep one
    // ownership-release barrier per coordinator even after a later generation has closed successfully.
    private final Map<TransactionCoordinatorID, Throwable> unsafeHistory = new HashMap<>();
    private final ExecutorService internalPinnedExecutor;
    private CompletableFuture<Void> closeFuture;

    public TransactionMetadataStoreService(TransactionMetadataStoreProvider transactionMetadataStoreProvider,
                                           PulsarService pulsarService, TransactionBufferClient tbClient,
                                           HashedWheelTimer timer) {
        this.pulsarService = pulsarService;
        this.stores = new ConcurrentHashMap<>();
        this.transactionMetadataStoreProvider = transactionMetadataStoreProvider;
        this.tbClient = tbClient;
        this.timeoutTrackerFactory = new TransactionTimeoutTrackerFactoryImpl(this, timer);
        this.transactionOpRetryTimer = timer;
        ThreadFactory threadFactory =
                new ExecutorProvider.ExtendedThreadFactory("transaction-coordinator-thread-factory");
        this.internalPinnedExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    }

    public CompletableFuture<Void> handleTcClientConnect(TransactionCoordinatorID tcId) {
        Generation generation;
        boolean start = false;
        boolean waitForRemoval;
        synchronized (lifecycle) {
            if (closeFuture != null || pulsarService.getBrokerAdmission().isClosed()) {
                return CompletableFuture.failedFuture(new ServiceUnitNotReadyException("Broker is shutting down"));
            }
            generation = generations.get(tcId);
            if (generation == null) {
                generation = new Generation(tcId);
                generations.put(tcId, generation);
                start = true;
            }
            waitForRemoval = generation.sealed;
        }
        if (start) {
            generation.start();
        }
        if (waitForRemoval) {
            Generation previous = generation;
            return previous.closed.handle((__, error) -> {
                boolean removed;
                synchronized (lifecycle) {
                    removed = generations.get(tcId) != previous;
                }
                // Failed physical cleanup remains pinned. Retired legacy failures can retry with a fresh
                // ownership check, while their historical barrier still prevents graceful handoff.
                return removed ? handleTcClientConnect(tcId)
                        : CompletableFuture.<Void>failedFuture(FutureUtil.unwrapCompletionException(error));
            }).thenCompose(result -> result);
        }
        return generation.ready.copy();
    }

    public CompletableFuture<TransactionMetadataStore> openTransactionMetadataStore(TransactionCoordinatorID tcId,
            TransactionTimeoutTracker timeoutTracker, TransactionRecoverTracker recoverTracker) {
        CompletableFuture<TransactionMetadataStore> result = new CompletableFuture<>();
        CompletableFuture<Void> cleanup = new CompletableFuture<>();
        FutureUtil.supplySafely(() -> {
            Timer brokerClientSharedTimer = pulsarService.getBrokerClientSharedTimer();
            ServiceConfiguration config = pulsarService.getConfiguration();
            TxnLogBufferedWriterConfig writerConfig = new TxnLogBufferedWriterConfig();
            writerConfig.setBatchEnabled(config.isTransactionLogBatchedWriteEnabled());
            writerConfig.setBatchedWriteMaxRecords(config.getTransactionLogBatchedWriteMaxRecords());
            writerConfig.setBatchedWriteMaxSize(config.getTransactionLogBatchedWriteMaxSize());
            writerConfig.setBatchedWriteMaxDelayInMillis(config.getTransactionLogBatchedWriteMaxDelayInMillis());
            return pulsarService.getBrokerService().getManagedLedgerConfig(getMLTransactionLogName(tcId))
                    .thenApply(ledgerConfig -> {
                        var factory = pulsarService.getManagedLedgerStorage()
                                .getManagedLedgerStorageClass(ledgerConfig.getStorageClassName()).orElseThrow()
                                .getManagedLedgerFactory();
                        // Capture the capability at the virtual provider boundary, before flattening any future.
                        return TransactionMetadataStoreOpening.from(FutureUtil.supplySafely(() ->
                                transactionMetadataStoreProvider.openStore(tcId, factory, ledgerConfig,
                                        timeoutTracker, recoverTracker, config.getMaxActiveTransactionsPerCoordinator(),
                                        writerConfig, brokerClientSharedTimer)));
                    });
        }).whenComplete((opening, error) -> {
            if (error != null) {
                // Configuration/factory selection failed before dispatch: no provider-owned storage exists.
                cleanup.complete(null);
                result.completeExceptionally(FutureUtil.unwrapCompletionException(error));
            } else {
                opening.failedOpenCleanup().whenComplete((__, cleanupError) -> {
                    if (cleanupError == null) {
                        cleanup.complete(null);
                    } else {
                        cleanup.completeExceptionally(FutureUtil.unwrapCompletionException(cleanupError));
                    }
                });
                opening.store().whenComplete((store, openError) -> {
                    if (openError == null) {
                        result.complete(store);
                    } else {
                        result.completeExceptionally(FutureUtil.unwrapCompletionException(openError));
                    }
                });
            }
        });
        return new TransactionMetadataStoreOpening(result, cleanup);
    }

    public CompletableFuture<Void> removeTransactionMetadataStore(TransactionCoordinatorID tcId) {
        Generation generation;
        Throwable historicalFailure;
        synchronized (lifecycle) {
            generation = generations.get(tcId);
            historicalFailure = unsafeHistory.get(tcId);
            if (generation != null) {
                seal(generation);
            }
        }
        return closeSnapshot(generation == null ? List.of() : List.of(generation),
                historicalFailure == null ? List.of() : List.of(historicalFailure));
    }

    /** Includes admitted openings and retained cleanup failures, using the coordinator assignment bundle. */
    public CompletableFuture<Integer> closeStoresForBundle(NamespaceBundle bundle) {
        List<Generation> snapshot;
        List<Throwable> historicalFailures;
        synchronized (lifecycle) {
            snapshot = generations.values().stream().filter(generation -> belongsTo(generation.id, bundle)).toList();
            historicalFailures = unsafeHistory.entrySet().stream().filter(entry -> belongsTo(entry.getKey(), bundle))
                    .map(Map.Entry::getValue).toList();
            snapshot.forEach(this::seal);
        }
        return closeSnapshot(snapshot, historicalFailures).thenApply(__ -> snapshot.size());
    }

    public boolean hasStoresInBundle(NamespaceBundle bundle) {
        synchronized (lifecycle) {
            return generations.keySet().stream().anyMatch(id -> belongsTo(id, bundle))
                    || unsafeHistory.keySet().stream().anyMatch(id -> belongsTo(id, bundle));
        }
    }

    private static boolean belongsTo(TransactionCoordinatorID id, NamespaceBundle bundle) {
        return bundle.includes(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.getPartition((int) id.getId()));
    }

    private CompletableFuture<Void> closeSnapshot(List<Generation> snapshot, List<Throwable> historicalFailures) {
        List<CompletableFuture<Void>> closing = new ArrayList<>();
        snapshot.forEach(generation -> closing.add(closeGeneration(generation)));
        historicalFailures.forEach(error -> closing.add(CompletableFuture.failedFuture(error)));
        return FutureUtil.waitForAll(closing);
    }

    // Caller holds lifecycle. No future completion, resource calls or callbacks here.
    private void seal(Generation generation) {
        generation.sealed = true;
        if (generation.store != null) {
            stores.remove(generation.id, generation.store);
        }
    }

    private CompletableFuture<Void> closeGeneration(Generation generation) {
        boolean start;
        synchronized (lifecycle) {
            seal(generation);
            start = !generation.closeStarted;
            generation.closeStarted = true;
        }
        if (start) {
            generation.ready.completeExceptionally(new ServiceUnitNotReadyException("Coordinator is unloading"));
            generation.prepared.thenCompose(__ -> generation.store == null
                    ? generation.failedOpenCleanup.toCompletableFuture()
                    : FutureUtil.supplySafely(generation.store::closeAsync)).whenComplete((__, error) -> {
                        Throwable failure = error == null ? null : FutureUtil.unwrapCompletionException(error);
                        boolean unreportedCleanup = generation.store == null
                                && failure instanceof UnreportedFailedOpenCleanupException;
                        boolean trackerClosed = true;
                        try {
                            // The service always allocates the native, idempotently closeable tracker.
                            // Also covers pre-provider failure and providers that do not own their tracker.
                            if (generation.timeoutTracker != null) {
                                generation.timeoutTracker.close();
                            }
                        } catch (Throwable trackerError) {
                            trackerClosed = false;
                            if (failure == null) {
                                failure = trackerError;
                            } else if (failure != trackerError) {
                                failure.addSuppressed(trackerError);
                            }
                        }
                        synchronized (lifecycle) {
                            if (unreportedCleanup && trackerClosed) {
                                // Publish the barrier and retire the exact generation atomically: a bundle
                                // snapshot must see either the old generation or its historical failure.
                                unsafeHistory.putIfAbsent(generation.id, failure);
                                generations.remove(generation.id, generation);
                            } else if (failure == null) {
                                generations.remove(generation.id, generation);
                            }
                        }
                        if (failure == null) {
                            generation.closed.complete(null);
                        } else {
                            generation.closed.completeExceptionally(failure);
                        }
                    });
        }
        return generation.closed.copy();
    }

    private final class Generation {
        private final TransactionCoordinatorID id;
        private final CompletableFuture<Void> ready = new CompletableFuture<>();
        // Completed only after initialization and any activation reserved before sealing have finished.
        private final CompletableFuture<Void> prepared = new CompletableFuture<>();
        private final CompletableFuture<Void> closed = new CompletableFuture<>();
        private CompletionStage<Void> failedOpenCleanup = CompletableFuture.completedFuture(null);
        private TransactionTimeoutTracker timeoutTracker;
        private TransactionRecoverTracker recoverTracker;
        private TransactionMetadataStore store;
        private boolean sealed;
        private boolean closeStarted;

        private Generation(TransactionCoordinatorID id) {
            this.id = id;
        }

        private void start() {
            FutureUtil.supplySafely(() -> pulsarService.getBrokerService().checkTopicNsOwnership(
                    SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN.getPartition((int) id.getId()).toString()))
                    .thenCompose(__ -> {
                        synchronized (lifecycle) {
                            if (sealed) {
                                return CompletableFuture.<TransactionMetadataStore>failedFuture(
                                        new ServiceUnitNotReadyException("Coordinator is unloading"));
                            }
                        }
                        timeoutTracker = timeoutTrackerFactory.newTracker(id);
                        recoverTracker = new TransactionRecoverTrackerImpl(TransactionMetadataStoreService.this,
                                timeoutTracker, id.getId());
                        TransactionMetadataStoreOpening opening = TransactionMetadataStoreOpening.from(
                                FutureUtil.supplySafely(() -> openTransactionMetadataStore(id, timeoutTracker,
                                        recoverTracker)));
                        failedOpenCleanup = opening.failedOpenCleanup();
                        return opening.store();
                    }).whenComplete(this::initialized);
        }

        private void initialized(TransactionMetadataStore value, Throwable error) {
            if (error == null && value == null) {
                error = new IllegalStateException("Transaction provider returned no store");
                failedOpenCleanup = CompletableFuture.failedFuture(error);
            }
            if (error != null) {
                synchronized (lifecycle) {
                    seal(this);
                }
                ready.completeExceptionally(FutureUtil.unwrapCompletionException(error));
                prepared.complete(null);
                closeGeneration(this);
                return;
            }
            boolean activate;
            synchronized (lifecycle) {
                store = value;
                activate = !sealed && closeFuture == null && !pulsarService.getBrokerAdmission().isClosed();
                if (activate) {
                    // Recovery actions find their coordinator through this publication.
                    stores.put(id, store);
                } else {
                    seal(this);
                }
            }
            if (!activate) {
                prepared.complete(null);
                closeGeneration(this);
                return;
            }
            FutureUtil.supplySafely(() -> CompletableFuture.runAsync(() -> {
                recoverTracker.handleCommittingAndAbortingTransaction();
                timeoutTracker.start();
            }, internalPinnedExecutor)).whenComplete((__, activationError) -> {
                boolean stillReady;
                synchronized (lifecycle) {
                    stillReady = !sealed && closeFuture == null && !pulsarService.getBrokerAdmission().isClosed();
                }
                if (activationError != null) {
                    synchronized (lifecycle) {
                        seal(this);
                    }
                    ready.completeExceptionally(FutureUtil.unwrapCompletionException(activationError));
                }
                prepared.complete(null);
                if (activationError == null && stillReady) {
                    ready.complete(null);
                } else {
                    closeGeneration(this);
                }
            });
        }
    }

    public CompletableFuture<TxnID> newTransaction(TransactionCoordinatorID tcId, long timeoutInMills,
                                                   String owner) {
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.newTransaction(timeoutInMills, owner);
    }

    public CompletableFuture<Void> addProducedPartitionToTxn(TxnID txnId, List<String> partitions) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.addProducedPartitionToTxn(txnId, partitions);
    }

    public CompletableFuture<Void> addAckedPartitionToTxn(TxnID txnId, List<TransactionSubscription> partitions) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.addAckedPartitionToTxn(txnId, partitions);
    }

    public CompletableFuture<TxnMeta> getTxnMeta(TxnID txnId) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.getTxnMeta(txnId);
    }

    public long getLowWaterMark(TxnID txnID) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnID);
        TransactionMetadataStore store = stores.get(tcId);

        if (store == null) {
            return 0;
        }
        return store.getLowWaterMark();
    }

    public CompletableFuture<Void> updateTxnStatus(TxnID txnId, TxnStatus newStatus, TxnStatus expectedStatus,
                                                   boolean isTimeout) {
        TransactionCoordinatorID tcId = getTcIdFromTxnId(txnId);
        TransactionMetadataStore store = stores.get(tcId);
        if (store == null) {
            return FutureUtil.failedFuture(new CoordinatorNotFoundException(tcId));
        }
        return store.updateTxnStatus(txnId, newStatus, expectedStatus, isTimeout);
    }

    public CompletableFuture<Void> endTransaction(TxnID txnID, int txnAction, boolean isTimeout) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        endTransaction(txnID, txnAction, isTimeout, future);
        return future;
    }

    public void endTransaction(TxnID txnID, int txnAction, boolean isTimeout,
                                                  CompletableFuture<Void> future) {
        TxnStatus newStatus;
        switch (txnAction) {
            case TxnAction.COMMIT_VALUE:
                newStatus = COMMITTING;
                break;
            case TxnAction.ABORT_VALUE:
                newStatus = ABORTING;
                break;
            default:
                TransactionCoordinatorException.UnsupportedTxnActionException exception =
                        new TransactionCoordinatorException.UnsupportedTxnActionException(txnID, txnAction);
                log.error(exception.getMessage());
                future.completeExceptionally(exception);
                return;
        }
        getTxnMeta(txnID)
                .thenCompose(txnMeta -> {
                    if (txnMeta.status() == TxnStatus.OPEN) {
                        return updateTxnStatus(txnID, newStatus, TxnStatus.OPEN, isTimeout)
                                .thenCompose(__ -> endTxnInTransactionBuffer(txnID, txnAction));
                    }
                    return fakeAsyncCheckTxnStatus(txnMeta.status(), txnAction, txnID, newStatus)
                            .thenCompose(__ -> endTxnInTransactionBuffer(txnID, txnAction));
                }).whenComplete((__, ex)-> {
                    if (ex == null) {
                        future.complete(null);
                        return;
                    }
                    if (!isRetryableException(ex)) {
                        log.error()
                                .attr("txnId", txnID)
                                .attr("txnAction", txnAction)
                                .exception(ex)
                                .log("End transaction fail! TxnId: , TxnAction");
                        future.completeExceptionally(ex);
                        return;
                    }
                        log.debug()
                                .attr("txnId", txnID)
                                .attr("txnAction", txnAction)
                                .exception(ex)
                                .log("EndTxnInTransactionBuffer retry! TxnId: , TxnAction");
                                        transactionOpRetryTimer.newTimeout(timeout ->
                                    endTransaction(txnID, txnAction, isTimeout, future),
                            endTransactionRetryIntervalTime, TimeUnit.MILLISECONDS);
                });
    }

    private CompletionStage<Void> fakeAsyncCheckTxnStatus(TxnStatus txnStatus, int txnAction,
                                                          TxnID txnID, TxnStatus expectStatus) {
        boolean isLegal = switch (txnStatus) {
            case COMMITTING -> (txnAction == TxnAction.COMMIT.getValue());
            case ABORTING -> (txnAction == TxnAction.ABORT.getValue());
            default -> false;
        };
        if (!isLegal) {
                log.debug()
                        .attr("txnId", txnID)
                        .attr("txnAction", txnAction)
                        .log("EndTxnInTransactionBuffer op retry! TxnId : , TxnAction");
                        return FutureUtil.failedFuture(
                    new InvalidTxnStatusException(txnID, expectStatus, txnStatus));
        }
       return CompletableFuture.completedFuture(null);
    }

    // when managedLedger fence will remove this tc and reload
    public void handleOpFail(Throwable e, TransactionCoordinatorID tcId) {
        if (e instanceof ManagedLedgerException.ManagedLedgerFencedException) {
            removeTransactionMetadataStore(tcId);
        }
    }

    public void endTransactionForTimeout(TxnID txnID) {
        getTxnMeta(txnID).thenCompose(txnMeta -> {
            if (txnMeta.status() == TxnStatus.OPEN) {
                return endTransaction(txnID, TxnAction.ABORT_VALUE, true);
            } else {
                return null;
            }
        }).exceptionally(e -> {
            if (isRetryableException(e)) {
                endTransaction(txnID, TxnAction.ABORT_VALUE, true);
            } else {
                    log.debug()
                            .attr("txnId", txnID)
                            .log("Transaction have been handle complete, don't need to handle by transaction"
                                    + " timeout! TxnId");
                            }
            return null;
        });
    }

    private CompletableFuture<Void> endTxnInTransactionBuffer(TxnID txnID, int txnAction) {
        return getTxnMeta(txnID)
                .thenCompose(txnMeta -> {
                    long lowWaterMark = getLowWaterMark(txnID);
                    Stream<CompletableFuture<?>> onSubFutureStream = txnMeta.ackedPartitions().stream().map(tbSub -> {
                        switch (txnAction) {
                            case TxnAction.COMMIT_VALUE:
                                return tbClient.commitTxnOnSubscription(
                                        tbSub.getTopic(), tbSub.getSubscription(), txnID.getMostSigBits(),
                                        txnID.getLeastSigBits(), lowWaterMark);
                            case TxnAction.ABORT_VALUE:
                                return tbClient.abortTxnOnSubscription(
                                        tbSub.getTopic(), tbSub.getSubscription(), txnID.getMostSigBits(),
                                        txnID.getLeastSigBits(), lowWaterMark);
                            default:
                                return FutureUtil.failedFuture(
                                        new IllegalStateException("Unsupported txnAction " + txnAction));
                        }
                    });
                    Stream<CompletableFuture<?>> onTopicFutureStream =
                            txnMeta.producedPartitions().stream().map(partition -> {
                                switch (txnAction) {
                                    case TxnAction.COMMIT_VALUE:
                                        return tbClient.commitTxnOnTopic(partition, txnID.getMostSigBits(),
                                                txnID.getLeastSigBits(), lowWaterMark);
                                    case TxnAction.ABORT_VALUE:
                                        return tbClient.abortTxnOnTopic(partition, txnID.getMostSigBits(),
                                            txnID.getLeastSigBits(), lowWaterMark);
                                    default:
                                        return FutureUtil.failedFuture(
                                                new IllegalStateException("Unsupported txnAction " + txnAction));
                        }
                    });
                    return FutureUtil.waitForAll(Stream.concat(onSubFutureStream, onTopicFutureStream)
                                    .collect(Collectors.toList()))
                            .thenCompose(__ -> endTxnInTransactionMetadataStore(txnID, txnAction));
                });
    }

    private static boolean isRetryableException(Throwable ex) {
        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
        return (realCause instanceof TransactionMetadataStoreStateException
                || realCause instanceof RequestTimeoutException
                || realCause instanceof ManagedLedgerException
                || realCause instanceof BrokerPersistenceException
                || realCause instanceof LookupException
                || realCause instanceof ReachMaxPendingOpsException
                || realCause instanceof ConnectException)
                && !(realCause instanceof ManagedLedgerException.ManagedLedgerFencedException);
    }

    private CompletableFuture<Void> endTxnInTransactionMetadataStore(TxnID txnID, int txnAction) {
        if (TxnAction.COMMIT.getValue() == txnAction) {
            return updateTxnStatus(txnID, TxnStatus.COMMITTED, COMMITTING, false);
        } else if (TxnAction.ABORT.getValue() == txnAction) {
            return updateTxnStatus(txnID, TxnStatus.ABORTED, ABORTING, false);
        } else {
            return FutureUtil.failedFuture(new InvalidTxnStatusException("Unsupported txnAction " + txnAction));
        }
    }

    private TransactionCoordinatorID getTcIdFromTxnId(TxnID txnId) {
        return new TransactionCoordinatorID(txnId.getMostSigBits());
    }

    @VisibleForTesting
    public Map<TransactionCoordinatorID, TransactionMetadataStore> getStores() {
        return Collections.unmodifiableMap(stores);
    }

    public CompletableFuture<Boolean> verifyTxnOwnership(TxnID txnID, String checkOwner) {
        return getTxnMeta(txnID)
                .thenCompose(meta -> {
                    // owner was null in the old versions or no auth enabled
                    if (meta.getOwner() == null) {
                        return CompletableFuture.completedFuture(true);
                    }
                    if (meta.getOwner().equals(checkOwner)) {
                        return CompletableFuture.completedFuture(true);
                    }
                    return CompletableFuture.completedFuture(false);
                });
    }

    @VisibleForTesting
    ExecutorService activationExecutor() {
        return internalPinnedExecutor;
    }

    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> result;
        List<Generation> snapshot;
        List<Throwable> historicalFailures;
        synchronized (lifecycle) {
            if (closeFuture != null) {
                return closeFuture.copy();
            }
            result = new CompletableFuture<>();
            closeFuture = result;
            snapshot = new ArrayList<>(generations.values());
            historicalFailures = new ArrayList<>(unsafeHistory.values());
            snapshot.forEach(this::seal);
        }
        closeSnapshot(snapshot, historicalFailures).whenComplete((__, error) -> {
            internalPinnedExecutor.shutdown();
            if (error == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(FutureUtil.unwrapCompletionException(error));
            }
        });
        return result.copy();
    }

    public void close() {
        closeAsync().exceptionally(error -> {
            log.warn().exception(error).log("Closing transaction metadata stores failed");
            return null;
        });
    }
}
