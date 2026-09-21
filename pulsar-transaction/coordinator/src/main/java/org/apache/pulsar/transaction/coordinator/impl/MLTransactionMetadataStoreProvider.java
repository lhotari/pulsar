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
package org.apache.pulsar.transaction.coordinator.impl;

import io.netty.util.Timer;
import io.prometheus.client.CollectorRegistry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStore;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreOpening;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider;
import org.apache.pulsar.transaction.coordinator.TransactionRecoverTracker;
import org.apache.pulsar.transaction.coordinator.TransactionTimeoutTracker;

/**
 * The provider that offers managed ledger implementation of {@link TransactionMetadataStore}.
 */
public class MLTransactionMetadataStoreProvider implements TransactionMetadataStoreProvider {


    private static volatile TxnLogBufferedWriterMetricsStats bufferedWriterMetrics =
            DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS;

    public static void initBufferedWriterMetrics(String brokerAdvertisedAddress){
        if (bufferedWriterMetrics != DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS) {
            return;
        }
        synchronized (MLTransactionMetadataStoreProvider.class){
            if (bufferedWriterMetrics != DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS) {
                return;
            }
            bufferedWriterMetrics = new MLTransactionMetadataStoreBufferedWriterMetrics(brokerAdvertisedAddress);
        }
    }

    public static void closeBufferedWriterMetrics() {
        synchronized (MLTransactionMetadataStoreProvider.class){
            if (bufferedWriterMetrics == DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS) {
                return;
            }
            bufferedWriterMetrics.close();
            bufferedWriterMetrics = DisabledTxnLogBufferedWriterMetricsStats.DISABLED_BUFFERED_WRITER_METRICS;
        }
    }

    @Override
    public CompletableFuture<TransactionMetadataStore> openStore(TransactionCoordinatorID transactionCoordinatorId,
                                                                 ManagedLedgerFactory managedLedgerFactory,
                                                                 ManagedLedgerConfig managedLedgerConfig,
                                                                 TransactionTimeoutTracker timeoutTracker,
                                                                 TransactionRecoverTracker recoverTracker,
                                                                 long maxActiveTransactionsPerCoordinator,
                                                                 TxnLogBufferedWriterConfig txnLogBufferedWriterConfig,
                                                                 Timer timer) {
        StoreOpening opening = new StoreOpening(timeoutTracker);
        opening.start(transactionCoordinatorId, managedLedgerFactory, managedLedgerConfig, recoverTracker,
                maxActiveTransactionsPerCoordinator, txnLogBufferedWriterConfig, timer);
        return new TransactionMetadataStoreOpening(opening.result, opening.cleanup);
    }

    private static final class StoreOpening {
        private final TransactionTimeoutTracker timeoutTracker;
        private final CompletableFuture<TransactionMetadataStore> result = new CompletableFuture<>();
        private final CompletableFuture<Void> cleanup = new CompletableFuture<>();
        private MLTransactionLogImpl log;
        private MLTransactionMetadataStore store;

        private StoreOpening(TransactionTimeoutTracker timeoutTracker) {
            this.timeoutTracker = timeoutTracker;
        }

        private void start(TransactionCoordinatorID tcId, ManagedLedgerFactory factory, ManagedLedgerConfig config,
                           TransactionRecoverTracker recoverTracker, long maxActiveTransactions,
                           TxnLogBufferedWriterConfig writerConfig, Timer timer) {
            FutureUtil.supplySafely(() -> {
                MLTransactionSequenceIdGenerator sequenceIdGenerator = new MLTransactionSequenceIdGenerator();
                config.setManagedLedgerInterceptor(sequenceIdGenerator);
                log = new MLTransactionLogImpl(tcId, factory, config, writerConfig, timer, bufferedWriterMetrics);
                return log.initialize().thenCompose(__ -> {
                    store = new MLTransactionMetadataStore(tcId, log, timeoutTracker, sequenceIdGenerator,
                            maxActiveTransactions);
                    return store.init(recoverTracker);
                });
            }).whenComplete((value, error) -> {
                if (error == null) {
                    cleanup.complete(null);
                    result.complete(value);
                } else {
                    closeFailedOpen(error);
                }
            });
        }

        private void closeFailedOpen(Throwable error) {
            CompletableFuture<Void> closed;
            if (store != null) {
                // The store owns the log and tracker, including when its initialization failed.
                closed = FutureUtil.supplySafely(store::closeAsync);
            } else {
                closed = log == null ? CompletableFuture.completedFuture(null)
                        : FutureUtil.supplySafely(log::closeAsync);
                closed = closed.handle((__, closeError) -> {
                    Throwable failure = closeError == null ? null : FutureUtil.unwrapCompletionException(closeError);
                    try {
                        timeoutTracker.close();
                    } catch (Throwable trackerError) {
                        if (failure == null) {
                            failure = trackerError;
                        } else if (failure != trackerError) {
                            failure.addSuppressed(trackerError);
                        }
                    }
                    if (failure != null) {
                        throw new CompletionException(failure);
                    }
                    return null;
                });
            }
            closed.whenComplete((__, closeError) -> {
                if (closeError == null) {
                    cleanup.complete(null);
                } else {
                    cleanup.completeExceptionally(FutureUtil.unwrapCompletionException(closeError));
                }
                // Preserve the original initialization error independently of the cleanup outcome.
                result.completeExceptionally(FutureUtil.unwrapCompletionException(error));
            });
        }
    }

    private static class MLTransactionMetadataStoreBufferedWriterMetrics extends TxnLogBufferedWriterMetricsStats {

        private MLTransactionMetadataStoreBufferedWriterMetrics(String brokerAdvertisedAddress) {
            super("pulsar_txn_tc",
                    new String[]{"broker"},
                    new String[]{brokerAdvertisedAddress},
                    CollectorRegistry.defaultRegistry);
        }
    }
}