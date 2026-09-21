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
package org.apache.pulsar.transaction.coordinator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Optional result of {@link TransactionMetadataStoreProvider#openStore} that also reports failed-open cleanup.
 * The inherited future is an observer: completing or cancelling it cannot change either retained stage.
 * Providers retain ownership of the supplied stages and must not expose their mutable futures to callers.
 */
public final class TransactionMetadataStoreOpening extends CompletableFuture<TransactionMetadataStore> {
    private final CompletionStage<TransactionMetadataStore> store;
    private final CompletionStage<Void> failedOpenCleanup;

    public TransactionMetadataStoreOpening(CompletableFuture<TransactionMetadataStore> store,
                                          CompletableFuture<Void> failedOpenCleanup) {
        this.store = store.minimalCompletionStage();
        this.failedOpenCleanup = failedOpenCleanup.minimalCompletionStage();
        this.store.whenComplete((value, error) -> {
            if (error == null) {
                complete(value);
            } else {
                completeExceptionally(error);
            }
        });
    }

    /** The provider's result, unaffected by mutation of the public observer. */
    public CompletionStage<TransactionMetadataStore> store() {
        return store;
    }

    /**
     * Physical cleanup after a failed open; failure means ownership must not be released based on this open.
     * A successful open completes this stage successfully without closing the returned store: its new owner
     * must join {@link TransactionMetadataStore#closeAsync()} separately when removing that store.
     */
    public CompletionStage<Void> failedOpenCleanup() {
        return failedOpenCleanup;
    }

    /** Legacy provider failure without a physical-cleanup result; the original open failure is unchanged. */
    public static final class UnreportedFailedOpenCleanupException extends IllegalStateException {
        private UnreportedFailedOpenCleanupException(Throwable cause) {
            super("Transaction provider did not report failed-open cleanup", cause);
        }
    }

    /**
     * Adapt immediately after the virtual provider call, before a future transformation loses the capability.
     * Legacy providers keep their original result, but a failed open supplies no physical-cleanup proof.
     */
    public static TransactionMetadataStoreOpening from(CompletableFuture<TransactionMetadataStore> result) {
        if (result instanceof TransactionMetadataStoreOpening opening) {
            return opening;
        }
        CompletableFuture<Void> cleanup = result.handle((store, error) -> {
            if (error != null) {
                throw new UnreportedFailedOpenCleanupException(error);
            }
            return null;
        });
        return new TransactionMetadataStoreOpening(result, cleanup);
    }
}
