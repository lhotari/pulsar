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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreOpening.UnreportedFailedOpenCleanupException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TransactionMetadataStoreOpeningTest {
    @DataProvider
    public Object[][] observerMutations() {
        return new Object[][] {{0}, {1}, {2}, {3}};
    }

    @Test(dataProvider = "observerMutations")
    public void testObserverMutationCannotChangeRetainedStages(int mutation) throws Exception {
        CompletableFuture<TransactionMetadataStore> result = new CompletableFuture<>();
        CompletableFuture<Void> cleanup = new CompletableFuture<>();
        TransactionMetadataStoreOpening observer = new TransactionMetadataStoreOpening(result, cleanup);
        switch (mutation) {
            case 0 -> observer.cancel(false);
            case 1 -> observer.complete(mock(TransactionMetadataStore.class));
            case 2 -> observer.completeExceptionally(new IllegalStateException("observer error"));
            case 3 -> observer.obtrudeValue(mock(TransactionMetadataStore.class));
            default -> throw new AssertionError();
        }
        TransactionMetadataStoreOpening retained = TransactionMetadataStoreOpening.from(observer);
        retained.store().toCompletableFuture().cancel(false);
        retained.failedOpenCleanup().toCompletableFuture().complete(null);
        assertThat(retained.store().toCompletableFuture()).isNotDone();
        assertThat(retained.failedOpenCleanup().toCompletableFuture()).isNotDone();
        IllegalStateException openError = new IllegalStateException("original open error");
        IllegalStateException cleanupError = new IllegalStateException("cleanup error");
        result.completeExceptionally(openError);
        cleanup.completeExceptionally(cleanupError);
        assertThatThrownBy(() -> retained.store().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .hasCause(openError);
        assertThatThrownBy(() -> retained.failedOpenCleanup().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .hasCause(cleanupError);
    }

    @Test
    public void testLegacySuccessTransfersStoreOwnership() throws Exception {
        TransactionMetadataStore store = mock(TransactionMetadataStore.class);
        TransactionMetadataStoreOpening opening = TransactionMetadataStoreOpening.from(
                CompletableFuture.completedFuture(store));
        assertThat(opening.store().toCompletableFuture().get(5, TimeUnit.SECONDS)).isSameAs(store);
        opening.failedOpenCleanup().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @DataProvider
    public Object[][] plainResults() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "plainResults")
    public void testLegacyOrTransformedFailureHasNoCleanupProof(boolean transformed) throws Exception {
        CompletableFuture<TransactionMetadataStore> result = new CompletableFuture<>();
        CompletableFuture<TransactionMetadataStore> observer = transformed
                ? new TransactionMetadataStoreOpening(result, CompletableFuture.completedFuture(null))
                        .thenApply(store -> store) : result;
        TransactionMetadataStoreOpening adapted = TransactionMetadataStoreOpening.from(observer);
        IllegalArgumentException openError = new IllegalArgumentException("original error");
        result.completeExceptionally(openError);
        assertThatThrownBy(() -> adapted.store().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .hasCause(openError);
        assertThatThrownBy(() -> adapted.failedOpenCleanup().toCompletableFuture().get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(UnreportedFailedOpenCleanupException.class).hasRootCause(openError);
    }
}
