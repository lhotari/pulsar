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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.CreateBuilder;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerNotFoundException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerOpenCleanupTest extends MockedBookKeeperTestCase {
    @DataProvider
    public Object[][] failures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "failures")
    public void testLogicalTimeoutAndCancellationDoNotRetirePhysicalCreate(boolean closeFailure) throws Exception {
        LedgerHandle realHandle = bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]);
        LedgerHandle handle = spy(realHandle);
        CompletableFuture<Void> closeStarted = new CompletableFuture<>();
        CompletableFuture<Void> handleClosed = new CompletableFuture<>();
        doAnswer(invocation -> {
            closeStarted.complete(null);
            return handleClosed;
        }).when(handle).closeAsync();
        BookKeeper bookKeeper = spy(bkc);
        CreateBuilder builder = mock(CreateBuilder.class, RETURNS_SELF);
        CompletableFuture<WriteHandle> created = new CompletableFuture<>();
        CompletableFuture<Void> createStarted = new CompletableFuture<>();
        doAnswer(invocation -> {
            createStarted.complete(null);
            return created;
        }).when(builder).execute();
        doReturn(builder).when(bookKeeper).newCreateLedgerOp();
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, bookKeeper);
        ManagedLedgerConfig config = defaultConfig().setMetadataOperationsTimeoutSeconds(1);
        Attempt first = open(localFactory, "failed-open-create", config);
        createStarted.get(5, TimeUnit.SECONDS);
        Attempt second = open(localFactory, "failed-open-create", config);
        first.result().cancel(false);
        try {
            assertThatThrownBy(() -> second.result().get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);
            CompletionStage<Void> firstCleanup = first.cleanup().get(5, TimeUnit.SECONDS);
            CompletionStage<Void> secondCleanup = second.cleanup().get(5, TimeUnit.SECONDS);
            firstCleanup.toCompletableFuture().cancel(false);
            assertPending(secondCleanup.toCompletableFuture());
            created.complete(handle);
            closeStarted.get(5, TimeUnit.SECONDS);
            assertPending(secondCleanup.toCompletableFuture());
            if (closeFailure) {
                handleClosed.completeExceptionally(BKException.create(BKException.Code.WriteException));
                assertThatThrownBy(() -> secondCleanup.toCompletableFuture().get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerException.class);
            } else {
                handleClosed.complete(null);
                secondCleanup.toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            assertThat(first.result()).isCancelled();
        } finally {
            created.complete(handle);
            handleClosed.complete(null);
            realHandle.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "failures")
    public void testFailedMetadataOpenJoinsWriterClose(boolean closeFailure) throws Exception {
        CompletableFuture<ManagedLedgerImpl> created = new CompletableFuture<>();
        CompletableFuture<AsyncCallback.CloseCallback> closeStarted = new CompletableFuture<>();
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                MetaStore failingStore = spy(store);
                doAnswer(invocation -> {
                    ManagedLedgerImpl ledger = created.getNow(null);
                    LedgerHandle handle = spy(ledger.currentLedger);
                    ledger.currentLedger = handle;
                    doAnswer(close -> {
                        closeStarted.complete(close.getArgument(0));
                        return null;
                    }).when(handle).asyncClose(any(), any());
                    MetaStoreCallback<Void> callback = invocation.getArgument(3);
                    callback.operationFailed(new MetaStoreException("initial publication failed"));
                    return null;
                }).when(failingStore).asyncUpdateLedgerIds(any(), any(), any(), any());
                ManagedLedgerImpl ledger = new ManagedLedgerImpl(this, bk, failingStore, config, scheduledExecutor,
                        name, ownershipChecker);
                created.complete(ledger);
                return ledger;
            }
        };
        AtomicBoolean cleanupStartedBeforeFailure = new AtomicBoolean();
        Attempt opening = open(localFactory, "failed-open-metadata", defaultConfig(),
                () -> cleanupStartedBeforeFailure.set(closeStarted.isDone()));
        assertThatThrownBy(() -> opening.result().get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerException.class);
        AsyncCallback.CloseCallback callback = closeStarted.get(5, TimeUnit.SECONDS);
        boolean released = false;
        try {
            assertThat(cleanupStartedBeforeFailure).isTrue();
            CompletableFuture<Void> cleanup = opening.cleanup().get(5, TimeUnit.SECONDS).toCompletableFuture();
            assertPending(cleanup);
            released = true;
            callback.closeComplete(closeFailure ? BKException.Code.WriteException : BKException.Code.OK,
                    created.getNow(null).currentLedger, null);
            if (closeFailure) {
                assertThatThrownBy(() -> cleanup.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerException.class);
            } else {
                cleanup.get(5, TimeUnit.SECONDS);
            }
        } finally {
            if (!released) {
                callback.closeComplete(BKException.Code.OK, created.getNow(null).currentLedger, null);
            }
        }
    }

    @Test
    public void testNoResourceFailurePreservesNotFoundType() throws Exception {
        Attempt opening = open(factory, "does-not-exist", defaultConfig().setCreateIfMissing(false));
        assertThatThrownBy(() -> opening.result().get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerNotFoundException.class);
        opening.cleanup().get(5, TimeUnit.SECONDS).toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    public void testLegacyCachedFailureDoesNotClaimResourceCleanup() throws Exception {
        factory.ledgers.put("legacy-open",
                CompletableFuture.failedFuture(new ManagedLedgerException("legacy failure")));
        Attempt opening = open(factory, "legacy-open", defaultConfig());
        assertThatThrownBy(() -> opening.result().get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ManagedLedgerException.class);
        assertPending(opening.cleanup());
    }

    @Test
    public void testSupersededInitializationClosesWithoutRemovingReplacement() throws Exception {
        CompletableFuture<Runnable> metadata = new CompletableFuture<>();
        AtomicBoolean first = new AtomicBoolean(true);
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                MetaStore heldStore = spy(store);
                if (first.compareAndSet(true, false)) {
                    doAnswer(invocation -> {
                        MetaStoreCallback<ManagedLedgerInfo> callback =
                                invocation.getArgument(3);
                        AtomicBoolean released = new AtomicBoolean();
                        metadata.complete(() -> {
                            if (released.compareAndSet(false, true)) {
                                store.getManagedLedgerInfo(name, true, callback);
                            }
                        });
                        return null;
                    }).when(heldStore).getManagedLedgerInfo(any(), anyBoolean(), any(), any());
                }
                return new ManagedLedgerImpl(this, bk, heldStore, config, scheduledExecutor, name, ownershipChecker);
            }
        };
        ManagedLedgerConfig config = defaultConfig().setMetadataOperationsTimeoutSeconds(1);
        Attempt original = open(localFactory, "replaced-open", config);
        Runnable release = metadata.get(5, TimeUnit.SECONDS);
        try {
            long admittedAt = System.currentTimeMillis();
            Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> System.currentTimeMillis() > admittedAt + 1000);
            Attempt replacement = open(localFactory, "replaced-open", config);
            assertThatThrownBy(() -> original.result().get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);
            ManagedLedger active = replacement.result().get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> cleanup = original.cleanup().get(5, TimeUnit.SECONDS).toCompletableFuture();
            assertPending(cleanup);
            release.run();
            cleanup.get(5, TimeUnit.SECONDS);
            assertThat(localFactory.open("replaced-open", config)).isSameAs(active);
            active.addEntry(new byte[] {1});
            active.openCursor("cursor");
        } finally {
            release.run();
        }
    }

    private record Attempt(CompletableFuture<ManagedLedger> result,
                           CompletableFuture<CompletionStage<Void>> cleanup) { }

    private static Attempt open(ManagedLedgerFactoryImpl factory, String name, ManagedLedgerConfig config) {
        return open(factory, name, config, () -> { });
    }

    private static Attempt open(ManagedLedgerFactoryImpl factory, String name, ManagedLedgerConfig config,
                                Runnable failureObserved) {
        Attempt attempt = new Attempt(new CompletableFuture<>(), new CompletableFuture<>());
        factory.asyncOpen(name, config, new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                attempt.result().complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                attempt.result().completeExceptionally(exception);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, CompletionStage<Void> cleanup, Object ctx) {
                failureObserved.run();
                attempt.cleanup().complete(cleanup);
                openLedgerFailed(exception, ctx);
            }
        }, null, null);
        return attempt;
    }

    private static void assertPending(CompletableFuture<?> future) {
        assertThatThrownBy(() -> future.get(200, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }
}
