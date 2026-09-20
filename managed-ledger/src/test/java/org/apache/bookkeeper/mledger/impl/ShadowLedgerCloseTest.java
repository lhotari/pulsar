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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.UpdateCallback;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.util.Futures.CloseFuture;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ShadowLedgerCloseTest extends MockedBookKeeperTestCase {
    @DataProvider
    public Object[][] failures() {
        return new Object[][] {{false}, {true}};
    }

    private static ManagedLedgerConfig shadowConfig() {
        ManagedLedgerConfig config = defaultConfig();
        config.setShadowSourceName("source");
        config.setProperties(Map.of(ManagedLedgerConfig.PROPERTY_SOURCE_TOPIC_KEY, "source-topic"));
        return config;
    }

    @Test(dataProvider = "failures")
    public void testInitialMetadataIsJoinedAndWatchCannotAbandonInitialization(boolean updateDuringInitialization)
            throws Exception {
        ManagedLedger source = factory.open("source", defaultConfig());
        source.addEntry(new byte[] {1});
        CompletableFuture<ShadowManagedLedgerImpl> created = new CompletableFuture<>();
        CompletableFuture<Runnable> metadata = new CompletableFuture<>();
        CompletableFuture<UpdateCallback<ManagedLedgerInfo>> watcher = new CompletableFuture<>();
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                MetaStore heldStore = spy(store);
                doAnswer(invocation -> {
                    watcher.complete(invocation.getArgument(1));
                    return null;
                }).when(heldStore).watchManagedLedgerInfo(eq("source"), any());
                doAnswer(invocation -> {
                    MetaStoreCallback<ManagedLedgerInfo> callback = invocation.getArgument(3);
                    AtomicBoolean released = new AtomicBoolean();
                    metadata.complete(() -> {
                        if (released.compareAndSet(false, true)) {
                            store.getManagedLedgerInfo("source", false, callback);
                        }
                    });
                    return null;
                }).when(heldStore).getManagedLedgerInfo(eq("source"), anyBoolean(), any(), any());
                ShadowManagedLedgerImpl ledger = new ShadowManagedLedgerImpl(this, bk, heldStore, config,
                        scheduledExecutor, name, ownershipChecker);
                created.complete(ledger);
                return ledger;
            }
        };
        CompletableFuture<ManagedLedger> opening = open(localFactory);
        ShadowManagedLedgerImpl shadow = created.get(5, TimeUnit.SECONDS);
        Runnable release = metadata.get(5, TimeUnit.SECONDS);
        try {
            CloseFuture closing = new CloseFuture();
            if (updateDuringInitialization) {
                LedgerHandle next = bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]);
                watcher.get(5, TimeUnit.SECONDS).onUpdate(info(next.getId()), stat(100));
                drainExecutor(shadow);
                release.run();
                assertThat(opening.get(5, TimeUnit.SECONDS)).isSameAs(shadow);
                Awaitility.await().untilAsserted(() ->
                        assertThat(shadow.currentLedger.getId()).isEqualTo(next.getId()));
                shadow.asyncClose(closing, null);
            } else {
                shadow.asyncClose(closing, null);
                assertPending(closing);
                release.run();
                assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerException.class);
            }
            closing.get(5, TimeUnit.SECONDS);
            assertThat(shadow.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
        } finally {
            release.run();
        }
    }

    @Test(dataProvider = "failures")
    public void testInitialSourceHandleIsClosedAfterShutdown(boolean fail) throws Exception {
        ManagedLedger source = factory.open("source", defaultConfig());
        long id = source.addEntry(new byte[] {1}).getLedgerId();
        HeldRead read = heldRead(id);
        BookKeeper bookKeeper = spy(bkc);
        doReturn(read.builder()).when(bookKeeper).newOpenLedgerOp();
        CompletableFuture<ShadowManagedLedgerImpl> created = new CompletableFuture<>();
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = new ManagedLedgerFactoryImpl(metadataStore, bookKeeper) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                ShadowManagedLedgerImpl ledger = new ShadowManagedLedgerImpl(this, bk, store, config, scheduledExecutor,
                        name, ownershipChecker);
                created.complete(ledger);
                return ledger;
            }
        };
        CompletableFuture<ManagedLedger> opening = open(localFactory);
        ShadowManagedLedgerImpl shadow = created.get(5, TimeUnit.SECONDS);
        read.started().get(5, TimeUnit.SECONDS);
        try {
            CloseFuture first = new CloseFuture();
            CloseFuture second = new CloseFuture();
            shadow.asyncClose(first, null);
            shadow.asyncClose(second, null);
            assertPending(first);
            read.opened().complete(read.handle());
            read.closeStarted().get(5, TimeUnit.SECONDS);
            assertPending(second);
            release(read, fail);
            assertClosed(first, fail);
            assertClosed(second, fail);
            assertThatThrownBy(() -> opening.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);
            assertThat(shadow.currentLedger).isNull();
            assertThat(shadow.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
            verify(read.builder(), times(1)).withRecovery(false);
            verify(bookKeeper, times(1)).newOpenLedgerOp();
        } finally {
            dispose(read);
        }
    }

    @Test(dataProvider = "failures")
    public void testSourceWatchOpenIsJoinedAcrossClose(boolean fail) throws Exception {
        ManagedLedger source = factory.open("source", defaultConfig());
        source.addEntry(new byte[] {1});
        BookKeeper bookKeeper = spy(bkc);
        CompletableFuture<UpdateCallback<ManagedLedgerInfo>> watcher = new CompletableFuture<>();
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = watchedFactory(bookKeeper, watcher);
        ShadowManagedLedgerImpl shadow = (ShadowManagedLedgerImpl) localFactory.open("shadow", shadowConfig());
        LedgerHandle prior = shadow.currentLedger;
        HeldRead read = heldRead(bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]).getId());
        doReturn(read.builder()).when(bookKeeper).newOpenLedgerOp();
        watcher.get(5, TimeUnit.SECONDS).onUpdate(info(read.handle().getId()), stat(100));
        read.started().get(5, TimeUnit.SECONDS);
        try {
            CloseFuture first = new CloseFuture();
            CloseFuture second = new CloseFuture();
            shadow.asyncClose(first, null);
            shadow.asyncClose(second, null);
            assertPending(first);
            read.opened().complete(read.handle());
            read.closeStarted().get(5, TimeUnit.SECONDS);
            assertPending(second);
            release(read, fail);
            assertClosed(first, fail);
            assertClosed(second, fail);
            assertThat(shadow.currentLedger).isSameAs(prior);
            assertThat(shadow.getState()).isEqualTo(ManagedLedgerImpl.State.Closed);
            watcher.getNow(null).onUpdate(info(read.handle().getId() + 1), stat(101));
            drainExecutor(shadow);
            verify(read.builder(), times(1)).execute();
            // One initial source read and one watched update, with no extra recovery of the source writer.
            verify(bookKeeper, times(2)).newOpenLedgerOp();
            source.addEntry(new byte[] {2});
        } finally {
            dispose(read);
        }
    }

    @Test(dataProvider = "failures")
    public void testOlderSourceOpenCannotReplaceNewerHandle(boolean failBeforeClose) throws Exception {
        ManagedLedger source = factory.open("source", defaultConfig());
        source.addEntry(new byte[] {1});
        BookKeeper bookKeeper = spy(bkc);
        CompletableFuture<UpdateCallback<ManagedLedgerInfo>> watcher = new CompletableFuture<>();
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl localFactory = watchedFactory(bookKeeper, watcher);
        ShadowManagedLedgerImpl shadow = (ShadowManagedLedgerImpl) localFactory.open("shadow", shadowConfig());
        HeldRead older = heldRead(bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]).getId());
        HeldRead newer = heldRead(bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]).getId());
        doReturn(older.builder(), newer.builder()).when(bookKeeper).newOpenLedgerOp();
        UpdateCallback<ManagedLedgerInfo> watch = watcher.get(5, TimeUnit.SECONDS);
        watch.onUpdate(info(older.handle().getId()), stat(100));
        older.started().get(5, TimeUnit.SECONDS);
        watch.onUpdate(info(newer.handle().getId()), stat(101));
        newer.started().get(5, TimeUnit.SECONDS);
        try {
            newer.opened().complete(newer.handle());
            Awaitility.await().untilAsserted(() -> assertThat(shadow.currentLedger).isSameAs(newer.handle()));
            older.opened().complete(older.handle());
            older.closeStarted().get(5, TimeUnit.SECONDS);
            assertThat(shadow.currentLedger).isSameAs(newer.handle());
            if (failBeforeClose) {
                release(older, true);
            }
            CloseFuture closing = new CloseFuture();
            shadow.asyncClose(closing, null);
            newer.closeStarted().get(5, TimeUnit.SECONDS);
            release(newer, false);
            if (!failBeforeClose) {
                assertPending(closing);
                release(older, false);
            }
            assertClosed(closing, failBeforeClose);
        } finally {
            dispose(older);
            dispose(newer);
        }
    }

    private ManagedLedgerFactoryImpl watchedFactory(BookKeeper bookKeeper,
            CompletableFuture<UpdateCallback<ManagedLedgerInfo>> watcher) throws Exception {
        return new ManagedLedgerFactoryImpl(metadataStore, bookKeeper) {
            @Override
            protected ManagedLedgerImpl createManagedLedger(BookKeeper bk, MetaStore store, String name,
                    ManagedLedgerConfig config, Supplier<CompletableFuture<Boolean>> ownershipChecker) {
                MetaStore watchedStore = spy(store);
                doAnswer(invocation -> {
                    watcher.complete(invocation.getArgument(1));
                    return null;
                }).when(watchedStore).watchManagedLedgerInfo(eq("source"), any());
                return new ShadowManagedLedgerImpl(this, bk, watchedStore, config, scheduledExecutor, name,
                        ownershipChecker);
            }
        };
    }

    private record HeldRead(LedgerHandle real, LedgerHandle handle, OpenBuilder builder,
                            CompletableFuture<ReadHandle> opened, CompletableFuture<Void> started,
                            CompletableFuture<Void> closed, CompletableFuture<Void> closeStarted) { }

    private HeldRead heldRead(long id) throws Exception {
        LedgerHandle real = (LedgerHandle) bkc.newOpenLedgerOp().withLedgerId(id).withRecovery(false)
                .withDigestType(BookKeeper.DigestType.CRC32C.toApiDigestType()).withPassword(new byte[0])
                .execute().get(5, TimeUnit.SECONDS);
        LedgerHandle handle = spy(real);
        OpenBuilder builder = mock(OpenBuilder.class, RETURNS_SELF);
        HeldRead read = new HeldRead(real, handle, builder, new CompletableFuture<>(), new CompletableFuture<>(),
                new CompletableFuture<>(), new CompletableFuture<>());
        doAnswer(invocation -> {
            read.started().complete(null);
            return read.opened();
        }).when(builder).execute();
        doAnswer(invocation -> {
            read.closeStarted().complete(null);
            return read.closed();
        }).when(handle).closeAsync();
        doAnswer(invocation -> {
            AsyncCallback.CloseCallback callback = invocation.getArgument(0);
            Object context = invocation.getArgument(1);
            read.closeStarted().complete(null);
            read.closed().whenComplete((__, error) -> callback.closeComplete(
                    error == null ? BKException.Code.OK : BKException.Code.WriteException, handle, context));
            return null;
        }).when(handle).asyncClose(any(), any());
        return read;
    }

    private static void release(HeldRead read, boolean fail) {
        if (fail) {
            read.closed().completeExceptionally(BKException.create(BKException.Code.WriteException));
        } else {
            read.closed().complete(null);
        }
    }

    private static void dispose(HeldRead read) throws Exception {
        read.opened().complete(read.handle());
        read.closed().complete(null);
        read.real().closeAsync().get(5, TimeUnit.SECONDS);
    }

    private static ManagedLedgerInfo info(long id) {
        ManagedLedgerInfo info = new ManagedLedgerInfo();
        info.addLedgerInfo().setLedgerId(id).setEntries(0).setSize(0);
        return info;
    }

    private static Stat stat(long version) {
        return new Stat("/managed-ledgers/source", version, 0, 0, false, false);
    }

    private static void drainExecutor(ManagedLedgerImpl ledger) throws Exception {
        CompletableFuture.runAsync(() -> { }, ledger.getExecutor()).get(5, TimeUnit.SECONDS);
    }

    private static CompletableFuture<ManagedLedger> open(ManagedLedgerFactoryImpl factory) {
        CompletableFuture<ManagedLedger> opening = new CompletableFuture<>();
        factory.asyncOpen("shadow", shadowConfig(), new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                opening.complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                opening.completeExceptionally(exception);
            }
        }, null, null);
        return opening;
    }

    private static void assertPending(CompletableFuture<?> future) {
        assertThatThrownBy(() -> future.get(200, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }

    private static void assertClosed(CloseFuture future, boolean fail) throws Exception {
        if (fail) {
            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(ManagedLedgerException.class);
        } else {
            future.get(5, TimeUnit.SECONDS);
        }
    }
}
