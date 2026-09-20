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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.namespace.TopicExistsInfo;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentReplicator;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Isolated broker contexts are required because admission cutoff is irreversible. */
@Test(groups = "broker")
public class BrokerTopicLoadCleanupTest {
    private static final TopicName NAME = TopicName.get("persistent://prop/ns-abc/late-load");

    @DataProvider
    public Object[][] lateOpenCases() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "lateOpenCases")
    public void testLateLedgerCloseOutlivesRequest(boolean timeout, boolean failClose) throws Exception {
        try (PulsarTestContext context = context()) {
            BrokerService broker = context.getBrokerService();
            BrokerAdmission admission = context.getPulsarService().getBrokerAdmission();
            CompletableFuture<OpenLedgerCallback> opening = holdOpen(context);
            CompletableFuture<Optional<Topic>> request = broker.getTopic(NAME, true, null);
            OpenLedgerCallback callback = opening.get(10, TimeUnit.SECONDS);
            if (timeout) {
                assertThatThrownBy(() -> request.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
            } else {
                assertThat(request.cancel(false)).isTrue();
            }
            admission.close().forEach(Runnable::run);
            assertThat(admission.getShutdownTopicLoads()).hasSize(1);
            CompletableFuture<Optional<Topic>> physical = admission.getShutdownTopicLoads().get(0).completion();
            assertPending(physical);
            CompletableFuture<Integer> unloading = unload(context);
            NamespaceBundle bundle = mock(NamespaceBundle.class);
            when(bundle.includes(any(TopicName.class))).thenReturn(true);
            CompletableFuture<Void> snapshotStorage = broker.captureShutdownBundle(bundle).closeStorage();
            assertPending(snapshotStorage);
            assertPending(unloading);
            var canceledObserver = admission.getShutdownTopicLoads().get(0).completion();
            assertThat(canceledObserver.cancel(false)).isTrue();
            assertPending(physical);
            ManagedLedger ledger = mock(ManagedLedger.class);
            CompletableFuture<CloseCallback> closing = new CompletableFuture<>();
            doAnswer(invocation -> {
                closing.complete(invocation.getArgument(0));
                return null;
            }).when(ledger).asyncClose(any(), any());
            callback.openLedgerComplete(ledger, null);
            CloseCallback closeCallback = closing.get(10, TimeUnit.SECONDS);
            assertPending(physical);
            ManagedLedgerException failure = new ManagedLedgerException("late handle close failed");
            if (failClose) {
                closeCallback.closeFailed(failure, null);
                assertThatThrownBy(() -> physical.get(10, TimeUnit.SECONDS))
                        .hasRootCauseMessage("late handle close failed");
                assertThatThrownBy(() -> unloading.get(10, TimeUnit.SECONDS))
                        .hasRootCauseMessage("late handle close failed");
                assertThatThrownBy(() -> snapshotStorage.get(10, TimeUnit.SECONDS))
                        .hasRootCauseMessage("late handle close failed");
            } else {
                closeCallback.closeComplete(null);
                assertThat(physical.get(10, TimeUnit.SECONDS)).isEmpty();
                unloading.get(10, TimeUnit.SECONDS);
                snapshotStorage.get(10, TimeUnit.SECONDS);
            }
            assertThat(request).isCompletedExceptionally();
        }
    }

    @DataProvider
    public Object[][] cleanupResults() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "cleanupResults")
    public void testFailedOpenJoinsFactoryCleanup(boolean failCleanup) throws Exception {
        try (PulsarTestContext context = context()) {
            CompletableFuture<OpenLedgerCallback> opening = holdOpen(context);
            CompletableFuture<Optional<Topic>> request = context.getBrokerService().getTopic(NAME, true, null);
            CompletableFuture<Void> cleanup = new CompletableFuture<>();
            opening.get(10, TimeUnit.SECONDS).openLedgerFailed(
                    new ManagedLedgerException("open failed"), cleanup.minimalCompletionStage(), null);
            assertThatThrownBy(() -> request.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("open failed");
            BrokerAdmission admission = context.getPulsarService().getBrokerAdmission();
            admission.close().forEach(Runnable::run);
            assertThat(admission.getShutdownTopicLoads()).hasSize(1);
            CompletableFuture<Optional<Topic>> physical = admission.getShutdownTopicLoads().get(0).completion();
            assertPending(physical);
            CompletableFuture<Integer> unloading = unload(context);
            assertPending(unloading);
            if (failCleanup) {
                cleanup.completeExceptionally(new ManagedLedgerException("cleanup failed"));
                assertThatThrownBy(() -> physical.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("cleanup failed");
                assertThatThrownBy(() -> unloading.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("cleanup failed");
            } else {
                cleanup.complete(null);
                assertThat(physical.get(10, TimeUnit.SECONDS)).isEmpty();
                unloading.get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void testLegacyFactoryFailureDoesNotInventCleanupProof() throws Exception {
        try (PulsarTestContext context = context()) {
            CompletableFuture<OpenLedgerCallback> opening = holdOpen(context);
            CompletableFuture<Optional<Topic>> request = context.getBrokerService().getTopic(NAME, true, null);
            opening.get(10, TimeUnit.SECONDS).openLedgerFailed(new ManagedLedgerException("legacy failure"), null);
            assertThatThrownBy(() -> request.get(10, TimeUnit.SECONDS)).hasRootCauseMessage("legacy failure");
            BrokerAdmission admission = context.getPulsarService().getBrokerAdmission();
            admission.close().forEach(Runnable::run);
            assertThat(admission.getShutdownTopicLoads()).hasSize(1);
            var physical = admission.getShutdownTopicLoads().get(0).completion();
            assertThatThrownBy(() -> physical.get(10, TimeUnit.SECONDS))
                    .hasMessageContaining("did not report failed-open cleanup");
        }
    }

    @Test
    public void testPreInsertionLoadCannotMaterializeAfterCutoff() throws Exception {
        try (PulsarTestContext context = context()) {
            CompletableFuture<OpenLedgerCallback> opening = holdOpen(context);
            CompletableFuture<Optional<TopicPolicies>> policy = new CompletableFuture<>();
            CompletableFuture<Void> policyRequested = new CompletableFuture<>();
            TopicPoliciesService policies = mock(TopicPoliciesService.class);
            when(policies.getTopicPoliciesAsync(NAME, TopicPoliciesService.GetType.LOCAL_ONLY))
                    .thenAnswer(ignored -> {
                        policyRequested.complete(null);
                        return policy;
                    });
            var pulsar = context.getPulsarService();
            doReturn(policies).when(pulsar).getTopicPoliciesService();
            CompletableFuture<Optional<Topic>> request = context.getBrokerService().getTopic(NAME, true, null);
            policyRequested.get(10, TimeUnit.SECONDS);
            assertThat(context.getBrokerService().getTopics()).doesNotContainKey(NAME.toString());
            BrokerAdmission admission = context.getPulsarService().getBrokerAdmission();
            admission.close().forEach(Runnable::run);
            assertThat(admission.getShutdownTopicLoads()).hasSize(1);
            assertThat(admission.getShutdownTopicLoads().get(0).completion().get(10, TimeUnit.SECONDS)).isEmpty();
            unload(context).get(10, TimeUnit.SECONDS);
            policy.complete(Optional.empty());
            assertThat(request).isCompletedExceptionally();
            assertThat(opening).isNotDone();
            verify(context.getDefaultManagedLedgerFactory(), never()).asyncOpen(any(), any(ManagedLedgerConfig.class),
                    any(OpenLedgerCallback.class), any(), any());
        }
    }

    @Test
    public void testConcurrentCacheMissHasOnePhysicalWinner() throws Exception {
        try (PulsarTestContext context = context()) {
            CompletableFuture<OpenLedgerCallback> opening = holdOpen(context);
            CompletableFuture<Optional<TopicPolicies>> policy = new CompletableFuture<>();
            TopicPoliciesService policies = mock(TopicPoliciesService.class);
            when(policies.getTopicPoliciesAsync(any(), any())).thenReturn(policy);
            var pulsar = context.getPulsarService();
            doReturn(policies).when(pulsar).getTopicPoliciesService();
            BrokerService broker = context.getBrokerService();
            CompletableFuture<Optional<Topic>> first = broker.getTopic(NAME, true, null);
            CompletableFuture<Optional<Topic>> second = broker.getTopic(NAME, true, null);
            verify(policies, timeout(10000).times(2))
                    .getTopicPoliciesAsync(NAME, TopicPoliciesService.GetType.LOCAL_ONLY);
            assertThat(first).isNotSameAs(second);
            policy.complete(Optional.empty());
            OpenLedgerCallback callback = opening.get(10, TimeUnit.SECONDS);
            verify(context.getDefaultManagedLedgerFactory(), times(1)).asyncOpen(any(), any(ManagedLedgerConfig.class),
                    any(OpenLedgerCallback.class), any(), any());
            BrokerAdmission admission = pulsar.getBrokerAdmission();
            admission.close().forEach(Runnable::run);
            assertThat(admission.getShutdownTopicLoads()).hasSize(1);
            CompletableFuture<Integer> unloading = unload(context);
            assertPending(unloading);
            ManagedLedger ledger = mock(ManagedLedger.class);
            CompletableFuture<CloseCallback> closing = new CompletableFuture<>();
            doAnswer(invocation -> {
                closing.complete(invocation.getArgument(0));
                return null;
            }).when(ledger).asyncClose(any(), any());
            callback.openLedgerComplete(ledger, null);
            CloseCallback closeCallback = closing.get(10, TimeUnit.SECONDS);
            assertPending(unloading);
            closeCallback.closeComplete(null);
            unloading.get(10, TimeUnit.SECONDS);
            verify(ledger, times(1)).asyncClose(any(), any());
            assertThat(first).isCompletedExceptionally();
            assertThat(second).isCompletedExceptionally();
        }
    }

    @Test(dataProvider = "cleanupResults")
    public void testNonPersistentInitializationJoinsStrictCleanup(boolean failCleanup) throws Exception {
        try (PulsarTestContext context = context()) {
            BrokerService broker = context.getBrokerService();
            String name = NAME.toString().replace("persistent://", "non-persistent://");
            NonPersistentTopic topic = spy(new NonPersistentTopic(name, broker));
            CompletableFuture<Void> initialized = new CompletableFuture<>();
            doReturn(initialized).when(topic).initialize();
            doReturn(CompletableFuture.completedFuture(null)).when(topic).checkReplication();
            doReturn(topic).when(broker).newTopic(name, null, broker, NonPersistentTopic.class);
            NonPersistentReplicator replicator = mock(NonPersistentReplicator.class);
            CompletableFuture<Void> stopped = new CompletableFuture<>();
            when(replicator.terminate()).thenReturn(stopped);
            topic.getReplicators().put("remote", replicator);
            try {
                CompletableFuture<Optional<Topic>> request = broker.getTopic(name, true);
                verify(topic, timeout(10000)).initialize();
                BrokerAdmission admission = context.getPulsarService().getBrokerAdmission();
                admission.close().forEach(Runnable::run);
                assertThat(admission.getShutdownTopicLoads()).hasSize(1);
                CompletableFuture<Optional<Topic>> physical = admission.getShutdownTopicLoads().get(0).completion();
                CompletableFuture<Integer> unloading = unload(context);
                assertPending(physical);
                assertPending(unloading);
                initialized.complete(null);
                verify(replicator, timeout(10000)).terminate();
                assertPending(physical);
                assertPending(unloading);
                if (failCleanup) {
                    stopped.completeExceptionally(new IllegalStateException("replication cleanup failed"));
                    assertThatThrownBy(() -> physical.get(10, TimeUnit.SECONDS))
                            .hasRootCauseMessage("replication cleanup failed");
                    assertThatThrownBy(() -> unloading.get(10, TimeUnit.SECONDS))
                            .hasRootCauseMessage("replication cleanup failed");
                } else {
                    stopped.complete(null);
                    assertThat(physical.get(10, TimeUnit.SECONDS)).isEmpty();
                    unloading.get(10, TimeUnit.SECONDS);
                }
                verify(topic, never()).close(true);
                verify(topic, never()).close(true, true);
                assertThat(request).isCompletedExceptionally();
            } finally {
                initialized.complete(null);
                stopped.complete(null);
                topic.getReplicators().clear();
            }
        }
    }

    @Test(dataProvider = "cleanupResults")
    public void testBundleSnapshotRetainsBothPhasesAndOriginalTopic(boolean failStorage) throws Exception {
        try (PulsarTestContext context = context()) {
            BrokerService broker = context.getBrokerService();
            Topic original = mock(Topic.class);
            Topic replacement = mock(Topic.class);
            CompletableFuture<Void> storage = new CompletableFuture<>();
            CompletableFuture<Void> notifications = new CompletableFuture<>();
            when(original.close(false, false)).thenReturn(storage);
            when(original.close(true, false)).thenReturn(notifications);
            broker.getTopics().put(NAME.toString(), CompletableFuture.completedFuture(Optional.of(original)));
            context.getPulsarService().getBrokerAdmission().close().forEach(Runnable::run);
            NamespaceBundle bundle = mock(NamespaceBundle.class);
            when(bundle.includes(any(TopicName.class))).thenReturn(true);
            BrokerService.BundleUnload unload = broker.captureShutdownBundle(bundle);
            var replacementFuture = CompletableFuture.completedFuture(Optional.of(replacement));
            broker.getTopics().put(NAME.toString(), replacementFuture);
            try {
                CompletableFuture<Void> canceled = unload.closeStorage();
                assertThat(canceled.cancel(false)).isTrue();
                CompletableFuture<Void> first = unload.closeStorage();
                CompletableFuture<Void> second = unload.disconnectClients();
                assertThat(second.cancel(false)).isTrue();
                second = unload.disconnectClients();
                verify(original, timeout(10000)).close(false, false);
                assertPending(first);
                assertPending(second);
                verify(original, never()).close(true, false);
                if (failStorage) {
                    storage.completeExceptionally(new IllegalStateException("storage close failed"));
                    assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
                            .hasRootCauseMessage("storage close failed");
                    CompletableFuture<Void> failedDisconnect = second;
                    assertThatThrownBy(() -> failedDisconnect.get(10, TimeUnit.SECONDS))
                            .hasRootCauseMessage("storage close failed");
                    verify(original, never()).close(true, false);
                } else {
                    storage.complete(null);
                    first.get(10, TimeUnit.SECONDS);
                    verify(original, timeout(10000)).close(true, false);
                    assertPending(second);
                    notifications.complete(null);
                    second.get(10, TimeUnit.SECONDS);
                }
                verify(original).close(false, false);
                verify(replacement, never()).close(false, false);
                verify(replacement, never()).close(true, false);
                assertThat(broker.getTopics().get(NAME.toString())).isSameAs(replacementFuture);
            } finally {
                broker.getTopics().remove(NAME.toString(), replacementFuture);
                storage.complete(null);
                notifications.complete(null);
            }
        }
    }

    @Test
    public void testCanceledCachedTopicDoesNotAbandonSiblingStorageClose() throws Exception {
        try (PulsarTestContext context = context()) {
            BrokerService broker = context.getBrokerService();
            Topic topic = mock(Topic.class);
            CompletableFuture<Void> physical = new CompletableFuture<>();
            when(topic.close(false, false)).thenReturn(physical);
            CompletableFuture<Optional<Topic>> canceled = new CompletableFuture<>();
            canceled.cancel(false);
            broker.getTopics().put(NAME.toString(), CompletableFuture.completedFuture(Optional.of(topic)));
            String canceledName = NAME + "-canceled";
            broker.getTopics().put(canceledName, canceled);
            context.getPulsarService().getBrokerAdmission().close().forEach(Runnable::run);
            NamespaceBundle bundle = mock(NamespaceBundle.class);
            when(bundle.includes(any(TopicName.class))).thenReturn(true);
            try {
                CompletableFuture<Void> closing = broker.captureShutdownBundle(bundle).closeStorage();
                verify(topic, timeout(10000)).close(false, false);
                assertPending(closing);
                physical.complete(null);
                assertThatThrownBy(() -> closing.get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(CancellationException.class);
            } finally {
                broker.getTopics().remove(NAME.toString());
                broker.getTopics().remove(canceledName);
                physical.complete(null);
            }
        }
    }

    private static CompletableFuture<Integer> unload(PulsarTestContext context) {
        NamespaceBundle bundle = mock(NamespaceBundle.class);
        doReturn(true).when(bundle).includes(any(TopicName.class));
        BrokerService broker = context.getBrokerService();
        return broker.unloadServiceUnit(bundle, false, false, 10, TimeUnit.SECONDS,
                broker.getTopicFuturesInBundle(bundle));
    }

    private static CompletableFuture<OpenLedgerCallback> holdOpen(PulsarTestContext context) {
        CompletableFuture<OpenLedgerCallback> opening = new CompletableFuture<>();
        var factory = context.getDefaultManagedLedgerFactory();
        doAnswer(invocation -> {
            opening.complete(invocation.getArgument(2));
            return null;
        }).when(factory).asyncOpen(any(), any(ManagedLedgerConfig.class),
                any(OpenLedgerCallback.class), any(), any());
        return opening;
    }

    private static PulsarTestContext context() throws Exception {
        PulsarTestContext context = PulsarTestContext.builderForNonStartableContext().spyByDefault()
                .configCustomizer(config -> {
                    config.setTopicLoadTimeoutSeconds(1);
                    config.setBrokerShutdownTimeoutMs(0L);
                }).build();
        NamespaceService namespace = context.getPulsarService().getNamespaceService();
        doReturn(CompletableFuture.completedFuture(mock(NamespaceBundle.class))).when(namespace).getBundleAsync(any());
        doReturn(CompletableFuture.completedFuture(true)).when(namespace).checkBundleOwnership(any(), any());
        doReturn(true).when(namespace).isServiceUnitOwned(any());
        doAnswer(ignored -> CompletableFuture.completedFuture(TopicExistsInfo.newTopicNotExists()))
                .when(namespace).checkTopicExistsAsync(any());
        return context;
    }

    private static void assertPending(CompletableFuture<?> future) {
        assertThatThrownBy(() -> future.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
    }
}
