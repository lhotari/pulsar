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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.opentelemetry.api.OpenTelemetry;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarMetadataEventSynchronizer;
import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.coordination.CoordinationService;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** These tests own their service lifecycle because they deliberately stop it in the middle of cleanup. */
@Test(groups = "broker")
public class PulsarServiceShutdownTest extends BaseMetadataStoreTest {
    private PulsarService newService(long timeoutMs) {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("shutdown-test");
        config.setMetadataStoreUrl("memory:" + UUID.randomUUID());
        config.setBrokerShutdownTimeoutMs(timeoutMs);
        return new PulsarService(config);
    }

    @DataProvider
    public Object[][] shutdownPaths() {
        return new Object[][]{{true}, {false}};
    }

    @DataProvider
    public Object[][] leadershipSuccessors() {
        return new Object[][]{
                {true, false, false, true, true},
                {true, true, false, true, true},
                {true, true, true, true, true},
                {true, true, false, false, true},
                {true, true, false, true, false},
                {false, false, false, true, true}
        };
    }

    public static class CustomExtensibleLoadManager extends ExtensibleLoadManagerImpl {
    }

    @Test
    public void leadershipSuccessorUsesTheSameElectionIncludingCustomLoadManagers() {
        assertThat(PulsarService.sharesLeaderElection(true, CustomExtensibleLoadManager.class.getName())).isTrue();
        assertThat(PulsarService.sharesLeaderElection(false, CustomExtensibleLoadManager.class.getName())).isFalse();
        assertThat(PulsarService.sharesLeaderElection(true, ExtensibleLoadManagerImpl.class.getName())).isTrue();
        assertThat(PulsarService.sharesLeaderElection(false, ModularLoadManagerImpl.class.getName())).isTrue();
        assertThat(PulsarService.sharesLeaderElection(true, ModularLoadManagerImpl.class.getName())).isFalse();
        assertThat(PulsarService.sharesLeaderElection(false, null)).isTrue();
        assertThat(PulsarService.sharesLeaderElection(true, null)).isFalse();
        assertThat(PulsarService.sharesLeaderElection(false, "unavailable.LoadManager")).isFalse();
    }

    @Test(dataProvider = "leadershipSuccessors")
    public void handOffLeadershipBeforeDrainingOnlyWithAnotherBroker(boolean leader, boolean otherBroker,
                                                                    boolean differentElection, boolean ready,
                                                                    boolean eligible) throws Exception {
        LeaderElectionService election = mock(LeaderElectionService.class);
        when(election.isLeader()).thenReturn(leader);
        Brokers brokers = mock(Brokers.class);
        when(brokers.checkReadyAsync()).thenReturn(ready ? CompletableFuture.completedFuture(null)
                : CompletableFuture.failedFuture(new IllegalStateException("Peer is closing")));
        when(brokers.isLeaderElectionEnabledAsync()).thenReturn(CompletableFuture.completedFuture(eligible));
        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(admin.brokers()).thenReturn(brokers);
        PulsarAdminBuilder adminBuilder = mock(PulsarAdminBuilder.class, RETURNS_SELF);
        when(adminBuilder.build()).thenReturn(admin);
        when(election.setElectionEnabled(false)).thenReturn(CompletableFuture.completedFuture(null));
        when(election.readCurrentLeader()).thenReturn(CompletableFuture.completedFuture(
                Optional.of(new LeaderBroker("other-broker:8080", "http://other-broker:8080"))));
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("shutdown-test");
        config.setBrokerShutdownTimeoutMs(10000);
        PulsarService service = new PulsarService(config) {
            @Override
            protected PulsarAdminBuilder getCreateAdminClientBuilder() {
                return adminBuilder;
            }

            @Override
            public LeaderElectionService getLeaderElectionService() {
                return election;
            }

            @Override
            public String getBrokerId() {
                return "this-broker:8080";
            }
        };
        LoadManager loadManager = mock(LoadManager.class);
        service.getLoadManager().set(loadManager);
        when(loadManager.getAvailableBrokersAsync()).thenReturn(CompletableFuture.completedFuture(
                otherBroker ? Set.of(service.getBrokerId(), "other-broker:8080") : Set.of(service.getBrokerId())));
        CoordinationService coordination = mock(CoordinationService.class);
        service.setCoordinationService(coordination);
        @SuppressWarnings("unchecked")
        LockManager<BrokerLookupData> registrations = mock(LockManager.class);
        when(coordination.getLockManager(BrokerLookupData.class)).thenReturn(registrations);
        BrokerLookupData lookupData = mock(BrokerLookupData.class);
        when(lookupData.getWebServiceUrl()).thenReturn("http://other-broker:8080");
        when(lookupData.getLoadManagerClassName()).thenReturn(differentElection
                ? ExtensibleLoadManagerImpl.class.getName() : ModularLoadManagerImpl.class.getName());
        when(registrations.readLock(LoadManager.LOADBALANCE_BROKERS_ROOT + "/other-broker:8080"))
                .thenReturn(CompletableFuture.completedFuture(Optional.of(lookupData)));
        BrokerService broker = mock(BrokerService.class);
        service.setBrokerService(broker);
        when(broker.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        doAnswer(invocation -> {
            verify(election, times(!leader || otherBroker && !differentElection && ready && eligible ? 1 : 0))
                    .setElectionEnabled(false);
            return null;
        }).when(broker).unloadNamespaceBundlesGracefully(anyInt(), anyBoolean());
        service.closeAsync().get(10, TimeUnit.SECONDS);
        awaitWorkerCleanup(service);
    }

    @Test
    public void invalidShutdownRequestDoesNotConsumeAdmission() throws Exception {
        PulsarService service = newService(10000);
        assertThatThrownBy(() -> service.shutdownAsync(0, true, 0L).join())
                .hasCauseInstanceOf(IllegalArgumentException.class);
        service.shutdownAsync(0, true, 10000L).get(10, TimeUnit.SECONDS);
        assertThatThrownBy(() -> service.shutdownAsync(0, true, 10000L).join())
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test(dataProvider = "shutdownPaths")
    public void blockedDrainAndConcurrentShutdown(boolean waitForWebService) throws Exception {
        PulsarService service = newService(500);
        MetadataStoreExtended local = mock(MetadataStoreExtended.class);
        MetadataStoreExtended configuration = mock(MetadataStoreExtended.class);
        service.setLocalMetadataStore(local);
        service.setConfigurationMetadataStore(configuration);
        service.setShouldShutdownConfigurationMetadataStore(true);
        BrokerService broker = mock(BrokerService.class);
        when(broker.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        service.setBrokerService(broker);
        CountDownLatch draining = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            draining.countDown();
            release.await();
            return null;
        }).when(broker).unloadNamespaceBundlesGracefully(anyInt(), anyBoolean());
        doAnswer(invocation -> {
            assertThat(service.isMetadataSessionsClosing()).isTrue();
            assertThat(service.isRunning()).isFalse();
            return null;
        }).when(local).close();
        try {
            CompletableFuture<Void> result = service.closeAsync(waitForWebService, 1, false);
            assertThat(draining.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(service.closeAsync()).isSameAs(result);
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
            verify(local).close();
            verify(configuration).close();
            service.waitUntilClosed();
            assertThat(service.getState()).isEqualTo(PulsarService.State.Closed);
        } finally {
            release.countDown();
            awaitWorkerCleanup(service);
        }
        verify(broker).unloadNamespaceBundlesGracefully(1, false);
    }

    @Test
    public void hungStoreDoesNotPreventOtherStoreClose() throws Exception {
        PulsarService service = newService(500);
        MetadataStoreExtended local = mock(MetadataStoreExtended.class);
        MetadataStoreExtended configuration = mock(MetadataStoreExtended.class);
        service.setLocalMetadataStore(local);
        service.setConfigurationMetadataStore(configuration);
        service.setShouldShutdownConfigurationMetadataStore(true);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        doAnswer(invocation -> {
            release.await();
            closed.countDown();
            return null;
        }).when(local).close();
        try {
            assertThatThrownBy(() -> service.closeAsync().get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TimeoutException.class);
            verify(configuration).close();
        } finally {
            release.countDown();
            assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
            awaitWorkerCleanup(service);
        }
    }

    @Test
    public void sharedAndExternalStoresAreNotClosedTwice() throws Exception {
        for (boolean owned : new boolean[]{true, false}) {
            PulsarService service = newService(5000);
            MetadataStoreExtended local = mock(MetadataStoreExtended.class);
            MetadataStoreExtended configuration = owned ? local : mock(MetadataStoreExtended.class);
            service.setLocalMetadataStore(local);
            service.setConfigurationMetadataStore(configuration);
            service.setShouldShutdownConfigurationMetadataStore(owned);
            service.closeAsync().get(10, TimeUnit.SECONDS);
            service.closeAsync().get(10, TimeUnit.SECONDS);
            verify(local, times(1)).close();
            if (!owned) {
                verify(configuration, never()).close();
            }
        }
    }

    @Test
    public void shutdownClosesMetadataClientCreatedByLateStartup() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("late-startup-test");
        config.setMetadataStoreUrl("memory:" + UUID.randomUUID());
        config.setConfigurationMetadataStoreUrl("memory:" + UUID.randomUUID());
        config.setBrokerShutdownTimeoutMs(500);
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch configurationClosed = new CountDownLatch(1);
        MetadataStore configuration = mock(MetadataStore.class);
        doAnswer(invocation -> {
            configurationClosed.countDown();
            return null;
        }).when(configuration).close();
        PulsarService service = new PulsarService(config) {
            @Override
            public MetadataStore createConfigurationMetadataStore(PulsarMetadataEventSynchronizer synchronizer,
                                                                    OpenTelemetry openTelemetry) {
                creating.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return configuration;
            }
        };
        CompletableFuture<Void> started = new CompletableFuture<>();
        Thread startup = new Thread(() -> {
            try {
                service.start();
                started.complete(null);
            } catch (Throwable error) {
                started.completeExceptionally(error);
            }
        }, "late-broker-startup-test");
        startup.start();
        try {
            assertThat(creating.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> service.closeAsync().get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(TimeoutException.class);
            release.countDown();
            assertThatThrownBy(() -> started.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(PulsarServerException.class);
            assertThat(configurationClosed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(service.getState()).isEqualTo(PulsarService.State.Closed);
            assertThat(service.isRunning()).isFalse();
            verify(configuration).close();
        } finally {
            release.countDown();
            startup.join(10000);
            awaitWorkerCleanup(service);
        }
    }

    @Test
    public void externallyOwnedLocalStoreRetainsItsCloseHook() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("external-store-test");
        config.setMetadataStoreUrl("memory:" + UUID.randomUUID());
        PulsarService service = new PulsarService(config) {
            @Override
            protected CompletableFuture<Void> closeLocalMetadataStore() {
                return CompletableFuture.completedFuture(null);
            }
        };
        MetadataStoreExtended external = mock(MetadataStoreExtended.class);
        service.setLocalMetadataStore(external);
        service.setConfigurationMetadataStore(external);
        service.closeAsync().get(10, TimeUnit.SECONDS);
        verify(external, never()).close();
    }

    @Test
    public void coordinationCloseFailureStillClosesSessions() throws Exception {
        PulsarService service = newService(5000);
        MetadataStoreExtended local = mock(MetadataStoreExtended.class);
        MetadataStoreExtended configuration = mock(MetadataStoreExtended.class);
        service.setLocalMetadataStore(local);
        service.setConfigurationMetadataStore(configuration);
        service.setShouldShutdownConfigurationMetadataStore(true);
        CoordinationService coordination = mock(CoordinationService.class);
        service.setCoordinationService(coordination);
        doThrow(new IllegalStateException("injected coordination failure")).when(coordination).close();
        try {
            assertThatThrownBy(() -> service.closeAsync().get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            verify(local).close();
            verify(configuration).close();
        } finally {
            service.getIoEventLoopGroup().shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
        }
    }

    @Test
    public void nonPositiveTimeoutsRetainEmbeddedServiceSemantics() throws Exception {
        for (long timeoutMs : new long[]{0, -100}) {
            PulsarService service = newService(timeoutMs);
            MetadataStoreExtended local = mock(MetadataStoreExtended.class);
            service.setLocalMetadataStore(local);
            service.closeAsync().get(10, TimeUnit.SECONDS);
            verify(local).close();
        }
    }

    @Test
    public void storeFailureStillClosesOtherStore() throws Exception {
        PulsarService service = newService(5000);
        MetadataStoreExtended local = mock(MetadataStoreExtended.class);
        MetadataStoreExtended configuration = mock(MetadataStoreExtended.class);
        service.setLocalMetadataStore(local);
        service.setConfigurationMetadataStore(configuration);
        service.setShouldShutdownConfigurationMetadataStore(true);
        doThrow(new IllegalStateException("injected metadata failure")).when(local).close();
        assertThatThrownBy(() -> service.closeAsync().get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(IllegalStateException.class);
        verify(configuration).close();
        awaitWorkerCleanup(service);
    }

    @Test
    public void blockedCoordinationCloseDoesNotBlockSessions() throws Exception {
        PulsarService service = newService(500);
        MetadataStoreExtended local = mock(MetadataStoreExtended.class);
        service.setLocalMetadataStore(local);
        CoordinationService coordination = mock(CoordinationService.class);
        service.setCoordinationService(coordination);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            release.await();
            return null;
        }).when(coordination).close();
        try {
            CompletableFuture<Void> result = service.closeAsync();
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(TimeoutException.class);
            verify(local).close();
        } finally {
            release.countDown();
            awaitWorkerCleanup(service);
        }
    }

    @Test
    public void immediateShutdownAttemptsBothStoresBeforeTerminatingOnce() throws Exception {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("immediate-shutdown-test");
        config.setMetadataStoreUrl("memory:" + UUID.randomUUID());
        config.setBrokerShutdownTimeoutMs(1000);
        MetadataStoreExtended local = mock(MetadataStoreExtended.class);
        MetadataStoreExtended configuration = mock(MetadataStoreExtended.class);
        AtomicInteger terminations = new AtomicInteger();
        CountDownLatch terminated = new CountDownLatch(1);
        PulsarService service = new PulsarService(config, Optional.empty(), exitCode -> {
            terminations.incrementAndGet();
            terminated.countDown();
        });
        service.setLocalMetadataStore(local);
        service.setConfigurationMetadataStore(configuration);
        service.setShouldShutdownConfigurationMetadataStore(true);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closeFinished = new CountDownLatch(1);
        doAnswer(invocation -> {
            release.await();
            closeFinished.countDown();
            return null;
        }).when(local).close();
        try {
            service.shutdownNow();
            service.shutdownNow();
            assertThat(terminated.await(5, TimeUnit.SECONDS)).isTrue();
            verify(local).close();
            verify(configuration).close();
            assertThat(terminations).hasValue(1);
        } finally {
            release.countDown();
            assertThat(closeFinished.await(5, TimeUnit.SECONDS)).isTrue();
            awaitWorkerCleanup(service);
        }
    }

    @Test(dataProvider = "distributedImpl")
    public void deadlineReleasesOwnershipBeforeSessionExpiry(String provider, Supplier<String> url) throws Exception {
        MetadataStoreConfig config = MetadataStoreConfig.builder().sessionTimeoutMillis(30000).build();
        try (MetadataStoreExtended ownerStore = MetadataStoreExtended.create(url.get(), config);
             MetadataStoreExtended otherStore = MetadataStoreExtended.create(url.get(), config);
             CoordinationServiceImpl owner = new CoordinationServiceImpl(ownerStore);
             CoordinationServiceImpl other = new CoordinationServiceImpl(otherStore)) {
            String path = "/namespace/shutdown/" + UUID.randomUUID();
            owner.getLockManager(String.class).acquireLock(path, "old-broker")
                    .get(10, TimeUnit.SECONDS);
            assertThat(otherStore.exists(path).get(5, TimeUnit.SECONDS)).isTrue();
            PulsarService service = newService(1000);
            service.setLocalMetadataStore(ownerStore);
            BrokerService broker = mock(BrokerService.class);
            when(broker.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            service.setBrokerService(broker);
            CountDownLatch release = new CountDownLatch(1);
            doAnswer(invocation -> {
                release.await();
                return null;
            }).when(broker).unloadNamespaceBundlesGracefully(anyInt(), anyBoolean());
            try {
                assertThatThrownBy(() -> service.closeAsync().get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(TimeoutException.class);
                // The new owner is different: this cannot pass through same-identity lease recovery.
                Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertThat(otherStore.exists(path).get(1, TimeUnit.SECONDS)).isFalse());
                ResourceLock<String> replacement = other.getLockManager(String.class)
                        .acquireLock(path, "different-broker").get(5, TimeUnit.SECONDS);
                assertThat(replacement.getValue()).isEqualTo("different-broker");
                replacement.release().get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                awaitWorkerCleanup(service);
                // Session closure deliberately precedes lock-manager cleanup in this failure scenario.
                owner.getLockManager(String.class).asyncClose().exceptionally(error -> null).get(5, TimeUnit.SECONDS);
            }
        }
    }

    private void awaitWorkerCleanup(PulsarService service) {
        Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> service.getIoEventLoopGroup().isTerminated());
    }
}
