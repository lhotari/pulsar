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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.CustomLog;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.PulsarServiceNameResolver;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Bounded outbound preparation before shutting down the broker's existing messaging entities.
 * A lookup peer must expose reachable binary and HTTP(S) readiness endpoints using broker-client TLS settings.
 */
@CustomLog
final class BrokerShutdownPreparation {
    private final PulsarService pulsar;
    private final long startedNanos = System.nanoTime();
    private final long budgetNanos;
    private String handoff = "skipped";
    private boolean routed;

    BrokerShutdownPreparation(PulsarService pulsar) {
        this.pulsar = pulsar;
        budgetNanos = Math.min(pulsar.getRemainingShutdownDrainNanos() / 4,
                TimeUnit.SECONDS.toNanos(pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds()));
    }

    private long remaining() {
        return Math.max(0, Math.min(budgetNanos - (System.nanoTime() - startedNanos),
                pulsar.getRemainingShutdownDrainNanos()));
    }

    private <T> T await(CompletableFuture<T> future) throws Exception {
        long timeout = remaining();
        if (timeout == 0) {
            throw new TimeoutException("Shutdown preparation budget exhausted");
        }
        return future.get(timeout, TimeUnit.NANOSECONDS);
    }

    void prepare() {
        LeaderElectionService election = pulsar.getLeaderElectionService();
        LoadManager loadManager = pulsar.getLoadManager().get();
        if (loadManager == null || remaining() == 0) {
            return;
        }
        ExecutorService probes = Executors.newFixedThreadPool(4,
                new DefaultThreadFactory("pulsar-shutdown-peer", true));
        try {
            // A follower must not become leader while draining, even if no successor can be probed.
            if (election != null && !election.isLeader()) {
                await(election.setElectionEnabled(false));
                handoff = "follower disabled";
            }
            CompletableFuture<BrokerLookupData> route = new CompletableFuture<>();
            CompletableFuture<Void> successor = new CompletableFuture<>();
            List<CompletableFuture<Void>> candidates = new ArrayList<>();
            for (String broker : await(loadManager.getAvailableBrokersAsync())) {
                if (!broker.equals(pulsar.getBrokerId()) && remaining() > 0) {
                    candidates.add(pulsar.getCoordinationService().getLockManager(BrokerLookupData.class)
                            .readLock(LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + broker)
                            .thenAcceptAsync(registration -> registration.filter(data ->
                                            PulsarService.sharesLeaderElection(
                                                    ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar),
                                                    data.getLoadManagerClassName()))
                                    .ifPresent(data -> probe(data, route, successor)), probes)
                            .exceptionally(error -> null));
                }
            }
            // Install a route as soon as one succeeds. A slow or broken peer must not prevent using another.
            CompletableFuture<Void> all = FutureUtil.waitForAll(candidates);
            await(CompletableFuture.anyOf(route, all));
            if (route.isDone()) {
                pulsar.setShutdownLookupBroker(route.join());
                routed = true;
            }
            if (election != null && election.isLeader()) {
                handoff = "leader retained";
                await(CompletableFuture.anyOf(successor, all));
                // A current leader retains leadership if there is no ready, explicitly eligible peer.
                if (successor.isDone()) {
                    await(election.setElectionEnabled(false));
                    handoff = "leader change not observed";
                    while (remaining() > 0) {
                        var leader = await(election.readCurrentLeader());
                        if (leader.isPresent() && !leader.get().getBrokerId().equals(pulsar.getBrokerId())) {
                            handoff = "completed";
                            return;
                        }
                        TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.MILLISECONDS.toNanos(10), remaining()));
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn().exceptionMessage(e).log("Shutdown peer preparation did not complete");
        } finally {
            probes.shutdownNow();
            log.info().attr("handoff", handoff).attr("lookupRouted", routed)
                    .attr("elapsedMs", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos))
                    .log("Broker shutdown preparation summary");
        }
    }

    private void probe(BrokerLookupData data, CompletableFuture<BrokerLookupData> route,
                       CompletableFuture<Void> successor) {
        String url = probeRoute(data);
        boolean eligible = false;
        boolean ready = false;
        String adminUrl = pulsar.getConfiguration().isBrokerClientTlsEnabled()
                ? data.getWebServiceUrlTls() : data.getWebServiceUrl();
        String adminScheme = pulsar.getConfiguration().isBrokerClientTlsEnabled() ? "https://" : "http://";
        if (adminUrl != null && adminUrl.startsWith(adminScheme) && remaining() > 0) {
            try (PulsarAdmin admin = pulsar.getCreateAdminClientBuilder().serviceHttpUrl(adminUrl)
                    .requestTimeout((int) Math.min(Integer.MAX_VALUE,
                            Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining()))), TimeUnit.MILLISECONDS)
                    .build()) {
                await(admin.brokers().checkReadyAsync());
                ready = true;
                // CONNECT is also accepted during broker initialization. Require full readiness before
                // installing its lookup route; election eligibility is checked separately below.
                if (url != null) {
                    route.complete(data);
                }
                eligible = await(admin.brokers().isLeaderElectionEnabledAsync());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (url != null && !ready) {
                    log.info().attr("broker", data.getBrokerId()).exceptionMessage(e)
                            .log("Reachable peer was not selected for shutdown routing: readiness check failed");
                } else {
                    log.debug().attr("broker", data.getBrokerId()).exceptionMessage(e)
                            .log("Peer is not an early shutdown election successor");
                }
            }
        } else if (url != null) {
            log.info().attr("broker", data.getBrokerId()).attr("remainingBudgetNanos", remaining())
                    .log("Reachable peer was not selected for shutdown routing: no matching readiness endpoint "
                            + "or preparation budget exhausted");
        }
        if (eligible) {
            successor.complete(null);
        }
    }

    private String probeRoute(BrokerLookupData data) {
        if (remaining() == 0) {
            return null;
        }
        boolean tls = pulsar.getConfiguration().isBrokerClientTlsEnabled();
        String url = tls ? data.getPulsarServiceUrlTls() : data.getPulsarServiceUrl();
        if (url == null || !url.startsWith(tls ? "pulsar+ssl://" : "pulsar://")) {
            return null;
        }
        PulsarClientImpl probe = null;
        try {
            PulsarServiceNameResolver resolver = new PulsarServiceNameResolver();
            resolver.updateServiceUrl(url);
            ClientConfigurationData conf = pulsar.createClientConfigurationData();
            conf.setServiceUrl(url);
            probe = pulsar.createClientImpl(conf);
            // Successful CONNECT verifies the advertised endpoint with normal broker-client TLS and credentials.
            var connection = await(probe.getCnxPool().getConnection(resolver));
            probe.getCnxPool().releaseConnection(connection);
            return url;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.debug().attr("broker", data.getBrokerId()).exceptionMessage(e)
                    .log("Peer cannot provide a shutdown lookup route");
        } finally {
            if (probe != null) {
                probe.closeAsync();
            }
        }
        return null;
    }

}
