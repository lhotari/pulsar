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
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerAdmissionTest {
    @Test
    public void cutoffRejectsPendingButPreservesCommittedRequests() {
        BrokerAdmission gate = new BrokerAdmission();
        List<Runnable> channelTasks = new ArrayList<>();
        AtomicInteger canceled = new AtomicInteger();
        var admitted = gate.register(channelTasks::add, canceled::incrementAndGet);
        var pending = gate.register(task -> {
            assertFalse(Thread.holdsLock(gate));
            channelTasks.add(task);
        }, () -> {
            assertFalse(Thread.holdsLock(gate));
            canceled.incrementAndGet();
        });
        assertTrue(admitted.commit());
        var rejected = gate.close();
        assertEquals(canceled.get(), 0);
        assertTrue(channelTasks.isEmpty());
        assertFalse(pending.commit());
        assertTrue(gate.isClosed());
        assertNull(gate.register(channelTasks::add, canceled::incrementAndGet));
        rejected.forEach(Runnable::run);
        assertEquals(channelTasks.size(), 1);
        channelTasks.forEach(Runnable::run);
        assertEquals(canceled.get(), 1);
        assertFalse(gate.close().iterator().hasNext());
    }

    @Test(timeOut = 20000)
    public void commitAndCutoffHaveExactlyOneWinner() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            for (int iteration = 0; iteration < 1000; iteration++) {
                BrokerAdmission gate = new BrokerAdmission();
                AtomicInteger canceled = new AtomicInteger();
                var ticket = gate.register(Runnable::run, canceled::incrementAndGet);
                CyclicBarrier race = new CyclicBarrier(2);
                var committed = CompletableFuture.supplyAsync(() -> {
                    try {
                        race.await(5, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                    return ticket.commit();
                }, worker);
                race.await(5, TimeUnit.SECONDS);
                gate.close().forEach(Runnable::run);
                assertEquals(canceled.get() + (committed.get(5, TimeUnit.SECONDS) ? 1 : 0), 1);
            }
        } finally {
            worker.shutdownNow();
        }
    }
    @Test
    public void topicLoadCutoffIncludesPreInsertionAndPublicationRaces() throws Exception {
        BrokerAdmission gate = new BrokerAdmission();
        TopicName name = TopicName.get("persistent://tenant/ns/topic");
        CompletableFuture<Optional<Topic>> preInsertion = new CompletableFuture<>();
        var waiting = gate.registerTopicLoad(name, preInsertion);
        CompletableFuture<Optional<Topic>> candidate = new CompletableFuture<>();
        var materializing = gate.registerTopicLoad(name, candidate);
        assertTrue(materializing.beginMaterialization());
        assertTrue(materializing.canPublish());
        CompletableFuture<Optional<Topic>> acceptedResult = new CompletableFuture<>();
        var accepted = gate.registerTopicLoad(name, acceptedResult);
        assertTrue(accepted.beginMaterialization());
        Topic topic = mock(Topic.class);
        acceptedResult.complete(Optional.of(topic));
        gate.close().forEach(Runnable::run);
        assertThat(gate.getShutdownTopicLoads()).containsExactlyInAnyOrder(waiting, materializing, accepted);
        assertFalse(waiting.beginMaterialization());
        assertFalse(materializing.canPublish());
        assertNull(gate.registerTopicLoad(name, new CompletableFuture<>()));
        assertThat(waiting.completion().get(1, TimeUnit.SECONDS)).isEmpty();
        assertThat(candidate).isCompletedExceptionally();
        assertThat(materializing.completion()).isNotDone();
        materializing.cleaned(CompletableFuture.completedFuture(null));
        assertThat(materializing.completion().get(1, TimeUnit.SECONDS)).isEmpty();
        // Publication accepted before cutoff remains represented until its final bookkeeping completes.
        accepted.published(topic);
        assertThat(accepted.completion().get(1, TimeUnit.SECONDS)).contains(topic);
        assertThat(gate.getShutdownTopicLoads()).hasSize(3);
    }

    @Test
    public void failedCleanupRetainedBeforeCutoffAndCallbacksRunOutsideMonitor() throws Exception {
        BrokerAdmission gate = new BrokerAdmission();
        TopicName name = TopicName.get("persistent://tenant/ns/topic");
        var failed = gate.registerTopicLoad(name, new CompletableFuture<>());
        assertTrue(failed.beginMaterialization());
        CompletableFuture<Void> cleanup = new CompletableFuture<>();
        CompletableFuture<Void> callback = failed.completion().handle((ignored, error) -> {
            assertFalse(Thread.holdsLock(gate));
            return null;
        });
        failed.cleaned(cleanup);
        cleanup.completeExceptionally(new IllegalStateException("physical close failed"));
        callback.get(1, TimeUnit.SECONDS);
        // A later no-resource attempt must not erase an older, failed physical cleanup.
        var retry = gate.registerTopicLoad(name, new CompletableFuture<>());
        retry.finishWithoutMaterialization();
        gate.close();
        assertThat(gate.getShutdownTopicLoads()).containsExactly(failed);
        assertThatThrownBy(() -> failed.completion().get(1, TimeUnit.SECONDS))
                .hasRootCauseMessage("physical close failed");
    }

    @Test
    public void canceledBeforeMaterializationCannotStartLater() throws Exception {
        BrokerAdmission gate = new BrokerAdmission();
        CompletableFuture<Optional<Topic>> request = new CompletableFuture<>();
        var load = gate.registerTopicLoad(TopicName.get("persistent://tenant/ns/topic"), request);
        request.cancel(false);
        assertFalse(load.beginMaterialization());
        gate.close();
        assertThat(gate.getShutdownTopicLoads()).isEmpty();
        assertThat(load.completion().get(1, TimeUnit.SECONDS)).isEmpty();
    }

}
