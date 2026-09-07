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
package org.apache.pulsar.client.impl.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.pulsar.client.api.v5.Producer;
import org.apache.pulsar.client.api.v5.ProducerBuilder;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.V5ClientBaseTest;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.MemorySize;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.MemoryLimitController;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * The V5 producer's async send path has to push back on a caller that outruns the broker.
 *
 * <p>It is the only place that can. Handing a message to a segment's dispatch chain is a pure
 * allocation that always succeeds, so every bound further down — the v4 pending queue, the netty
 * write queue — is invisible to the application. Without a bound at the boundary, a loop that
 * ignores the returned futures (which {@code pulsar-perf} is) allocates until the heap is gone: a
 * 200 MiB client memory limit died at 2 GiB of heap, because the limit meters payload bytes while
 * a 128-byte message costs some 2 KiB of client bookkeeping.
 *
 * <p>The companion plumbing test — that the V5 builder knobs reach every per-segment v4 producer —
 * is {@code V5ProducerFlowControlTest}.
 */
public class V5ProducerBackpressureTest extends V5ClientBaseTest {

    /** Budget for 32 outstanding sends: small enough that a local broker cannot outrun it. */
    private static final long TINY_MEMORY_LIMIT_BYTES =
            32L * ScalableTopicProducer.PENDING_SEND_MEMORY_OVERHEAD_BYTES;

    /** Sends per burst. Well past any bound under test, and quick at 32 messages in flight. */
    private static final int BURST = 20_000;

    private static final byte[] SMALL_PAYLOAD = new byte[8];

    /**
     * The regression test for the OOM: a caller firing sends as fast as it can, never looking at
     * the futures, must not accumulate more outstanding sends than the memory limit pays for.
     * Before the send path had admission control, this grew without bound.
     */
    @Test(timeOut = 180_000)
    public void testBlockingSendKeepsOutstandingSendsWithinTheMemoryBudget() throws Exception {
        @Cleanup
        PulsarClient client = clientWithMemoryLimit(TINY_MEMORY_LIMIT_BYTES);
        @Cleanup
        Producer<byte[]> producer = unbatchedProducer(client, newScalableTopic(1)).create();
        ScalableTopicProducer<byte[]> impl = implOf(producer);

        assertThat(impl.maxPendingSends())
                .as("the memory limit must buy a bounded number of sends")
                .isEqualTo(32);

        @Cleanup
        OutstandingSampler sampler = new OutstandingSampler(impl);
        burst(producer, SMALL_PAYLOAD, BURST).get(120, TimeUnit.SECONDS);

        assertThat(sampler.peak())
                .as("a blocking producer must wait for room rather than queue without bound")
                .isLessThanOrEqualTo(slack(impl.maxPendingSends()));
    }

    /**
     * The same loop against a producer configured to fail rather than wait: once the budget is
     * gone the sends have to be rejected, not accepted and buffered.
     */
    @Test(timeOut = 180_000)
    public void testNonBlockingSendFailsOnceTheMemoryBudgetIsGone() throws Exception {
        @Cleanup
        PulsarClient client = clientWithMemoryLimit(TINY_MEMORY_LIMIT_BYTES);
        @Cleanup
        Producer<byte[]> producer = unbatchedProducer(client, newScalableTopic(1))
                .blockIfQueueFull(false)
                .create();
        ScalableTopicProducer<byte[]> impl = implOf(producer);

        @Cleanup
        OutstandingSampler sampler = new OutstandingSampler(impl);

        AtomicInteger rejected = new AtomicInteger();
        List<CompletableFuture<?>> sends = new ArrayList<>(BURST);
        for (int i = 0; i < BURST; i++) {
            sends.add(producer.async().newMessage().value(SMALL_PAYLOAD).send()
                    .handle((__, ex) -> {
                        if (ex != null) {
                            assertThat(FutureUtil.unwrapCompletionException(ex))
                                    .isInstanceOf(PulsarClientException.MemoryBufferIsFullException.class);
                            rejected.incrementAndGet();
                        }
                        return null;
                    }));
        }
        FutureUtil.waitForAll(sends).get(120, TimeUnit.SECONDS);

        assertThat(rejected.get())
                .as("a non-blocking producer must reject sends it has no budget for")
                .isPositive();
        assertThat(sampler.peak()).isLessThanOrEqualTo(slack(impl.maxPendingSends()));
    }

    /**
     * Messages large enough that the v4 producer's own payload accounting runs out before the V5
     * per-send budget does. That makes the v4 enqueue wait, and it must not wait on the
     * connection's IO thread: the acknowledgements that would drain the queue are delivered by
     * that same thread, so the send would be waiting on itself. Every send here still has to land.
     *
     * <p>Sized so the v4 side admits ~16 messages where the V5 budget would allow 512, and issued
     * straight after {@code create()} so the burst queues behind the lazy creation of the segment
     * producer — the future that completes on the IO thread and cascades the whole chain.
     */
    @Test(timeOut = 180_000)
    public void testLargeMessagesDoNotWedgeTheConnectionIoThread() throws Exception {
        @Cleanup
        PulsarClient client = clientWithMemoryLimit(
                512L * ScalableTopicProducer.PENDING_SEND_MEMORY_OVERHEAD_BYTES);
        @Cleanup
        Producer<byte[]> producer = unbatchedProducer(client, newScalableTopic(1))
                .blockIfQueueFull(true)
                .create();

        MemoryLimitController payloadBudget =
                ((PulsarClientV5) client).v4Client().getMemoryLimitController();
        AtomicBoolean payloadBudgetRanOut = new AtomicBoolean();
        @Cleanup
        Sampler watcher = new Sampler(() -> {
            if (payloadBudget.currentUsage() >= payloadBudget.memoryLimit()) {
                payloadBudgetRanOut.set(true);
            }
        });

        burst(producer, new byte[64 * 1024], 200).get(120, TimeUnit.SECONDS);

        assertThat(payloadBudgetRanOut.get())
                .as("the premise: the per-segment producer's payload budget has to be the one that "
                        + "runs out, or nothing ever waits and the hazard is not exercised")
                .isTrue();
    }

    /**
     * A budget big enough for the caller to run thousands of sends ahead of the broker, which is
     * what the memory limits real deployments configure buy. The whole burst queues behind the
     * lazy creation of the segment producer, so the dispatch chain is thousands of links deep by
     * the time it starts draining, and the drain is one cascade.
     *
     * <p>Every send still has to land. A chain that completes each link with a future of its own,
     * rather than the stage the JDK completes, turns that cascade into recursion a frame per link
     * — it overflows the stack partway through and leaves every link behind it stranded, with
     * nothing left running to complete them.
     */
    @Test(timeOut = 180_000)
    public void testADeepDispatchChainDrainsCompletely() throws Exception {
        final int inFlight = 20_000;
        @Cleanup
        PulsarClient client = clientWithMemoryLimit(
                (long) inFlight * ScalableTopicProducer.PENDING_SEND_MEMORY_OVERHEAD_BYTES);
        @Cleanup
        Producer<byte[]> producer = unbatchedProducer(client, newScalableTopic(1)).create();
        ScalableTopicProducer<byte[]> impl = implOf(producer);

        @Cleanup
        OutstandingSampler sampler = new OutstandingSampler(impl);
        burst(producer, new byte[128], 3 * inFlight).get(120, TimeUnit.SECONDS);

        assertThat(sampler.peak())
                .as("the premise: the chain has to get thousands of links deep for this to mean "
                        + "anything")
                .isGreaterThan(1_500);
        assertThat(impl.inFlightSendCount()).isZero();
    }

    /**
     * The same hazard reached the other way round: not through the chain's cascade, but through a
     * caller that is itself a client IO thread. Chaining a send onto the future of a previous one
     * is the standard bounded-concurrency idiom, and the v4 producer completes send futures on the
     * connection's event loop — so the continuation, and the send it issues, run there. Once the
     * dispatch chain has caught up, that send is handed to the v4 producer inline, on that thread,
     * where waiting for a full queue waits for an acknowledgement the same thread has to deliver.
     *
     * <p>Messages are sized so the v4 producer's payload budget is the one that runs out, and the
     * chains are primed with an awaited send first so the dispatch chain is caught up — the state
     * in which the dispatch runs inline.
     */
    @Test(timeOut = 120_000)
    public void testSendsChainedOnAnIoThreadDoNotWedgeIt() throws Exception {
        // Enough concurrent chains that the v4 producer's payload budget — 1 MiB against 64 KiB
        // messages, so ~17 in flight — is exhausted whenever a chain wants to send.
        final int chains = 64;
        final int perChain = 20;
        @Cleanup
        PulsarClient client = clientWithMemoryLimit(
                512L * ScalableTopicProducer.PENDING_SEND_MEMORY_OVERHEAD_BYTES);
        @Cleanup
        Producer<byte[]> producer = unbatchedProducer(client, newScalableTopic(1)).create();

        byte[] payload = new byte[64 * 1024];
        // Prime the chain so the head is complete and the dispatch runs inline from here on.
        producer.async().newMessage().value(payload).send().get(60, TimeUnit.SECONDS);

        AtomicInteger continuationsOnIoThread = new AtomicInteger();
        List<CompletableFuture<Void>> chainsDone = new ArrayList<>(chains);
        for (int i = 0; i < chains; i++) {
            CompletableFuture<Void> done = new CompletableFuture<>();
            chainsDone.add(done);
            chainNextSend(producer, payload, new AtomicInteger(perChain), done, continuationsOnIoThread);
        }
        FutureUtil.waitForAll(chainsDone).get(90, TimeUnit.SECONDS);

        assertThat(continuationsOnIoThread.get())
                .as("the premise: the chained sends must actually be issued from a client IO thread")
                .isPositive();
    }

    /** Issue the next send from the completion of the previous one — i.e. from a client IO thread. */
    private static void chainNextSend(Producer<byte[]> producer, byte[] payload,
                                      AtomicInteger remaining, CompletableFuture<Void> done,
                                      AtomicInteger continuationsOnIoThread) {
        if (remaining.decrementAndGet() < 0) {
            done.complete(null);
            return;
        }
        producer.async().newMessage().value(payload).send().whenComplete((__, ex) -> {
            if (Thread.currentThread().getName().startsWith("pulsar-client-io")) {
                continuationsOnIoThread.incrementAndGet();
            }
            if (ex != null) {
                done.completeExceptionally(ex);
            } else {
                chainNextSend(producer, payload, remaining, done, continuationsOnIoThread);
            }
        });
    }

    /**
     * The reported failure also reproduced on a regular {@code persistent://} topic, which the V5
     * client drives through the same producer over a synthetic single-segment layout. The bound
     * has to hold there too.
     */
    @Test(timeOut = 180_000)
    public void testBoundAppliesToRegularTopicsToo() throws Exception {
        String topic = "persistent://" + getNamespace() + "/regular-"
                + UUID.randomUUID().toString().substring(0, 8);
        admin.topics().createNonPartitionedTopic(topic);

        @Cleanup
        PulsarClient client = clientWithMemoryLimit(TINY_MEMORY_LIMIT_BYTES);
        @Cleanup
        Producer<byte[]> producer = unbatchedProducer(client, topic).create();
        ScalableTopicProducer<byte[]> impl = implOf(producer);

        @Cleanup
        OutstandingSampler sampler = new OutstandingSampler(impl);
        burst(producer, SMALL_PAYLOAD, BURST).get(120, TimeUnit.SECONDS);

        assertThat(sampler.peak()).isLessThanOrEqualTo(slack(impl.maxPendingSends()));
    }

    /**
     * Dispatching a send creates the target segment's producer on demand, so a send accepted after
     * close would rebuild what close just tore down — and go on holding its share of the
     * client-wide budget while it did. Closing must be the end of it.
     */
    @Test(timeOut = 180_000)
    public void testClosedProducerAcceptsNoFurtherSends() throws Exception {
        @Cleanup
        PulsarClient client = clientWithMemoryLimit(TINY_MEMORY_LIMIT_BYTES);
        Producer<byte[]> producer = unbatchedProducer(client, newScalableTopic(1)).create();
        ScalableTopicProducer<byte[]> impl = implOf(producer);

        // Close while a sender is mid-flight and the budget is saturated. Closing the per-segment
        // producers fails those sends with AlreadyClosed, which reads as "segment gone" — and
        // retrying one would have a closed producer build its segment producers all over again.
        AtomicBoolean stop = new AtomicBoolean();
        List<CompletableFuture<?>> sends = Collections.synchronizedList(new ArrayList<>());
        Thread sender = new Thread(() -> {
            while (!stop.get()) {
                sends.add(producer.async().newMessage().value(SMALL_PAYLOAD).send()
                        .handle((__, ex) -> null));
            }
        }, "closing-producer-sender");
        sender.setDaemon(true);
        sender.start();
        Awaitility.await().atMost(Duration.ofSeconds(30))
                .until(() -> impl.inFlightSendCount() >= impl.maxPendingSends());
        producer.close();
        stop.set(true);
        sender.join(TimeUnit.SECONDS.toMillis(30));
        FutureUtil.waitForAll(List.copyOf(sends)).get(120, TimeUnit.SECONDS);

        assertThat(catchThrowable(() ->
                producer.async().newMessage().value(SMALL_PAYLOAD).send().get(30, TimeUnit.SECONDS)))
                .as("a send after close must be refused, not dispatched")
                .hasRootCauseInstanceOf(PulsarClientException.AlreadyClosedException.class);
        assertThat(impl.segmentProducerCount())
                .as("a refused send must not have rebuilt the segment producers close tore down")
                .isZero();
        assertThat(impl.inFlightSendCount()).isZero();

        // The budget is client-wide, so a fresh producer on the same client must still be able to
        // fill it — a blocking producer would simply never finish the burst if a share of the
        // budget had walked off with the closed one.
        @Cleanup
        Producer<byte[]> next = unbatchedProducer(client, newScalableTopic(1)).create();
        burst(next, SMALL_PAYLOAD, 1_000).get(120, TimeUnit.SECONDS);
    }

    /**
     * With the client memory limit turned off there is no budget to derive a bound from, and the
     * V5 API has no message-count knob to fall back on — so the producer supplies its own default
     * rather than buffering without any limit at all.
     */
    @Test(timeOut = 180_000)
    public void testSendsAreStillBoundedWithoutAClientMemoryLimit() throws Exception {
        @Cleanup
        PulsarClient client = clientWithMemoryLimit(0);
        @Cleanup
        Producer<byte[]> producer = unbatchedProducer(client, newScalableTopic(1)).create();
        ScalableTopicProducer<byte[]> impl = implOf(producer);

        assertThat(impl.maxPendingSends())
                .isEqualTo(ScalableTopicProducer.NO_MEMORY_LIMIT_MAX_PENDING_SENDS);

        @Cleanup
        OutstandingSampler sampler = new OutstandingSampler(impl);
        burst(producer, SMALL_PAYLOAD, BURST).get(120, TimeUnit.SECONDS);

        assertThat(sampler.peak()).isLessThanOrEqualTo(slack(impl.maxPendingSends()));
    }

    // --- Helpers ---

    private PulsarClient clientWithMemoryLimit(long bytes) throws Exception {
        return track(PulsarClient.builder()
                .serviceUrl(getBrokerServiceUrl())
                .memoryLimit(MemorySize.ofBytes(bytes))
                .operationTimeout(Duration.ofSeconds(30))
                .build());
    }

    /**
     * Batching off, as in the reported failure: it keeps one send equal to one v4 pending message,
     * so the per-send budget and the v4 payload accounting can be reasoned about separately.
     */
    private static ProducerBuilder<byte[]> unbatchedProducer(PulsarClient client, String topic) {
        return client.newProducer(Schema.bytes())
                .topic(topic)
                .batchingPolicy(BatchingPolicy.ofDisabled());
    }

    @SuppressWarnings("unchecked")
    private static ScalableTopicProducer<byte[]> implOf(Producer<byte[]> producer) {
        return (ScalableTopicProducer<byte[]>) producer;
    }

    /**
     * The bound plus one send: {@code MemoryLimitController#tryReserveMemory} lets a single
     * request cross the limit rather than pay for a tighter notification path.
     */
    private static int slack(long maxPendingSends) {
        return (int) maxPendingSends + 1;
    }

    private static CompletableFuture<Void> burst(Producer<byte[]> producer, byte[] payload, int count) {
        List<CompletableFuture<?>> sends = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            sends.add(producer.async().newMessage().value(payload).send());
        }
        return FutureUtil.waitForAll(sends);
    }

    /**
     * Runs a check every millisecond for as long as a burst is in flight. Sampling is what makes an
     * assertion mean "never exceeded" — or "did happen at some point" — rather than "was true once
     * the burst had drained", which holds either way.
     */
    private static class Sampler implements AutoCloseable {

        private final Thread thread;
        private volatile boolean running = true;

        Sampler(Runnable sample) {
            this.thread = new Thread(() -> {
                while (running) {
                    sample.run();
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "backpressure-sampler");
            this.thread.setDaemon(true);
            this.thread.start();
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            thread.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    /** A {@link Sampler} that remembers the highest outstanding-send count it saw. */
    private static final class OutstandingSampler extends Sampler {

        private final AtomicInteger peak;

        private OutstandingSampler(ScalableTopicProducer<?> producer, AtomicInteger peak) {
            super(() -> peak.accumulateAndGet(producer.inFlightSendCount(), Math::max));
            this.peak = peak;
        }

        OutstandingSampler(ScalableTopicProducer<?> producer) {
            this(producer, new AtomicInteger());
        }

        int peak() {
            return peak.get();
        }
    }
}
