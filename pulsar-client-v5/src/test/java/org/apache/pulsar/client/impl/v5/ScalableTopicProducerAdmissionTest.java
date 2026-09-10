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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.impl.MemoryLimitController;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.api.proto.ScalableTopicDAG;
import org.apache.pulsar.common.api.proto.SegmentState;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ScalableTopicProducerAdmissionTest {

    @Test
    public void rejectsBeforeSegmentProducerIsReady() throws Exception {
        try (Fixture fixture = new Fixture(1024)) {
            var first = fixture.send(new byte[128]);
            assertThat(first).isNotDone();
            assertFull(fixture.send(new byte[128]));
            fixture.creation.complete(fixture.transport);
            assertThat(first).isNotDone();
            fixture.ack.complete(org.apache.pulsar.client.api.MessageId.earliest);
            assertThat(first.get(5, TimeUnit.SECONDS)).isNotNull();
            assertThat(fixture.send(new byte[128]).get(5, TimeUnit.SECONDS)).isNotNull();
        }
    }

    @Test
    public void chargesEmptyMessagesAndRetainsReservationUntilAcknowledged() throws Exception {
        try (Fixture fixture = new Fixture(1024)) {
            fixture.creation.complete(fixture.transport);
            var first = fixture.send(new byte[0]);
            assertThat(first).isNotDone();
            assertFull(fixture.send(new byte[0]));
            first.cancel(false);
            assertFull(fixture.send(new byte[0]));
            fixture.ack.complete(org.apache.pulsar.client.api.MessageId.earliest);
            assertThat(fixture.send(new byte[0]).get(5, TimeUnit.SECONDS)).isNotNull();
        }
    }

    @Test
    public void boundsDispatchChainAfterProducerCreationCompletes() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (Fixture fixture = new Fixture(4096)) {
            CountDownLatch dispatchEntered = new CountDownLatch(1);
            CountDownLatch resumeDispatch = new CountDownLatch(1);
            when(fixture.transport.sendAsync(any(Message.class))).thenAnswer(invocation -> {
                MessageImpl<?> message = invocation.getArgument(0);
                message.getDataBuffer().release();
                message.recycle();
                dispatchEntered.countDown();
                assertThat(resumeDispatch.await(5, TimeUnit.SECONDS)).isTrue();
                return fixture.ack;
            });
            try {
                fixture.send(new byte[128]);
                var completion = executor.submit(() -> fixture.creation.complete(fixture.transport));
                assertThat(dispatchEntered.await(5, TimeUnit.SECONDS)).isTrue();
                // The producer is ready, but its first dispatch has not returned. Subsequent
                // sends append to that chain, the steady-state failure described in #26470.
                assertThat(fixture.send(new byte[128])).isNotDone();
                assertFull(fixture.send(new byte[128]));
                resumeDispatch.countDown();
                completion.get(5, TimeUnit.SECONDS);
            } finally {
                resumeDispatch.countDown();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void disabledLimitAllowsPendingSends() throws Exception {
        try (Fixture fixture = new Fixture(0)) {
            for (int i = 0; i < 100; i++) {
                assertThat(fixture.send(new byte[128])).isNotDone();
            }
        }
    }

    @Test
    public void releasesReservationAfterSendFailure() throws Exception {
        try (Fixture fixture = new Fixture(1024)) {
            fixture.creation.complete(fixture.transport);
            var first = fixture.send(new byte[128]);
            fixture.ack.completeExceptionally(new IllegalStateException("send failed"));
            assertThatThrownBy(first::join).hasCauseInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> fixture.send(new byte[128]).join())
                    .hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    public void sharesBudgetAcrossProducers() throws Exception {
        try (Fixture fixture = new Fixture(1024)) {
            fixture.send(new byte[128]);
            var second = fixture.newProducer();
            try {
                assertFull(second.async().newMessage().value(new byte[128]).send());
            } finally {
                second.closeAsync().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void releasesReservationWhenCreationFails() throws Exception {
        try (Fixture fixture = new Fixture(1024)) {
            var first = fixture.send(new byte[128]);
            fixture.creation.completeExceptionally(new IllegalStateException("creation failed"));
            assertThatThrownBy(first::join).hasCauseInstanceOf(IllegalStateException.class);
            // A failed creation remains cached, but admission must succeed and surface that error
            // again instead of leaking the reservation and reporting a full memory buffer.
            assertThatThrownBy(() -> fixture.send(new byte[128]).join())
                    .hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    public void chargesPayloadAndPropertiesBeforeDispatch() throws Exception {
        try (Fixture fixture = new Fixture(4096)) {
            fixture.producer.async().newMessage().value(new byte[2048])
                    .property("property", "value").send();
            assertFull(fixture.send(new byte[0]));
        }
        try (Fixture fixture = new Fixture(4096)) {
            fixture.producer.async().newMessage().value(new byte[0])
                    .property("property", "x".repeat(1024)).send();
            assertFull(fixture.send(new byte[0]));
        }
    }

    @Test
    public void synchronousSendWaitsForAdmission() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (Fixture fixture = new Fixture(1024)) {
            fixture.creation.complete(fixture.transport);
            fixture.send(new byte[128]);
            var blockingProducer = fixture.newProducer(true);
            CountDownLatch started = new CountDownLatch(1);
            var send = executor.submit(() -> {
                started.countDown();
                return blockingProducer.newMessage().value(new byte[128]).send();
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> send.get(50, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            fixture.ack.complete(org.apache.pulsar.client.api.MessageId.earliest);
            assertThat(send.get(5, TimeUnit.SECONDS)).isNotNull();
            blockingProducer.closeAsync().get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @DataProvider
    public Object[][] producerReady() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "producerReady")
    public void asyncSendBlocksBeforeReturningFutureWhenConfigured(boolean ready) throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (Fixture fixture = new Fixture(1024)) {
            if (ready) {
                fixture.creation.complete(fixture.transport);
            }
            fixture.send(new byte[128]);
            var blockingProducer = fixture.newProducer(true);
            try {
                CountDownLatch started = new CountDownLatch(1);
                var invocation = executor.submit(() -> {
                    started.countDown();
                    return blockingProducer.async().newMessage().value(new byte[128]).send();
                });
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                // Waiting on invocation distinguishes blocking admission from returning a pending
                // acknowledgement future and silently adding another link to the dispatch chain.
                assertThatThrownBy(() -> invocation.get(50, TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);
                fixture.creation.complete(fixture.transport);
                fixture.ack.complete(org.apache.pulsar.client.api.MessageId.earliest);
                assertThat(invocation.get(5, TimeUnit.SECONDS).get(5, TimeUnit.SECONDS)).isNotNull();
            } finally {
                blockingProducer.closeAsync().get(5, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void interruptedAsyncAdmissionReturnsFailedFutureWithoutLeakingMemory() throws Exception {
        try (Fixture fixture = new Fixture(1024)) {
            fixture.send(new byte[128]);
            var blockingProducer = fixture.newProducer(true);
            AtomicReference<CompletableFuture<MessageId>> result = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean();
            CountDownLatch started = new CountDownLatch(1);
            Thread caller = new Thread(() -> {
                started.countDown();
                result.set(blockingProducer.async().newMessage().value(new byte[128]).send());
                interrupted.set(Thread.currentThread().isInterrupted());
            }, "blocking-v5-admission-test");
            try {
                caller.start();
                assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
                caller.interrupt();
                caller.join(5000);
                assertThat(caller.isAlive()).isFalse();
                assertThat(interrupted).isTrue();
                assertThat(result.get()).isNotNull().isCompletedExceptionally();
                assertThatThrownBy(result.get()::join).hasRootCauseInstanceOf(InterruptedException.class);
                fixture.creation.complete(fixture.transport);
                fixture.ack.complete(org.apache.pulsar.client.api.MessageId.earliest);
                assertThat(fixture.send(new byte[128]).get(5, TimeUnit.SECONDS)).isNotNull();
            } finally {
                caller.interrupt();
                caller.join(5000);
                blockingProducer.closeAsync().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void synchronousSendRejectsWhenWaitingIsDisabled() throws Exception {
        try (Fixture fixture = new Fixture(1024)) {
            fixture.send(new byte[128]);
            var producer = fixture.newProducer(false);
            try {
                assertThatThrownBy(() -> producer.newMessage().value(new byte[128]).send())
                        .isInstanceOf(PulsarClientException.MemoryBufferIsFullException.class);
            } finally {
                producer.closeAsync().get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    public void transportNeverBlocksEvenWhenSendAdmissionWaits() throws Exception {
        try (Fixture fixture = new Fixture(1024)) {
            var blockingProducer = fixture.newProducer(true);
            blockingProducer.async().newMessage().value(new byte[128]).send();
            verify(fixture.v4Client).createSegmentProducerAsync(
                    argThat(conf -> !conf.isBlockIfQueueFull()), any());
            fixture.creation.complete(fixture.transport);
            fixture.ack.complete(org.apache.pulsar.client.api.MessageId.earliest);
            blockingProducer.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    private static void assertFull(CompletableFuture<MessageId> send) {
        assertThat(send).isCompletedExceptionally();
        assertThatThrownBy(send::join)
                .hasCauseInstanceOf(PulsarClientException.MemoryBufferIsFullException.class);
    }

    private static final class Fixture implements AutoCloseable {
        private final PulsarClientImpl v4Client = mock(PulsarClientImpl.class);
        private final PulsarClientV5 client;
        private final CompletableFuture<Producer<byte[]>> creation =
                new CompletableFuture<>();
        private final ProducerBase<byte[]> transport;
        private final CompletableFuture<org.apache.pulsar.client.api.MessageId> ack = new CompletableFuture<>();
        private final ScalableTopicProducer<byte[]> producer;

        @SuppressWarnings("unchecked")
        Fixture(long memoryLimit) {
            when(v4Client.getMemoryLimitController()).thenReturn(new MemoryLimitController(memoryLimit));
            client = new PulsarClientV5(v4Client, "test", Duration.ofSeconds(10));
            transport = mock(ProducerBase.class);
            when(transport.getTopic()).thenReturn("segment://tenant/ns/admission-test/0000-ffff-0");
            when(transport.sendAsync(any(Message.class))).thenAnswer(invocation -> {
                MessageImpl<?> message = invocation.getArgument(0);
                message.getDataBuffer().release();
                message.recycle();
                return ack;
            });
            when(transport.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            when(v4Client.<byte[]>createSegmentProducerAsync(any(), any())).thenReturn(creation);
            producer = newProducer();
        }

        ScalableTopicProducer<byte[]> newProducer() {
            return newProducer(false);
        }

        ScalableTopicProducer<byte[]> newProducer(boolean blockIfQueueFull) {
            TopicName topic = TopicName.get("topic://tenant/ns/admission-test");
            DagWatchClient watch = mock(DagWatchClient.class);
            when(watch.topicName()).thenReturn(topic);
            ScalableTopicDAG dag = new ScalableTopicDAG().setEpoch(0);
            dag.addSegment().setSegmentId(0).setHashStart(0).setHashEnd(65535)
                    .setState(SegmentState.ACTIVE).setCreatedAtEpoch(0);
            ProducerConfigurationData conf = new ProducerConfigurationData();
            conf.setBlockIfQueueFull(blockIfQueueFull);
            return new ScalableTopicProducer<>(client, Schema.bytes(), conf, watch,
                    ClientSegmentLayout.fromProto(dag, topic));
        }

        CompletableFuture<MessageId> send(byte[] payload) {
            return producer.async().newMessage().value(payload).send();
        }

        @Override
        public void close() throws Exception {
            creation.complete(transport);
            ack.complete(org.apache.pulsar.client.api.MessageId.earliest);
            producer.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }
}
