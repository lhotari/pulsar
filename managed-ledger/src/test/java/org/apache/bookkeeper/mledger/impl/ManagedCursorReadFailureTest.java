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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.mockito.AdditionalAnswers;
import org.testng.annotations.Test;

public class ManagedCursorReadFailureTest extends MockedBookKeeperTestCase {

    @Test(timeOut = 20_000)
    public void testRejectedTailWaitRegistrationReleasesReadAndAllowsRetry() throws Exception {
        int delayMillis = 10;
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setNewEntriesCheckDelayInMillis(delayMillis);
        @Cleanup
        ManagedLedgerImpl ledger = spy((ManagedLedgerImpl) factory.open("rejected-tail-wait",
                config));
        @Cleanup
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        OrderedScheduler realScheduler = ledger.getScheduledExecutor();
        OrderedScheduler scheduler = mock(OrderedScheduler.class, AdditionalAnswers.delegatesTo(realScheduler));
        Thread callingThread = Thread.currentThread();
        AtomicBoolean rejectNextSchedule = new AtomicBoolean(true);
        RejectedExecutionException rejection = new RejectedExecutionException("tail-wait scheduler rejected read");
        doAnswer(invocation -> {
            if (Thread.currentThread() == callingThread && rejectNextSchedule.compareAndSet(true, false)) {
                // The actual cursor has installed the waiting op before submitting this delayed check.
                assertThat(cursor.hasPendingReadRequest()).isTrue();
                throw rejection;
            }
            return realScheduler.schedule(invocation.<Runnable>getArgument(0), invocation.<Long>getArgument(1),
                    invocation.getArgument(2));
        }).when(scheduler).schedule(any(Runnable.class), eq((long) delayMillis), eq(TimeUnit.MILLISECONDS));
        doReturn(scheduler).when(ledger).getScheduledExecutor();

        CompletableFuture<byte[]> rejectedRead = new CompletableFuture<>();
        cursor.asyncReadEntriesOrWait(1, callback(rejectedRead), null, PositionFactory.LATEST);
        assertThat(rejectNextSchedule).isFalse();
        try {
            rejectedRead.get(5, TimeUnit.SECONDS);
            throw new AssertionError("the rejected registration must fail its read callback");
        } catch (ExecutionException failure) {
            assertThat(failure.getCause()).isInstanceOf(ManagedLedgerException.class).hasCause(rejection);
        }
        assertThat(cursor.hasPendingReadRequest()).isFalse();
        assertThat(cursor.getPendingReadOpsCount()).isZero();
        assertThat(cursor.cancelPendingReadRequest()).isFalse();

        CompletableFuture<byte[]> retriedRead = new CompletableFuture<>();
        cursor.asyncReadEntriesOrWait(1, callback(retriedRead), null, PositionFactory.LATEST);
        byte[] payload = "after-rejected-registration".getBytes(StandardCharsets.UTF_8);
        ledger.addEntry(payload);
        assertThat(retriedRead.get(5, TimeUnit.SECONDS)).isEqualTo(payload);
        assertThat(cursor.getPendingReadOpsCount()).isZero();
    }

    @Test(timeOut = 20_000)
    public void testClosingWaitingReadKeepsPendingReadCountAtZero() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setNewEntriesCheckDelayInMillis(0);
        @Cleanup
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("close-waiting-read", config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        CompletableFuture<byte[]> read = new CompletableFuture<>();
        cursor.asyncReadEntriesOrWait(1, callback(read), null, PositionFactory.LATEST);
        assertThat(cursor.hasPendingReadRequest()).isTrue();
        assertThat(cursor.getPendingReadOpsCount()).isZero();

        cursor.close();
        try {
            read.get(5, TimeUnit.SECONDS);
            throw new AssertionError("closing the cursor must fail the waiting read");
        } catch (ExecutionException failure) {
            assertThat(failure.getCause()).isInstanceOf(ManagedLedgerException.CursorAlreadyClosedException.class);
        }
        assertThat(cursor.hasPendingReadRequest()).isFalse();
        assertThat(cursor.getPendingReadOpsCount()).isZero();
    }

    @Test(timeOut = 20_000)
    public void testNotificationClaimedReadIsNotFailedWhenSchedulingRejects() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setNewEntriesCheckDelayInMillis(0);
        @Cleanup
        ManagedLedgerImpl ledger = spy((ManagedLedgerImpl) factory.open("notification-before-rejection", config));
        @Cleanup
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");

        // Cancellation leaves an existing notification registration in the ledger. A new append can
        // consume the replacement waiting op before that read submits its own delayed registration.
        CompletableFuture<byte[]> cancelledRead = new CompletableFuture<>();
        cursor.asyncReadEntriesOrWait(1, callback(cancelledRead), null, PositionFactory.LATEST);
        assertThat(ledger.getWaitingCursorsCount()).isEqualTo(1);
        assertThat(cursor.cancelPendingReadRequest()).isTrue();
        config.setNewEntriesCheckDelayInMillis(10);

        CompletableFuture<byte[]> read = new CompletableFuture<>();
        AtomicInteger callbackCount = new AtomicInteger();
        byte[] payload = "notification-won".getBytes(StandardCharsets.UTF_8);
        OrderedScheduler realScheduler = ledger.getScheduledExecutor();
        OrderedScheduler scheduler = mock(OrderedScheduler.class, AdditionalAnswers.delegatesTo(realScheduler));
        AtomicBoolean rejectNextSchedule = new AtomicBoolean(true);
        Thread callingThread = Thread.currentThread();
        doAnswer(invocation -> {
            if (Thread.currentThread() == callingThread && rejectNextSchedule.compareAndSet(true, false)) {
                assertThat(cursor.hasPendingReadRequest()).isTrue();
                // Publish through the real ledger notification path, and finish the read before rejecting.
                ledger.addEntry(payload);
                assertThat(read.get(5, TimeUnit.SECONDS)).isEqualTo(payload);
                throw new RejectedExecutionException("notification already claimed the waiting read");
            }
            return realScheduler.schedule(invocation.<Runnable>getArgument(0), invocation.<Long>getArgument(1),
                    invocation.getArgument(2));
        }).when(scheduler).schedule(any(Runnable.class), eq(10L), eq(TimeUnit.MILLISECONDS));
        doReturn(scheduler).when(ledger).getScheduledExecutor();

        cursor.asyncReadEntriesOrWait(1, callback(read, callbackCount), null, PositionFactory.LATEST);
        assertThat(rejectNextSchedule).isFalse();
        assertThat(read.get(5, TimeUnit.SECONDS)).isEqualTo(payload);
        assertThat(callbackCount).hasValue(1);
        assertThat(cancelledRead.isDone()).isFalse();
        assertThat(cursor.hasPendingReadRequest()).isFalse();
        assertThat(cursor.getPendingReadOpsCount()).isZero();
    }

    @Test(timeOut = 20_000)
    public void testRejectedScheduleDoesNotFailCancelledReadsReplacement() throws Exception {
        // Netty only pools Recycler objects on threads that clean up their FastThreadLocal state.
        FutureTask<Void> task = new FutureTask<>(() -> {
            assertRejectedScheduleDoesNotFailCancelledReadsReplacement();
            return null;
        });
        FastThreadLocalThread thread = new FastThreadLocalThread(task, "cursor-read-recycler-reuse");
        thread.start();
        try {
            task.get(10, TimeUnit.SECONDS);
        } finally {
            task.cancel(true);
            thread.join(TimeUnit.SECONDS.toMillis(5));
            assertThat(thread.isAlive()).as("the cursor test thread must terminate").isFalse();
        }
    }

    private void assertRejectedScheduleDoesNotFailCancelledReadsReplacement() throws Exception {
        ManagedLedgerConfig config = initManagedLedgerConfig(defaultConfig());
        config.setNewEntriesCheckDelayInMillis(0);
        @Cleanup
        ManagedLedgerImpl ledger = spy((ManagedLedgerImpl) factory.open("cancel-rearm-before-rejection", config));
        @Cleanup
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor("cursor");
        // Warm the thread-local Recycler using actual cursor reads and cancellations. This crosses its
        // allocation sampling interval so the rejected read and replacement can share a pooled instance.
        for (int i = 0; i < 32; i++) {
            CompletableFuture<byte[]> warmupRead = new CompletableFuture<>();
            cursor.asyncReadEntriesOrWait(1, callback(warmupRead), null, PositionFactory.LATEST);
            assertThat(cursor.cancelPendingReadRequest()).isTrue();
            assertThat(warmupRead.isDone()).isFalse();
        }
        config.setNewEntriesCheckDelayInMillis(10);
        OrderedScheduler realScheduler = ledger.getScheduledExecutor();
        OrderedScheduler scheduler = mock(OrderedScheduler.class, AdditionalAnswers.delegatesTo(realScheduler));
        AtomicBoolean rejectNextSchedule = new AtomicBoolean(true);
        CompletableFuture<byte[]> cancelledRead = new CompletableFuture<>();
        CompletableFuture<byte[]> replacementRead = new CompletableFuture<>();
        AtomicInteger cancelledCallbackCount = new AtomicInteger();
        AtomicInteger replacementCallbackCount = new AtomicInteger();
        Thread callingThread = Thread.currentThread();
        doAnswer(invocation -> {
            if (Thread.currentThread() == callingThread && rejectNextSchedule.compareAndSet(true, false)) {
                OpReadEntry acceptedOperation = cursor.getWaitingReadOp();
                assertThat(acceptedOperation).isNotNull();
                int acceptedOperationId = acceptedOperation.id;
                assertThat(cursor.cancelPendingReadRequest()).isTrue();
                // Re-arm on the same thread so the recycled OpReadEntry can be reused. Its new id must
                // keep the old submission's rejection cleanup from claiming this replacement read.
                config.setNewEntriesCheckDelayInMillis(0);
                cursor.asyncReadEntriesOrWait(1, callback(replacementRead, replacementCallbackCount), null,
                        PositionFactory.LATEST);
                OpReadEntry replacementOperation = cursor.getWaitingReadOp();
                assertThat(replacementOperation)
                        .as("the rejection must race reuse of the same pooled operation")
                        .isSameAs(acceptedOperation);
                assertThat(replacementOperation.id)
                        .as("recycled operations need a new identity even when the object is reused")
                        .isNotEqualTo(acceptedOperationId);
                assertThat(cursor.hasPendingReadRequest()).isTrue();
                throw new RejectedExecutionException("cancelled submission rejected after replacement was armed");
            }
            return realScheduler.schedule(invocation.<Runnable>getArgument(0), invocation.<Long>getArgument(1),
                    invocation.getArgument(2));
        }).when(scheduler).schedule(any(Runnable.class), eq(10L), eq(TimeUnit.MILLISECONDS));
        doReturn(scheduler).when(ledger).getScheduledExecutor();

        cursor.asyncReadEntriesOrWait(1, callback(cancelledRead, cancelledCallbackCount), null, PositionFactory.LATEST);
        assertThat(rejectNextSchedule).isFalse();
        assertThat(cancelledRead.isDone()).isFalse();
        assertThat(replacementRead.isDone()).isFalse();
        assertThat(cursor.hasPendingReadRequest()).isTrue();

        byte[] payload = "replacement-survived".getBytes(StandardCharsets.UTF_8);
        ledger.addEntry(payload);
        assertThat(replacementRead.get(5, TimeUnit.SECONDS)).isEqualTo(payload);
        assertThat(cancelledCallbackCount).hasValue(0);
        assertThat(replacementCallbackCount).hasValue(1);
        assertThat(cursor.getPendingReadOpsCount()).isZero();
    }

    private ReadEntriesCallback callback(CompletableFuture<byte[]> result) {
        return callback(result, new AtomicInteger());
    }

    private ReadEntriesCallback callback(CompletableFuture<byte[]> result, AtomicInteger callbackCount) {
        return new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                callbackCount.incrementAndGet();
                try {
                    assertThat(entries).hasSize(1);
                    result.complete(entries.get(0).getData());
                } catch (Throwable failure) {
                    result.completeExceptionally(failure);
                } finally {
                    entries.forEach(Entry::release);
                }
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                callbackCount.incrementAndGet();
                result.completeExceptionally(exception);
            }
        };
    }
}
