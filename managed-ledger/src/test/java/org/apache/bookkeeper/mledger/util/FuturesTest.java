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
package org.apache.bookkeeper.mledger.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FuturesTest {

    @DataProvider
    public Object[][] cleanupFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "cleanupFailures")
    public void testPhysicalCloseAdapterPreservesSeparateResult(boolean failed) throws Exception {
        ManagedLedgerException logical = new ManagedLedgerException.ManagedLedgerFencedException();
        ManagedLedgerException failure = new ManagedLedgerException("physical cleanup failed");
        CompletableFuture<Void> cleanup = new CompletableFuture<>();
        Futures.CloseFuture legacy = new Futures.CloseFuture();
        Futures.PhysicalCloseFuture physical = new Futures.PhysicalCloseFuture();
        Futures.PhysicalCloseFuture canceled = new Futures.PhysicalCloseFuture();
        legacy.closeFailed(logical, cleanup.minimalCompletionStage(), null);
        physical.closeFailed(logical, cleanup.minimalCompletionStage(), null);
        canceled.closeFailed(logical, cleanup.minimalCompletionStage(), null);
        assertThat(canceled.cancel(false)).isTrue();
        assertThatThrownBy(() -> legacy.get(5, TimeUnit.SECONDS)).hasCause(logical);
        assertThat(physical).isNotDone();
        assertThat(cleanup).isNotDone();
        if (failed) {
            cleanup.completeExceptionally(failure);
            assertThatThrownBy(() -> physical.get(5, TimeUnit.SECONDS)).hasCause(failure);
        } else {
            cleanup.complete(null);
            physical.get(5, TimeUnit.SECONDS);
        }
        assertThat(canceled).isCancelled();
    }

    @Test
    public void testPhysicalCloseAdapterKeepsUntrackedFailure() throws Exception {
        ManagedLedgerException failure = new ManagedLedgerException.ManagedLedgerFencedException();
        Futures.PhysicalCloseFuture oldCallback = new Futures.PhysicalCloseFuture();
        oldCallback.closeFailed(failure, null);
        assertThatThrownBy(() -> oldCallback.get(5, TimeUnit.SECONDS)).hasCause(failure);
        Futures.PhysicalCloseFuture direct = new Futures.PhysicalCloseFuture();
        direct.completeExceptionally(failure);
        assertThatThrownBy(() -> direct.get(5, TimeUnit.SECONDS)).hasCause(failure);
    }

    @Test
    public void testExecuteWithRetryHandlesSynchronousFailure() throws Exception {
        AtomicInteger attempts = new AtomicInteger();

        CompletableFuture<String> result = Futures.executeWithRetry(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw new CompletionException(new ManagedLedgerException.MetaStoreException("sync fail"));
            }
            return CompletableFuture.completedFuture("ok");
        }, ManagedLedgerException.MetaStoreException.class, 1);

        assertEquals(result.get(2, TimeUnit.SECONDS), "ok");
        assertEquals(attempts.get(), 2);
    }
}
