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
package org.apache.pulsar.broker.transaction.timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import org.apache.pulsar.broker.TransactionMetadataStoreService;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TransactionTimeoutTrackerCloseTest {
    @DataProvider
    public Object[][] startedCases() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "startedCases")
    public void testCloseSealsReplayAddStartAndDispatchedCallback(boolean started) {
        Timer timer = mock(Timer.class);
        Timeout timeout = mock(Timeout.class);
        when(timer.newTimeout(any(), anyLong(), any())).thenReturn(timeout);
        TransactionMetadataStoreService service = mock(TransactionMetadataStoreService.class);
        TransactionTimeoutTrackerImpl tracker = new TransactionTimeoutTrackerImpl(1, timer, 100, service);
        tracker.replayAddTransaction(2, 0);
        if (started) {
            tracker.start();
        }
        tracker.close();
        tracker.close();
        // A timer callback can already be dispatched when Timeout.cancel is called.
        tracker.run(timeout);
        tracker.replayAddTransaction(3, 0);
        tracker.addTransaction(4, 1);
        tracker.addTransaction(5, 1000);
        tracker.start();
        tracker.run(timeout);
        verify(timer, times(started ? 1 : 0)).newTimeout(any(), anyLong(), any());
        verify(timeout, times(started ? 1 : 0)).cancel();
        verify(service, never()).endTransactionForTimeout(any());
    }

    @Test
    public void testExpiryNotificationCanCloseTrackerWithoutAccessingClosedQueue() {
        Timer timer = mock(Timer.class);
        TransactionMetadataStoreService service = mock(TransactionMetadataStoreService.class);
        TransactionTimeoutTrackerImpl tracker = new TransactionTimeoutTrackerImpl(1, timer, 100, service);
        tracker.replayAddTransaction(2, 0);
        tracker.replayAddTransaction(3, 0);
        doAnswer(invocation -> {
            assertThat(Thread.holdsLock(tracker)).isFalse();
            tracker.close();
            return null;
        }).when(service).endTransactionForTimeout(any());
        tracker.run(mock(Timeout.class));
        verify(service).endTransactionForTimeout(new TxnID(1, 2));
        verify(service, times(1)).endTransactionForTimeout(any());
        verify(timer, never()).newTimeout(any(), anyLong(), any());
    }
}
