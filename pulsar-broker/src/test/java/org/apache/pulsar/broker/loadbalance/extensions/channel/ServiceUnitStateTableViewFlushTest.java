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

package org.apache.pulsar.broker.loadbalance.extensions.channel;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TableView;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ServiceUnitStateTableViewFlushTest {
    @Test
    public void testRefreshRetriesReaderHandoffWithinOriginalBudget() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.tableview.refreshAsync()).thenReturn(CompletableFuture.failedFuture(
                new PulsarClientException.BrokerMetadataException("Consumer not found")),
                CompletableFuture.completedFuture(null));

        fixture.view.flush(1000);

        verify(fixture.producer).flushAsync();
        verify(fixture.tableview, times(2)).refreshAsync();
    }

    @Test
    public void testRefreshDoesNotRetryAuthorizationFailure() {
        Fixture fixture = new Fixture();
        var error = new PulsarClientException.AuthorizationException("Denied");
        when(fixture.tableview.refreshAsync()).thenReturn(CompletableFuture.failedFuture(error));

        assertSame(expectThrows(ExecutionException.class, () -> fixture.view.flush(1000)).getCause(), error);
        verify(fixture.tableview).refreshAsync();
    }

    @Test
    public void testExpiredBudgetDoesNotRetryMetadataFailure() {
        Fixture fixture = new Fixture();
        var error = new PulsarClientException.BrokerMetadataException("Consumer not found");
        when(fixture.tableview.refreshAsync()).thenReturn(CompletableFuture.failedFuture(error));

        assertSame(expectThrows(ExecutionException.class, () -> fixture.view.flush(0)).getCause(), error);
        verify(fixture.tableview).refreshAsync();
    }

    @Test
    public void testPendingRefreshIsBoundedWithoutCancellingIt() {
        Fixture fixture = new Fixture();
        CompletableFuture<Void> pending = new CompletableFuture<>();
        when(fixture.tableview.refreshAsync()).thenReturn(pending);

        expectThrows(TimeoutException.class, () -> fixture.view.flush(10));
        assertFalse(pending.isDone());
        pending.complete(null);
        verify(fixture.tableview).refreshAsync();
    }

    @Test
    public void testPendingPublishPreventsRefreshAfterBudget() {
        Fixture fixture = new Fixture();
        CompletableFuture<Void> pending = new CompletableFuture<>();
        when(fixture.producer.flushAsync()).thenReturn(pending);

        expectThrows(TimeoutException.class, () -> fixture.view.flush(10));
        verifyNoInteractions(fixture.tableview);
        assertFalse(pending.isDone());
        pending.complete(null);
    }

    @SuppressWarnings("unchecked")
    private static final class Fixture {
        private final Producer<ServiceUnitStateData> producer = mock(Producer.class);
        private final TableView<ServiceUnitStateData> tableview = mock(TableView.class);
        private final ServiceUnitStateTableViewImpl view = new ServiceUnitStateTableViewImpl(producer, tableview);

        private Fixture() {
            when(producer.flushAsync()).thenReturn(CompletableFuture.completedFuture(null));
        }
    }
}
