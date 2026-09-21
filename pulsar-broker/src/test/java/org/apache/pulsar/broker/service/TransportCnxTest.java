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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.util.Optional;
import org.testng.annotations.Test;

public class TransportCnxTest {
    @Test
    public void legacyTransportReportsUntrackedNotification() {
        TransportCnx connection = mock(TransportCnx.class, CALLS_REAL_METHODS);
        Producer producer = mock(Producer.class);
        Consumer consumer = mock(Consumer.class);
        assertThat(connection.closeProducerAsync(producer, Optional.empty()).join())
                .isEqualTo(TransportCnx.CloseNotification.UNTRACKED);
        assertThat(connection.closeConsumerAsync(consumer, Optional.empty()).join())
                .isEqualTo(TransportCnx.CloseNotification.UNTRACKED);
        verify(connection).closeProducer(producer, Optional.empty());
        verify(connection).closeConsumer(consumer, Optional.empty());
    }

    @Test
    public void legacyTransportSynchronousFailureBecomesFailedFuture() {
        TransportCnx connection = mock(TransportCnx.class, CALLS_REAL_METHODS);
        Producer producer = mock(Producer.class);
        Consumer consumer = mock(Consumer.class);
        RuntimeException failure = new RuntimeException("transport close failed");
        doThrow(failure).when(connection).closeProducer(producer, Optional.empty());
        doThrow(failure).when(connection).closeConsumer(consumer, Optional.empty());
        assertThat(connection.closeProducerAsync(producer, Optional.empty())).isCompletedExceptionally();
        assertThat(connection.closeConsumerAsync(consumer, Optional.empty())).isCompletedExceptionally();
    }
}
