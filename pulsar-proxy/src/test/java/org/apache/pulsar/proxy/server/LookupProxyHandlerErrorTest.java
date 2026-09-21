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
package org.apache.pulsar.proxy.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.ServerError;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class LookupProxyHandlerErrorTest {
    @DataProvider
    public Object[][] failurePaths() {
        return new Object[][] {
                {false, false}, {false, true}, {true, false}, {true, true}
        };
    }

    @Test(dataProvider = "failurePaths")
    public void metadataFailurePreservesRequestTypeAndConnection(boolean partitioned, boolean connectFailure) {
        ProxyConfiguration config = new ProxyConfiguration();
        config.setBrokerServiceURL("pulsar://broker:6650");
        ProxyService proxy = mock(ProxyService.class);
        when(proxy.getConfiguration()).thenReturn(config);
        when(proxy.getLookupRequestSemaphore()).thenReturn(new Semaphore(1));
        ProxyConnection connection = mock(ProxyConnection.class);
        ConnectionPool pool = mock(ConnectionPool.class);
        when(connection.getConnectionPool()).thenReturn(pool);
        when(connection.newRequestId()).thenReturn(456L);
        ClientCnx backend = mock(ClientCnx.class);
        Throwable failure = new CompletionException(
                new PulsarClientException.BrokerMetadataException("Broker draining"));
        when(pool.getConnection(any(InetSocketAddress.class))).thenReturn(connectFailure
                ? CompletableFuture.failedFuture(failure) : CompletableFuture.completedFuture(backend));
        when(backend.newLookup(any(ByteBuf.class), anyLong())).thenAnswer(invocation -> {
            ((ByteBuf) invocation.getArgument(0)).release();
            return CompletableFuture.failedFuture(failure);
        });
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        when(connection.ctx()).thenReturn(channel.pipeline().firstContext());
        try {
            LookupProxyHandler handler = new LookupProxyHandler(proxy, connection);
            if (partitioned) {
                handler.handlePartitionMetadataResponse(new CommandPartitionedTopicMetadata()
                        .setTopic("persistent://public/default/topic").setRequestId(123));
            } else {
                handler.handleLookup(new CommandLookupTopic()
                        .setTopic("persistent://public/default/topic").setRequestId(123));
            }
            ByteBuf response = channel.readOutbound();
            assertThat(response).isNotNull();
            try {
                response.readInt();
                int commandSize = response.readInt();
                BaseCommand command = new BaseCommand();
                command.parseFrom(response, commandSize);
                if (partitioned) {
                    assertThat(command.getType()).isEqualTo(BaseCommand.Type.PARTITIONED_METADATA_RESPONSE);
                    assertThat(command.getPartitionMetadataResponse().getRequestId()).isEqualTo(123);
                    assertThat(command.getPartitionMetadataResponse().getError()).isEqualTo(ServerError.MetadataError);
                } else {
                    assertThat(command.getType()).isEqualTo(BaseCommand.Type.LOOKUP_RESPONSE);
                    assertThat(command.getLookupTopicResponse().getRequestId()).isEqualTo(123);
                    assertThat(command.getLookupTopicResponse().getError()).isEqualTo(ServerError.MetadataError);
                }
                assertThat(channel.isActive()).isTrue();
                assertThat((Object) channel.readOutbound()).isNull();
            } finally {
                response.release();
            }
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}
