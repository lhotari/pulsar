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
package org.apache.pulsar.client.impl;

import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;

/**
 * TableView implementation that applies a mapper function to the messages.
 * @param <T> the message schema type
 * @param <V> the value type returned by the mapper function
 */
@Slf4j
public class MessageMapperTableViewImpl<T, V> extends AbstractTableViewImpl<T, V> {
    private final Function<Message<T>, V> mapper;
    private final boolean shouldReleasePooledMessage;

    MessageMapperTableViewImpl(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf,
                               Function<Message<T>, V> mapper, boolean shouldReleasePooledMessage) {
        super(client, schema, conf);
        this.mapper = mapper;
        this.shouldReleasePooledMessage = shouldReleasePooledMessage;
    }

    @Override
    protected boolean shouldReleasePooledMessage() {
        return shouldReleasePooledMessage;
    }

    @Override
    protected V getValue(Message<T> msg) {
        return mapper.apply(msg);
    }
}
