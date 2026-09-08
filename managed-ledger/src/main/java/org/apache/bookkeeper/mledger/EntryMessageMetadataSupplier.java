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
package org.apache.bookkeeper.mledger;

import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.api.proto.MessageMetadata;

/**
 * Implemented by the context object passed to
 * {@link ManagedLedger#asyncAddEntry(ByteBuf, int, AsyncCallbacks.AddEntryCallback, Object)} by callers that have
 * already parsed the message metadata of the entry they are adding, so that the entry cache doesn't have to parse
 * the same bytes again for tailing readers.
 *
 * <p>The managed ledger only asks for the metadata when the entry is going to be cached, so implementations are
 * free to parse it lazily.
 */
public interface EntryMessageMetadataSupplier {

    /**
     * Returns the message metadata of the entry being added, or null when it isn't available.
     *
     * <p>Called on the thread that called {@code asyncAddEntry}, while {@code entryData} is still intact.
     *
     * <p>The returned instance is attached to the cached entry and read for as long as the entry stays in the
     * cache, which is long after {@code entryData} has been released, and possibly from several threads at once.
     * It must therefore be detached from that buffer — see {@link MessageMetadata#materialize()}, since a parsed
     * {@link MessageMetadata} otherwise decodes its string and bytes fields lazily out of the buffer it was
     * parsed from — and it must not be an instance that is shared or reused across messages, such as the thread
     * local one returned by {@code Commands.parseMessageMetadata(ByteBuf)}.
     *
     * @param entryData the data of the entry being added
     * @return the message metadata, detached from {@code entryData}, or null
     */
    MessageMetadata getMessageMetadataForEntryCache(ByteBuf entryData);
}
