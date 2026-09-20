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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.function.ToIntFunction;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.EntryReadCountHandler;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.jspecify.annotations.Nullable;

public class EntryAndMetadata implements Entry {
    private static final int STICKY_KEY_HASH_NOT_INITIALIZED = -1;
    private static final Recycler<EntryAndMetadata> RECYCLER = new Recycler<>() {
        @Override
        protected EntryAndMetadata newObject(Handle<EntryAndMetadata> handle) {
            return new EntryAndMetadata(handle);
        }
    };

    private final Handle<EntryAndMetadata> recyclerHandle;
    private Entry entry;
    @Getter
    @Nullable
    private MessageMetadata metadata;
    int stickyKeyHash = STICKY_KEY_HASH_NOT_INITIALIZED;

    private EntryAndMetadata(Handle<EntryAndMetadata> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public static EntryAndMetadata create(final Entry entry, final MessageMetadata metadata) {
        if (entry instanceof EntryAndMetadata entryAndMetadata) {
            return entryAndMetadata;
        }
        EntryAndMetadata entryAndMetadata = RECYCLER.get();
        entryAndMetadata.entry = entry;
        entryAndMetadata.metadata = metadata;
        return entryAndMetadata;
    }

    @VisibleForTesting
    public static EntryAndMetadata create(final Entry entry) {
        if (entry instanceof EntryAndMetadata entryAndMetadata) {
            return entryAndMetadata;
        }
        MessageMetadata msgMetadata = entry.getMessageMetadata();
        if (msgMetadata == null) {
            msgMetadata = Commands.peekAndCopyMessageMetadata(entry.getDataBuffer(), "", -1);
        }
        return create(entry, msgMetadata);
    }

    public byte[] getStickyKey() {
        if (metadata != null) {
            return Commands.resolveStickyKey(metadata);
        }
        return Commands.NONE_KEY;
    }

    @Override
    public String toString() {
        String s = entry.getLedgerId() + ":" + entry.getEntryId();
        if (metadata != null) {
            s += ("@" + metadata.getProducerName() + "-" + metadata.getSequenceId());
            if (metadata.hasChunkId() && metadata.hasNumChunksFromMsg()) {
                s += ("-" + metadata.getChunkId() + "-" + metadata.getNumChunksFromMsg());
            }
        }
        return s;
    }

    @Override
    public byte[] getData() {
        return entry.getData();
    }

    @Override
    public byte[] getDataAndRelease() {
        byte[] data = entry.getDataAndRelease();
        recycle();
        return data;
    }

    @Override
    public int getLength() {
        return entry.getLength();
    }

    @Override
    public ByteBuf getDataBuffer() {
        return entry.getDataBuffer();
    }

    @Override
    public Position getPosition() {
        return entry.getPosition();
    }

    @Override
    public long getLedgerId() {
        return entry.getLedgerId();
    }

    @Override
    public long getEntryId() {
        return entry.getEntryId();
    }

    @Override
    public boolean release() {
        boolean released = entry.release();
        recycle();
        return released;
    }

    private void recycle() {
        entry = null;
        metadata = null;
        stickyKeyHash = STICKY_KEY_HASH_NOT_INITIALIZED;
        recyclerHandle.recycle(this);
    }

    /**
     * Get cached sticky key hash or calculate it based on the sticky key if it's not cached.
     *
     * @param makeStickyKeyHash function to calculate the sticky key hash
     * @return the sticky key hash
     */
    public int getOrUpdateCachedStickyKeyHash(ToIntFunction<byte[]> makeStickyKeyHash) {
        if (stickyKeyHash == STICKY_KEY_HASH_NOT_INITIALIZED) {
            stickyKeyHash = makeStickyKeyHash.applyAsInt(getStickyKey());
        }
        return stickyKeyHash;
    }

    /**
     * Get cached sticky key hash or return STICKY_KEY_HASH_NOT_SET if it's not cached.
     *
     * @return the cached sticky key hash or STICKY_KEY_HASH_NOT_SET if it's not cached
     */
    public int getCachedStickyKeyHash() {
        return stickyKeyHash != STICKY_KEY_HASH_NOT_INITIALIZED ? stickyKeyHash
                : StickyKeyConsumerSelector.STICKY_KEY_HASH_NOT_SET;
    }

    @VisibleForTesting
    public Entry unwrap() {
        return entry;
    }

    @Override
    public EntryReadCountHandler getReadCountHandler() {
        return entry.getReadCountHandler();
    }
}
