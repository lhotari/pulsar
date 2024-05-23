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
package org.apache.pulsar.common.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DuplicatedByteBuf;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * A read-only {@link DuplicatedByteBuf} implementation to be used for {@link ByteBufPair}
 * ensuring that the ChannelHandlers cannot modify the content of the buffer.
 *
 * The Netty SslHandler will modify the content of the buffer when encrypting the data by
 * default. For a buffer that returns false for {@link #isWritable(int)}, the SslHandler
 * will avoid modifying the content of the buffer. It is not sufficient that the {@link #isReadOnly()}
 * method returns true (there's PR 14071 in Netty to fix this.).
 *
 * Another reason to use this class is to overcome another limitation of Netty's
 * {@link io.netty.buffer.ReadOnlyByteBuf} which does not keep reference count or support releasing
 * the wrapped buffer.
 *
 * When using Netty's ReadOnlyByteBuf, it also leads to a performance problem where the direct buffer content
 * couldn't be copied using memory copy operations. This is because the Netty's ReadOnlyByteBuf doesn't allow
 * memory access or array access. There's no need to prevent memory access or array access in this implementation
 * that is only used for {@link ByteBufPair}'s encoder usage.
 */
class ReadOnlyDuplicatedByteBuf extends DuplicatedByteBuf {
    public ReadOnlyDuplicatedByteBuf(ByteBuf buffer) {
        super(buffer);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public ByteBuf duplicate() {
        return new ReadOnlyDuplicatedByteBuf(this);
    }

    @Override
    public ByteBuf slice(int index, int length) {
        return new ReadOnlyDuplicatedByteBuf(unwrap().slice(index, length));
    }

    @Override
    public ByteBuf asReadOnly() {
        return this;
    }

    @Override
    public boolean isWritable() {
        return false;
    }

    @Override
    public boolean isWritable(int numBytes) {
        return false;
    }

    @Override
    public int ensureWritable(int minWritableBytes, boolean force) {
        return 1;
    }

    @Override
    public ByteBuf ensureWritable(int minWritableBytes) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf discardReadBytes() {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setByte(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setByte(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setShort(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setShort(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setShortLE(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setShortLE(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setMedium(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setMedium(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setMediumLE(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setMediumLE(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setInt(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setInt(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setIntLE(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setIntLE(int index, int value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setLong(int index, long value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setLong(int index, long value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf setLongLE(int index, long value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    protected void _setLongLE(int index, long value) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public int setBytes(int index, InputStream in, int length) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public int setBytes(int index, ScatteringByteChannel in, int length) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public int setBytes(int index, FileChannel in, long position, int length) {
        throw new ReadOnlyBufferException();
    }

    @Override
    public ByteBuf capacity(int newCapacity) {
        throw new ReadOnlyBufferException();
    }
}
