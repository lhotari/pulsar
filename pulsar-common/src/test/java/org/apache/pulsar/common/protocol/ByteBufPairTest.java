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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.testng.annotations.Test;

public class ByteBufPairTest {

    @Test
    public void testDoubleByteBuf() throws Exception {
        ByteBuf b1 = PulsarByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b1.writerIndex(b1.capacity());
        ByteBuf b2 = PulsarByteBufAllocator.DEFAULT.heapBuffer(128, 128);
        b2.writerIndex(b2.capacity());
        ByteBufPair buf = ByteBufPair.get(b1, b2);

        assertEquals(buf.readableBytes(), 256);
        assertEquals(buf.getFirst(), b1);
        assertEquals(buf.getSecond(), b2);

        assertEquals(buf.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);
        assertEquals(b2.refCnt(), 1);

        buf.release();

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEncoderHandsOverTheExclusiveBuffersOfAPairWithOneReference() throws Exception {
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes());
        ByteBufPair pair = ByteBufPair.get(b1, b2).markBuffersExclusive();
        List<ByteBuf> written = new ArrayList<>();
        ChannelHandlerContext ctx = writingContext(written, false);

        ByteBufPair.ENCODER.write(ctx, pair, null);

        // the writes got the pair's buffers themselves, without retaining them, and the pair was recycled
        assertSame(written.get(0), b1);
        assertSame(written.get(1), b2);
        assertEquals(pair.refCnt(), 0);
        assertEquals(b1.refCnt(), 1);
        assertEquals(b2.refCnt(), 1);
        b1.release();
        b2.release();
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEncoderWritesDuplicatesOfBuffersThatMayBeShared() throws Exception {
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes());
        // a pair whose buffers aren't marked exclusive, with one reference, and one that a client keeps to resend
        ByteBufPair notExclusive = ByteBufPair.get(b1.retain(), b2.retain());
        ByteBufPair kept = ByteBufPair.get(b1, b2).markBuffersExclusive();
        kept.retain();
        for (ByteBufPair pair : List.of(notExclusive, kept)) {
            List<ByteBuf> written = new ArrayList<>();
            ByteBufPair.ENCODER.write(writingContext(written, true), pair, null);
            // the writes consumed duplicates, which left the buffers' reader indexes as they were
            assertNotSame(written.get(0), b1);
            assertNotSame(written.get(1), b2);
            assertEquals(b1.readerIndex(), 0);
            assertEquals(b2.readerIndex(), 0);
        }
        assertEquals(notExclusive.refCnt(), 0);
        assertEquals(kept.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);
        kept.release();
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    /** A context whose writes record the buffers, and when {@code consume} is set, read and release them. */
    private static ChannelHandlerContext writingContext(List<ByteBuf> written, boolean consume) {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.write(any(), any())).then(invocation -> {
            ByteBuf buf = (ByteBuf) invocation.getArguments()[0];
            written.add(buf);
            if (consume) {
                buf.skipBytes(buf.readableBytes());
                buf.release();
            }
            return null;
        });
        return ctx;
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEncoder() throws Exception {
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes());
        ByteBufPair buf = ByteBufPair.get(b1, b2);

        assertEquals(buf.readableBytes(), 10);
        assertEquals(buf.getFirst(), b1);
        assertEquals(buf.getSecond(), b2);

        assertEquals(buf.refCnt(), 1);
        assertEquals(b1.refCnt(), 1);
        assertEquals(b2.refCnt(), 1);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.write(any(), any())).then(invocation -> {
            // Simulate a write on the context which releases the buffer
            ((ByteBuf) invocation.getArguments()[0]).release();
            return null;
        });

        ByteBufPair.ENCODER.write(ctx, buf, null);

        assertEquals(buf.refCnt(), 0);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
    }

    @Test
    public void testCoalesce() {
        ByteBuf b1 = Unpooled.wrappedBuffer("hello".getBytes());
        ByteBuf b2 = Unpooled.wrappedBuffer("world".getBytes());
        ByteBufPair buf = ByteBufPair.get(b1, b2);
        ByteBuf coalesced = ByteBufPair.coalesce(buf);
        assertEquals(b1.refCnt(), 0);
        assertEquals(b2.refCnt(), 0);
        assertEquals(new String(ByteBufUtil.getBytes(coalesced)), "helloworld");
        coalesced.release();
    }
}
