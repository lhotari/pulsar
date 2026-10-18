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
package org.apache.pulsar.compaction;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

public class CompactedLedgerEntryCacheTest {
    @Test
    public void keepsCopiesOfTheEntries() {
        CompactedLedgerEntryCache cache = new CompactedLedgerEntryCache(1024 * 1024);
        assertTrue(cache.isEnabled());
        ByteBuf entry = Unpooled.copiedBuffer("entry-0", StandardCharsets.UTF_8);
        entry.readByte();
        cache.put(7, 0, entry);
        // the buffer is unchanged and can be released
        assertEquals(entry.readerIndex(), 1);
        assertEquals(entry.refCnt(), 1);
        entry.release();
        assertEquals(new String(cache.get(7, 0), StandardCharsets.UTF_8), "ntry-0");
        assertNull(cache.get(7, 1));
        assertNull(cache.get(8, 0));
        assertTrue(cache.size() > 0);
    }

    @Test
    public void keepsNothingWhenDisabled() {
        CompactedLedgerEntryCache cache = CompactedLedgerEntryCache.DISABLED;
        assertFalse(cache.isEnabled());
        ByteBuf entry = Unpooled.copiedBuffer("entry-0", StandardCharsets.UTF_8);
        cache.put(7, 0, entry);
        entry.release();
        assertNull(cache.get(7, 0));
        assertEquals(cache.size(), 0);
    }
}
