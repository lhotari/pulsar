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
package org.apache.bookkeeper.mledger.impl;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.mledger.EntryReadCountHandler;

public class EntryReadCountHandlerImpl implements EntryReadCountHandler {
    private static AtomicIntegerFieldUpdater<EntryReadCountHandlerImpl> expectedReadCountUpdater =
            AtomicIntegerFieldUpdater.newUpdater(EntryReadCountHandlerImpl.class, "expectedReadCount");

    private volatile int expectedReadCount;

    private EntryReadCountHandlerImpl(int expectedReadCount) {
        this.expectedReadCount = expectedReadCount;
    }

    public int getExpectedReadCount() {
        return expectedReadCount;
    }

    @Override
    public boolean incrementExpectedReadCount() {
        expectedReadCountUpdater.incrementAndGet(this);
        return true;
    }

    @Override
    public void markRead() {
        expectedReadCountUpdater.decrementAndGet(this);
    }

    public static EntryReadCountHandlerImpl create(int expectedReadCount) {
        return new EntryReadCountHandlerImpl(expectedReadCount);
    }
}
