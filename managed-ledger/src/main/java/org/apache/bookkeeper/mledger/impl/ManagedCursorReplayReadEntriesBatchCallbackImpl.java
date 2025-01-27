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

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursorReplayReadEntriesCallback;
import org.apache.bookkeeper.mledger.ManagedCursorReplayReadRange;
import org.apache.bookkeeper.mledger.ManagedLedgerException;

@ToString
@Slf4j
class ManagedCursorReplayReadEntriesBatchCallbackImpl implements AsyncCallbacks.ReadEntriesCallback {
    private final List<ManagedCursorReplayReadRange> ranges;
    private final boolean invokeCallbacksInOrder;
    private final ManagedCursorReplayReadEntriesCallback callback;
    private final Object callerCtx;
    int pendingCallbacks;
    SortedMap<ManagedCursorReplayReadRange, List<Entry>> pendingResults;
    int nextResultIndex;
    ManagedLedgerException exception;
    ManagedCursorReplayReadRange failedRange;

    public ManagedCursorReplayReadEntriesBatchCallbackImpl(List<ManagedCursorReplayReadRange> ranges,
                                                           boolean invokeCallbacksInOrder,
                                                           ManagedCursorReplayReadEntriesCallback callback,
                                                           Object callerCtx) {
        this.ranges = ranges;
        this.invokeCallbacksInOrder = invokeCallbacksInOrder;
        this.callback = callback;
        this.callerCtx = callerCtx;
        pendingCallbacks = ranges.size();
        pendingResults = new TreeMap<>();
        nextResultIndex = 0;
    }

    @Override
    public synchronized void readEntriesComplete(List<Entry> entries, Object ctx) {
        if (invokeCallbacksInOrder && exception != null) {
            // if there is already a previous failure, we should release the entry straight away
            // and not add it to the list
            entries.forEach(Entry::release);
            if (--pendingCallbacks == 0) {
                callback.readEntriesFailed(failedRange, true, exception, callerCtx);
            }
        } else {
            ManagedCursorReplayReadRange range = (ManagedCursorReplayReadRange) ctx;
            if (!invokeCallbacksInOrder || nextResultIndex == range.rangeIndex()) {
                if (!invokeCallbacksInOrder) {
                    nextResultIndex++;
                }
                boolean isLast = (--pendingCallbacks == 0);
                callback.readEntriesComplete(range, isLast, entries, callerCtx);
                if (invokeCallbacksInOrder) {
                    finishPossiblePendingResults();
                }
            } else {
                pendingResults.put(range, entries);
            }
        }
    }

    private void finishPossiblePendingResults() {
        while (!pendingResults.isEmpty()) {
            ManagedCursorReplayReadRange range = pendingResults.firstKey();
            if (range.rangeIndex() == nextResultIndex) {
                nextResultIndex++;
                List<Entry> entries = pendingResults.remove(range);
                boolean isLast = (--pendingCallbacks == 0);
                callback.readEntriesComplete(range, isLast, entries, callerCtx);
            } else {
                break;
            }
        }
    }

    @Override
    public synchronized void readEntriesFailed(ManagedLedgerException mle, Object ctx) {
        failedRange = (ManagedCursorReplayReadRange) ctx;
        exception = mle;
        if (!(mle instanceof ManagedLedgerException.CursorAlreadyClosedException)) {
            log.warn("Error while replaying entries for range {}", mle, failedRange);
        }
        // when invoking in order is required, fail any pending entries
        if (!pendingResults.isEmpty()) {
            for (List<Entry> entries : pendingResults.values()) {
                entries.forEach(Entry::release);
                pendingCallbacks--;
            }
            pendingResults.clear();
        }
        if (--pendingCallbacks == 0) {
            callback.readEntriesFailed(failedRange, true, exception, callerCtx);
        }
    }
}
