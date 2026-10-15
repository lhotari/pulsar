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


import io.netty.util.Recycler;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.LongArrayAckSets;

@SuppressWarnings("unchecked")
public class EntryBatchIndexesAcks {
    private int size;
    // Created by the first ack set of a dispatch: most dispatches have none, and without the recycler, each dispatch
    // would allocate the array. It's kept by a recycled instance, null or at least as long as the size
    private Pair<Integer, long[]>[] indexesAcks;

    public void setIndexesAcks(int entryIdx, Pair<Integer, long[]> indexesAcks) {
        if (this.indexesAcks == null || this.indexesAcks.length < size) {
            this.indexesAcks = new Pair[size];
        }
        this.indexesAcks[entryIdx] = indexesAcks;
    }

    public long[] getAckSet(int entryIdx) {
        if (indexesAcks == null) {
            return null;
        }
        Pair<Integer, long[]> pair = indexesAcks[entryIdx];
        return pair == null ? null : pair.getRight();
    }

    public int getTotalAckedIndexCount() {
        if (indexesAcks == null) {
            return 0;
        }
        int count = 0;
        for (int i = 0; i < size; i++) {
            Pair<Integer, long[]> pair = indexesAcks[i];
            if (pair != null) {
                count += pair.getLeft() - LongArrayAckSets.cardinality(pair.getRight());
            }
        }
        return count;
    }

    public void recycle() {
        if (indexesAcks != null) {
            for (int i = 0; i < size; i++) {
                indexesAcks[i] = null;
            }
        }
        handle.recycle(this);
    }

    private void ensureCapacityAndSetSize(int entriesListSize) {
        size = entriesListSize;
        if (indexesAcks != null && indexesAcks.length < size) {
            // a recycled instance's array, too short for this dispatch's ack sets
            indexesAcks = null;
        }
    }

    public static EntryBatchIndexesAcks get(int entriesListSize) {
        EntryBatchIndexesAcks ebi = RECYCLER.get();
        ebi.ensureCapacityAndSetSize(entriesListSize);
        return ebi;
    }

    private EntryBatchIndexesAcks(Recycler.Handle<EntryBatchIndexesAcks> handle) {
        this.handle = handle;
    }

    private final Recycler.Handle<EntryBatchIndexesAcks> handle;
    private static final Recycler<EntryBatchIndexesAcks> RECYCLER = new Recycler<EntryBatchIndexesAcks>() {
        @Override
        protected EntryBatchIndexesAcks newObject(Handle<EntryBatchIndexesAcks> handle) {
            return new EntryBatchIndexesAcks(handle);
        }
    };
}
