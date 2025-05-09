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

import org.jspecify.annotations.Nullable;

/**
 * Interface to manage the ackSet state attached to a position.
 * Helpers in {@link AckSetStateUtil} to create positions with
 * ackSet state and to extract the state.
 */
public interface AckSetState {
    /**
     * Get the ackSet bitset information encoded as a long array.
     * @return the ackSet
     */
    @Nullable long[] getAckSet();

    /**
     * Set the ackSet bitset information as a long array.
     * @param ackSet the ackSet
     */
    void setAckSet(long[] ackSet);

    /**
     * Check if the ackSet is set.
     * @return true if the ackSet is set, false otherwise
     */
    default boolean hasAckSet() {
        return getAckSet() != null;
    }
}
