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

import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.apache.bookkeeper.mledger.Position;

@UtilityClass
public class AckSetStateUtil {
    public static Position createPositionWithAckSet(long ledgerId, long entryId, long[] ackSet) {
        return new AckSetPositionImpl(ledgerId, entryId, ackSet);
    }

    public static Optional<AckSetState> maybeGetAckSetState(Position position) {
        return position.getExtension(AckSetState.class);
    }

    public static long[] getAckSetArrayOrNull(Position position) {
        return maybeGetAckSetState(position).map(AckSetState::getAckSet).orElse(null);
    }

    public static AckSetState getAckSetState(Position position) {
        return maybeGetAckSetState(position)
                .orElseThrow(() ->
                        new IllegalStateException("Position does not have AckSetState. position=" + position));
    }

    public static boolean hasAckSet(Position position) {
        return maybeGetAckSetState(position).map(AckSetState::hasAckSet).orElse(false);
    }
}
