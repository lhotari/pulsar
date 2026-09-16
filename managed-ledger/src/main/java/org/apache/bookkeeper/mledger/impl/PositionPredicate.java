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

import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;

/** An internal predicate that can evaluate position IDs without constructing a lookup object. */
@FunctionalInterface
interface PositionPredicate extends Predicate<Position> {
    boolean test(long ledgerId, long entryId);

    @Override
    default boolean test(Position position) {
        return test(position.getLedgerId(), position.getEntryId());
    }

    /** Legacy predicates continue to receive an ordinary immutable Position that they may retain. */
    static boolean test(Predicate<Position> predicate, long ledgerId, long entryId) {
        if (predicate instanceof PositionPredicate primitivePredicate) {
            return primitivePredicate.test(ledgerId, entryId);
        }
        return predicate.test(PositionFactory.create(ledgerId, entryId));
    }
}
