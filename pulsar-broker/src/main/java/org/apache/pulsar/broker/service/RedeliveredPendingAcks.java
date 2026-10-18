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

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.List;
import org.apache.bookkeeper.mledger.Position;

/**
 * The pending acks that a consumer removed to redeliver them: their positions and the sticky key hash of each,
 * {@code STICKY_KEY_HASH_NOT_SET} when it has none.
 */
public record RedeliveredPendingAcks(List<Position> positions, IntList stickyKeyHashes) {
}
