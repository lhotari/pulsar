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
package org.apache.pulsar.common.tls;

import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Runtime services handed to {@link PulsarTlsMaterialProvider#initialize} (PIP-478).
 *
 * <p>The framework owns and closes these shared services; the provider may retain references for its
 * lifetime and releases its own resources in {@link PulsarTlsMaterialProvider#close()}.
 */
public interface TlsMaterialProviderInitContext {

    /**
     * @return an executor the provider may use for off-loop work such as periodic file-rotation
     *         checks; never a Netty event loop
     */
    ScheduledExecutorService scheduler();

    /**
     * @return the clock used by implementations that schedule against wall-clock time
     */
    Clock clock();

    /**
     * @return a stable id of the owning component (client/broker/proxy) for logging correlation
     */
    String instanceId();
}
