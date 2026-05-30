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
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A simple immutable {@link TlsMaterialProviderInitContext} backed by a caller-supplied scheduler
 * (PIP-478). Used by client, broker, proxy and other components to initialize their
 * {@link PulsarTlsMaterialProvider} with a shared executor.
 */
public final class DefaultTlsMaterialProviderInitContext implements TlsMaterialProviderInitContext {

    private final ScheduledExecutorService scheduler;
    private final Clock clock;
    private final String instanceId;

    /**
     * @param scheduler  the executor for off-loop work (e.g. file-rotation polling)
     * @param clock      the clock; if {@code null}, {@link Clock#systemUTC()} is used
     * @param instanceId a stable id of the owning component for logging correlation
     */
    public DefaultTlsMaterialProviderInitContext(ScheduledExecutorService scheduler, Clock clock,
                                                 String instanceId) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.clock = clock != null ? clock : Clock.systemUTC();
        this.instanceId = instanceId != null ? instanceId : "pulsar";
    }

    @Override
    public ScheduledExecutorService scheduler() {
        return scheduler;
    }

    @Override
    public Clock clock() {
        return clock;
    }

    @Override
    public String instanceId() {
        return instanceId;
    }
}
