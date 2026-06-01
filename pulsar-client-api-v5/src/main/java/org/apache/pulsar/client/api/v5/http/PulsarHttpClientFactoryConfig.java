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
package org.apache.pulsar.client.api.v5.http;

import io.opentelemetry.api.OpenTelemetry;

/**
 * The framework-owned shared resources handed to a {@link PulsarHttpClientProvider} once per
 * {@code PulsarClient} when it builds its {@link PulsarHttpClientFactory} (PIP-478).
 *
 * <p>This type intentionally exposes only transport-neutral handles. Backend-specific shared
 * resources (such as a Netty event-loop group, timer or DNS resolver) are conveyed to the default
 * provider through a richer implementation-private subtype, keeping this public SPI free of any
 * transport-library dependency.
 */
public interface PulsarHttpClientFactoryConfig {

    /**
     * @return a stable identifier of the owning {@code PulsarClient}, for logging correlation
     */
    String clientInstanceId();

    /**
     * @return the telemetry root; the framework defaults to {@link OpenTelemetry#noop()} if unset
     */
    OpenTelemetry openTelemetry();
}
