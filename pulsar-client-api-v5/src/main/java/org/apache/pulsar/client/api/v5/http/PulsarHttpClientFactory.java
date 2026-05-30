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

/**
 * Framework-owned factory for {@link PulsarHttpClient} instances (PIP-478).
 *
 * <p>Plugins obtain HTTP clients via
 * {@link org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext#httpClientFactory()}; they
 * MUST NOT construct private clients directly. Multiple instances per {@code PulsarClient} are
 * supported — for example OAuth2 mTLS to the identity provider uses a different
 * TLS-purpose-driven configuration than HTTP topic lookup, but they share the underlying event
 * loop / timer / DNS resources owned by the framework.
 */
public interface PulsarHttpClientFactory {

    /**
     * Create a new {@link PulsarHttpClient} configured per the supplied config.
     *
     * <p>The returned instance is owned by the framework; the framework closes it when the
     * {@code PulsarClient} is closed.
     *
     * @param config per-instance configuration (timeouts, proxy, TLS purpose, ...)
     * @return a configured HTTP client
     */
    PulsarHttpClient newHttpClient(PulsarHttpClientConfig config);
}
