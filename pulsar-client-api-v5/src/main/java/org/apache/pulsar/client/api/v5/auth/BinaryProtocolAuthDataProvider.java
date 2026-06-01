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
package org.apache.pulsar.client.api.v5.auth;

import java.util.concurrent.CompletableFuture;

/**
 * Capability: single-pass credential exchange for the Pulsar binary protocol (PIP-478).
 *
 * <p>A plugin implements this when it can present a credential the broker accepts or rejects in a
 * single round trip (token, basic, OAuth2 access token, Athenz role token, or mTLS via the built-in
 * {@code TlsAuthentication}).
 */
public interface BinaryProtocolAuthDataProvider {

    /**
     * @return the stable identifier sent in {@code CommandConnect.auth_method_name}
     */
    String authMethodName();

    /**
     * Produce a credential for the binary-protocol connection.
     *
     * <p>The returned {@link BinaryProtocolAuthData} carries the {@code auth_method_name} and the
     * {@code auth_data} bytes for {@code CommandConnect}. Completes exceptionally on failure (see the
     * error model in {@link org.apache.pulsar.client.api.v5.PulsarClientException}).
     *
     * @param ctx the per-call context
     * @return a future of the binary-protocol credential
     */
    CompletableFuture<BinaryProtocolAuthData> getAuthDataAsync(AuthenticationCallContext ctx);
}
