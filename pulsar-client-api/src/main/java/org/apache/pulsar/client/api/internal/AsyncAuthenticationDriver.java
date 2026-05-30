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
package org.apache.pulsar.client.api.internal;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.api.AuthData;

/**
 * Stable-internal marker that lets {@code ClientCnx} drive authentication asynchronously (PIP-478).
 *
 * <p>This is the only carve-out from the otherwise-untouched v4 client. When the configured
 * {@link org.apache.pulsar.client.api.Authentication} instance also implements this interface,
 * {@code ClientCnx} routes connect / refresh / challenge handling through these non-blocking methods
 * and pipes the result back onto the Netty event loop; plain v4 instances keep the existing
 * synchronous path verbatim. It is generic across all challenge types (initial connect, the broker's
 * {@code REFRESH_AUTH_DATA} sentinel, SASL multi-round, and custom challenge/response).
 *
 * <p>The {@code .internal.} subpackage signals "stable internal" — application code should not
 * implement this interface; it is observed by the framework.
 */
public interface AsyncAuthenticationDriver {

    /**
     * Asynchronously produce the initial authentication data for {@code CommandConnect}.
     *
     * @param brokerHostName the broker host being connected to (for per-host credentials)
     * @return a future of the auth data; completes exceptionally on failure
     */
    CompletableFuture<AuthData> getAuthDataAsync(String brokerHostName);

    /**
     * Asynchronously compute the response to a broker {@code CommandAuthChallenge} (or to the
     * {@code REFRESH_AUTH_DATA} refresh sentinel).
     *
     * @param challenge      the challenge payload from the broker
     * @param brokerHostName the broker host being connected to
     * @return a future of the response auth data; completes exceptionally on failure
     */
    CompletableFuture<AuthData> authenticateAsync(AuthData challenge, String brokerHostName);
}
