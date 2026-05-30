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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Pluggable authentication provider for the Pulsar v5 client (PIP-478).
 *
 * <p>The core interface carries only lifecycle. The actual credential work lives on opt-in
 * <em>capability</em> interfaces — segregated by transport (Pulsar binary protocol vs HTTP) and by
 * style (single-pass vs multi-round challenge/response) — that an implementation declares only when
 * it supports them:
 * <ul>
 *   <li>{@link BinaryProtocolAuthDataProvider} — single-pass credential for the binary protocol</li>
 *   <li>{@link HttpAuthHeadersProvider} — single-pass credential for HTTP</li>
 *   <li>{@link ChallengeResponseHandler} — multi-round challenge/response for the binary protocol</li>
 *   <li>{@link HttpHeaderChallengeResponseHandler} — SASL-style multi-round over HTTP</li>
 * </ul>
 * The convenience composites {@link SinglePassAuthentication} and
 * {@link ChallengeResponseAuthentication} bundle the common combinations.
 *
 * <p>All capability methods are asynchronous ({@link CompletableFuture}), so credential acquisition
 * never blocks the Netty event loop. Implementations must be thread-safe.
 */
public interface Authentication extends AutoCloseable {

    /**
     * Configuration step. Called once by the framework AFTER no-arg construction when the plugin was
     * loaded reflectively from {@code authPluginClassName} + {@code authParams}. NOT called when the
     * plugin was constructed programmatically by the user — that path presumes the instance is
     * already configured. Default: no-op. Implementations override to read their parameters.
     *
     * <p>Configuration is intentionally separate from {@link #initializeAsync}: configuration is
     * static plugin-level data (file paths, URLs, scopes); initialization gives the plugin runtime
     * services and may do I/O.
     *
     * @param authParams the parsed authentication parameters
     */
    default void configure(Map<String, String> authParams) {
    }

    /**
     * Initialization step. Called once by the framework with runtime services after configuration.
     * May do I/O; the returned future completes when the implementation is ready to serve
     * credentials.
     *
     * @param ctx the runtime services context
     * @return a future completing when the plugin is ready
     */
    CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx);

    /**
     * Capability lookup for delegating wrappers. Direct implementations are discovered with
     * {@code instanceof}; wrappers that delegate to another instance override this to expose the
     * wrapped capabilities.
     *
     * @param kind the capability interface to look up
     * @param <T>  the capability type
     * @return the capability if supported
     */
    default <T> Optional<T> capability(Class<T> kind) {
        return kind.isInstance(this) ? Optional.of(kind.cast(this)) : Optional.empty();
    }

    @Override
    default void close() throws Exception {
    }
}
