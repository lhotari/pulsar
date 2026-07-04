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
package org.apache.pulsar.client.impl.auth.v5;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.v5.http.HttpRequest;
import org.apache.pulsar.client.api.v5.http.HttpResponse;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClient;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientConfig;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactory;

/**
 * Placeholder {@link PulsarHttpClientFactory} that wires the client-side seam without a real HTTP
 * backend (PIP-478 stage 3b).
 *
 * <p>The real AsyncHttpClient-backed factory — sharing the client's event loop, timer, DNS resolver and
 * TLS material — lands in stage 3c. Until then this stub hands out clients whose {@code execute(...)}
 * completes exceptionally with an actionable message, so a plugin that reaches for HTTP before 3c fails
 * loudly rather than silently no-op'ing. Binary-transport plugins (the stage-3b scope) never touch it.
 */
public final class StubHttpClientFactory implements PulsarHttpClientFactory {

    private static final String MESSAGE =
            "The framework HTTP client is not available yet: PulsarHttpClientFactory is a stub in PIP-478 "
                    + "stage 3b; the real AsyncHttpClient-backed client lands in stage 3c.";

    @Override
    public PulsarHttpClient newHttpClient(PulsarHttpClientConfig config) {
        return new StubHttpClient();
    }

    private static final class StubHttpClient implements PulsarHttpClient {
        @Override
        public CompletableFuture<HttpResponse> execute(HttpRequest request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(MESSAGE));
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
