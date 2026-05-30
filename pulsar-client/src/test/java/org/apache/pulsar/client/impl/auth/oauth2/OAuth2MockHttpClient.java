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
package org.apache.pulsar.client.impl.auth.oauth2;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.tls.FileBasedTlsMaterialProvider;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.mockito.MockedConstruction;

final class OAuth2MockHttpClient {

    private OAuth2MockHttpClient() {
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * Run {@code runnable} with the OAuth2 flow's TLS material provider and async HTTP client mocked, so
     * tests that exercise parameter parsing can configure TLS cert/key paths without those files existing
     * (PIP-478: {@code FlowBase} now builds a {@link FileBasedTlsMaterialProvider} instead of the removed
     * {@code DefaultPulsarSslFactory}).
     */
    static void withMockedSslFactory(ThrowingRunnable runnable) throws Exception {
        try (MockedConstruction<FileBasedTlsMaterialProvider> ignoredTlsProvider =
                     mockConstruction(FileBasedTlsMaterialProvider.class, (mock, context) ->
                         when(mock.initialize(any())).thenReturn(CompletableFuture.completedFuture(null)));
             MockedConstruction<DefaultAsyncHttpClient> ignoredHttpClient =
                     mockConstruction(DefaultAsyncHttpClient.class)) {
            runnable.run();
        }
    }
}
