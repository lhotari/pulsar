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

/**
 * Framework-managed pluggable HTTP client SPI (PIP-478).
 *
 * <p>Authentication plugins that need HTTP (for example OAuth2's token endpoint or Athenz's ZTS)
 * obtain a {@link org.apache.pulsar.client.api.v5.http.PulsarHttpClient} from the framework via
 * {@link org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext#httpClientFactory()} rather
 * than constructing their own. The framework owns the lifecycle (Netty event loop, timer, DNS cache,
 * TLS material refresh integration); plugins describe what they need through a
 * {@link org.apache.pulsar.client.api.v5.http.PulsarHttpClientConfig}.
 *
 * <p>Backends are discovered via {@link java.util.ServiceLoader} through
 * {@link org.apache.pulsar.client.api.v5.http.PulsarHttpClientProvider}; the default implementation
 * is backed by AsyncHttpClient.
 */
package org.apache.pulsar.client.api.v5.http;
