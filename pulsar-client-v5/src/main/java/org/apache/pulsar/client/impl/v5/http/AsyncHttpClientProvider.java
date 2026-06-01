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
package org.apache.pulsar.client.impl.v5.http;

import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactory;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientFactoryConfig;
import org.apache.pulsar.client.api.v5.http.PulsarHttpClientProvider;
import org.apache.pulsar.common.tls.PulsarTlsMaterialProvider;

/**
 * The default {@link PulsarHttpClientProvider} (PIP-478), backed by
 * <a href="https://github.com/AsyncHttpClient/async-http-client">AsyncHttpClient</a> over Netty.
 *
 * <p>Discovered via {@link java.util.ServiceLoader}. It registers under the stable name
 * {@code "asynchttpclient"} with priority {@code 100}, so it is selected by default ahead of any
 * lower-priority backend unless an operator pins a different provider by name.
 *
 * <h2>TLS material plumbing</h2>
 *
 * <p>The public {@link PulsarHttpClientProvider#newFactory(PulsarHttpClientFactoryConfig)} entry
 * point only conveys a {@link PulsarHttpClientFactoryConfig} (a {@code clientInstanceId} and an
 * {@code OpenTelemetry} handle). That transport-neutral SPI deliberately does not expose a
 * {@link PulsarTlsMaterialProvider}, a Netty event-loop group or a timer yet (see the
 * {@code PulsarHttpClientFactoryConfig} javadoc, which notes that backend-specific shared resources
 * are conveyed through a richer implementation-private subtype that does not exist at this stage).
 * Consequently, a factory built solely from the public SPI uses the platform default trust store and
 * no client certificate — equivalent to {@link org.apache.pulsar.client.api.v5.http.ClientHttpTlsPurpose#GENERIC}.
 *
 * <p>To remain forward-compatible, this provider is pluggable: {@link #withTlsMaterialProvider} (and
 * the package-private {@link #newFactory(PulsarHttpClientFactoryConfig, PulsarTlsMaterialProvider)})
 * let the framework wire in a {@link PulsarTlsMaterialProvider}. When one is supplied, the factory
 * maps each {@code ClientHttpTlsPurpose} one-to-one (by name) onto a
 * {@code ClientTlsPurposeContext.ClientPurpose}, threads any usage identifier through, and bridges to
 * AsyncHttpClient via {@code PulsarTlsAsyncHttpSslEngineFactory}.
 */
public class AsyncHttpClientProvider implements PulsarHttpClientProvider {

    /** The stable provider name used for {@link java.util.ServiceLoader} selection. */
    public static final String PROVIDER_NAME = "asynchttpclient";

    private final PulsarTlsMaterialProvider tlsMaterialProvider;

    /**
     * Public no-arg constructor required for {@link java.util.ServiceLoader} discovery. No TLS
     * material provider is wired, so factories built by this instance default to the platform trust
     * store with no client certificate.
     */
    public AsyncHttpClientProvider() {
        this(null);
    }

    private AsyncHttpClientProvider(PulsarTlsMaterialProvider tlsMaterialProvider) {
        this.tlsMaterialProvider = tlsMaterialProvider;
    }

    /**
     * Return a copy of this provider that resolves client TLS material from the supplied provider.
     *
     * <p>This is the seam the framework uses to inject TLS material once the richer shared-resource
     * handle becomes available (the public SPI does not carry it today). The returned provider keeps
     * the same {@link #name()} and {@link #priority()}.
     *
     * @param materialProvider the source of client TLS material; {@code null} restores the
     *                         platform-trust default
     * @return a provider bound to the supplied material provider
     */
    public AsyncHttpClientProvider withTlsMaterialProvider(PulsarTlsMaterialProvider materialProvider) {
        return new AsyncHttpClientProvider(materialProvider);
    }

    @Override
    public String name() {
        return PROVIDER_NAME;
    }

    @Override
    public int priority() {
        return 100;
    }

    @Override
    public PulsarHttpClientFactory newFactory(PulsarHttpClientFactoryConfig sharedResources) {
        return new AsyncHttpClientFactory(sharedResources, tlsMaterialProvider);
    }

    /**
     * Package-private factory entry point that lets a caller (e.g. a test or the framework) inject a
     * {@link PulsarTlsMaterialProvider} for a single factory without first wrapping the provider.
     *
     * @param sharedResources    the framework-owned shared resources
     * @param materialProvider   the source of client TLS material, or {@code null} for the
     *                          platform-trust default
     * @return a factory bound to those resources and (optionally) TLS material
     */
    AsyncHttpClientFactory newFactory(PulsarHttpClientFactoryConfig sharedResources,
                                      PulsarTlsMaterialProvider materialProvider) {
        return new AsyncHttpClientFactory(sharedResources, materialProvider);
    }
}
