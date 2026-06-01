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
 * A {@link java.util.ServiceLoader}-discovered HTTP backend (AsyncHttpClient, JDK HttpClient, ...).
 *
 * <p>Operators don't interact with this directly: the framework picks a provider once per
 * {@code PulsarClient} (by configured name or, failing that, by highest {@link #priority()}, ties
 * broken by {@link #name()} ascending) and uses it to build every {@link PulsarHttpClient} instance.
 */
public interface PulsarHttpClientProvider {

    /**
     * @return the stable provider name, e.g. {@code "asynchttpclient"} or {@code "jdk"}
     */
    String name();

    /**
     * @return the selection priority; higher wins, ties broken by {@link #name()} ascending
     */
    default int priority() {
        return 0;
    }

    /**
     * Create the factory used to build every {@link PulsarHttpClient} for one {@code PulsarClient}.
     *
     * @param sharedResources framework-owned shared resources (event loop, timer, DNS, ...)
     * @return a factory bound to those shared resources
     */
    PulsarHttpClientFactory newFactory(PulsarHttpClientFactoryConfig sharedResources);
}
