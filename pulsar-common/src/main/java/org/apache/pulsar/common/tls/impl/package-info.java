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
 * The default, file-based implementation of the PIP-478 TLS SPI
 * ({@link org.apache.pulsar.common.tls.PulsarTlsFactory}).
 *
 * <p>This package lives in {@code pulsar-common} rather than the dependency-light SPI module
 * {@code pulsar-common-api}: it needs {@code netty-handler} and the {@code netty-tcnative} OpenSSL
 * binding to build native contexts, both already present here. The distinct {@code .impl} package name
 * keeps the SPI package ({@code org.apache.pulsar.common.tls}) single-owner (decision D6).
 *
 * <ul>
 *   <li>{@link org.apache.pulsar.common.tls.impl.FileBasedTlsFactory} — the factory: purpose registry,
 *       single-level fallback resolution, one-shot and subscribing handles, and the rotation
 *       reload fan-out.</li>
 *   <li>{@code TlsMaterialSource} / {@code TlsMaterial} — load, watch, cache one material set with a
 *       fixed mtime baseline and value-equality change suppression.</li>
 *   <li>{@link org.apache.pulsar.common.tls.impl.TlsContexts} — build the Netty/JDK contexts and
 *       synthesize a Netty context from a JDK {@code SSLContext}.</li>
 *   <li>{@link org.apache.pulsar.common.tls.impl.TlsFactoryProbe} — fail-fast boot-time probing.</li>
 * </ul>
 */
package org.apache.pulsar.common.tls.impl;
