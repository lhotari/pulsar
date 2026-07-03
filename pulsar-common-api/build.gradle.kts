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

// PIP-478: neutral, dependency-light API module hosting the TLS SPI
// (org.apache.pulsar.common.tls) and the HTTP client SPI
// (org.apache.pulsar.client.api.v5.http). Published so that server-side modules
// (pulsar-common, broker-common, proxy, ...) may depend on it in later stages —
// the published-module dependency guard only allows published-on-published deps.
plugins {
    id("pulsar.public-java-library-conventions")
}

dependencies {
    // Only TlsFactoryInitContext / AuthenticationInitContext expose OpenTelemetry; kept compileOnly
    // to keep this API module dependency-light. The Netty/Jetty well-known TLS classes appear only as
    // documented Class<T> values referenced in plain-text javadoc, so no netty/jetty dependency is needed.
    compileOnly(libs.opentelemetry.api)
}
