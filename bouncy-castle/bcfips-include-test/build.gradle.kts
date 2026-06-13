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

plugins {
    id("pulsar.java-conventions")
    id("pulsar.test-certs-conventions")
}


// Exclude the non-FIPS BouncyCastle module and libraries — this module tests with the FIPS provider
// only. Having both the non-FIPS bc (bcprov) and bcfips (bc-fips) on the classpath causes
// CryptoServicesRegistrar signer conflicts. The non-FIPS module's Maven artifactId is
// "bouncy-castle-bc" (it was previously excluded as "bc", which silently stopped matching after the
// module was renamed).
configurations.all {
    exclude(group = "org.apache.pulsar", module = "bouncy-castle-bc")
    exclude(group = "org.bouncycastle", module = "bcprov-jdk18on")
    exclude(group = "org.bouncycastle", module = "bcprov-ext-jdk18on")
    exclude(group = "org.bouncycastle", module = "bcpkix-jdk18on")
    exclude(group = "org.bouncycastle", module = "bcutil-jdk18on")
}

dependencies {
    implementation(libs.slog)
    testImplementation(project(":bouncy-castle:bcfips"))
    testImplementation(project(":pulsar-common"))
    testImplementation(project(":pulsar-broker"))
    testImplementation(project(path = ":pulsar-broker", configuration = "testJar"))
    testImplementation(project(":pulsar-client-original"))
    testImplementation(libs.guava)
}
