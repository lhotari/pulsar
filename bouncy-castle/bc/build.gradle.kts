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
    id("pulsar.public-java-library-conventions")
}

dependencies {
    // Artifact bundle: puts the non-FIPS BouncyCastle provider jars on the runtime classpath for
    // consumers that need the JCA provider at runtime (e.g. pulsar-client) without a compile-time
    // dependency on BouncyCastle. This module carries no classes of its own.
    implementation(libs.bcpkix.jdk18on)
    implementation(libs.bcprov.jdk18on)
}
