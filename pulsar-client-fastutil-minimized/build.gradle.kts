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

// Jar of just the fastutil classes reachable from the Pulsar client. It is bundled (and
// relocated) by :pulsar-client-shaded, :pulsar-client-all and :pulsar-client-admin-shaded
// so the full ~25MB fastutil jar is not shipped. See pulsar.minimized-dependencies-conventions.

plugins {
    id("pulsar.minimized-dependencies-conventions")
    // Published to Maven so it can be referenced from pulsar-client-original's published metadata
    // (where the full fastutil dependency is replaced by this minimized jar). The broker variant is
    // not published — it is only consumed by the server distribution.
    id("pulsar.publish-conventions")
}

dependencies {
    // Reachability root: pulsar-client-original (its NegativeAcksTracker is the only direct
    // fastutil consumer on the client side). minimize() seeds from its transitive closure.
    api(project(":pulsar-client-original"))
}

minimizedJar {
    minimizedDependencies.set(listOf("it.unimi.dsi:fastutil"))
    // The reachable set is ~591 classes; fail the build if it grows past this (e.g. if
    // minimize() regresses and ships the full ~12,965-class jar).
    maxRetainedClasses.set(600)
}
