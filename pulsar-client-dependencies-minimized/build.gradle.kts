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

// Imported explicitly: the `java` plugin contributes a `java { }` extension accessor,
// so an unqualified `java.util.zip.ZipFile` would resolve `java` to that extension.
import java.util.zip.ZipFile

// Produces a jar containing ONLY the classes of the "minimized" dependencies that
// are reachable from pulsar-client-original (and its full transitive closure) —
// exactly the classes that end up shaded into the final pulsar-client jars. It is
// bundled (and relocated) by :pulsar-client-shaded, :pulsar-client-all and
// :pulsar-client-admin-shaded so the full dependency jars are not shipped.
//
// This is the Gradle equivalent of the branch-4.2 Maven module, which uses
// maven-shade-plugin's <minimizeJar> over {pulsar-client-original, fastutil}.
//
// How it works:
//   * pulsar-client-original is declared with the `api` scope. The Shadow plugin's
//     minimize() seeds its reachability analysis (UnusedTracker) from the project's
//     own source classes plus its `api`-scoped jars — so the entire pulsar-client
//     closure becomes the set of reachability roots. (With `implementation`, or no
//     source, minimize() has no roots and keeps the full ~12,900-class fastutil jar.)
//   * Only the minimized libraries are bundled into the jar (the `include` filter
//     below); pulsar-client-original itself is read purely as a reachability root.
//   * minimize() then drops every bundled class not reachable from those roots.

plugins {
    id("pulsar.java-conventions")
    id("pulsar.shadow-conventions")
}

// Dependencies to minimize, as "group:name" entries. Only the classes from these
// artifacts that are actually reachable from the pulsar-client closure are kept. Add
// more entries here when another heavy dependency needs to be minimized the same way.
val minimizedDependencies: List<String> = listOf(
    "it.unimi.dsi:fastutil",
)

dependencies {
    // `api` (not `implementation`) so minimize() uses pulsar-client-original and its
    // transitive closure as the reachability roots. Its own classes are not bundled.
    api(project(":pulsar-client-original"))
}

// The api dependency above is a BUILD-ONLY reachability seed for minimize(). This module
// ships a self-contained jar of minimized fastutil classes and must not drag
// pulsar-client-original (or anything else) onto consumers' classpaths, so strip all
// inherited dependencies from the consumable (outgoing) variants.
listOf("apiElements", "runtimeElements").forEach { variant ->
    configurations.named(variant) {
        setExtendsFrom(emptySet())
    }
}

tasks.shadowJar {
    // Bundle ONLY the minimized libraries; pulsar-client-original is read by minimize()
    // as a reachability root but is excluded from the output jar.
    dependencies {
        minimizedDependencies.forEach { coords -> include(dependency("$coords:.*")) }
    }
    // Drop every bundled class not reachable from the api reachability roots above.
    minimize()
}

// ---------------------------------------------------------------------------
// Verification: fail the build if the entry points statically referenced by
// pulsar-client-original (NegativeAcksTracker, the only direct fastutil consumer)
// are ever dropped by over-pruning, or if minimize() silently becomes a no-op (which
// would ship the full fastutil jar). Add to this list when new usage is introduced.
// ---------------------------------------------------------------------------
val requiredMinimizedClasses = listOf(
    "it/unimi/dsi/fastutil/longs/Long2ObjectAVLTreeMap.class",
    "it/unimi/dsi/fastutil/longs/Long2ObjectMap.class",
    "it/unimi/dsi/fastutil/longs/Long2ObjectOpenHashMap.class",
    "it/unimi/dsi/fastutil/longs/Long2ObjectSortedMap.class",
    "it/unimi/dsi/fastutil/longs/LongBidirectionalIterator.class",
)

// Upper bound that comfortably exceeds the reachable set (~1k classes) but is well
// below the full fastutil jar (~12,965 classes), so a minimize() regression fails loudly.
val maxRetainedClasses = 5000

val verifyMinimizedJar by tasks.registering {
    val jarFile = tasks.shadowJar.flatMap { it.archiveFile }
    val required = requiredMinimizedClasses
    val maxClasses = maxRetainedClasses
    inputs.file(jarFile)
    doLast {
        val jar = jarFile.get().asFile
        val entries = mutableSetOf<String>()
        ZipFile(jar).use { zf ->
            val e = zf.entries()
            while (e.hasMoreElements()) {
                entries.add(e.nextElement().name)
            }
        }
        val classCount = entries.count { it.endsWith(".class") }
        val missing = required.filterNot { it in entries }
        if (missing.isNotEmpty()) {
            throw GradleException("Minimized jar is missing required classes (over-pruned): $missing")
        }
        if (classCount > maxClasses) {
            throw GradleException(
                "Minimized jar retained $classCount classes (> $maxClasses) — minimize() is not pruning."
            )
        }
        logger.lifecycle("Minimized jar OK: $classCount classes retained.")
    }
}

tasks.named("check") {
    dependsOn(verifyMinimizedJar)
}
