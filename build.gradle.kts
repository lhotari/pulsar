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

buildscript {
    // The license plugin pulls in plexus-utils:2.0.6 which conflicts with
    // the Shadow plugin's plexus-utils:4.0.2 (missing 4-arg matchPath method).
    // Force the newer version to avoid NoSuchMethodError at shading time.
    configurations.classpath {
        resolutionStrategy.force("org.codehaus.plexus:plexus-utils:4.0.2")
    }
}

plugins {
    alias(libs.plugins.rat)
    alias(libs.plugins.version.catalog.update)
    alias(libs.plugins.versions)
}

versionCatalogUpdate {
    keep {
        keepUnusedVersions.set(true)
    }
}

tasks.named<com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask>("dependencyUpdates") {
    outputFormatter = "html"
    rejectVersionIf {
        val nonStable = candidate.version.contains("alpha") || candidate.version.contains("beta") || candidate.version.contains("rc")
        // OpenTelemetry publishes stable releases with -alpha suffix for some modules
        val isOpenTelemetry = candidate.group.startsWith("io.opentelemetry")
        nonStable && !(isOpenTelemetry && candidate.version.contains("alpha"))
    }
}

val catalog = the<VersionCatalogsExtension>().named("libs")
val pulsarVersion = catalog.findVersion("pulsar").get().requiredVersion

// ── Apache RAT (Release Audit Tool) ─────────────────────────────────────────
tasks.named("rat").configure {
    // Use reflection since type-safe accessors aren't available for applied plugins
    val excludesProp = this.javaClass.getMethod("getExcludes").invoke(this)
    @Suppress("UNCHECKED_CAST")
    val excludes = excludesProp as MutableCollection<String>
    excludes.addAll(listOf(
        // License files
        "licenses/LICENSE-*.txt",
        "src/assemble/README.bin.txt",
        "src/assemble/LICENSE.bin.txt",
        "src/assemble/NOTICE.bin.txt",
        // Services files
        "**/META-INF/services/*",
        // Generated Protobuf files
        "src/main/java/org/apache/bookkeeper/mledger/proto/MLDataFormats.java",
        "src/main/java/org/apache/pulsar/broker/service/schema/proto/SchemaRegistryFormat.java",
        "bin/proto/MLDataFormats_pb2.py",
        // Generated Avro files
        "**/avro/generated/*.java",
        "**/*.avsc",
        // Generated Flatbuffer files (Kinesis)
        "**/org/apache/pulsar/io/kinesis/fbs/*.java",
        // Imported from Netty
        "src/main/java/org/apache/bookkeeper/mledger/util/AbstractCASReferenceCounted.java",
        // Maven build artifacts
        "**/dependency-reduced-pom.xml",
        // HdrHistogram output files
        "**/*.hgrm",
        // ProGuard/R8 rules
        "**/*.pro",
        // Go module configs
        "pulsar-client-go/go.mod",
        "pulsar-client-go/go.sum",
        "pulsar-function-go/go.mod",
        "pulsar-function-go/go.sum",
        "pulsar-function-go/examples/go.mod",
        "pulsar-function-go/examples/go.sum",
        // HashProvider service file
        "**/META-INF/services/com.scurrilous.circe.HashProvider",
        // Django generated code
        "**/django/stats/migrations/*.py",
        "**/conf/uwsgi_params",
        // Certificates and keys
        "**/*.crt",
        "**/*.key",
        "**/*.csr",
        "**/*.srl",
        "**/*.txt",
        "**/*.pem",
        "**/*.json",
        "**/*.htpasswd",
        "**/src/test/resources/athenz.conf.test",
        "deployment/terraform-ansible/templates/myid",
        "**/certificate-authority/index.txt",
        "**/certificate-authority/serial",
        "**/certificate-authority/README.md",
        // ZK test data
        "**/zk-3.5-test-data/*",
        // Python requirements
        "**/requirements.txt",
        // Configuration templates
        "conf/schema_example.json",
        "**/templates/*.tpl",
        // Helm
        "**/.helmignore",
        "**/_helpers.tpl",
        // Project/IDE files
        "**/*.md",
        ".github/**",
        "**/*.nar",
        "**/.terraform/**",
        "**/.gitignore",
        "**/.gitattributes",
        "**/.svn",
        "**/*.iws",
        "**/*.ipr",
        "**/*.iml",
        "**/*.cbp",
        "**/*.pyc",
        "**/.classpath",
        "**/.project",
        "**/.settings",
        "**/target/**",
        "**/*.log",
        "**/build/**",
        "**/file:/**",
        "**/SecurityAuth.audit*",
        "**/site2/**",
        "**/.idea/**",
        "**/.vscode/**",
        "**/.mvn/**",
        "**/*.a",
        "**/*.so",
        "**/*.so.*",
        "**/*.dylib",
        "**/*.patch",
        "src/test/resources/*.txt",
        "**/*_pb2.py",
        "**/*_pb2_grpc.py",
        // Test output (local builds)
        "**/test-output/**",
        // Generated LightProto files
        "**/generated-lightproto/**",
        // Generated source files (e.g. Protobuf, Avro)
        "**/generated-sources/**",
        // Local runtime data
        "**/data/**",
        "**/logs/**",
        // Hidden directories (AI tools, etc.)
        ".*/**",
        // Gradle/Kotlin files
        ".gradle/**",
        "gradle/wrapper/**",
        "**/.gradle/**",
        "**/.kotlin/**",
        "**/gradle/wrapper/**",
        "gradlew",
        "gradlew.bat",
        "gradle/libs.versions.toml",
    ))
}

apply(from = "gradle/verify-test-groups.gradle.kts")

// ── Root lifecycle tasks ────────────────────────────────────────────────────

tasks.register("serverDistTar") {
    dependsOn(":distribution:pulsar-server-distribution:serverDistTar")
}

tasks.register("docker") {
    description = "Build the Pulsar Docker image"
    group = "docker"
    dependsOn(":docker:pulsar-docker-image:dockerBuild")
}

// ── Filtered BookKeeper test JAR ────────────────────────────────────────────
// Filtered bookkeeper-server test-jar that excludes classes conflicting with testmocks
// (BookKeeperTestClient and TestStatsProvider have Pulsar-specific versions in testmocks).
// Exposed as a consumable configuration so consuming projects can depend on it via:
//   testImplementation(project(path = ":", configuration = "filteredBkServerTestJar"))
val bkServerTestJarResolvable by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    isTransitive = false
}
dependencies {
    bkServerTestJarResolvable(libs.bookkeeper.server) { artifact { classifier = "tests" } }
}
val filteredBkServerTestJarTask = tasks.register<Jar>("filteredBkServerTestJarTask") {
    archiveFileName.set("bookkeeper-server-tests-filtered.jar")
    destinationDirectory.set(layout.buildDirectory.dir("libs"))
    from(zipTree(bkServerTestJarResolvable.singleFile)) {
        exclude("org/apache/bookkeeper/client/BookKeeperTestClient*")
        exclude("org/apache/bookkeeper/client/TestStatsProvider*")
    }
}
configurations.consumable("filteredBkServerTestJar")
artifacts.add("filteredBkServerTestJar", filteredBkServerTestJarTask)
