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

import org.gradle.api.plugins.quality.Checkstyle
import org.gradle.api.plugins.quality.CheckstyleExtension

plugins {
    checkstyle
    id("com.github.hierynomus.license")
}

val catalog = the<VersionCatalogsExtension>().named("libs")

// ── Checkstyle ──────────────────────────────────────────────────────────────

configure<CheckstyleExtension> {
    toolVersion = catalog.findVersion("checkstyle").get().requiredVersion
    configFile = rootProject.file("buildtools/src/main/resources/pulsar/checkstyle.xml")
    configProperties["checkstyle.suppressions.file"] =
        rootProject.file("buildtools/src/main/resources/pulsar/suppressions.xml").absolutePath
}

tasks.withType<Checkstyle>().configureEach {
    // Broker module has very large files that need more heap
    maxHeapSize.set("1g")
    // Exclude generated source files (proto, lightproto, etc.)
    exclude { it.file.path.contains("/build/") }
    exclude { it.file.path.contains("/generated-lightproto/") }
    exclude { it.file.path.contains("/generated-sources/") }
    // Match Maven exclusion: **/proto/*
    exclude("**/proto/*")
}

// ── License header check (Mycila/hierynomus) ────────────────────────────────

// The hierynomus license plugin calls Task.project at execution time,
// which is incompatible with Gradle's configuration cache.
tasks.matching { it.name.startsWith("license") }.configureEach {
    notCompatibleWithConfigurationCache("license plugin uses Task.project at execution time")
}

// Configure the license extension. The plugin is on the classpath (declared as a
// dependency of the conventions project), so we can access it. However, the
// extension type is not easily accessible as a public API type, so we use the
// same reflection approach but without afterEvaluate since we're in a convention
// plugin that runs after the plugin is applied.
run {
    val licenseExt = extensions.getByName("license")
    val cls = licenseExt.javaClass

    cls.getMethod("setHeader", File::class.java).invoke(licenseExt, rootProject.file("src/license-header.txt"))
    cls.getMethod("setSkipExistingHeaders", Boolean::class.java).invoke(licenseExt, true)
    cls.getMethod("setStrictCheck", Boolean::class.java).invoke(licenseExt, true)

    val mappingMethod = cls.getMethod("mapping", String::class.java, String::class.java)
    mappingMethod.invoke(licenseExt, "java", "SLASHSTAR_STYLE")
    mappingMethod.invoke(licenseExt, "proto", "JAVADOC_STYLE")
    mappingMethod.invoke(licenseExt, "go", "DOUBLESLASH_STYLE")
    mappingMethod.invoke(licenseExt, "conf", "SCRIPT_STYLE")
    mappingMethod.invoke(licenseExt, "ini", "SCRIPT_STYLE")
    mappingMethod.invoke(licenseExt, "yaml", "SCRIPT_STYLE")
    mappingMethod.invoke(licenseExt, "tf", "SCRIPT_STYLE")
    mappingMethod.invoke(licenseExt, "cfg", "SCRIPT_STYLE")
    mappingMethod.invoke(licenseExt, "cc", "JAVADOC_STYLE")
    mappingMethod.invoke(licenseExt, "scss", "JAVADOC_STYLE")

    val excludeMethod = cls.getMethod("exclude", String::class.java)
    listOf(
        "**/*.txt", "**/*.pem", "**/*.crt", "**/*.key", "**/*.csr",
        "**/*.log", "**/*.patch", "**/*.avsc", "**/*.versionsBackup",
        "**/*.pyc", "**/*.graffle", "**/*.hgrm", "**/*.md", "**/*.json",
        "**/proto/MLDataFormats.java",
        "**/proto/PulsarTransactionMetadata.java",
        "**/proto/SchemaRegistryFormat.java",
        "**/common/api/proto/*.java",
        "**/kinesis/fbs/*.java",
        "**/AbstractCASReferenceCounted.java",
        "**/ByteBufCodedInputStream.java",
        "**/ByteBufCodedOutputStream.java",
        "**/ahc.properties",
        "**/circe/**",
        "**/generated/**",
        "**/generated-lightproto/**",
        "**/generated-sources/**",
        "**/zk-3.5-test-data/*",
        "**/*_pb2.py",
        "**/*_pb2_grpc.py",
        "**/data/**",
        "**/logs/**",
        "**/.kotlin/**",
    ).forEach { excludeMethod.invoke(licenseExt, it) }
}

// Limit license check to only hand-written source files (exclude generated code).
// The license plugin's tasks extend SourceTask, so we can filter the source.
tasks.withType(org.gradle.api.tasks.SourceTask::class.java).matching {
    it.name.startsWith("license")
}.configureEach {
    // Only scan files under src/ directories, not build/generated/
    source = project.files(source).asFileTree.matching {
        exclude { it.file.path.contains("/build/") }
        exclude { it.file.path.contains("/generated-lightproto/") }
        exclude { it.file.path.contains("/generated-sources/") }
    }
}
