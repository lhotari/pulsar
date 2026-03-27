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

// ── Checkstyle ──────────────────────────────────────────────────────────────
subprojects {
    apply(plugin = "checkstyle")

    configure<CheckstyleExtension> {
        toolVersion = "10.14.2"
        configFile = rootProject.file("buildtools/src/main/resources/pulsar/checkstyle.xml")
        configProperties["checkstyle.suppressions.file"] =
            rootProject.file("buildtools/src/main/resources/pulsar/suppressions.xml").absolutePath
    }

    tasks.withType<Checkstyle> {
        // Broker module has very large files that need more heap
        maxHeapSize.set("1g")
        // Exclude generated source files (proto, lightproto, etc.)
        exclude { it.file.path.contains("/build/") }
        exclude { it.file.path.contains("/generated-lightproto/") }
        exclude { it.file.path.contains("/generated-sources/") }
        // Match Maven exclusion: **/proto/*
        exclude("**/proto/*")
    }
}

// ── Apache RAT (Release Audit Tool) ─────────────────────────────────────────
// The RAT plugin is applied in the root build.gradle.kts; configure the task here.
tasks.named("rat").configure {
    // Use reflection since type-safe accessors aren't available in applied scripts
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
        // Gradle files
        ".gradle/**",
        "gradle/wrapper/**",
        "**/.gradle/**",
        "**/gradle/wrapper/**",
        "gradlew",
        "gradlew.bat",
        "gradle/libs.versions.toml",
    ))
}

// ── License header check (Mycila/hierynomus) ────────────────────────────────
subprojects {
    apply(plugin = "com.github.hierynomus.license")

    // The hierynomus license plugin calls Task.project at execution time,
    // which is incompatible with Gradle's configuration cache.
    tasks.matching { it.name.startsWith("license") }.configureEach {
        notCompatibleWithConfigurationCache("license plugin uses Task.project at execution time")
    }

    afterEvaluate {
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

        // Limit license check to only hand-written source files (exclude generated code).
        // The license plugin's tasks extend SourceTask, so we can filter the source.
        project.tasks.withType(org.gradle.api.tasks.SourceTask::class.java).matching {
            it.name.startsWith("license")
        }.configureEach {
            // Only scan files under src/ directories, not build/generated/
            source = project.files(source).asFileTree.matching {
                exclude { it.file.path.contains("/build/") }
                exclude { it.file.path.contains("/generated-lightproto/") }
                exclude { it.file.path.contains("/generated-sources/") }
            }
        }

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
        ).forEach { excludeMethod.invoke(licenseExt, it) }
    }
}
