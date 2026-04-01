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

// Convention plugin for publishing Pulsar modules to Maven repositories.
// Configures maven-publish, GPG signing, POM metadata, sources/javadoc JARs,
// and a local deploy repository for testing.

plugins {
    `maven-publish`
    signing
}

// --- java-library projects: JAR + sources + javadoc ---
pluginManager.withPlugin("java-library") {
    val sourceSets = the<SourceSetContainer>()

    // Match Maven's javadoc configuration: no doclint, don't fail on errors
    tasks.withType<Javadoc>().configureEach {
        (options as StandardJavadocDocletOptions).apply {
            addStringOption("Xdoclint:none", "-quiet")
        }
        isFailOnError = false
    }

    val sourcesJar by tasks.registering(Jar::class) {
        archiveClassifier.set("sources")
        from(sourceSets["main"].allJava)
    }

    val javadocJar by tasks.registering(Jar::class) {
        archiveClassifier.set("javadoc")
        from(tasks.named(JavaPlugin.JAVADOC_TASK_NAME))
    }

    // NAR modules disable the jar task and produce .nar files instead.
    // Detect the NAR plugin and configure the publication accordingly.
    val isNarModule = plugins.hasPlugin("io.github.merlimat.nar")

    if (isNarModule) {
        // NAR modules: publish the .nar artifact with packaging=nar
        publishing {
            publications {
                create<MavenPublication>("maven") {
                    artifact(tasks.named("nar"))
                    artifact(sourcesJar)
                    artifact(javadocJar)
                    // Set packaging to "nar" in POM
                    pom.packaging = "nar"
                }
            }
        }
    } else {
        // Standard java-library modules: publish from components["java"]
        publishing {
            publications {
                create<MavenPublication>("maven") {
                    from(components["java"])
                    artifact(sourcesJar)
                    artifact(javadocJar)

                    versionMapping {
                        usage(Usage.JAVA_RUNTIME) {
                            fromResolutionResult()
                        }
                        usage(Usage.JAVA_API) {
                            fromResolutionOf("runtimeClasspath")
                        }
                    }
                }
            }
        }
    }
}

// --- java-platform projects (BOM, dependencies): POM-only, no JAR ---
pluginManager.withPlugin("java-platform") {
    publishing {
        publications {
            create<MavenPublication>("maven") {
                from(components["javaPlatform"])
            }
        }
    }
}

// --- Common POM metadata for all publications ---
run {
    // Capture values in a local scope so withXml closures don't capture the script object
    // (which would break configuration cache serialization)
    val projectName = project.name
    val projectDescription = project.description
    val archivesNameValue = the<BasePluginExtension>().archivesName.get()
    val isPlatformProject = plugins.hasPlugin("java-platform")
    val localDeployRepoDir = rootProject.layout.buildDirectory.dir("local-deploy-repo")

    publishing {
        publications {
            withType<MavenPublication>().configureEach {
                artifactId = archivesNameValue

                pom {
                    name.set("Apache Pulsar :: $projectName")
                    description.set(projectDescription ?: "Apache Pulsar :: $projectName")
                    url.set("https://pulsar.apache.org")
                    inceptionYear.set("2017")

                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                            distribution.set("repo")
                        }
                    }

                    organization {
                        name.set("Apache Software Foundation")
                        url.set("https://www.apache.org/")
                    }

                    issueManagement {
                        system.set("GitHub Issues")
                        url.set("https://github.com/apache/pulsar/issues")
                    }

                    scm {
                        connection.set("scm:git:https://github.com/apache/pulsar.git")
                        developerConnection.set("scm:git:https://github.com/apache/pulsar.git")
                        url.set("https://github.com/apache/pulsar")
                        tag.set("HEAD")
                    }

                    mailingLists {
                        mailingList {
                            name.set("Apache Pulsar developers list")
                            subscribe.set("dev-subscribe@pulsar.apache.org")
                            unsubscribe.set("dev-unsubscribe@pulsar.apache.org")
                            post.set("dev@pulsar.apache.org")
                            archive.set("https://lists.apache.org/list.html?dev@pulsar.apache.org")
                        }
                    }

                    // Clean up POM XML: remove Maven defaults and dependencyManagement
                    // (resolved versions are inlined via versionMapping)
                    withXml {
                        val sb = asString()
                        var s = sb.toString()
                        // <scope>compile</scope> is the Maven default — remove for cleaner POM
                        s = s.replace("<scope>compile</scope>", "")
                        // Remove dependencyManagement from non-platform POMs
                        // (platform POMs need it — their dependencies ARE the management section)
                        if (!isPlatformProject) {
                            s = s.replace(
                                Regex(
                                    "<dependencyManagement>.*?</dependencyManagement>",
                                    RegexOption.DOT_MATCHES_ALL
                                ),
                                ""
                            )
                        }
                        sb.setLength(0)
                        sb.append(s)
                        // Re-format the XML
                        asNode()
                    }
                }
            }
        }

        // Local Maven repository for testing/comparison
        repositories {
            maven {
                name = "localDeploy"
                url = uri(localDeployRepoDir)
            }
        }
    }
}

// --- GPG signing ---
signing {
    isRequired = !version.toString().endsWith("-SNAPSHOT")

    val useGpgCmd = providers.gradleProperty("useGpgCmd").orNull?.toBoolean() ?: false
    if (useGpgCmd) {
        useGpgCmd()
    }

    sign(publishing.publications)
}

// Disable signing tasks when no key is configured (local dev without signing)
tasks.withType<Sign>().configureEach {
    enabled = providers.gradleProperty("signing.keyId").isPresent ||
        providers.gradleProperty("signing.gnupg.keyName").isPresent
}

// Suppress enforced-platform validation: all java-library modules use
// enforcedPlatform(":pulsar-dependencies") for internal version alignment,
// but this should not leak to consumers. The dependencyManagement section
// is stripped from published POMs via withXml above.
tasks.withType<GenerateModuleMetadata>().configureEach {
    suppressedValidationErrors.add("enforced-platform")
}
