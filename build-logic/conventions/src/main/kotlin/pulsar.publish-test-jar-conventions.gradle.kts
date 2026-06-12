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

// Convention plugin for modules that publish their test classes as a "tests"-classifier
// artifact (the equivalent of the Maven test-jar) in addition to the main artifacts.
// Apply only to modules whose tests contain reusable test infrastructure (mocks, base
// classes, test utilities) that downstream projects build on — not to modules whose test
// sources are only test cases. The testJar task itself is registered for every module by
// pulsar.java-conventions for cross-module test dependencies.

plugins {
    id("pulsar.publish-conventions")
}

publishing {
    publications {
        withType<MavenPublication>().configureEach {
            artifact(tasks.named("testJar"))
        }
    }
}
