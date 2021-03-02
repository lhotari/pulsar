#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e
set -o pipefail
set -o errexit

MVN_TEST_OPTIONS='mvn -B -ntp -Dassembly.skipAssembly=true -DskipSourceReleaseAssembly=true -DskipBuildDistribution=true -Dspotbugs.skip=true -Dlicense.skip=true -DnifiNarPhase=none'
MVN_SKIP_SHADE='-DshadePluginPhase=none'

function mvn_test() {
  $MVN_TEST_OPTIONS test "$@" "${COMMANDLINE_ARGS[@]}"
}

function mvn_verify() {
  $MVN_TEST_OPTIONS $MVN_SKIP_SHADE verify "$@" "${COMMANDLINE_ARGS[@]}"
}

function mvn_install() {
  $MVN_TEST_OPTIONS -Dmaven.test.skip=true install "$@" "${COMMANDLINE_ARGS[@]}"
}

function mvn_install_and_test() {
  $MVN_TEST_OPTIONS install "$@" "${COMMANDLINE_ARGS[@]}"
}

# Test Groups  -- start --
function test_group_broker_group_1() {
  mvn_test -pl pulsar-broker -Dinclude="**/AdminApiOffloadTest.java" \
                             -DtestForkCount=1 \
                             -DtestReuseFork=true

  mvn_test -pl pulsar-broker -Dinclude="org/apache/pulsar/broker/**/*.java" \
                    -Dexclude="org/apache/pulsar/broker/zookeeper/**/*.java,
                               org/apache/pulsar/broker/loadbalance/**/*.java,
                               org/apache/pulsar/broker/service/**/*.java,
                               **/AdminApiOffloadTest.java"
}

function test_group_broker_group_2() {
  mvn_test -pl pulsar-broker -Dinclude="**/MessagePublishBufferThrottleTest.java" \
                             -DtestForkCount=1 \
                             -DtestReuseFork=true

  mvn_test -pl pulsar-broker -Dinclude="**/ReplicatorTest.java" \
                             -DtestForkCount=1 \
                             -DtestReuseFork=true

  mvn_test -pl pulsar-broker -Dinclude="**/TopicOwnerTest.java" \
                             -DtestForkCount=1 \
                             -DtestReuseFork=true

  mvn_test -pl pulsar-broker -Dinclude="**/AntiAffinityNamespaceGroupTest.java" \
                             -DtestForkCount=1 \
                             -DtestReuseFork=true

  mvn_test -pl pulsar-broker -Dinclude="**/*StreamingDispatcher*Test.java" \
                             -DtestForkCount=1 \
                             -DtestReuseFork=true

  mvn_test -pl pulsar-broker -Dinclude="org/apache/pulsar/broker/zookeeper/**/*.java,
                                        org/apache/pulsar/broker/loadbalance/**/*.java,
                                        org/apache/pulsar/broker/service/**/*.java" \
                             -Dexclude="**/ReplicatorTest.java,
                                        **/MessagePublishBufferThrottleTest.java,
                                        **/TopicOwnerTest.java,
                                        **/*StreamingDispatcher*Test.java,
                                        **/AntiAffinityNamespaceGroupTest.java"
}

function test_group_broker_client_api() {
  mvn_test -pl pulsar-broker -Dinclude="**/DispatcherBlockConsumerTest.java" \
                             -DtestForkCount=1 \
                             -DtestReuseFork=true

  mvn_test -pl pulsar-broker -Dinclude="**/SimpleProducerConsumerTest.java" \
                             -DtestForkCount=1 \
                             -DtestReuseFork=true

  mvn_test -pl pulsar-broker -Dinclude="org/apache/pulsar/client/api/**/*.java" \
                             -Dexclude="**/DispatcherBlockConsumerTest.java,
                                        **/SimpleProducerConsumerTest.java"
}

function test_group_broker_client_impl() {
  mvn_test -pl pulsar-broker -Dinclude="org/apache/pulsar/client/impl/**/*.java"
}

function test_group_broker_client_other() {
  mvn_test -pl pulsar-broker -Dexclude="org/apache/pulsar/broker/**/*.java,
                                        org/apache/pulsar/client/**/*.java"
}

function test_group_proxy() {
  mvn_test -pl pulsar-proxy -DtestForkCount=1 \
                            -DtestReuseFork=true \
                            -Dinclude="**/ProxyRolesEnforcementTest.java"

  mvn_test -pl pulsar-proxy -DtestForkCount=1 \
                            -DtestReuseFork=true \
                            -Dinclude="**/ProxyAuthenticationTest.java"

  mvn_test -pl pulsar-proxy -DtestForkCount=1 \
                            -DtestReuseFork=true \
                            -Dinclude="**/ProxyTest.java"

  mvn_test -pl pulsar-proxy -DtestForkCount=1 \
                            -DtestReuseFork=true \
                            -Dinclude="**/MessagePublishBufferThrottleTest.java"

  mvn_test -pl pulsar-proxy -DtestForkCount=1 \
                            -DtestReuseFork=true \
                            -Dexclude="**/ProxyRolesEnforcementTest.java,
                                       **/ProxyAuthenticationTest.java,
                                       **/ProxyTest.java,
                                       **/MessagePublishBufferThrottleTest.java"

}

function test_group_other() {
  # shaded kafka-connect-avro-converter-shaded is required test and install it first
  mvn_install_and_test -pl org.apache.pulsar:kafka-connect-avro-converter-shaded

  # skip projects that are covered by other test runs
  local skipProjects='!pulsar-proxy,!pulsar-broker,!org.apache.pulsar:pulsar-io-flume'
  # skip distribution modules
  local skipDistributionModules='!org.apache.pulsar:distribution,!org.apache.pulsar:pulsar-server-distribution,
  !org.apache.pulsar:pulsar-offloader-distribution,!org.apache.pulsar:pulsar-io-distribution,
  !org.apache.pulsar:pulsar-presto-distribution,!org.apache.pulsar:pulsar-io-docs'
  # skip docker build modules
  local skipDockerBuildModules='!org.apache.pulsar:docker-images,!org.apache.pulsar:grafana-docker-image,
  !org.apache.pulsar:pulsar-docker-image, !org.apache.pulsar:dashboard-docker-image,
  !org.apache.pulsar:pulsar-all-docker-image,!org.apache.pulsar:pulsar-standalone-docker-image'
  # skip modules referencing shaded classes which don't include unit tests or are covered by other test runs
  local skipModuleReferencingShadedClasses='!org.apache.pulsar:pulsar-client-1x,!org.apache.pulsar:kafka-connect-avro-converter-shaded,!org.apache.pulsar:tiered-storage-jcloud'
  # skip prebuilt modules
  local skipPrebuiltModules='!org.apache.pulsar:pulsar-functions-api-examples,!org.apache.pulsar:pulsar-io-twitter,!org.apache.pulsar:pulsar-io-cassandra'
  local skipModules="${skipProjects},${skipDistributionModules},${skipDockerBuildModules},${skipModuleReferencingShadedClasses},${skipPrebuiltModules}"

  mvn_test -pl "$skipModules" \
             -Dexclude="**/ManagedLedgerTest.java,
                        **/PrimitiveSchemaTest.java"

  mvn_test -pl managed-ledger -Dinclude="**/ManagedLedgerTest.java" \
                              -DtestForkCount=1 \
                              -DtestReuseFork=true

  mvn_test -pl pulsar-client -Dinclude="**/PrimitiveSchemaTest.java" \
                             -DtestForkCount=1 \
                             -DtestReuseFork=true

  # run unit tests for tiered-storage-jcloud which requires shaded dependencies
  mvn_install -pl jclouds-shaded
  mvn_test -pl org.apache.pulsar:tiered-storage-jcloud -DtestForkCount=1 \
                                                       -DtestReuseFork=true

  # run pulsar-io-flume tests separately
  mvn_test -pl org.apache.pulsar:pulsar-io-flume -DtestForkCount=1 \
                                                 -DtestReuseFork=true
}

# Test Groups  -- end --

TEST_GROUP=$1
if [ -z "$TEST_GROUP" ]; then
  echo "usage: $0 [test_group]"
  exit 1
fi
shift
COMMANDLINE_ARGS=("$@")
echo "Test Group : $TEST_GROUP"
test_group_function_name="test_group_$(echo "$TEST_GROUP"| tr '[:upper:]' '[:lower:]')"
if [[ "$(LC_ALL=C type -t $test_group_function_name)" == "function" ]]; then
  set -x
  eval "$test_group_function_name"
else
  echo "INVALID TEST GROUP"
  exit 1
fi