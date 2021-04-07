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
set -x
set -o pipefail
set -o errexit

MVN_TEST_COMMAND='build/retry.sh mvn -B -ntp test'

echo -n "Test Group : $TEST_GROUP"

# Test Groups  -- start --
function broker_group_1() {
  $MVN_TEST_COMMAND -pl pulsar-broker -Dgroups='broker'
}

function broker_group_2() {
  $MVN_TEST_COMMAND -pl pulsar-broker -Dgroups='schema,utils,functions-worker,broker-io,broker-discovery,broker-compaction,broker-naming'
}

function broker_client_api() {
  $MVN_TEST_COMMAND -pl pulsar-broker -Dgroups='broker-api'
}

function broker_client_impl() {
  $MVN_TEST_COMMAND -pl pulsar-broker -Dgroups='broker-impl'
}

function broker_flaky() {
  echo "::endgroup::"
  echo "::group::Running quarantined tests"
  mvn -B -ntp test -pl pulsar-broker -Dgroups='quarantine' -DexcludedGroups='' \
    -DtestForkCount=1 -DfailIfNoTests=false || \
      echo "::warning::There were test failures in the 'quarantine' test group."
  echo "::endgroup::"
  echo "::group::Running flaky tests"
  $MVN_TEST_COMMAND -pl pulsar-broker -Dgroups='flaky' -DtestForkCount=1
  echo "::endgroup::"
}

function proxy() {
  echo "::endgroup::"
  echo "::group::Running quarantined pulsar-proxy tests"
  mvn -B -ntp test -pl pulsar-proxy -Dgroups='quarantine' -DexcludedGroups='' \
    -DtestForkCount=1 -DfailIfNoTests=false || \
      echo "::warning::There were test failures in the 'quarantine' test group."
  echo "::endgroup::"
  echo "::group::Running pulsar-proxy tests"
  $MVN_TEST_COMMAND -pl pulsar-proxy
  echo "::endgroup::"
}

function other() {
  build/retry.sh mvn -B -ntp install -PbrokerSkipTest \
                                     -Dexclude='org/apache/pulsar/proxy/**/*.java,
                                                **/ManagedLedgerTest.java,
                                                **/TestPulsarKeyValueSchemaHandler.java,
                                                **/PrimitiveSchemaTest.java,
                                                BlobStoreManagedLedgerOffloaderTest.java'

  $MVN_TEST_COMMAND -pl managed-ledger -Dinclude='**/ManagedLedgerTest.java,
                                                  **/OffloadersCacheTest.java' \
                                       -DtestForkCount=1 \
                                       -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-sql/presto-pulsar-plugin -Dinclude='**/TestPulsarKeyValueSchemaHandler.java' \
                                                        -DtestForkCount=1

  $MVN_TEST_COMMAND -pl pulsar-client -Dinclude='**/PrimitiveSchemaTest.java' \
                                      -DtestForkCount=1

  $MVN_TEST_COMMAND -pl tiered-storage/jcloud -Dinclude='**/BlobStoreManagedLedgerOffloaderTest.java' \
                                              -DtestForkCount=1

  echo "::endgroup::"
  local modules_with_quarantined_tests=$(git grep -l '@Test.*"quarantine"' | grep '/src/test/java/' | \
    awk -F '/src/test/java/' '{ print $1 }' | egrep -v 'pulsar-broker|pulsar-proxy' | sort | uniq | \
    perl -0777 -p -e 's/\n(\S)/,$1/g')
  if [ -n "${modules_with_quarantined_tests}" ]; then
    echo "::group::Running quarantined tests outside of pulsar-broker & pulsar-proxy (if any)"
    mvn -B -ntp -pl "${modules_with_quarantined_tests}" test -Dgroups='quarantine' -DexcludedGroups='' \
      -DtestForkCount=1 -DfailIfNoTests=false || \
        echo "::warning::There were test failures in the 'quarantine' test group."
    echo "::endgroup::"
  fi
}

# Test Groups  -- end --

TEST_GROUP=$1

echo -n "Test Group : $TEST_GROUP"

case $TEST_GROUP in

  BROKER_GROUP_1)
    broker_group_1
    ;;

  BROKER_GROUP_2)
    broker_group_2
    ;;

  BROKER_CLIENT_API)
    broker_client_api
    ;;

  BROKER_CLIENT_IMPL)
    broker_client_impl
    ;;

  BROKER_FLAKY)
    broker_flaky
    ;;

  PROXY)
    proxy
    ;;

  OTHER)
    other
    ;;

  *)
    echo -n "INVALID TEST GROUP"
    exit 1
    ;;
esac
