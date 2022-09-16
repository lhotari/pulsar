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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

set -e
set -o pipefail
set -o errexit

# input is https://whimsy.apache.org/roster/committee/pulsar.json which is only available to committers
# store it to ~/Downloads/pulsar.json and run this script
pulsar_roster="${1:-"$HOME/Downloads/pulsar.json"}"
if [[ ! -f "${pulsar_roster}" ]]; then
  echo &2> "The pulsar.json roster file wasn't found at ${pulsar_roster}. The expected input file is https://whimsy.apache.org/roster/committee/pulsar.json which is only available to committers. Please download it and save it as ${pulsar_roster}."
fi
pulsarci_yaml="${SCRIPT_DIR}/../.pulsarci.yaml"
yq eval-all -i -P 'select(fileIndex==0).committers = select(fileIndex==1).committers | select(fileIndex==0)' "$pulsarci_yaml" <(cat "$pulsar_roster" | jq '{"committers": [.roster[] | .githubUsername | gsub("^\\s+|\\s+$";"") | split(", ") | flatten[] | del(select(. == "")) | select(. != null)] | sort_by(.|ascii_downcase)}')


