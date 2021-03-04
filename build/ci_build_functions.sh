# shell function library for CI builds
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

function install_tool() {
  local tool_executable=$1
  local tool_package=${2:-$1}
  if ! command -v $tool_executable &>/dev/null; then
    echo "::group::Installing ${tool_package}"
    sudo apt-get -y install ${tool_package} >/dev/null
    echo '::endgroup::'
  fi
}

function fail() {
  echo $1 >&2
  exit 1
}

function retry() {
  local n=1
  local max=3
  local delay=10
  while true; do
    "$@" && break || {
      if [[ $n -lt $max ]]; then
        ((n++))
        echo "::warning::Command failed. Attempt $n/$max:"
        sleep $delay
      else
        fail "::error::The command has failed after $n attempts."
      fi
    }
  done
}

function docker_save_image_to_github_actions_cache() {
  local image=$1
  local cachekey=$2
  install_tool pv
  echo "::group::Saving docker image ${image} with key ${cachekey} in GitHub Actions Cache"
  retry bash -c "docker save ${image} | zstd | pv -ft -i 5 | pv -Wbaf -i 5 | curl -s -H 'Content-Type: application/octet-stream' -X PUT --data-binary @- http://localhost:12321/${cachekey}"
  echo "::endgroup::"
}

function docker_load_image_from_github_actions_cache() {
  local cachekey=$1
  install_tool pv
  echo "::group::Loading docker image from key ${cachekey} in GitHub Actions Cache"
  retry bash -c "curl -s http://localhost:12321/${cachekey} | pv -batf -i 5 | unzstd | docker load"
  echo "::endgroup::"
}

function restore_tar_from_github_actions_cache() {
  local cachekey=$1
  install_tool pv
  echo "::group::Restoring tar from key ${cachekey} to $PWD"
  retry bash -c "curl -s http://localhost:12321/${cachekey} | pv -batf -i 5 | tar -I zstd -xf -"
  echo "::endgroup::"
}

function store_to_github_actions_cache() {
  local cachekey=$1
  shift
  install_tool pv
  echo "::group::Storing $1 command output to key ${cachekey}"
  retry bash -c '"$@" | pv -ft -i 5 | pv -Wbaf -i 5 | curl -s -H "Content-Type: application/octet-stream" \
            -X PUT --data-binary @- \
            http://localhost:12321/'${cachekey} bash "$@"
  echo "::endgroup::"
}

# copies test reports into test-reports and surefire-reports directory
# subsequent runs of tests might overwrite previous reports. This ensures that all test runs get reported.
function move_test_reports() {
  (
    if [ -n "${GITHUB_WORKSPACE}" ]; then
      cd "${GITHUB_WORKSPACE}"
      mkdir -p test-reports
      mkdir -p surefire-reports
    fi
    # aggregate all junit xml reports in a single directory
    if [ -d test-reports ]; then
      # copy test reports to single directory, rename duplicates
      find -path '*/target/surefire-reports/junitreports/TEST-*.xml' | xargs -r -n 1 mv -t test-reports --backup=numbered
      # rename possible duplicates to have ".xml" extension
      (
        for f in test-reports/*~; do
          mv -- "$f" "${f}.xml"
        done 2>/dev/null
      ) || true
    fi
    # aggregate all surefire-reports in a single directory
    if [ -d surefire-reports ]; then
      (
        find . -type d -path '*/target/surefire-reports' -not -path './surefire-reports/*' |
          while IFS=$'\n' read -r directory; do
            echo "Copying reports from $directory"
            target_dir="surefire-reports/${directory}"
            if [ -d "$target_dir" ]; then
              # rotate backup directory names *~3 -> *~2, *~2 -> *~3, *~1 -> *~2, ...
              ( command ls -vr1d "${target_dir}~"* 2> /dev/null | awk '{print "mv "$0" "substr($0,0,length-1)substr($0,length,1)+1}' | sh ) || true
              # backup existing target directory, these are the results of the previous test run
              mv "$target_dir" "${target_dir}~1"
            fi
            # copy files
            cp -R --parents "$directory" surefire-reports
            # remove the original directory
            rm -rf "$directory"
          done
      )
    fi
  )
}
