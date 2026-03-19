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

# Bash script to automate the generation of the api package using Docker.
#
# It uses the .proto files included in Pulsar's source to compile the Go code
# capable of encoding/decoding the wire format used by Pulsar brokers.
#
# Requirements:
#  * Docker is installed and running
#  * The Pulsar project is checked out somewhere on the file system
#    in order to source the .proto files
#
# Tools:
#  * protobuf/protobuf-dev - installed via apk; provides protoc and well-known .proto includes
#  * protoc-gen-go     - installed via go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#  * protoc-gen-go-grpc - installed via go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# Note: the old plugins=grpc single-file mode was only in github.com/golang/protobuf/cmd/protoc-gen-go,
# which was removed in v1.4.0. Modern tooling generates gRPC service code into separate
# *_grpc.pb.go files alongside the message *.pb.go files.

echo "generate pulsar go function protobuf code..."

set -xeuo pipefail

pkg="api"
module="github.com/apache/pulsar/pulsar-function-go/pb"

defaultPulsarSrc="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." &>/dev/null && pwd)"

help="usage: ${0} <path to Pulsar repo (default \"${defaultPulsarSrc}\")>"

pulsarSrc="${1-${defaultPulsarSrc}}"
if [ ! -d "${pulsarSrc}" ]; then
  echo "error: Pulsar source is not a directory: ${pulsarSrc}"
  echo "${help}"
  exit 1
fi
protoDefinitions="${pulsarSrc}/pulsar-functions/proto/src/main/proto"
if [ ! -d "${protoDefinitions}" ]; then
  echo "error: Proto definitions directory not found: ${protoDefinitions}"
  echo "${help}"
  exit 1
fi

outDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Write the generation script to a temp file so variable expansion and
# quoting inside `docker run sh -c` stay simple.
genScript="$(mktemp)"
trap 'rm -f "${genScript}"' EXIT

cat >"${genScript}" <<EOF
#!/bin/sh
set -xeu

apk add --no-cache protobuf protobuf-dev >/dev/null 2>&1
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Build per-file -M mapping flags so each .proto resolves to our Go package.
# Required because the Pulsar proto files do not declare option go_package.
m_opts=""
for f in /proto/*.proto; do
    fname=\$(basename "\$f")
    m_opts="\${m_opts} --go_opt=M\${fname}=${module};${pkg} --go-grpc_opt=M\${fname}=${module};${pkg}"
done

# shellcheck disable=SC2086
protoc \\
    --go_out=/out \\
    --go_opt=paths=source_relative \\
    --go-grpc_out=/out \\
    --go-grpc_opt=paths=source_relative \\
    \${m_opts} \\
    --proto_path=/proto \\
    /proto/*.proto

# Prepend ASF license header to every generated file.
LICENSE_HEADER='// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.'

for f in /out/*.pb.go; do
    tmp="\${f}.tmp"
    printf '%s\n\n' "\${LICENSE_HEADER}" | cat - "\$f" > "\${tmp}" && mv "\${tmp}" "\$f"
done
EOF

chmod +x "${genScript}"

echo "Running protoc in Docker (golang:1.24-alpine)..."
docker run --rm \
  -v "${protoDefinitions}:/proto:ro" \
  -v "${outDir}:/out" \
  -v "${genScript}:/generate_pb.sh:ro" \
  golang:1.24-alpine \
  /generate_pb.sh

pulsarGitRev=$(git -C "${pulsarSrc}" rev-parse HEAD)
pulsarGitTag=$(git -C "${pulsarSrc}" describe --tags HEAD)

# Generate godoc describing this package and the git sha it was created from
cat <<EOF >"${outDir}/doc.go"
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package ${pkg} provides the protocol buffer messages that Pulsar
// uses for the client/broker wire protocol.
// See "Pulsar binary protocol specification" for more information.
// https://pulsar.apache.org/docs/developing-binary-protocol/
//
// The protocol definition files are part of the main Pulsar source,
// located within the Pulsar repository at:
// https://github.com/apache/pulsar/tree/master/pulsar-functions/proto/src/main/proto
//
// The generated Go code was created from the source Pulsar files at git:
//    tag:      ${pulsarGitTag}
//    revision: ${pulsarGitRev}
//
// Files generated by the protoc-gen-go program should not be modified.
package ${pkg}
EOF

echo "Done. Generated files are in: ${outDir}"
