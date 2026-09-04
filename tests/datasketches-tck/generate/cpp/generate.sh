#!/usr/bin/env bash
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

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 OUTPUT_DIR" >&2
  exit 2
fi

readonly TCK_REVISION=c0a180708c6e6433e4cba7fba091713eb8af3eaa
readonly CPP_REVISION=fe0261aa043c1d3af9a92a62fa286caabbf6fa84
script_dir=$(cd "$(dirname "$0")" && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/novarocks-datasketches-cpp.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

git init --quiet "$work_dir/tck"
git -C "$work_dir/tck" remote add origin https://github.com/apache/datasketches-tck.git
git -C "$work_dir/tck" fetch --quiet --depth 1 origin "$TCK_REVISION"
git -C "$work_dir/tck" checkout --quiet --detach FETCH_HEAD
test "$(git -C "$work_dir/tck" rev-parse HEAD)" = "$TCK_REVISION"
test "$(sed -n '/\[snapshot.cpp\]/,/^$/s/^commit = "\([0-9a-f]*\)"/\1/p' "$work_dir/tck/config.toml")" = "$CPP_REVISION"

mkdir -p "$1/theta" "$1/hll"
while IFS= read -r name; do
  [[ -z "$name" || "$name" == \#* ]] && continue
  cp "$work_dir/tck/serialization/cpp/snapshots/$name" "$1/theta/$name"
done < "$script_dir/theta-files.txt"
while IFS= read -r name; do
  [[ -z "$name" || "$name" == \#* ]] && continue
  cp "$work_dir/tck/serialization/cpp/snapshots/$name" "$1/hll/$name"
done < "$script_dir/hll-files.txt"
