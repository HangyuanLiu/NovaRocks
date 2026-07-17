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

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
profile=""
output_dir=""

usage() {
  echo "Usage: $0 --profile <profile> --output-dir <directory>" >&2
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --profile)
      [ "$#" -ge 2 ] || { usage; exit 2; }
      profile="$2"
      shift 2
      ;;
    --output-dir)
      [ "$#" -ge 2 ] || { usage; exit 2; }
      output_dir="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [ -z "$profile" ] || [ -z "$output_dir" ]; then
  usage
  exit 2
fi

mkdir -p "$output_dir"
output_dir="$(cd "$output_dir" && pwd -P)"
target_dir="$output_dir/target"
build_command=(cargo build --profile "$profile" --features compat --bin novarocks)

if [ -n "${SCT_COMPAT_BUILD_HOOK:-}" ]; then
  (
    cd "$REPO_ROOT"
    CARGO_TARGET_DIR="$target_dir" "$SCT_COMPAT_BUILD_HOOK" "${build_command[@]}"
  )
else
  (
    cd "$REPO_ROOT"
    CARGO_TARGET_DIR="$target_dir" \
      cargo build --profile "$profile" --features compat --bin novarocks
  )
fi

case "$profile" in
  dev) artifact_profile=debug ;;
  *) artifact_profile="$profile" ;;
esac
built_binary="$target_dir/$artifact_profile/novarocks"
if [ ! -x "$built_binary" ]; then
  echo "Compat build did not produce an executable: $built_binary" >&2
  exit 1
fi

mkdir -p "$output_dir/bin"
compat_binary="$output_dir/bin/novarocks-compat"
cp "$built_binary" "$compat_binary"
chmod +x "$compat_binary"

sha256="$(shasum -a 256 "$compat_binary" | awk '{print $1}')"
git_head="$(git -C "$REPO_ROOT" rev-parse HEAD)"
manifest_tmp="$(mktemp "$output_dir/manifest.txt.XXXXXX")"
trap 'rm -f "$manifest_tmp"' EXIT
{
  echo "format=novarocks-compat-artifact-v1"
  echo "binary=$compat_binary"
  echo "sha256=$sha256"
  echo "git_head=$git_head"
  echo "profile=$profile"
  echo "features=compat"
} >"$manifest_tmp"
mv "$manifest_tmp" "$output_dir/manifest.txt"
trap - EXIT
