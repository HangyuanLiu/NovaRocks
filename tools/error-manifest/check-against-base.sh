#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0.

set -euo pipefail

base_ref=${1:?usage: check-against-base.sh <base-ref>}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../.." && pwd)
merge_base=$(git -C "$repo_root" merge-base "$base_ref" HEAD)
ledger_path=tools/error-manifest/error-ledger-v1.tsv
baseline=$(mktemp)
trap 'rm -f "$baseline"' EXIT

if git -C "$repo_root" cat-file -e "$merge_base:$ledger_path" 2>/dev/null; then
  git -C "$repo_root" show "$merge_base:$ledger_path" > "$baseline"
else
  printf 'schema_version=0\n' > "$baseline"
fi

cargo run --quiet --manifest-path "$script_dir/Cargo.toml" -- --check-against "$baseline"
