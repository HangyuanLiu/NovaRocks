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


# Design: ADR-0181 (docs/adr/ADR-0181-unified-development-capabilities-and-executable-provenance.md)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/lib/build_capabilities.sh"
cd "$REPO_ROOT"
ci_validate_build_environment
ci_set_default_build_jobs
action="${1:-test}"
[ "$#" -eq 0 ] || shift
case "$action" in
  prepare)
    [ "$#" -eq 1 ] || { echo "usage: dev-test.sh prepare <artifact-receipt>" >&2; exit 2; }
    ci_prepare_main "$1" ;;
  test|list)
    # Keep metadata as generated target-local cache. Native nextest remains the
    # foreground process after preparation, preserving its TTY and job control.
    prepared="$(ci_target_directory main)/nextest-prepared/$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
    ci_prepare_nextest "$prepared" "$@"
    if [ "$action" = list ]; then
      cat "$prepared/selected-tests.stdout"
    else
      export NOVAROCKS_BIN="$(ci_bound_binary novarocks "$prepared/artifacts.json")"
      export NOVAROCKS_WORKSPACE_ROOT="$REPO_ROOT"
      exec python3 "$SCRIPT_DIR/test-run.py" exec-argv --output "$prepared"
    fi ;;
  doc) ci_main_cargo test --doc "$@" ;;
  check)
    # IDE checks must explicitly pass their effective private target directory.
    if [ "${1:-}" = --target-dir ]; then
      [ "$#" -ge 2 ] || exit 2
      ide_target="$2"; shift 2
      cargo check --workspace --all-targets --profile dev --locked --target-dir "$ide_target" "$@"
    else
      ci_main_cargo check --all-targets "$@"
    fi ;;
  *) echo "usage: dev-test.sh {prepare|test|list|doc|check} [native execution arguments]" >&2; exit 2 ;;
esac
