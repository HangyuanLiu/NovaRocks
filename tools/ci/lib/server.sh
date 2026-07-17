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


CI_SERVER_PID=""

ci_novarocks_binary_path() {
  local cargo_profile="${1:-dev-opt}"

  case "$cargo_profile" in
    dev)
      echo "target/debug/novarocks"
      ;;
    release)
      echo "target/release/novarocks"
      ;;
    *)
      echo "target/$cargo_profile/novarocks"
      ;;
  esac
}

ci_start_standalone_server() {
  local config_path="$1"
  local log_path="$2"
  local timeout_seconds="$3"
  local cargo_profile="${4:-dev-opt}"
  local binary_path
  local i

  binary_path="$(ci_novarocks_binary_path "$cargo_profile")"

  {
    printf "+ NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD=1 NO_PROXY=127.0.0.1,localhost %q standalone --config %q\n" \
      "$binary_path" \
      "$config_path"
    NOVAROCKS_ENABLE_TEST_IMV_STATELESS_REBUILD=1 \
    NO_PROXY=127.0.0.1,localhost \
      "$binary_path" standalone \
        --config "$config_path"
  } >"$log_path" 2>&1 &
  CI_SERVER_PID=$!

  i=0
  while [ "$i" -lt "$timeout_seconds" ]; do
    if grep -q '^NOVAROCKS_READY mysql_port=' "$log_path" 2>/dev/null; then
      return 0
    fi

    if ! kill -0 "$CI_SERVER_PID" 2>/dev/null; then
      wait "$CI_SERVER_PID" 2>/dev/null || true
      CI_SERVER_PID=""
      return 1
    fi

    sleep 1
    i=$((i + 1))
  done

  if kill -0 "$CI_SERVER_PID" 2>/dev/null; then
    kill "$CI_SERVER_PID" 2>/dev/null || true
    wait "$CI_SERVER_PID" 2>/dev/null || true
  fi
  CI_SERVER_PID=""
  return 2
}

ci_stop_standalone_server() {
  if [ -n "$CI_SERVER_PID" ] && kill -0 "$CI_SERVER_PID" 2>/dev/null; then
    kill "$CI_SERVER_PID" 2>/dev/null || true
    wait "$CI_SERVER_PID" 2>/dev/null || true
  fi
  CI_SERVER_PID=""
}
