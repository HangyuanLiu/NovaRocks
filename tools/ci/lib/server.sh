#!/usr/bin/env bash

CI_SERVER_PID=""

ci_start_standalone_server() {
  local config_path="$1"
  local log_path="$2"
  local timeout_seconds="$3"
  local cargo_profile="${4:-dev-opt}"
  local binary_dir
  local binary_path
  local i

  case "$cargo_profile" in
    dev)
      binary_dir="target/debug"
      ;;
    release)
      binary_dir="target/release"
      ;;
    *)
      binary_dir="target/$cargo_profile"
      ;;
  esac
  binary_path="$binary_dir/novarocks"

  {
    printf "+ NO_PROXY=127.0.0.1,localhost %q standalone-server --config %q\n" \
      "$binary_path" \
      "$config_path"
    NO_PROXY=127.0.0.1,localhost \
      "$binary_path" standalone-server \
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
