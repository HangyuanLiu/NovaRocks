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

# Docker-name-CAS lease helpers.  This file is intentionally sourceable: the
# bootstrap owns policy while this module only handles exact-container fencing.

fixture_lease_now() { date +%s; }

fixture_lease_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "$1" | sha256sum | awk '{print $1}'
  else
    printf '%s' "$1" | shasum -a 256 | awk '{print $1}'
  fi
}

fixture_lease_docker() { "${BENCHMARK_FIXTURE_DOCKER_BIN:-docker}" "$@"; }

fixture_lease_name() {
  local compose_project="$1" dataset_key="$2"
  printf 'nr-benchmark-%s' "$(fixture_lease_sha256 "${compose_project}|${dataset_key}" | cut -c1-24)"
}

fixture_lease_inspect() {
  local id_or_name="$1"
  fixture_lease_docker container inspect --format '{{.Id}} {{index .Config.Labels "novarocks.fixture.dataset-key"}} {{index .Config.Labels "novarocks.fixture.owner"}} {{index .Config.Labels "novarocks.fixture.staging"}}' "$id_or_name"
}

fixture_lease_heartbeat() {
  local exact_id="$1" epoch
  epoch="$(fixture_lease_now)"
  fixture_lease_docker container exec "$exact_id" /bin/sh -ceu "printf '%s\\n' '$epoch' > /tmp/novarocks-fixture-heartbeat" >/dev/null
  printf '%s\n' "$epoch"
}

fixture_lease_heartbeat_epoch() {
  fixture_lease_docker container exec "$1" /bin/sh -ceu 'cat /tmp/novarocks-fixture-heartbeat' 2>/dev/null
}

fixture_lease_matches() {
  local exact_id="$1" expected_key="$2" expected_owner="$3" expected_staging="$4" observed
  observed="$(fixture_lease_inspect "$exact_id" 2>/dev/null)" || return 1
  local id key owner staging
  read -r id key owner staging <<<"$observed"
  [[ "$id" == "$exact_id" && "$key" == "$expected_key" && "$owner" == "$expected_owner" && "$staging" == "$expected_staging" ]]
}

fixture_lease_acquire() {
  # stdout is exactly the immutable container id.  A 409/name conflict exits 75.
  local lease_name="$1" dataset_key="$2" owner="$3" staging="$4" image="$5"
  [[ "$image" == *@sha256:* ]] || { echo 'lease image must be digest-pinned' >&2; return 64; }
  # Docker container create implicitly pulls a missing image and can block for
  # minutes without exposing a useful owner/lease state.  A benchmark fixture
  # must fail closed before it claims the writer election when the local image
  # is unavailable; callers can explicitly provision the pinned image first.
  fixture_lease_docker image inspect "$image" >/dev/null 2>&1 || return 1
  local id
  if ! id="$(fixture_lease_docker container create --name "$lease_name" \
      --label "novarocks.fixture.dataset-key=$dataset_key" \
      --label "novarocks.fixture.owner=$owner" \
      --label "novarocks.fixture.staging=$staging" \
      "$image" /bin/sh -ceu 'trap : TERM INT; while :; do sleep 3600; done' 2>/dev/null)"; then
    # A 409 name conflict is a normal concurrent-writer condition.  An image
    # pull/create failure is not: treating both as a conflict makes callers
    # wait for the full lease timeout even though no writer owns the fixture.
    if fixture_lease_inspect "$lease_name" >/dev/null 2>&1; then
      return 75
    fi
    return 1
  fi
  if ! fixture_lease_docker container start "$id" >/dev/null; then
    fixture_lease_docker container rm -f "$id" >/dev/null 2>&1 || true
    return 1
  fi
  fixture_lease_heartbeat "$id" >/dev/null || {
    fixture_lease_docker container rm -f "$id" >/dev/null 2>&1 || true
    return 1
  }
  printf '%s\n' "$id"
}

fixture_lease_release() {
  # Never remove by lease name.  A replacement can use that name after stale takeover.
  fixture_lease_docker container rm -f "$1" >/dev/null 2>&1 || true
}

fixture_lease_takeover_stale() {
  local exact_old_id="$1" expected_key="$2" expiry_seconds="$3" now heartbeat rechecked observed id key
  # A stale owner's owner/staging labels are intentionally unknown to a waiter;
  # only its immutable exact id and dataset key authorize the takeover attempt.
  observed="$(fixture_lease_inspect "$exact_old_id" 2>/dev/null)" || return 1
  read -r id key _ <<<"$observed"
  [[ "$id" == "$exact_old_id" && "$key" == "$expected_key" ]] || return 1
  heartbeat="$(fixture_lease_heartbeat_epoch "$exact_old_id")" || return 1
  [[ "$heartbeat" =~ ^[0-9]+$ ]] || return 1
  now="$(fixture_lease_now)"
  (( now - heartbeat > expiry_seconds )) || return 1
  # Re-read and re-evaluate the exact id immediately before removal. Do not
  # resolve the name again: a live owner that refreshed in this interval must
  # never be deleted by a stale waiter.
  rechecked="$(fixture_lease_heartbeat_epoch "$exact_old_id")" || return 1
  [[ "$rechecked" =~ ^[0-9]+$ ]] || return 1
  now="$(fixture_lease_now)"
  (( now - rechecked > expiry_seconds )) || return 1
  fixture_lease_docker container rm -f "$exact_old_id" >/dev/null
}
