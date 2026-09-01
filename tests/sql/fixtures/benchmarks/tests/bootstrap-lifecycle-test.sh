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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
BOOTSTRAP="$ROOT/tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh"
LEASE="$ROOT/tests/sql/fixtures/benchmarks/benchmark_fixture_lease.sh"
PUBLICATION="$ROOT/tests/sql/fixtures/benchmarks/benchmark_fixture_publication.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
[[ "$(basename "$ROOT")" == NovaRocks ]] || fail "incorrect workspace root: $ROOT"
bash -n "$BOOTSTRAP"; bash -n "$LEASE"; bash -n "$PUBLICATION"

cat > "$TMP/fake-docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
state="${FAKE_DOCKER_STATE:?}"; mkdir -p "$state"
cmd="$1"; shift
[[ "$cmd" == container ]] || exit 64
sub="$1"; shift
case "$sub" in
  create)
    name=""; key=""; owner=""; staging=""
    while (($#)); do
      case "$1" in
        --name) name="$2"; shift 2;;
        --label) case "$2" in novarocks.fixture.dataset-key=*) key="${2#*=}";; novarocks.fixture.owner=*) owner="${2#*=}";; novarocks.fixture.staging=*) staging="${2#*=}";; esac; shift 2;;
        *) break;;
      esac
    done
    [[ -n "$name" && ! -e "$state/name-$name" ]] || exit 1
    id="id-$(date +%s%N)-$RANDOM"; printf '%s\n%s\n%s\n%s\n' "$id" "$key" "$owner" "$staging" > "$state/$id"
    ln -s "$id" "$state/name-$name"; printf '%s\n' "$id";;
  start) :;;
  inspect)
    [[ "$1" == --format ]]; shift 2
    target="$1"; [[ -e "$state/name-$target" ]] && target="$(readlink "$state/name-$target")"
    [[ -f "$state/$target" ]] || exit 1
    tr '\n' ' ' < "$state/$target"; printf '\n';;
  exec)
    id="$1"; shift; [[ -f "$state/$id" ]] || exit 1
    if [[ "$*" == *'cat /tmp/novarocks-fixture-heartbeat'* ]]; then cat "$state/$id.heartbeat"; else date +%s > "$state/$id.heartbeat"; fi;;
  rm)
    [[ "$1" == -f ]] && shift; id="$1"; [[ -f "$state/$id" ]] || exit 1
    find "$state" -type l -lname "$id" -delete; rm -f "$state/$id" "$state/$id.heartbeat";;
esac
EOF
chmod +x "$TMP/fake-docker"

export BENCHMARK_FIXTURE_STORAGE_DIR="$TMP/storage"
export BENCHMARK_FIXTURE_DOCKER_BIN="$TMP/fake-docker"
export FAKE_DOCKER_STATE="$TMP/docker"
export AWS_S3_ENDPOINT="http://unused"
export AWS_S3_ACCESS_KEY_ID="test-key"
export AWS_S3_SECRET_ACCESS_KEY="test-secret"

# Conditional READY races: a second absent writer loses, and a rebuild needs
# the exact ETag it observed.  No helper ever exposes credentials in argv.
source "$PUBLICATION"
printf '{"winner":1}\n' > "$TMP/ready-one"
uri='s3://fixture/bench/READY.json'
[[ "$(fixture_publication_put_conditional "$uri" "$TMP/ready-one" absent)" == 200 ]] || fail 'first absent publication'
[[ "$(fixture_publication_put_conditional "$uri" "$TMP/ready-one" absent)" == 412 ]] || fail 'second absent publication'
fixture_publication_get "$uri"
etag="$FIXTURE_PUBLICATION_ETAG"
printf '{"winner":2}\n' > "$TMP/ready-two"
[[ "$(fixture_publication_put_conditional "$uri" "$TMP/ready-two" rebuild bad-etag)" == 412 ]] || fail 'stale rebuild ETag'
[[ "$(fixture_publication_put_conditional "$uri" "$TMP/ready-two" rebuild "$etag")" == 200 ]] || fail 'exact rebuild ETag'

# Docker name creation is the CAS.  A holder can be checked/released only by
# immutable ID; create after a conflict cannot delete a replacement by name.
source "$LEASE"
key='{"fixture_contract_id":"abc","scale":"1","suite":"ssb"}'
name="$(fixture_lease_name project-a "$key")"
id="$(fixture_lease_acquire "$name" "$key" owner-a stage-a 'example.invalid/lease@sha256:deadbeef')"
[[ -n "$id" ]] || fail 'lease create'
if fixture_lease_acquire "$name" "$key" owner-b stage-b 'example.invalid/lease@sha256:deadbeef' >/dev/null; then fail 'lease name conflict'; else [[ "$?" == 75 ]] || fail 'wrong conflict status'; fi
fixture_lease_matches "$id" "$key" owner-a stage-a || fail 'exact lease match'
fixture_lease_release "$id"
id2="$(fixture_lease_acquire "$name" "$key" owner-b stage-b 'example.invalid/lease@sha256:deadbeef')"
fixture_lease_release "$id" # stale release must not remove id2
fixture_lease_matches "$id2" "$key" owner-b stage-b || fail 'exact-id release fence'
fixture_lease_release "$id2"

# A stale waiter must re-evaluate the heartbeat immediately before exact-ID
# removal. A live owner that refreshed during that interval stays fenced in.
id3="$(fixture_lease_acquire "$name" "$key" owner-c stage-c 'example.invalid/lease@sha256:deadbeef')"
lease_race_counter="$TMP/lease-race-counter"
fixture_lease_now() { printf '100\n'; }
fixture_lease_heartbeat_epoch() {
  local count=0
  [[ -f "$lease_race_counter" ]] && count="$(cat "$lease_race_counter")"
  printf '%s' "$((count + 1))" > "$lease_race_counter"
  if [[ "$count" == 0 ]]; then printf '1\n'; else printf '100\n'; fi
}
if fixture_lease_takeover_stale "$id3" "$key" 10; then fail 'live heartbeat refresh lost its lease'; fi
[[ -f "$FAKE_DOCKER_STATE/$id3" ]] || fail 'refreshed exact lease was removed'
unset -f fixture_lease_now fixture_lease_heartbeat_epoch
fixture_lease_release "$id3"

# Runner-facing --check is typed JSON and uses only direct storage objects.
env_file="$TMP/env.sh"
cat > "$env_file" <<EOF
NOVA_ENV_COMPOSE_ENV=$TMP/compose.env
NOVA_ENV_COMPOSE_PROJECT=fake-project
NOVA_ENV_COMPOSE_FILE=$TMP/compose.yml
NOVA_ENV_SHARED_BENCHMARK_ROOT=s3://fixture/shared/benchmarks
NOVA_ENV_BENCHMARK_LEASE_NAMESPACE=fake-project
NOVA_ENV_BENCHMARK_LEASE_IMAGE=example.invalid/lease@sha256:deadbeef
AWS_S3_ENDPOINT=http://unused
AWS_S3_ACCESS_KEY_ID=test-key
AWS_S3_SECRET_ACCESS_KEY=test-secret
EOF
: > "$TMP/compose.env"; : > "$TMP/compose.yml"
resolved="$TMP/resolved.json"
python3 "$ROOT/tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py" --workspace-root "$ROOT" --suite ssb --scale 1 --shared-root s3://fixture/shared/benchmarks > "$resolved"
python3 - "$resolved" "$BENCHMARK_FIXTURE_STORAGE_DIR" <<'PY'
import json, pathlib, sys
r=json.load(open(sys.argv[1])); root=pathlib.Path(sys.argv[2])
warehouse=r['staging_parent'] + '/writer-a/warehouse'; manifest=warehouse + '/manifest'
def path(uri): return root / uri.removeprefix('s3://')
p=path(manifest + '/_SUCCESS'); p.parent.mkdir(parents=True, exist_ok=True); p.write_text('', encoding='utf-8')
tables=[]
for table in r['contract']['tables']:
    metadata=warehouse + f'/{table}/metadata/v1.metadata.json'
    statistics=warehouse + f'/{table}/metadata/stats.puffin'
    for uri in (metadata, statistics):
        target=path(uri); target.parent.mkdir(parents=True, exist_ok=True); target.write_text(table, encoding='utf-8')
    tables.append({'name': table, 'metadata_uri': metadata, 'statistics_file': statistics})
part=path(manifest + '/part-00000')
part.write_text(json.dumps({'dataset_key':r['dataset_key'], 'fixture_contract':r['contract'], 'producer_fingerprint':r['producer_fingerprint'], 'tables':tables}), encoding='utf-8')
ready={"schema_version":1,"dataset_key":r['dataset_key'],"state":"ReadyValid","exact_warehouse":warehouse,"manifest_uri":manifest,"contract":r['contract'],"producer_fingerprint":r['producer_fingerprint'],"publication":{"ready_uri":r['ready_uri'],"identity":"writer-a"},"lease":{"container_id":"id-a","owner":"owner-a","staging_identity":"writer-a"}}
p=path(r['ready_uri']); p.parent.mkdir(parents=True, exist_ok=True); p.write_text(json.dumps(ready), encoding='utf-8')
PY
output="$(NOVA_ENV_REST_ENV_FILE="$env_file" BENCHMARK_FIXTURE_STORAGE_DIR="$BENCHMARK_FIXTURE_STORAGE_DIR" BENCHMARK_FIXTURE_UP_COMMAND=true "$BOOTSTRAP" --suite ssb --scale 1 --resolved-dataset "$resolved" --check)"
python3 "$ROOT/tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py" --suite ssb --scale 1 --shared-root s3://fixture/shared/benchmarks --validate-ensure-result <(printf '%s\n' "$output")
[[ "$output" != *test-secret* ]] || fail 'credential leaked to stdout'

# A malformed READY still exposes its exact ETag to explicit rebuild.  The
# bootstrap must retain it (rather than treating ReadyInvalid as Absent), and
# conditional repair cannot touch an unrelated sibling key.
source "$BOOTSTRAP"

# Cancellation must stop after cleanup.  A TERM delivered after fencing but
# before publication cannot return to the caller and continue with READY PUT.
term_continuation="$TMP/term-continuation"
if bash -ceu '
source "$1"
fixture_contract_file="$3/fixture-contract"
candidate="$3/ready-candidate"
: > "$fixture_contract_file"
: > "$candidate"
lease_id=""
owner_child_pid=""
trap cleanup_owner EXIT
trap '\''handle_owner_signal 143'\'' TERM
kill -TERM "$BASHPID"
touch "$2"
' bash "$BOOTSTRAP" "$term_continuation" "$TMP"; then
  fail 'TERM handler returned to publication path'
fi
[[ ! -e "$term_continuation" ]] || fail 'TERM handler continued after cleanup'
[[ ! -e "$TMP/fixture-contract" && ! -e "$TMP/ready-candidate" ]] || fail 'TERM cleanup left publication files'

# A Spark/docker descendant that ignores TERM must not survive cancellation.
if ! bash -ceu '
source "$1"
( trap "" TERM; while :; do sleep 1; done ) &
owner_child_pid="$!"
stubborn_pid="$owner_child_pid"
stop_owner_child
if kill -0 "$stubborn_pid" 2>/dev/null; then
  exit 1
fi
' bash "$BOOTSTRAP"; then
  fail 'stubborn owner child survived cleanup'
fi

resolved_dataset_file="$resolved"
ready_uri="$(python3 - "$resolved" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))['ready_uri'])
PY
)"
warehouse="$(python3 - "$resolved" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))['staging_parent'] + '/writer-a/warehouse')
PY
)"
fixture_publication_get "$ready_uri"
ready_etag="$FIXTURE_PUBLICATION_ETAG"
observed_ready_etag="$ready_etag"
mv "$BENCHMARK_FIXTURE_STORAGE_DIR/${warehouse#s3://}/customer/metadata/stats.puffin" "$TMP/missing-stats"
if check_readiness; then fail 'broken statistics accepted'; else [[ "$?" == 2 ]] || fail 'broken statistics status'; fi
[[ "$ready_etag" == "$observed_ready_etag" && -n "$ready_etag" ]] || fail 'invalid READY ETag was not preserved'
mv "$TMP/missing-stats" "$BENCHMARK_FIXTURE_STORAGE_DIR/${warehouse#s3://}/customer/metadata/stats.puffin"
sibling='s3://fixture/shared/benchmarks/sibling/READY.json'
printf '{"sibling":true}\n' > "$TMP/sibling"
fixture_publication_put_conditional "$sibling" "$TMP/sibling" absent >/dev/null
fixture_publication_get "$sibling"; sibling_etag="$FIXTURE_PUBLICATION_ETAG"; sibling_body="$FIXTURE_PUBLICATION_BODY"
printf '{"repaired":true}\n' > "$TMP/repaired"
[[ "$(fixture_publication_put_conditional "$ready_uri" "$TMP/repaired" rebuild "$ready_etag")" == 200 ]] || fail 'exact invalid READY rebuild'
fixture_publication_get "$sibling"
[[ "$FIXTURE_PUBLICATION_ETAG" == "$sibling_etag" && "$FIXTURE_PUBLICATION_BODY" == "$sibling_body" ]] || fail 'rebuild touched sibling key'
echo 'bootstrap lifecycle tests passed'
