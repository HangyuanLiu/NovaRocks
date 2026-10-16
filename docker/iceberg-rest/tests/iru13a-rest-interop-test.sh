#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fixture_dir=$(cd "$script_dir/.." && pwd)
repo_root=$(cd "$fixture_dir/../.." && pwd)

# Resolve once. The caller owns fixture startup; this driver never changes bindings or inputs.
publication=$(python3 - "$fixture_dir" <<'PY'
import json
from pathlib import Path
import sys

try:
    publication = (Path(sys.argv[1]) / 'runtime/current/published').resolve(strict=True)
    manifest = json.loads((publication / 'manifest.json').read_text())
    if not manifest.get('ready') or not manifest.get('fixture_inputs', {}).get('verified'):
        raise ValueError('publication is not ready with verified inputs')
    if not (publication / 'env.sh').is_file():
        raise ValueError('publication has no immutable environment')
except (OSError, ValueError) as error:
    print(f'BLOCKED: pinned fixture publication required: {error}', file=sys.stderr)
    sys.exit(75)
print(publication)
PY
)
source "$publication/env.sh"
if [[ "$NOVA_ENV_REST_ENV_FILE" != "$publication/env.sh" ]]; then
    echo "FAIL: sourced environment differs from pinned publication" >&2
    exit 1
fi

input_store=$(python3 - "$publication/manifest.json" <<'PY'
import json
from pathlib import Path
import sys
manifest = json.loads(Path(sys.argv[1]).read_text())
print(Path(manifest['fixture_inputs']['bom']).parent)
PY
)
# Read-only verification; missing pinned inputs stay BLOCKED. No provision/download/READY rewrite.
"$repo_root/docker/fixture-inputs/verify.sh" --store "$input_store" --repo-root "$repo_root" --consumer iceberg-rest
python3 - "$publication/manifest.json" "$input_store/bom.json" <<'PY'
import json
from pathlib import Path
import sys
manifest = json.loads(Path(sys.argv[1]).read_text())
bom = json.loads(Path(sys.argv[2]).read_text())
if manifest['fixture_inputs']['lock_sha256'] != bom['lock_sha256']:
    print('BLOCKED: pinned publication differs from verified fixture input lock', file=sys.stderr)
    sys.exit(75)
PY

: "${NOVAROCKS_ICEBERG_REST_URI:?pinned REST URI required}"
: "${NOVAROCKS_ICEBERG_REST_WAREHOUSE:?pinned REST warehouse required}"
: "${AWS_S3_ENDPOINT:?pinned S3 endpoint required}"
: "${AWS_S3_ACCESS_KEY_ID:?pinned S3 access key required}"
: "${AWS_S3_SECRET_ACCESS_KEY:?pinned S3 secret required}"

run_id=$(python3 -c 'import uuid; print(uuid.uuid4().hex)')
interop_root="${NR_IRU13A_INTEROP_ARTIFACT_DIR:-$NOVA_ENV_RUNTIME_DIR/iru13a-java-rest-$run_id}"
mkdir "$interop_root"
export NR_JAVA_REST_INTEROP_RUN_ROOT="$interop_root"
for family in IRU5 IRU13A IRU13B; do
    namespace_key="NR_${family}_INTEROP_NAMESPACE"
    artifact_key="NR_${family}_INTEROP_ARTIFACT_DIR"
    # Provider catalog names are normalized; the independent Java oracle must
    # address that same namespace rather than a differently cased identifier.
    family_slug=$(printf '%s' "$family" | tr '[:upper:]' '[:lower:]')
    printf -v "$namespace_key" '%s' "java_rest_${run_id}_${family_slug}"
    printf -v "$artifact_key" '%s' "$interop_root/$family"
    export "$namespace_key" "$artifact_key"
    mkdir "${!artifact_key}"
done
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$repo_root/target}"
python3 - "$publication/manifest.json" "$interop_root/fixture.json" "$publication" <<'PY'
import json
from pathlib import Path
import sys
manifest = json.loads(Path(sys.argv[1]).read_text())
facts = {
    'publication': sys.argv[3],
    'fixture_input_lock_sha256': manifest['fixture_inputs']['lock_sha256'],
    'boundary': 'Java REST acceptance of NovaRocks-issued REST requests; native 1FE+3BE SQL acceptance is separate',
}
Path(sys.argv[2]).write_text(json.dumps(facts, indent=2) + '\n')
PY

cd "$repo_root"
# Cargo supplies the actual test executable; preserve build, listing and run
# outputs separately, and reject missing, extra, ignored or zero-case success.
python3 - "$interop_root" <<'PY_RUN'
from pathlib import Path
import datetime, hashlib, json, os, re, subprocess, sys
root = Path(sys.argv[1]).resolve()
expected = {
    'commit::staging::interop_tests::iru5_java_rest_three_shapes_historic_spec_and_composition',
    'commit::staging::interop_tests::iru5_java_rest_d12_inheritance_and_physical_rewrite',
    'commit::staging::interop_tests::iru5_java_rest_two_canonical_clients_real_cas',
    'commit::staging::interop_tests::iru5_java_rest_attempt_runner_reloads_and_reprepares_after_real_cas',
    'commit::staging::interop_tests::iru13a_java_rest_namespace_table_lifecycle_and_interleaved_reads',
    'commit::staging::interop_tests::iru13a_java_rest_table_commit_shapes',
    'commit::staging::interop_tests::iru13a_java_rest_stage_create_and_ctas_final_commit',
    'commit::staging::interop_tests::iru13a_java_rest_view_create_replace_drop',
    'commit::staging::interop_tests::iru13a_java_rest_recreated_table_rejects_stale_partition_evolution',
    'commit::staging::interop_tests::iru13b_java_rest_ctas_publishes_staged_metadata_for_every_format_version',
    'commit::staging::interop_tests::iru13b_java_rest_schema_changes_keep_the_column_high_water_mark',
    'commit::staging::interop_tests::iru13b_java_rest_view_replacements_always_add_the_selected_version',
    'commit::staging::interop_tests::iru13b_java_rest_view_replacement_survives_version_id_reuse',
    'commit::staging::interop_tests::iru13b_java_rest_view_replacement_keeps_dialects_unless_allowed',
}
pattern = 'commit::staging::interop_tests::'
def run(name, command):
    (root / f'{name}-command.json').write_text(json.dumps(command, indent=2) + '\n')
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with (root / f'{name}-stdout.log').open('wb') as stdout, (root / f'{name}-stderr.log').open('wb') as stderr:
        result = subprocess.run(command, stdout=stdout, stderr=stderr)
    (root / f'{name}-result.json').write_text(json.dumps({
        'exit_code': result.returncode, 'started': started,
        'ended': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }, indent=2) + '\n')
    if result.returncode:
        print((root / f'{name}-stdout.log').read_text(errors='replace')[-12000:])
        print((root / f'{name}-stderr.log').read_text(errors='replace')[-12000:], file=sys.stderr)
        raise SystemExit(result.returncode)
    return (root / f'{name}-stdout.log').read_text(errors='replace')
facts = {
    'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
    'diff': subprocess.check_output(['git', 'diff', 'HEAD'], text=True),
    'cargo': subprocess.check_output(['cargo', '--version'], text=True).strip(),
    'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
    'profile': 'dev', 'target_dir': os.environ['CARGO_TARGET_DIR'],
    'lock_sha256': hashlib.sha256(Path('Cargo.lock').read_bytes()).hexdigest(),
}
(root / 'source-build-facts.json').write_text(json.dumps(facts, indent=2) + '\n')
output = run('build', [
    'cargo', 'test', '--locked', '--offline', '--profile', 'dev', '-j2',
    '-p', 'novarocks-connector-iceberg', '--lib', '--no-run', '--message-format=json',
])
executables = set()
for line in output.splitlines():
    try:
        row = json.loads(line)
    except ValueError:
        continue
    if (row.get('reason') == 'compiler-artifact' and row.get('executable')
            and row.get('target', {}).get('name') == 'novarocks_connector_iceberg'
            and row.get('profile', {}).get('test')):
        executables.add(row['executable'])
if len(executables) != 1:
    raise SystemExit(f'FAIL: expected one Cargo-reported provider test executable, got {len(executables)}')
binary = Path(executables.pop()).resolve(strict=True)
(root / 'test-binary.json').write_text(json.dumps({
    'path': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
}, indent=2) + '\n')
listing = run('list', [str(binary), pattern, '--ignored', '--list'])
listed = {line[:-6] for line in listing.splitlines() if line.endswith(': test')}
if listed != expected:
    raise SystemExit(f'FAIL: Java test inventory differs: missing={sorted(expected-listed)}, extra={sorted(listed-expected)}')
output = run('tests', [str(binary), pattern, '--ignored', '--test-threads=1', '--nocapture'])
passed = set(re.findall(r'^test (\S+) \.\.\. ok$', output, re.MULTILINE))
if passed != expected:
    raise SystemExit(f'FAIL: Java test results differ: missing={sorted(expected-passed)}, extra={sorted(passed-expected)}')
if not re.search(rf'test result: ok\. {len(expected)} passed; 0 failed; 0 ignored;', output):
    raise SystemExit('FAIL: expected all pinned Java tests to pass without skips')
print(f'Java REST interoperability: {len(expected)} passed; 0 failed; 0 ignored')
PY_RUN
printf 'IRU-13a Java REST interoperability artifacts: %s\n' "$interop_root"
