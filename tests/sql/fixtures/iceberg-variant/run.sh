#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file under the Apache License, Version 2.0.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
mode="" namespace="" table="" output=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) mode="$2"; shift 2 ;;
    --namespace) namespace="$2"; shift 2 ;;
    --table) table="$2"; shift 2 ;;
    --output) output="$2"; shift 2 ;;
    *) echo "usage: $0 --mode MODE --namespace NAME --table NAME --output FRESH_DIR" >&2; exit 2 ;;
  esac
done
case "$mode" in unshredded|shredded|bounds|nested|uuid|write-target|observe|unchanged) ;; *) echo "Invalid fixture mode: $mode" >&2;exit 2 ;; esac
[[ "$namespace" =~ ^[a-zA-Z0-9_]+$ && "$table" =~ ^[a-zA-Z0-9_]+$ && -n "$output" ]] || { echo 'Invalid fixture arguments' >&2;exit 2; }
printf 'IRU9_ARTIFACT_DIR=%s\n' "$output"
mkdir -p "$output"
[[ ! -e "$output/fixture.scala" && ! -e "$output/spark.log" ]] || { echo 'Use a fresh output directory' >&2;exit 2; }
python3 - "$SCRIPT_DIR" "$output/fixture.scala" "$mode" "$namespace" "$table" <<'PY'
import json,pathlib,sys
source,dest,mode,namespace,table=sys.argv[1:]
root=pathlib.Path(source)
body=(root/'generate.scala').read_text()
body+='\ntry {\n'
body+=f'VariantFixture.initialize(org.apache.spark.sql.SparkSession.active,{json.dumps(namespace)},{json.dumps(table)})\n'
body+=(root/(mode+'.scala')).read_text()
body+='\nprintln("IRU9_FIXTURE_COMPLETE")\n} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }\n'
pathlib.Path(dest).write_text(body)
PY
if ! "$WORKSPACE_ROOT/docker/iceberg-rest/spark-shell.sh" "$output/fixture.scala" >"$output/spark.log" 2>&1; then
  tail -80 "$output/spark.log" >&2;exit 1
fi
if ! grep -q '^IRU9_FIXTURE_COMPLETE$' "$output/spark.log"; then
  tail -80 "$output/spark.log" >&2;exit 1
fi
grep '^IRU9_' "$output/spark.log"
