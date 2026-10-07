-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,visible_bag,target_candidates,equality_delete
-- Initial seed writes an official no-op Equality artifact without a Catalog
-- commit. The debug provider composes B1 data and B2 Equality in the one normal
-- first publication transaction; P/E bind B2 through the normal publisher.
-- This proves Equality only, not legacy Parquet position delete support.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG bag_candidate_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE bag_candidate_${uuid0}.ns_${uuid0};
CREATE TABLE bag_candidate_${uuid0}.ns_${uuid0}.fact (id BIGINT NOT NULL, p BIGINT NOT NULL, label STRING, amount BIGINT)
PARTITION BY (p)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO bag_candidate_${uuid0}.ns_${uuid0}.fact VALUES (1,1,'same',10),(3,2,'same',10);
INSERT INTO bag_candidate_${uuid0}.ns_${uuid0}.fact VALUES (2,1,'same',10),(4,2,'same',10);
SET CATALOG bag_candidate_${uuid0};
USE ns_${uuid0};
SET enable_materialized_view_rewrite = false;
CREATE MATERIALIZED VIEW candidate_mv
PARTITION BY p
DISTRIBUTED BY HASH(id) BUCKETS 3
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT id,p,label,amount FROM fact;

-- query 2
-- @skip_result_check=true
-- @result_contains=TARGET_CANDIDATE_SEED_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/target-candidate/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/seed.scala"
uea_log="$uea_receipts/seed.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-target-candidate/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "candidate_seed")
  TargetCandidateFixture.seed("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^TARGET_CANDIDATE_SEED_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/seed.jsonl"
python3 - "$uea_receipts/seed.jsonl" '${query_lifecycle_fault_root}' <<'PYARM'
import json,sys,pathlib
rows=[json.loads(line.removeprefix('UEA4G_RECEIPT ')) for line in open(sys.argv[1])]
rows=[r for r in rows if r.get('record')=='candidate_seed_arm']
assert len(rows)==1 and rows[0]['catalog_commit'] is False
arm=rows[0]['arm']; encoded=json.dumps(arm,separators=(',',':'))
assert len(encoded.encode()) <= 16384
root=pathlib.Path(sys.argv[2]); assert root.is_absolute() and root.is_dir()
p=root/('mv-target-equality-seed-'+arm['table_uuid']+'.json')
with p.open('x') as out: out.write(encoded)
PYARM
printf 'TARGET_CANDIDATE_SEED_READY\n'

-- query 3
-- @skip_result_check=true
-- @imv_equivalence_check=candidate_mv
REFRESH MATERIALIZED VIEW candidate_mv WITH SYNC MODE;
SELECT * FROM candidate_mv;

-- query 4
-- @skip_result_check=true
-- @result_contains=TARGET_CANDIDATE_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/target-candidate/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/initial.scala"
uea_log="$uea_receipts/initial.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-target-candidate/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "candidate_seed")
  TargetCandidateFixture.observe("ns_${uuid0}", "initial")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^TARGET_CANDIDATE_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/initial.jsonl"
python3 - "$uea_receipts/initial.jsonl" '${query_lifecycle_fault_root}' "$uea_receipts/candidate-trace-arm.json" <<'PYTRACEARM'
import json,sys,pathlib,uuid
rows=[json.loads(line.removeprefix('UEA4G_RECEIPT ')) for line in open(sys.argv[1])]
rows=[r for r in rows if r.get('record')=='candidate_observation']; assert len(rows)==1
observed=rows[0]; assert observed['stage']=='initial' and observed['snapshot']>0
assert observed['p1_paths'] and observed['p2_paths']
assert not set(observed['p1_paths']) & set(observed['p2_paths'])
arm={'token':str(uuid.uuid4()),'table_uuid':observed['table_uuid'],'snapshot_id':observed['snapshot'],'table_location':observed['table_location']}
encoded=json.dumps(arm,separators=(',',':')); assert len(encoded.encode()) <= 16384
root=pathlib.Path(sys.argv[2]); assert root.is_absolute() and root.is_dir()
with (root/('mv-target-candidate-trace-'+arm['table_uuid']+'.json')).open('x') as out: out.write(encoded)
with pathlib.Path(sys.argv[3]).open('x') as out: out.write(encoded)
PYTRACEARM
printf 'TARGET_CANDIDATE_OBSERVED\n'

-- query 5
-- @skip_result_check=true
-- @result_contains=TARGET_CANDIDATE_SOURCE_COW_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/target-candidate/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/source-p1-cow.scala"
uea_log="$uea_receipts/source-p1-cow.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-target-candidate/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "candidate_seed")
  TargetCandidateFixture.retractP1("ns_${uuid0}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^TARGET_CANDIDATE_SOURCE_COW_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/source-p1-cow.jsonl"
printf 'TARGET_CANDIDATE_SOURCE_COW_READY\n'

-- query 6
-- @skip_result_check=true
-- @imv_equivalence_check=candidate_mv
REFRESH MATERIALIZED VIEW candidate_mv WITH SYNC MODE;
SELECT * FROM candidate_mv;

-- query 7
-- @skip_result_check=true
-- @result_contains=TARGET_CANDIDATE_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/target-candidate/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/p1.scala"
uea_log="$uea_receipts/p1.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-target-candidate/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "candidate_seed")
  TargetCandidateFixture.observe("ns_${uuid0}", "p1")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^TARGET_CANDIDATE_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/p1.jsonl"
python3 - "$uea_receipts/initial.jsonl" "$uea_receipts/candidate-trace-arm.json" '${query_lifecycle_fault_root}' "$uea_receipts" <<'PYTRACEVERIFY'
import json,sys,pathlib
rows=[json.loads(line.removeprefix('UEA4G_RECEIPT ')) for line in open(sys.argv[1])]
rows=[r for r in rows if r.get('record')=='candidate_observation']; assert len(rows)==1
observed=rows[0]; arm=json.load(open(sys.argv[2])); root=pathlib.Path(sys.argv[3]); reports=pathlib.Path(sys.argv[4])
for phase in ['writer-frozen','reader-pinned']:
 p=root/('mv-target-candidate-'+arm['table_uuid']+'-'+arm['token']+'-'+phase+'.json')
 assert p.stat().st_size <= 131072
 record=json.load(p.open())
 for field in ['token','table_uuid','snapshot_id']:
  assert record[field]==arm[field], 'Trace identity differs: '+field
 assert record['phase']==phase
 assert record['paths']==observed['p1_paths'], 'Actual candidate set differs from exact p1 files'
 assert not set(record['paths']) & set(observed['p2_paths']), 'Noncandidate p2 was consumed'
 (reports/(phase+'.json')).write_text(json.dumps(record,separators=(',',':')))
exact_arm=root/('mv-target-candidate-trace-'+arm['table_uuid']+'.json')
assert json.load(exact_arm.open())==arm
exact_arm.unlink()
(reports/'scan-bytes.json').write_text(json.dumps({'status':'Unavailable','reason':'No target-node-attributed scan bytes are exposed by REFRESH'}))
PYTRACEVERIFY
printf 'TARGET_CANDIDATE_OBSERVED\n'

-- query 8
-- @skip_result_check=true
DELETE FROM fact WHERE id = 3;
ALTER MATERIALIZED VIEW candidate_mv SET REFRESH ASYNC EVERY INTERVAL 1 SECOND;

-- query 9
-- @skip_result_check=true
-- @retry_count=30
-- @retry_interval_ms=1000
-- @result_contains=TARGET_REFUSED
-- @result_contains=ELIGIBLE
-- @result_not_contains=VALIDATION_PENDING
SHOW MATERIALIZED VIEWS;

-- query 10
-- @expect_error=uses unsupported Equality deletes
REFRESH MATERIALIZED VIEW candidate_mv WITH SYNC MODE;

-- query 11
-- @skip_result_check=true
-- @result_contains=TARGET_CANDIDATE_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/target-candidate/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/stopped.scala"
uea_log="$uea_receipts/stopped.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-target-candidate/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "candidate_seed")
  TargetCandidateFixture.observe("ns_${uuid0}", "stopped")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^TARGET_CANDIDATE_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/stopped.jsonl"
python3 - "$uea_receipts/p1.jsonl" "$uea_receipts/stopped.jsonl" <<'PYCOMPARE'
import json,sys
def observation(path):
 rows=[json.loads(line.removeprefix('UEA4G_RECEIPT ')) for line in open(path)]
 rows=[r for r in rows if r.get('record')=='candidate_observation']; assert len(rows)==1; return rows[0]
before,after=map(observation,sys.argv[1:])
for field in ['table_uuid','snapshot','publication','eligibility','files']:
 assert before[field]==after[field], 'Refusal/source progress changed '+field
PYCOMPARE
printf 'TARGET_CANDIDATE_OBSERVED\n'

-- query 12
-- @skip_result_check=true
INSERT INTO fact VALUES (5,2,'same',10);

-- query 13
-- @skip_result_check=true
-- @result_contains=TARGET_STOP_OBSERVATION_INTERVAL_OK
shell: sleep 3
printf 'TARGET_STOP_OBSERVATION_INTERVAL_OK\n'

-- query 14
-- @skip_result_check=true
-- @result_contains=TARGET_REFUSED
-- @result_contains=ELIGIBLE
-- @result_not_contains=VALIDATION_PENDING
SHOW MATERIALIZED VIEWS;

-- query 15
-- @skip_result_check=true
-- @result_contains=TARGET_CANDIDATE_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/target-candidate/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/progress.scala"
uea_log="$uea_receipts/progress.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-target-candidate/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "candidate_seed")
  TargetCandidateFixture.observe("ns_${uuid0}", "progress")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^TARGET_CANDIDATE_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/progress.jsonl"
python3 - "$uea_receipts/p1.jsonl" "$uea_receipts/progress.jsonl" <<'PYCOMPARE'
import json,sys
def observation(path):
 rows=[json.loads(line.removeprefix('UEA4G_RECEIPT ')) for line in open(path)]
 rows=[r for r in rows if r.get('record')=='candidate_observation']; assert len(rows)==1; return rows[0]
before,after=map(observation,sys.argv[1:])
for field in ['table_uuid','snapshot','publication','eligibility','files']:
 assert before[field]==after[field], 'Refusal/source progress changed '+field
PYCOMPARE
printf 'TARGET_CANDIDATE_OBSERVED\n'

-- query 16
-- @skip_result_check=true
-- @imv_equivalence_check=candidate_mv
REFRESH MATERIALIZED VIEW candidate_mv FULL WITH SYNC MODE;
SELECT * FROM candidate_mv;

-- query 17
-- @skip_result_check=true
-- @result_contains=TARGET_CANDIDATE_OBSERVED
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:-$uea_workspace/docker/iceberg-rest/runtime/current/env.sh}")"
uea_receipts="$uea_workspace/reports/uea7b3/target-candidate/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/full.scala"
uea_log="$uea_receipts/full.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/mv-target-candidate/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
  DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "ns_${uuid0}", "candidate_seed")
  TargetCandidateFixture.observe("ns_${uuid0}", "full")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^TARGET_CANDIDATE_OBSERVED$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/full.jsonl"
printf 'TARGET_CANDIDATE_OBSERVED\n'

-- query 18
-- @skip_result_check=true
-- @result_contains=ELIGIBLE
-- @result_not_contains=VALIDATION_PENDING
-- @result_not_contains=TARGET_REFUSED
SHOW MATERIALIZED VIEWS;

-- query 19
-- @cleanup=true
-- @skip_result_check=true
SET CATALOG bag_candidate_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.candidate_mv;
DROP TABLE IF EXISTS bag_candidate_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE bag_candidate_${uuid0}.ns_${uuid0};
DROP CATALOG bag_candidate_${uuid0};
