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

-- @order_sensitive=true
-- @sequential=true
-- The shared Spark-hosted Java fixture authors Iceberg fixed widths directly;
-- Spark SQL writes ordinary Binary values into that exact existing schema.

-- query 1
-- @result_contains=F3_FIXED_SPARK_CREATED
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-f3-fixed-create-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/generate.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SCALA'
try {
  val session = org.apache.spark.sql.SparkSession.active
  VariantFixture.initialize(session,"${case_db}","spark_fixed_cow_${uuid0}")
  val fixedSchema = new Schema(
    Types.NestedField.required(1,"id",Types.IntegerType.get()),
    Types.NestedField.required(2,"raw",Types.FixedType.ofLength(16)),
    Types.NestedField.optional(3,"st",Types.StructType.of(Types.NestedField.required(5,"f",Types.FixedType.ofLength(4)))),
    Types.NestedField.required(4,"n",Types.IntegerType.get()))
  VariantFixture.create(fixedSchema,properties=Map("write.update.mode"->"copy-on-write","write.row-lineage"->"true"))
  session.sql("INSERT INTO ice_rest.${case_db}.spark_fixed_cow_${uuid0} VALUES (1,unhex('00112233445566778899AABBCCDDEEFF'),named_struct('f',unhex('01020304')),10),(2,unhex('FFEEDDCCBBAA99887766554433221100'),NULL,20)")
  require(VariantFixture.load().schema().asStruct()==fixedSchema.asStruct(),"Spark changed the fixed declaration")
  val initial=session.sql("SELECT id,hex(raw),hex(st.f),n FROM ice_rest.${case_db}.spark_fixed_cow_${uuid0} ORDER BY id").collect().toVector
  require(initial.size==2 && initial(0).getString(1)=="00112233445566778899AABBCCDDEEFF" && initial(0).getString(2)=="01020304" && initial(1).getString(1)=="FFEEDDCCBBAA99887766554433221100" && initial(1).isNullAt(2),"Spark fixture payload differs")
  println("F3_FIXED_SPARK_CREATED")
} catch { case failure:Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
# Retain both actual process streams privately on either failure path.
umask 077
spark_diag_dir="$(mktemp -d "${TMPDIR:-/tmp}/novarocks-f3-fixed-create-diagnostics-XXXXXX")"
spark_status=0
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" \
  > "$spark_diag_dir/stdout" 2> "$spark_diag_dir/stderr" || spark_status=$?
printf '%s\n' "$spark_status" > "$spark_diag_dir/exit-status"
if [ "$spark_status" -ne 0 ]; then
  printf 'Spark create failed (exit=%s); original stdout/stderr and exit-status retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit "$spark_status"
fi
if ! grep -Fx 'F3_FIXED_SPARK_CREATED' "$spark_diag_dir/stdout"; then
  printf 'Spark create sentinel missing (exit=%s); original stdout/stderr and exit-status retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit 1
fi
rm -f "$spark_diag_dir/stdout" "$spark_diag_dir/stderr" "$spark_diag_dir/exit-status"
rmdir "$spark_diag_dir"

-- query 2
-- Native projections check root bytes and parent validity. The independent
-- Spark checks retain every nested fixed byte and its exact declared width.
SELECT id,hex(CAST(raw AS VARBINARY)) AS raw_hex,st IS NULL AS nested_is_null,n+1 AS next_n,n
FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_cow_${uuid0} ORDER BY id;

-- query 3
UPDATE iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_cow_${uuid0}
SET n=n+1;

-- query 4
UPDATE iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_cow_${uuid0}
SET raw=raw,st=st WHERE id=1;

-- query 5
SELECT id,hex(CAST(raw AS VARBINARY)) AS raw_hex,st IS NULL AS nested_is_null,n
FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_cow_${uuid0} ORDER BY id;

-- query 6
-- @result_contains=F3_FIXED_SPARK_VERIFIED
shell: set -eu
tmp_scala="$(mktemp "${TMPDIR:-/tmp}/novarocks-f3-fixed-verify-XXXXXX.scala")"
trap 'rm -f "$tmp_scala"' EXIT
cat "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/generate.scala" > "$tmp_scala"
cat >> "$tmp_scala" <<'SCALA'
try {
  val session = org.apache.spark.sql.SparkSession.active
  VariantFixture.initialize(session,"${case_db}","spark_fixed_cow_${uuid0}")
  val table=VariantFixture.load()
  require(table.schema().findType("raw")==Types.FixedType.ofLength(16) && table.schema().findType("st.f")==Types.FixedType.ofLength(4),"COW changed the exact fixed widths")
  val rows=session.sql("SELECT id,hex(raw),hex(st.f),n FROM ice_rest.${case_db}.spark_fixed_cow_${uuid0} ORDER BY id").collect().toVector
  require(rows.size==2 && rows(0).getInt(0)==1 && rows(0).getString(1)=="00112233445566778899AABBCCDDEEFF" && rows(0).getString(2)=="01020304" && rows(0).getInt(3)==11 && rows(1).getInt(0)==2 && rows(1).getString(1)=="FFEEDDCCBBAA99887766554433221100" && rows(1).isNullAt(2) && rows(1).getInt(3)==21,"COW changed Spark fixed payloads or row values")
  println("F3_FIXED_SPARK_VERIFIED")
} catch { case failure:Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
# Retain both actual process streams privately on either failure path.
umask 077
spark_diag_dir="$(mktemp -d "${TMPDIR:-/tmp}/novarocks-f3-fixed-verify-diagnostics-XXXXXX")"
spark_status=0
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-shell.sh" "$tmp_scala" \
  > "$spark_diag_dir/stdout" 2> "$spark_diag_dir/stderr" || spark_status=$?
printf '%s\n' "$spark_status" > "$spark_diag_dir/exit-status"
if [ "$spark_status" -ne 0 ]; then
  printf 'Spark verify failed (exit=%s); original stdout/stderr and exit-status retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit "$spark_status"
fi
if ! grep -Fx 'F3_FIXED_SPARK_VERIFIED' "$spark_diag_dir/stdout"; then
  printf 'Spark verify sentinel missing (exit=%s); original stdout/stderr and exit-status retained at %s\n' "$spark_status" "$spark_diag_dir" >&2
  exit 1
fi
rm -f "$spark_diag_dir/stdout" "$spark_diag_dir/stderr" "$spark_diag_dir/exit-status"
rmdir "$spark_diag_dir"

-- query 7
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.spark_fixed_cow_${uuid0} FORCE;
