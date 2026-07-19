// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Aggregate MV first-refresh preparation.
//!
//! This module owns state-shaped query preparation and result materialization.
//! The standalone engine supplies analysis and query execution through an
//! invocation-local callback; lifecycle and Iceberg writes stay in the engine.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::Chunk;
use crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls;
use crate::mv::aggregate_state::mv_agg_state::{
    AggregateMvLayout, materialize_aggregate_result_chunks,
};
use crate::mv::model::{AggregateStateRole, VisibleAggregateOutput};
use crate::mv::refresh::pin::{RefreshSnapshotPin, inject_pin_as_for_version_as_of};
use crate::runtime::query_result::{QueryResult, record_batch_to_chunk};

pub(crate) struct AggregateStateRead {
    pub(crate) result: QueryResult,
    pub(crate) source_layout: AggregateMvLayout,
}

pub(crate) fn prepare_aggregate_first_refresh_chunks<F>(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    read: &mut F,
) -> Result<Vec<Chunk>, String>
where
    F: FnMut(&str, &AggregateSqlCalls, sqlparser::ast::Query) -> Result<AggregateStateRead, String>,
{
    let state_sql =
        crate::mv::aggregate_state::mv_shape::rewrite_select_sql_for_state(select_sql, calls)?;
    let mut state_query = parse_stored_select_query(&state_sql)?;
    inject_pin_as_for_version_as_of(
        &mut state_query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;
    let read = read(select_sql, calls, state_query)?;
    let target_layout = read.source_layout.clone();
    normalize_and_materialize_aggregate_read(read, calls, &target_layout, calls)
}

fn parse_stored_select_query(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
        .map_err(|error| format!("stored MV SELECT normalize error: {error}"))?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|error| format!("sql parser error: {error}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("stored MV SQL must be a SELECT query".to_string());
    };
    Ok(*query)
}

fn normalize_and_materialize_aggregate_read(
    mut read: AggregateStateRead,
    source_calls: &AggregateSqlCalls,
    target_layout: &AggregateMvLayout,
    target_calls: &AggregateSqlCalls,
) -> Result<Vec<Chunk>, String> {
    validate_aggregate_layout_compatibility(
        0,
        source_calls,
        &read.source_layout,
        target_calls,
        target_layout,
    )?;
    let source_names = aggregate_state_result_column_names(&read.source_layout, source_calls)?;
    let target_names = aggregate_state_result_column_names(target_layout, target_calls)?;
    if source_names.len() != target_names.len() {
        return Err(format!(
            "aggregate MV state result source/target slot count mismatch: source={} target={}",
            source_names.len(),
            target_names.len()
        ));
    }

    let metadata_names = read
        .result
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let metadata_permutation = exact_name_permutation(
        &metadata_names,
        &source_names,
        "aggregate MV state result metadata",
    )?;
    let old_columns = std::mem::take(&mut read.result.columns);
    read.result.columns = metadata_permutation
        .iter()
        .zip(target_names.iter())
        .map(|(source_index, target_name)| {
            let mut column = old_columns[*source_index].clone();
            column.name.clone_from(target_name);
            column
        })
        .collect();

    read.result.chunks = read
        .result
        .chunks
        .into_iter()
        .map(|chunk| {
            let schema = chunk.batch.schema();
            let actual_names = schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>();
            let permutation = exact_name_permutation(
                &actual_names,
                &source_names,
                "aggregate MV state result chunk",
            )?;
            reorder_and_rename_chunk_columns(chunk, &target_names, &permutation)
        })
        .collect::<Result<Vec<_>, String>>()?;
    materialize_aggregate_result_chunks(read.result, target_layout)
}

fn validate_aggregate_layout_compatibility(
    branch_index: usize,
    source_calls: &AggregateSqlCalls,
    source_layout: &AggregateMvLayout,
    target_calls: &AggregateSqlCalls,
    target_layout: &AggregateMvLayout,
) -> Result<(), String> {
    if source_calls.visible_outputs != target_calls.visible_outputs
        || source_calls.group_keys.len() != target_calls.group_keys.len()
        || source_calls.aggregates.len() != target_calls.aggregates.len()
        || source_layout.visible_columns.len() != target_layout.visible_columns.len()
        || source_layout.state_columns.len() != target_layout.state_columns.len()
    {
        return Err(format!(
            "aggregate MV branch {branch_index} layout shape mismatch with branch 0"
        ));
    }
    Ok(())
}

fn exact_name_permutation(
    actual_names: &[&str],
    expected_names: &[String],
    label: &str,
) -> Result<Vec<usize>, String> {
    if actual_names.len() != expected_names.len() {
        return Err(format!(
            "{label} column count mismatch: actual={} expected={}",
            actual_names.len(),
            expected_names.len()
        ));
    }
    let mut used = vec![false; actual_names.len()];
    let mut permutation = Vec::with_capacity(expected_names.len());
    for expected_name in expected_names {
        let candidates = actual_names
            .iter()
            .enumerate()
            .filter(|(index, actual_name)| {
                !used[*index] && actual_name.eq_ignore_ascii_case(expected_name)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let [index] = candidates.as_slice() else {
            return Err(format!(
                "{label} requires exactly one column named `{expected_name}`, found {} in [{}]",
                candidates.len(),
                actual_names.join(", ")
            ));
        };
        used[*index] = true;
        permutation.push(*index);
    }
    Ok(permutation)
}

fn aggregate_state_result_column_names(
    layout: &AggregateMvLayout,
    calls: &AggregateSqlCalls,
) -> Result<Vec<String>, String> {
    let mut names = Vec::with_capacity(calls.visible_outputs.len() + layout.state_columns.len());
    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let visible_source_index = layout
                    .group_key_source_indexes
                    .get(*group_key_index)
                    .ok_or_else(|| {
                        format!(
                            "aggregate MV state result group key index {group_key_index} out of range"
                        )
                    })?;
                let visible = layout
                    .visible_columns
                    .get(*visible_source_index)
                    .ok_or_else(|| {
                        format!(
                            "aggregate MV state result visible source index {visible_source_index} out of range"
                        )
                    })?;
                names.push(visible.name.clone());
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let state_column = layout
                    .state_columns
                    .iter()
                    .find(|column| {
                        column.state_role == AggregateStateRole::Single
                            && column.aggregate_index == *aggregate_index
                    })
                    .ok_or_else(|| {
                        format!(
                            "aggregate MV state result missing state column for aggregate index {aggregate_index}"
                        )
                    })?;
                names.push(state_column.name.clone());
            }
        }
    }
    names.extend(
        layout
            .state_columns
            .iter()
            .filter(|column| column.state_role == AggregateStateRole::RetractionCount)
            .map(|column| column.name.clone()),
    );
    Ok(names)
}

fn reorder_and_rename_chunk_columns(
    chunk: Chunk,
    names: &[String],
    permutation: &[usize],
) -> Result<Chunk, String> {
    if chunk.batch.num_columns() != names.len() || permutation.len() != names.len() {
        return Err(format!(
            "aggregate MV state result chunk column count mismatch: columns={} names={} permutation={}",
            chunk.batch.num_columns(),
            names.len(),
            permutation.len()
        ));
    }
    if permutation
        .iter()
        .any(|source_index| *source_index >= chunk.batch.num_columns())
    {
        return Err(format!(
            "aggregate MV state result column permutation out of range: columns={} permutation={permutation:?}",
            chunk.batch.num_columns()
        ));
    }
    let source_schema = chunk.batch.schema();
    let fields = permutation
        .iter()
        .zip(names.iter())
        .map(|(source_index, name)| {
            Arc::new(
                source_schema
                    .field(*source_index)
                    .clone()
                    .with_name(name.clone()),
            )
        })
        .collect::<Vec<_>>();
    let columns = permutation
        .iter()
        .map(|source_index| chunk.batch.column(*source_index).clone())
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| format!("reorder aggregate MV state result columns failed: {error}"))?;
    record_batch_to_chunk(batch)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BinaryArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::catalog::schema::SqlType;
    use crate::mv::aggregate_state::mv_agg_state::{AggregateStateColumn, AggregateVisibleColumn};
    use crate::mv::aggregate_state::physical_column::starrocks_physical_column;
    use crate::mv::aggregate_state::state_codec::encode_count_state;
    use crate::mv::model::{AggregateFunctionKind, AggregateStateRole};
    use crate::runtime::query_result::{QueryResultColumn, record_batch_to_chunk};

    fn parse_calls(sql: &str) -> AggregateSqlCalls {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
            .expect("normalize aggregate select");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
            .expect("parse aggregate select");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(&query)
            .expect("extract aggregate calls")
    }

    fn count_layout(group_key: &str) -> AggregateMvLayout {
        let row_id = starrocks_physical_column(
            "__row_id__".to_string(),
            SqlType::String,
            false,
            false,
            true,
        );
        let group =
            starrocks_physical_column(group_key.to_string(), SqlType::String, true, true, false);
        let counter =
            starrocks_physical_column("c".to_string(), SqlType::BigInt, false, true, false);
        let state = starrocks_physical_column(
            "__agg_state_c".to_string(),
            SqlType::Binary,
            false,
            false,
            false,
        );
        AggregateMvLayout {
            row_id_column: row_id.clone(),
            visible_columns: vec![
                AggregateVisibleColumn {
                    name: group_key.to_string(),
                    data_type: DataType::Utf8,
                    sql_type: SqlType::String,
                    nullable: true,
                    source_index: 0,
                },
                AggregateVisibleColumn {
                    name: "c".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: false,
                    source_index: 1,
                },
            ],
            state_columns: vec![AggregateStateColumn {
                name: "__agg_state_c".to_string(),
                data_type: DataType::Binary,
                sql_type: SqlType::Binary,
                nullable: false,
                visible_source_index: 1,
                aggregate_index: 0,
                function: AggregateFunctionKind::Count,
                state_role: AggregateStateRole::Single,
                count_star: true,
            }],
            aggregate_input_types: vec![None],
            group_key_source_indexes: vec![0],
            physical_columns: vec![row_id, group, counter, state],
        }
    }

    fn reordered_count_result() -> QueryResult {
        let state = encode_count_state(2);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__agg_state_c", DataType::Binary, false),
                Field::new("region", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(BinaryArray::from_vec(vec![state.as_slice()])),
                Arc::new(StringArray::from(vec![Some("east")])),
            ],
        )
        .expect("state-shaped result batch");
        QueryResult {
            columns: vec![
                QueryResultColumn {
                    name: "__agg_state_c".to_string(),
                    data_type: DataType::Binary,
                    nullable: false,
                    logical_type: Some(SqlType::Binary),
                },
                QueryResultColumn {
                    name: "region".to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                    logical_type: Some(SqlType::String),
                },
            ],
            chunks: vec![record_batch_to_chunk(batch).expect("chunk")],
        }
    }

    #[test]
    fn single_preparation_injects_exact_pin_and_normalizes_reordered_result() {
        let select_sql = "select region, count(*) as c from ice.sales.fact group by region";
        let calls = parse_calls(select_sql);
        let pin =
            RefreshSnapshotPin::from_entries_for_tests(&[("ice.sales.fact", 42, "fact-uuid")]);
        let mut reads = 0;
        let mut read =
            |visible_sql: &str, actual_calls: &AggregateSqlCalls, query: sqlparser::ast::Query| {
                reads += 1;
                assert_eq!(visible_sql, select_sql);
                assert_eq!(actual_calls, &calls);
                assert!(
                    query.to_string().contains("VERSION AS OF 42"),
                    "query={query}"
                );
                Ok(AggregateStateRead {
                    result: reordered_count_result(),
                    source_layout: count_layout("region"),
                })
            };

        let chunks = prepare_aggregate_first_refresh_chunks(
            select_sql,
            &calls,
            &pin,
            Some("ice"),
            "sales",
            &mut read,
        )
        .expect("prepare aggregate first refresh");

        assert_eq!(reads, 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].batch.num_rows(), 1);
        assert_eq!(chunks[0].batch.schema().field(0).name(), "__row_id__");
        assert_eq!(chunks[0].batch.schema().field(1).name(), "region");
        assert_eq!(chunks[0].batch.schema().field(2).name(), "c");
        assert_eq!(chunks[0].batch.schema().field(3).name(), "__agg_state_c");
    }

    fn prepare_with_result(result: QueryResult) -> Result<Vec<Chunk>, String> {
        let select_sql = "select region, count(*) as c from ice.sales.fact group by region";
        let calls = parse_calls(select_sql);
        let pin =
            RefreshSnapshotPin::from_entries_for_tests(&[("ice.sales.fact", 42, "fact-uuid")]);
        let mut result = Some(result);
        prepare_aggregate_first_refresh_chunks(
            select_sql,
            &calls,
            &pin,
            Some("ice"),
            "sales",
            &mut |_, _, _| {
                Ok(AggregateStateRead {
                    result: result.take().expect("single read"),
                    source_layout: count_layout("region"),
                })
            },
        )
    }

    #[test]
    fn single_metadata_count_mismatch_fails_fast() {
        let mut result = reordered_count_result();
        result.columns.pop();

        let error = prepare_with_result(result).expect_err("metadata arity must be exact");

        assert!(error.contains("metadata column count mismatch"), "{error}");
    }

    #[test]
    fn single_missing_or_duplicate_metadata_name_fails_fast() {
        for names in [["unexpected", "region"], ["region", "region"]] {
            let mut result = reordered_count_result();
            for (column, name) in result.columns.iter_mut().zip(names) {
                column.name = name.to_string();
            }

            let error = prepare_with_result(result).expect_err("metadata names must be exact");

            assert!(
                error.contains("requires exactly one column named `__agg_state_c`")
                    || error.contains("requires exactly one column named `region`"),
                "{error}"
            );
        }
    }

    #[test]
    fn single_chunk_name_mismatch_fails_fast() {
        let mut result = reordered_count_result();
        let old = result.chunks.pop().expect("chunk");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("unexpected", DataType::Binary, false),
                Field::new("region", DataType::Utf8, true),
            ])),
            old.batch.columns().to_vec(),
        )
        .expect("mismatched chunk");
        result
            .chunks
            .push(record_batch_to_chunk(batch).expect("chunk"));

        let error = prepare_with_result(result).expect_err("chunk names must be exact");

        assert!(
            error.contains("state result chunk requires exactly one"),
            "{error}"
        );
    }

    #[test]
    fn single_callback_failure_returns_error_without_materialization() {
        let select_sql = "select region, count(*) as c from ice.sales.fact group by region";
        let calls = parse_calls(select_sql);
        let pin =
            RefreshSnapshotPin::from_entries_for_tests(&[("ice.sales.fact", 42, "fact-uuid")]);

        let error = prepare_aggregate_first_refresh_chunks(
            select_sql,
            &calls,
            &pin,
            Some("ice"),
            "sales",
            &mut |_, _, _| Err("query read failed".to_string()),
        )
        .expect_err("callback failure must propagate");

        assert_eq!(error, "query read failed");
    }
}
