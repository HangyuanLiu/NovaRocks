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

use novarocks::engine::statistics::{
    StatisticsEngine, StatisticsRequestContext, StatisticsStatementResult, StatisticsTableTarget,
};

use super::FrontendStatisticsService;
use super::model::ColumnStatRow;
use super::model::{HistogramStatRow, MultiColumnStatRow, TableKey};
use super::observation::{
    add_analyze_status, drop_all_table_stats, observe_update, replace_column_stats,
};
use super::query::{normalize_name, ok_result};

pub(super) fn try_handle_statement(
    service: &FrontendStatisticsService,
    engine: &dyn StatisticsEngine,
    sql: &str,
    context: StatisticsRequestContext<'_>,
) -> Result<Option<StatisticsStatementResult>, String> {
    let current_database = context.current_database;
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("admin ") {
        if lower.contains("enable_statistic_collect_on_first_load") {
            let enabled = !lower.contains("'false'") && !lower.contains("\"false\"");
            service
                .state
                .write()
                .expect("frontend statistics write lock")
                .collect_on_first_load = enabled;
        }
        return Ok(Some(StatisticsStatementResult::Ok));
    }
    if lower.starts_with("alter table ") && lower.contains("enable_statistic_collect_on_first_load")
    {
        let table = table_token_after(trimmed, "alter table")?;
        let key = table_key(&table, current_database)?;
        let enabled = !lower.contains("'false'") && !lower.contains("\"false\"");
        service
            .state
            .write()
            .expect("frontend statistics write lock")
            .table_collect_on_first_load
            .insert(key, enabled);
        return Ok(Some(StatisticsStatementResult::Ok));
    }
    if lower.starts_with("drop multiple columns stats ") {
        let table = table_token_after(trimmed, "drop multiple columns stats")?;
        let key = table_key(&table, current_database)?;
        service
            .state
            .write()
            .expect("frontend statistics write lock")
            .multi_column_stats
            .retain(|row| row.key != key);
        return Ok(Some(StatisticsStatementResult::Ok));
    }
    if lower.starts_with("drop stats ") {
        let table = table_token_after(trimmed, "drop stats")?;
        let key = table_key(&table, current_database)?;
        drop_all_table_stats(service, &key);
        return Ok(Some(StatisticsStatementResult::Ok));
    }
    if lower.starts_with("update ") && lower.contains("test_update_stats ") {
        observe_update(service, trimmed, current_database)?;
        return Ok(Some(StatisticsStatementResult::Ok));
    }
    if lower.starts_with("analyze ") {
        handle_analyze_statement(service, engine, trimmed, context)?;
        return Ok(Some(StatisticsStatementResult::Query(ok_result()?)));
    }
    Ok(None)
}

fn handle_analyze_statement(
    service: &FrontendStatisticsService,
    engine: &dyn StatisticsEngine,
    sql: &str,
    context: StatisticsRequestContext<'_>,
) -> Result<(), String> {
    let lower = sql.to_ascii_lowercase();
    let table = analyze_table_token(sql)?;
    let key = table_key(&table, context.current_database)?;
    if lower.contains(" drop histogram on ") {
        let columns = parse_columns_after_marker(sql, "drop histogram on")?;
        service
            .state
            .write()
            .expect("frontend statistics write lock")
            .histogram_stats
            .retain(|row| {
                row.key != key
                    || !columns
                        .iter()
                        .any(|column| column.eq_ignore_ascii_case(&row.column_name))
            });
        return Ok(());
    }

    let target = StatisticsTableTarget {
        current_catalog: context.current_catalog.map(str::to_owned),
        current_database: context.current_database.to_string(),
        name_parts: table_parts(&table)?,
    };
    let available = engine.resolve_table_columns(&target)?;
    if lower.contains(" update histogram on ") {
        let columns = if lower.contains(" on all columns") {
            available
                .iter()
                .map(|column| normalize_name(&column.name))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            parse_columns_after_marker(sql, "update histogram on")?
        };
        let mut state = service
            .state
            .write()
            .expect("frontend statistics write lock");
        for column in &columns {
            state
                .histogram_stats
                .retain(|row| row.key != key || row.column_name != *column);
            state.histogram_stats.push(HistogramStatRow {
                key: key.clone(),
                column_name: column.clone(),
                buckets: "[{\"lower\":\"\",\"upper\":\"\"}]".to_string(),
                mcv: "{}".to_string(),
            });
        }
        drop(state);
        add_analyze_status(service, &key, &columns.join(","), "HISTOGRAM", false);
        return Ok(());
    }
    if lower.contains(" predicate columns") {
        let columns = {
            let state = service.state.read().expect("frontend statistics read lock");
            state
                .column_usage
                .get(&key)
                .map(|usage| {
                    usage
                        .columns
                        .iter()
                        .filter(|(_, kinds)| {
                            kinds.contains("predicate")
                                || kinds.contains("join")
                                || kinds.contains("group_by")
                        })
                        .map(|(column, _)| column.clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        let status_columns = if columns.is_empty() {
            "ALL".to_string()
        } else {
            columns.join(",")
        };
        add_analyze_status(service, &key, &status_columns, "FULL", false);
        return Ok(());
    }
    if lower.contains(" multiple columns ") {
        let columns = parse_parenthesized_columns_after_marker(sql, "multiple columns")?;
        let column_names = columns.join(",");
        let mut state = service
            .state
            .write()
            .expect("frontend statistics write lock");
        state
            .multi_column_stats
            .retain(|row| row.key != key || row.column_names != column_names);
        state.multi_column_stats.push(MultiColumnStatRow {
            key: key.clone(),
            column_names: column_names.clone(),
        });
        drop(state);
        add_analyze_status(
            service,
            &key,
            &column_names,
            if lower.starts_with("analyze full ") {
                "FULL"
            } else {
                "SAMPLE"
            },
            false,
        );
        return Ok(());
    }
    if lower.starts_with("analyze sample table ") {
        add_analyze_status(service, &key, "ALL", "SAMPLE", false);
        return Ok(());
    }

    let columns = analyze_column_list(sql)?
        .unwrap_or_else(|| available.iter().map(|column| column.name.clone()).collect());
    let status_columns = if columns.len() == available.len() {
        "ALL".to_string()
    } else {
        columns.join(",")
    };
    let collected = engine.collect_table_statistics(&target, &columns)?;
    publish_collected_statistics(service, &key, collected, &status_columns)
}

fn publish_collected_statistics(
    service: &FrontendStatisticsService,
    key: &TableKey,
    collected: Vec<novarocks::engine::statistics::CollectedColumnStatistics>,
    status_columns: &str,
) -> Result<(), String> {
    let rows = collected
        .into_iter()
        .map(|row| {
            Ok(ColumnStatRow {
                key: key.clone(),
                column_name: normalize_name(&row.column_name)?,
                partition_name: key.table.clone(),
                row_count: row.row_count,
                max: row.max,
                min: row.min,
                ndv: row.ndv,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    replace_column_stats(service, key, rows);
    add_analyze_status(service, key, status_columns, "FULL", false);
    Ok(())
}

fn table_token_after(sql: &str, prefix: &str) -> Result<String, String> {
    sql.get(prefix.len()..)
        .map(str::trim)
        .and_then(|rest| {
            let end = rest
                .char_indices()
                .find(|(_, character)| character.is_whitespace() || *character == '(')
                .map(|(index, _)| index)
                .unwrap_or(rest.len());
            (end > 0).then(|| rest[..end].to_string())
        })
        .ok_or_else(|| format!("missing table name after {prefix}"))
}

fn analyze_table_token(sql: &str) -> Result<String, String> {
    for prefix in [
        "analyze full table",
        "analyze sample table",
        "analyze table",
    ] {
        if sql
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
        {
            return table_token_after(sql, prefix);
        }
    }
    Err(format!("unsupported ANALYZE statement: {sql}"))
}

fn analyze_column_list(sql: &str) -> Result<Option<Vec<String>>, String> {
    let table = analyze_table_token(sql)?;
    let lower = sql.to_ascii_lowercase();
    let Some(index) = lower.find(&table.to_ascii_lowercase()) else {
        return Ok(None);
    };
    let after = sql[index + table.len()..].trim_start();
    if !after.starts_with('(') {
        return Ok(None);
    }
    let Some(end) = after.find(')') else {
        return Err("unterminated ANALYZE column list".to_string());
    };
    split_columns(&after[1..end]).map(Some)
}

fn parse_columns_after_marker(sql: &str, marker: &str) -> Result<Vec<String>, String> {
    let lower = sql.to_ascii_lowercase();
    let Some(index) = lower.find(marker) else {
        return Err(format!("missing `{marker}` in `{sql}`"));
    };
    let mut rest = sql[index + marker.len()..].trim();
    for stop in [" properties", " with ", " order ", " limit "] {
        if let Some(stop_index) = rest.to_ascii_lowercase().find(stop) {
            rest = &rest[..stop_index];
        }
    }
    split_columns(rest)
}

fn parse_parenthesized_columns_after_marker(
    sql: &str,
    marker: &str,
) -> Result<Vec<String>, String> {
    let lower = sql.to_ascii_lowercase();
    let Some(index) = lower.find(marker) else {
        return Err(format!("missing `{marker}` in `{sql}`"));
    };
    let rest = &sql[index + marker.len()..];
    let Some(start) = rest.find('(') else {
        return Err(format!("missing column list after `{marker}`"));
    };
    let Some(end) = rest[start + 1..].find(')') else {
        return Err(format!("unterminated column list after `{marker}`"));
    };
    split_columns(&rest[start + 1..start + 1 + end])
}

fn split_columns(text: &str) -> Result<Vec<String>, String> {
    text.split(',')
        .map(|part| normalize_name(part.trim().trim_matches('`')))
        .filter(|result| result.as_ref().map(|name| !name.is_empty()).unwrap_or(true))
        .collect()
}

fn table_parts(name: &str) -> Result<Vec<String>, String> {
    name.split('.')
        .map(|part| normalize_name(part.trim_matches('`')))
        .collect()
}

fn table_key(name: &str, current_database: &str) -> Result<TableKey, String> {
    let parts = table_parts(name)?;
    match parts.as_slice() {
        [table] => Ok(TableKey {
            db: normalize_name(current_database)?,
            table: table.clone(),
        }),
        [db, table] | [_, db, table] => Ok(TableKey {
            db: db.clone(),
            table: table.clone(),
        }),
        _ => Err(format!("invalid statistics table name: {name}")),
    }
}
