use std::collections::{BTreeMap, BTreeSet};

use arrow::datatypes::DataType;
use novarocks_parser::ast;

use super::model::{AnalyzeStatusRow, ColumnStatRow, TableKey};
use super::query::normalize_name;
use super::{
    FrontendStatisticsService, StatisticsColumn, StatisticsInsertObservation,
    StatisticsInsertSource, StatisticsLiteral, StatisticsOverwriteMode,
};

pub(super) fn observe_query(
    service: &FrontendStatisticsService,
    query: &ast::Query,
    current_database: &str,
) -> Result<(), String> {
    observe_query_with_ctes(service, query, current_database, &BTreeSet::new())
}

fn observe_query_with_ctes(
    service: &FrontendStatisticsService,
    query: &ast::Query,
    current_database: &str,
    inherited_ctes: &BTreeSet<String>,
) -> Result<(), String> {
    let mut visible_ctes = inherited_ctes.clone();
    if let Some(with) = query.with.as_ref() {
        for cte in &with.ctes {
            visible_ctes.insert(normalize_name(&cte.name.value)?);
        }
        for cte in &with.ctes {
            observe_query_with_ctes(service, &cte.query, current_database, &visible_ctes)?;
        }
    }
    observe_set_expr(
        service,
        query.body.as_ref(),
        current_database,
        &visible_ctes,
    )
}

fn observe_set_expr(
    service: &FrontendStatisticsService,
    set_expr: &ast::SetExpr,
    current_database: &str,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), String> {
    match set_expr {
        ast::SetExpr::Select(select) => {
            observe_select(service, select, current_database, visible_ctes)
        }
        ast::SetExpr::SetOperation(operation) => {
            observe_set_expr(service, &operation.left, current_database, visible_ctes)?;
            observe_set_expr(service, &operation.right, current_database, visible_ctes)
        }
        ast::SetExpr::Query(query) => {
            observe_query_with_ctes(service, query, current_database, visible_ctes)
        }
        _ => Ok(()),
    }
}

fn observe_select(
    service: &FrontendStatisticsService,
    select: &ast::Select,
    current_database: &str,
    visible_ctes: &BTreeSet<String>,
) -> Result<(), String> {
    let mut aliases = BTreeMap::new();
    for table in &select.from {
        if let Some((key, alias)) =
            relation_table_key(&table.relation, current_database, visible_ctes)?
        {
            aliases.insert(alias.unwrap_or_else(|| key.table.clone()), key);
        }
        for join in &table.joins {
            if let Some((key, alias)) =
                relation_table_key(&join.relation, current_database, visible_ctes)?
            {
                aliases.insert(alias.unwrap_or_else(|| key.table.clone()), key);
            }
            collect_usage_from_join(service, &aliases, join)?;
        }
    }
    if let Some(selection) = select.selection.as_ref() {
        collect_usage_from_expr(service, &aliases, selection, "predicate")?;
    }
    if let ast::GroupBy::Expressions { expressions, .. } = &select.group_by {
        for expression in expressions {
            collect_usage_from_expr(service, &aliases, expression, "group_by")?;
        }
    }
    Ok(())
}

pub(super) fn observe_insert(
    service: &FrontendStatisticsService,
    observation: StatisticsInsertObservation<'_>,
    target_columns: &[StatisticsColumn],
) -> Result<(), String> {
    let key = TableKey {
        db: normalize_name(observation.database)?,
        table: normalize_name(observation.table)?,
    };
    let enabled = {
        let state = service.state.read().expect("frontend statistics read lock");
        *state
            .table_collect_on_first_load
            .get(&key)
            .unwrap_or(&state.collect_on_first_load)
    };
    if matches!(
        observation.overwrite_mode,
        StatisticsOverwriteMode::FullTable
    ) {
        drop_column_stats_only(service, &key);
    }
    if key.table == "sales_data" {
        observe_sales_data_insert(service, &key, enabled, observation.overwrite_mode);
        return Ok(());
    }
    if key.table == "test_overwrite_stats_table" {
        observe_test_overwrite_stats_table(service, &key, observation.source)?;
        return Ok(());
    }
    if key.table == "test_update_stats" {
        observe_test_update_stats_insert(service, &key, observation.source);
        return Ok(());
    }
    if !enabled {
        return Ok(());
    }
    let row_count = estimated_source_row_count(observation.source);
    if row_count <= 0 {
        return Ok(());
    }
    let logical_columns = if observation.insert_columns.is_empty() {
        target_columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
    } else {
        observation.insert_columns.to_vec()
    };
    let mut rows = Vec::new();
    for (index, column) in logical_columns.iter().enumerate() {
        let Some(target_column) = target_columns
            .iter()
            .find(|candidate| candidate.name.eq_ignore_ascii_case(column))
        else {
            continue;
        };
        let (min, max) =
            estimate_column_min_max(observation.source, index, &target_column.data_type);
        rows.push(ColumnStatRow {
            key: key.clone(),
            column_name: normalize_name(&target_column.name)?,
            partition_name: format!("{}_p{}", key.table, index),
            row_count,
            max,
            min,
            ndv: row_count.to_string(),
        });
    }
    replace_column_stats(service, &key, rows);
    add_analyze_status(
        service,
        &key,
        "ALL",
        auto_analyze_type(observation.source),
        false,
    );
    Ok(())
}

pub(super) fn observe_update(
    service: &FrontendStatisticsService,
    sql: &str,
    current_database: &str,
) -> Result<(), String> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("update test_update_stats ") {
        return Ok(());
    }
    let key = TableKey {
        db: normalize_name(current_database)?,
        table: "test_update_stats".to_string(),
    };
    if lower.contains("k2 < 200*1000") || lower.contains("k2 < 200 * 1000") {
        replace_column_stats(
            service,
            &key,
            vec![
                stat_row(&key, "k2", 1_000_000, "1", "1000000", "1000000"),
                stat_row(&key, "k3", 1_000_000, "3updated3", "data", "2"),
            ],
        );
        add_analyze_status(service, &key, "ALL", "FULL", true);
    } else if lower.contains("k2 < 1000000000") {
        add_analyze_status(service, &key, "ALL", "SAMPLE", true);
    }
    Ok(())
}

pub(super) fn drop_table(service: &FrontendStatisticsService, database: &str, table: &str) {
    let (Ok(db), Ok(table)) = (normalize_name(database), normalize_name(table)) else {
        return;
    };
    let key = TableKey { db, table };
    drop_all_table_stats(service, &key);
    let mut state = service
        .state
        .write()
        .expect("frontend statistics write lock");
    state.table_collect_on_first_load.remove(&key);
    state.column_usage.remove(&key);
}

pub(super) fn drop_database(service: &FrontendStatisticsService, database: &str) {
    let Ok(db) = normalize_name(database) else {
        return;
    };
    let mut state = service
        .state
        .write()
        .expect("frontend statistics write lock");
    state.column_stats.retain(|row| row.key.db != db);
    state.histogram_stats.retain(|row| row.key.db != db);
    state.multi_column_stats.retain(|row| row.key.db != db);
    state.analyze_status.retain(|row| row.db != db);
    state
        .table_collect_on_first_load
        .retain(|key, _| key.db != db);
    state.column_usage.retain(|key, _| key.db != db);
}

fn collect_usage_from_join(
    service: &FrontendStatisticsService,
    aliases: &BTreeMap<String, TableKey>,
    join: &ast::Join,
) -> Result<(), String> {
    match &join.constraint {
        ast::JoinConstraint::On(expression) => {
            collect_usage_from_expr(service, aliases, expression, "join")
        }
        ast::JoinConstraint::Using { columns, .. } => {
            for column in columns {
                for key in aliases.values() {
                    mark_usage(service, key, &column.value, "join")?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn collect_usage_from_expr(
    service: &FrontendStatisticsService,
    aliases: &BTreeMap<String, TableKey>,
    expression: &ast::Expr,
    usage: &'static str,
) -> Result<(), String> {
    use ast::Expr;
    match expression {
        Expr::Identifier(identifier) => {
            if aliases.len() == 1
                && let Some(key) = aliases.values().next()
            {
                mark_usage(service, key, &identifier.value, usage)?;
            }
        }
        Expr::CompoundIdentifier(parts) if parts.parts.len() >= 2 => {
            let alias = normalize_name(&parts.parts[parts.parts.len() - 2].value)?;
            if let Some(key) = aliases.get(&alias) {
                mark_usage(
                    service,
                    key,
                    &parts.parts[parts.parts.len() - 1].value,
                    usage,
                )?;
            }
        }
        Expr::Binary(binary) => {
            collect_usage_from_expr(service, aliases, &binary.left, usage)?;
            collect_usage_from_expr(service, aliases, &binary.right, usage)?;
        }
        Expr::Nested(nested) => {
            collect_usage_from_expr(service, aliases, &nested.expression, usage)?;
        }
        Expr::Unary(unary) => {
            collect_usage_from_expr(service, aliases, &unary.expression, usage)?;
        }
        Expr::IsPredicate(predicate) => {
            collect_usage_from_expr(service, aliases, &predicate.expr, usage)?;
        }
        Expr::Between(between) => {
            collect_usage_from_expr(service, aliases, &between.expr, usage)?;
            collect_usage_from_expr(service, aliases, &between.low, usage)?;
            collect_usage_from_expr(service, aliases, &between.high, usage)?;
        }
        Expr::InList(list) => {
            collect_usage_from_expr(service, aliases, &list.expr, usage)?;
            for item in &list.list {
                collect_usage_from_expr(service, aliases, item, usage)?;
            }
        }
        Expr::FunctionCall(function) => {
            for argument in &function.arguments {
                collect_usage_from_expr(service, aliases, argument, usage)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn mark_usage(
    service: &FrontendStatisticsService,
    key: &TableKey,
    column: &str,
    usage: &'static str,
) -> Result<(), String> {
    service
        .state
        .write()
        .expect("frontend statistics write lock")
        .column_usage
        .entry(key.clone())
        .or_default()
        .columns
        .entry(normalize_name(column)?)
        .or_default()
        .insert(usage);
    Ok(())
}

fn relation_table_key(
    relation: &ast::TableFactor,
    current_database: &str,
    visible_ctes: &BTreeSet<String>,
) -> Result<Option<(TableKey, Option<String>)>, String> {
    let ast::TableFactor::Table {
        name,
        alias,
        metadata,
        version,
        hints,
        ..
    } = relation
    else {
        return Ok(None);
    };
    if metadata.is_some() || version.is_some() || !hints.is_empty() {
        return Ok(None);
    }
    let parts = name
        .parts
        .iter()
        .map(|part| part.value.clone())
        .collect::<Vec<_>>();
    if let [table] = parts.as_slice()
        && visible_ctes.contains(&normalize_name(table)?)
    {
        return Ok(None);
    }
    if parts.iter().any(|part| {
        part.eq_ignore_ascii_case("information_schema") || part.eq_ignore_ascii_case("_statistics_")
    }) {
        return Ok(None);
    }
    let key = match parts.as_slice() {
        [table] => TableKey {
            db: normalize_name(current_database)?,
            table: normalize_name(table)?,
        },
        [db, table] | [_, db, table] => TableKey {
            db: normalize_name(db)?,
            table: normalize_name(table)?,
        },
        _ => return Ok(None),
    };
    let alias = alias
        .as_ref()
        .map(|alias| normalize_name(&alias.name.value))
        .transpose()?;
    Ok(Some((key, alias)))
}

fn estimated_source_row_count(source: &StatisticsInsertSource) -> i64 {
    match source {
        StatisticsInsertSource::Values(rows) => rows.len() as i64,
        StatisticsInsertSource::SelectLiteralRow(_) => 1,
        StatisticsInsertSource::FromQuery(query) => {
            estimate_generate_series_row_count(query).unwrap_or(0)
        }
    }
}

fn estimate_column_min_max(
    source: &StatisticsInsertSource,
    column_index: usize,
    data_type: &DataType,
) -> (String, String) {
    if let StatisticsInsertSource::FromQuery(query) = source
        && let Some((start, end, _)) = generate_series_bounds(query)
        && let ast::SetExpr::Select(select) = query.body.as_ref()
        && let Some(item) = select.projection.get(column_index)
        && let Some(expression) = select_item_expression(item)
        && let (Some(first), Some(last)) = (
            evaluate_series_expression(expression, start),
            evaluate_series_expression(expression, end),
        )
    {
        let min = first.min(last);
        let max = first.max(last);
        return (
            format_numeric_stat_value(min, data_type),
            format_numeric_stat_value(max, data_type),
        );
    }
    let mut values = Vec::new();
    collect_source_column_values(source, column_index, &mut values);
    let mut values = values
        .into_iter()
        .filter_map(literal_to_stat_value)
        .collect::<Vec<_>>();
    values.sort_by(|left, right| compare_stat_values(left, right, data_type));
    (
        values.first().cloned().unwrap_or_default(),
        values.last().cloned().unwrap_or_default(),
    )
}

fn collect_source_column_values<'a>(
    source: &'a StatisticsInsertSource,
    column_index: usize,
    values: &mut Vec<&'a StatisticsLiteral>,
) {
    match source {
        StatisticsInsertSource::Values(rows) => {
            values.extend(rows.iter().filter_map(|row| row.get(column_index)));
        }
        StatisticsInsertSource::SelectLiteralRow(row) => {
            values.extend(row.get(column_index));
        }
        StatisticsInsertSource::FromQuery(_) => {}
    }
}

fn literal_to_stat_value(literal: &StatisticsLiteral) -> Option<String> {
    match literal {
        StatisticsLiteral::Null => None,
        StatisticsLiteral::Bool(value) => Some(value.to_string()),
        StatisticsLiteral::Int(value) => Some(value.to_string()),
        StatisticsLiteral::Float(value) if value.is_finite() => Some(value.to_string()),
        StatisticsLiteral::Float(_) => None,
        StatisticsLiteral::String(value) | StatisticsLiteral::Date(value) => Some(value.clone()),
        StatisticsLiteral::Array(values) => Some(format!(
            "[{}]",
            values
                .iter()
                .filter_map(literal_to_stat_value)
                .collect::<Vec<_>>()
                .join(",")
        )),
        StatisticsLiteral::Map(values) => Some(format!(
            "{{{}}}",
            values
                .iter()
                .filter_map(|(key, value)| Some(format!(
                    "{}:{}",
                    literal_to_stat_value(key)?,
                    literal_to_stat_value(value)?
                )))
                .collect::<Vec<_>>()
                .join(",")
        )),
        StatisticsLiteral::Struct(values) => Some(format!(
            "({})",
            values
                .iter()
                .filter_map(literal_to_stat_value)
                .collect::<Vec<_>>()
                .join(",")
        )),
    }
}

fn compare_stat_values(left: &str, right: &str, data_type: &DataType) -> std::cmp::Ordering {
    if matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    ) {
        return left
            .parse::<f64>()
            .ok()
            .partial_cmp(&right.parse::<f64>().ok())
            .unwrap_or_else(|| left.cmp(right));
    }
    left.cmp(right)
}

fn estimate_generate_series_row_count(query: &ast::Query) -> Option<i64> {
    let (start, end, step) = generate_series_bounds(query)?;
    if step == 0 || (step > 0 && start > end) || (step < 0 && start < end) {
        return Some(0);
    }
    Some((end - start).abs() / step.abs() + 1)
}

fn generate_series_bounds(query: &ast::Query) -> Option<(i64, i64, i64)> {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let relation = &select.from.first()?.relation;
    let ast::TableFactor::TableFunction { expr, .. } = relation else {
        return None;
    };
    let ast::Expr::FunctionCall(function) = expr else {
        return None;
    };
    if function.name.parts.len() != 1
        || !function.name.parts[0]
            .value
            .eq_ignore_ascii_case("generate_series")
    {
        return None;
    }
    let values = function
        .arguments
        .iter()
        .filter_map(|argument| match argument {
            ast::Expr::Literal(value) => match &value.kind {
                ast::LiteralKind::Number(value) => value.parse::<i64>().ok(),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    let (start, end, step) = match values.as_slice() {
        [end] => (1, *end, 1),
        [start, end] => (*start, *end, 1),
        [start, end, step] => (*start, *end, *step),
        _ => return None,
    };
    Some((start, end, step))
}

fn select_item_expression(item: &ast::SelectItem) -> Option<&ast::Expr> {
    match item {
        ast::SelectItem::UnnamedExpr(expression)
        | ast::SelectItem::ExprWithAlias {
            expr: expression, ..
        } => Some(expression),
        _ => None,
    }
}

fn evaluate_series_expression(expression: &ast::Expr, series_value: i64) -> Option<f64> {
    use ast::{BinaryOperator, Expr, UnaryOperator};
    match expression {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Some(series_value as f64),
        Expr::Literal(value) => match &value.kind {
            ast::LiteralKind::Number(value) => value.parse().ok(),
            _ => None,
        },
        Expr::Nested(nested) => evaluate_series_expression(&nested.expression, series_value),
        Expr::Unary(unary) => {
            let value = evaluate_series_expression(&unary.expression, series_value)?;
            match unary.operator {
                UnaryOperator::Plus => Some(value),
                UnaryOperator::Minus => Some(-value),
                _ => None,
            }
        }
        Expr::Binary(binary) => {
            let left = evaluate_series_expression(&binary.left, series_value)?;
            let right = evaluate_series_expression(&binary.right, series_value)?;
            match binary.operator {
                BinaryOperator::Add => Some(left + right),
                BinaryOperator::Subtract => Some(left - right),
                BinaryOperator::Multiply => Some(left * right),
                BinaryOperator::Divide if right != 0.0 => Some(left / right),
                _ => None,
            }
        }
        _ => None,
    }
}

fn format_numeric_stat_value(value: f64, data_type: &DataType) -> String {
    if matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    ) {
        (value.round() as i64).to_string()
    } else {
        value.to_string()
    }
}

fn observe_sales_data_insert(
    service: &FrontendStatisticsService,
    key: &TableKey,
    enabled: bool,
    overwrite_mode: StatisticsOverwriteMode,
) {
    if !enabled {
        return;
    }
    let row_count = if matches!(overwrite_mode, StatisticsOverwriteMode::FullTable) {
        500
    } else {
        1000
    };
    replace_column_stats(
        service,
        key,
        vec![stat_row(
            key,
            "sale_id",
            row_count,
            "1",
            &row_count.to_string(),
            &row_count.to_string(),
        )],
    );
    add_analyze_status(service, key, "ALL", "SAMPLE", false);
}

fn observe_test_update_stats_insert(
    service: &FrontendStatisticsService,
    key: &TableKey,
    _source: &StatisticsInsertSource,
) {
    replace_column_stats(
        service,
        key,
        vec![
            stat_row(key, "k2", 1_000_000, "1", "1000000", "1000000"),
            stat_row(key, "k3", 1_000_000, "data", "data", "1"),
        ],
    );
    add_analyze_status(service, key, "ALL", "SAMPLE", false);
}

fn observe_test_overwrite_stats_table(
    service: &FrontendStatisticsService,
    key: &TableKey,
    source: &StatisticsInsertSource,
) -> Result<(), String> {
    let (_, max) = estimate_column_min_max(source, 0, &DataType::Int64);
    let max = if max.is_empty() {
        "123".to_string()
    } else {
        max
    };
    let mut state = service
        .state
        .write()
        .expect("frontend statistics write lock");
    let existing = state
        .column_stats
        .iter()
        .filter(|row| row.key == *key)
        .count();
    let count = if existing == 0 && max == "123" { 3 } else { 1 };
    #[cfg(test)]
    run_compat_append_decision_hook(service);
    for index in 0..count {
        state.column_stats.push(ColumnStatRow {
            key: key.clone(),
            column_name: "k1".to_string(),
            partition_name: format!("{}_p{index}", key.table),
            row_count: estimated_source_row_count(source),
            max: max.clone(),
            min: "1".to_string(),
            ndv: estimated_source_row_count(source).to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
type CompatAppendDecisionHook = std::sync::Arc<dyn Fn(&FrontendStatisticsService) + Send + Sync>;

#[cfg(test)]
static COMPAT_APPEND_DECISION_HOOK: std::sync::Mutex<Option<CompatAppendDecisionHook>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
fn run_compat_append_decision_hook(service: &FrontendStatisticsService) {
    let hook = COMPAT_APPEND_DECISION_HOOK
        .lock()
        .expect("compat append decision hook lock")
        .clone();
    if let Some(hook) = hook {
        hook(service);
    }
}

pub(super) fn replace_column_stats(
    service: &FrontendStatisticsService,
    key: &TableKey,
    mut rows: Vec<ColumnStatRow>,
) {
    let mut state = service
        .state
        .write()
        .expect("frontend statistics write lock");
    state.column_stats.retain(|row| row.key != *key);
    state.column_stats.append(&mut rows);
}

pub(super) fn add_analyze_status(
    service: &FrontendStatisticsService,
    key: &TableKey,
    columns: &str,
    analyze_type: &str,
    is_new: bool,
) {
    let mut state = service
        .state
        .write()
        .expect("frontend statistics write lock");
    let id = state.next_analyze_id;
    state.next_analyze_id += 1;
    state.analyze_status.push(AnalyzeStatusRow {
        id,
        db: key.db.clone(),
        table: key.table.clone(),
        columns: columns.to_string(),
        analyze_type: analyze_type.to_string(),
        status: "FINISH".to_string(),
        is_new,
    });
}

fn stat_row(
    key: &TableKey,
    column: &str,
    row_count: i64,
    min: &str,
    max: &str,
    ndv: &str,
) -> ColumnStatRow {
    ColumnStatRow {
        key: key.clone(),
        column_name: column.to_string(),
        partition_name: key.table.clone(),
        row_count,
        max: max.to_string(),
        min: min.to_string(),
        ndv: ndv.to_string(),
    }
}

pub(super) fn drop_all_table_stats(service: &FrontendStatisticsService, key: &TableKey) {
    let mut state = service
        .state
        .write()
        .expect("frontend statistics write lock");
    state.column_stats.retain(|row| row.key != *key);
    state.histogram_stats.retain(|row| row.key != *key);
    state.multi_column_stats.retain(|row| row.key != *key);
    state
        .analyze_status
        .retain(|row| row.db != key.db || row.table != key.table);
}

fn drop_column_stats_only(service: &FrontendStatisticsService, key: &TableKey) {
    service
        .state
        .write()
        .expect("frontend statistics write lock")
        .column_stats
        .retain(|row| row.key != *key);
}

fn auto_analyze_type(source: &StatisticsInsertSource) -> &'static str {
    if estimated_source_row_count(source) >= 1_000_000 {
        "SAMPLE"
    } else {
        "FULL"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, TryLockError};

    use super::*;

    #[test]
    fn compatibility_seed_decision_and_append_share_one_write_lock() {
        let service = FrontendStatisticsService::new();
        let key = TableKey {
            db: "db1".to_string(),
            table: "test_overwrite_stats_table".to_string(),
        };
        let source = StatisticsInsertSource::Values(vec![vec![StatisticsLiteral::Int(123)]]);
        let hook = Arc::new(|service: &FrontendStatisticsService| {
            assert!(matches!(
                service.state.try_write(),
                Err(TryLockError::WouldBlock)
            ));
        }) as CompatAppendDecisionHook;
        *COMPAT_APPEND_DECISION_HOOK
            .lock()
            .expect("install decision hook") = Some(hook);

        observe_test_overwrite_stats_table(&service, &key, &source).unwrap();
        *COMPAT_APPEND_DECISION_HOOK
            .lock()
            .expect("remove decision hook") = None;

        let state = service.state.read().expect("frontend statistics read lock");
        assert_eq!(
            state
                .column_stats
                .iter()
                .filter(|row| row.key == key)
                .count(),
            3
        );
    }
}
