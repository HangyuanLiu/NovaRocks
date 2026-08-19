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

//! DDL/DML statement handlers for the standalone engine.
//!
//! Top-level dispatchers route statement families that remain in the core
//! command kernel to connector-owned catalogs based on the parsed name and
//! current catalog/database session context.

use std::sync::Arc;

use crate::catalog_application::query_catalog::drop_local_table_registration_if_exists;
use crate::runtime::statement_result::StatementResult;
use bytes::Bytes;
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::identifier::resolve_local_table_name;
use novarocks_catalog::schema::SqlType;
use novarocks_spi::connector::ConnectorControlRegistry;
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorColumnAggregation, ConnectorColumnDefinition,
    ConnectorColumnPath, ConnectorColumnPosition, ConnectorDataType, ConnectorDefaultValue,
    ConnectorDropTableDataDisposition, ConnectorErrorKind, ConnectorInstanceId,
    ConnectorNamespaceIdentity, ConnectorPartitionTransform, ConnectorTableIdentity,
    ConnectorTableKey, ConnectorTableKeyKind, ConnectorViewIdentity, ConnectorViewRequest,
    CreatePolicy, DropPolicy,
};
use novarocks_sql::syntax::StarRocksDialect;
use novarocks_sql::syntax::{CreateTableKind, DefaultLiteral, Literal, ObjectName};
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Token;

use novarocks_sql::syntax::sqlparser_expr_to_literal;

/// Exact dependencies needed by catalog-drop statements.
///
/// This deliberately does not expose the standalone application aggregate:
/// catalog DDL needs only catalog admission, exact-generation connector
/// control, local catalog invalidation, MV guards, and view metadata lookup.
pub trait CatalogDropContext:
    crate::catalog_application::resolver::CatalogAdmission
    + crate::catalog_application::query_catalog::CatalogServiceSource
{
    fn connector_control(&self) -> &dyn ConnectorControlRegistry;
    fn mv_repository(&self) -> &dyn crate::mv::domain::repository::MvRepository;
    fn mv_storage_observation(&self) -> &dyn novarocks_spi::connector::MvStorageObservationPort;
}

/// Convert a sqlparser DELETE AST to our custom DeleteStmt.
///
/// Phase 1 restrictions:
/// - Exactly one table in `FROM`; `USING` clauses are rejected.
/// - `WHERE` is mandatory. `DELETE FROM t` (no filter) is rejected — the
///   spec recommends `INSERT OVERWRITE t SELECT * FROM t WHERE FALSE` instead.
/// - `LIMIT` and `ORDER BY` are rejected.
pub fn convert_sqlparser_delete_to_custom(
    delete: &sqlparser::ast::Delete,
) -> Result<novarocks_sql::syntax::DeleteStmt, String> {
    use sqlparser::ast as sqlast;

    let tables = match &delete.from {
        sqlast::FromTable::WithFromKeyword(tables) => tables,
        sqlast::FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return Err(format!(
            "phase 1 DELETE supports exactly one table in FROM, got {}",
            tables.len()
        ));
    }
    if !tables[0].joins.is_empty() {
        return Err("phase 1 DELETE does not support JOIN in FROM".to_string());
    }
    let table = match &tables[0].relation {
        sqlast::TableFactor::Table { name, .. } => {
            novarocks_sql::syntax::convert_object_name(name.clone())?
        }
        other => {
            return Err(format!(
                "phase 1 DELETE source must be a table, got {other:?}"
            ));
        }
    };
    if delete.using.as_ref().is_some_and(|u| !u.is_empty()) {
        return Err("phase 1 DELETE does not support USING".to_string());
    }
    if delete.limit.is_some() {
        return Err("phase 1 DELETE does not support LIMIT".to_string());
    }
    if !delete.order_by.is_empty() {
        return Err("phase 1 DELETE does not support ORDER BY".to_string());
    }
    let where_clause = delete.selection.clone().ok_or_else(|| {
        "DELETE requires a WHERE clause; for full table replacement use \
         INSERT OVERWRITE t SELECT * FROM t WHERE FALSE"
            .to_string()
    })?;
    Ok(novarocks_sql::syntax::DeleteStmt {
        table,
        where_clause,
    })
}

pub fn convert_sqlparser_update_to_custom(
    statement: &sqlparser::ast::Statement,
) -> Result<novarocks_sql::syntax::UpdateStmt, String> {
    use novarocks_sql::syntax::{UpdateAssignment, UpdateStmt};
    use sqlparser::ast as sqlast;

    let sqlast::Statement::Update(update) = statement else {
        return Err("expected UPDATE statement".to_string());
    };
    let sqlast::Update {
        update_token,
        optimizer_hint,
        table,
        assignments,
        from,
        selection,
        returning,
        or,
        limit,
    } = update;
    let _ = update_token;
    if optimizer_hint.is_some() {
        return Err("UPDATE optimizer hints are not supported".to_string());
    }
    if or.is_some() {
        return Err("UPDATE conflict clauses are not supported".to_string());
    }
    if returning.is_some() {
        return Err("UPDATE RETURNING is not supported".to_string());
    }
    if limit.is_some() {
        return Err("UPDATE LIMIT is not supported".to_string());
    }
    if !table.joins.is_empty() {
        return Err(
            "UPDATE target joins are not supported; use UPDATE ... FROM with a single source relation"
                .to_string(),
        );
    }

    let (target_name, target_alias) = match &table.relation {
        sqlast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => {
            reject_update_table_modifiers(
                args,
                with_hints,
                version,
                *with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
                "UPDATE target",
            )?;
            (
                novarocks_sql::syntax::convert_object_name(name.clone())?,
                update_alias_name(alias, "UPDATE target")?,
            )
        }
        sqlast::TableFactor::Pivot { .. } | sqlast::TableFactor::Unpivot { .. } => {
            return Err("UPDATE target pivot/unpivot are not supported".to_string());
        }
        other => return Err(format!("UPDATE target must be a table, got {other:?}")),
    };

    let mut out_assignments = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let sqlast::AssignmentTarget::ColumnName(column_name) = &assignment.target else {
            return Err("only single-column UPDATE assignments are supported".to_string());
        };
        let column = novarocks_sql::syntax::convert_object_name(column_name.clone())?;
        if column.parts.len() != 1 {
            return Err(format!(
                "UPDATE assignment must reference an unqualified target column, got `{column_name}`"
            ));
        }
        out_assignments.push(UpdateAssignment {
            column: column.parts[0].clone(),
            value: assignment.value.clone(),
        });
    }
    if out_assignments.is_empty() {
        return Err("UPDATE requires at least one assignment".to_string());
    }

    let source = convert_update_from_source(from)?;
    Ok(UpdateStmt {
        table: target_name,
        alias: target_alias,
        assignments: out_assignments,
        source,
        where_clause: selection.clone(),
    })
}

pub fn convert_sqlparser_merge_to_custom(
    statement: &sqlparser::ast::Statement,
) -> Result<novarocks_sql::syntax::MergeStmt, String> {
    use novarocks_sql::syntax::{
        MergeMatchedAction, MergeNotMatchedAction, MergeStmt, MergeWhenClause, MutationSource,
        UpdateAssignment,
    };
    use sqlparser::ast as sqlast;

    let sqlast::Statement::Merge(merge) = statement else {
        return Err("expected MERGE statement".to_string());
    };
    let sqlast::Merge {
        merge_token,
        optimizer_hint,
        into: _,
        table,
        source,
        on,
        clauses,
        output,
    } = merge;
    let _ = merge_token;
    if optimizer_hint.is_some() {
        return Err("MERGE optimizer hints are not supported".to_string());
    }
    if output.is_some() {
        return Err("MERGE OUTPUT is not supported".to_string());
    }

    let (target_name, target_alias) = match table {
        sqlast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => {
            reject_update_table_modifiers(
                args,
                with_hints,
                version,
                *with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
                "MERGE target",
            )?;
            (
                novarocks_sql::syntax::convert_object_name(name.clone())?,
                update_alias_name(alias, "MERGE target")?,
            )
        }
        sqlast::TableFactor::Pivot { .. } | sqlast::TableFactor::Unpivot { .. } => {
            return Err("MERGE target pivot/unpivot are not supported".to_string());
        }
        other => return Err(format!("MERGE target must be a table, got {other:?}")),
    };

    let source = match source {
        sqlast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => {
            reject_update_table_modifiers(
                args,
                with_hints,
                version,
                *with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
                "MERGE source",
            )?;
            MutationSource::Table {
                name: novarocks_sql::syntax::convert_object_name(name.clone())?,
                alias: update_alias_name(alias, "MERGE source")?,
            }
        }
        sqlast::TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if *lateral {
                return Err("MERGE source lateral subqueries are not supported".to_string());
            }
            if sample.is_some() {
                return Err("MERGE source samples are not supported".to_string());
            }
            MutationSource::Query {
                query: subquery.clone(),
                alias: update_alias_name(alias, "MERGE source")?,
            }
        }
        sqlast::TableFactor::Pivot { .. } | sqlast::TableFactor::Unpivot { .. } => {
            return Err("MERGE source pivot/unpivot are not supported".to_string());
        }
        other => return Err(format!("unsupported MERGE source: {other:?}")),
    };

    let mut matched: Option<MergeWhenClause<MergeMatchedAction>> = None;
    let mut not_matched: Option<MergeWhenClause<MergeNotMatchedAction>> = None;
    for clause in clauses {
        let sqlast::MergeClause {
            when_token,
            clause_kind,
            predicate,
            action,
        } = clause;
        let _ = when_token;
        match clause_kind {
            sqlast::MergeClauseKind::Matched => {
                if matched.is_some() {
                    return Err(
                        "MERGE supports at most one WHEN MATCHED clause in this implementation"
                            .to_string(),
                    );
                }
                let action = match action {
                    sqlast::MergeAction::Update(update) => {
                        let sqlast::MergeUpdateExpr {
                            update_token,
                            assignments,
                            update_predicate,
                            delete_predicate,
                        } = update;
                        let _ = update_token;
                        if update_predicate.is_some() || delete_predicate.is_some() {
                            return Err(
                                "MERGE WHEN MATCHED UPDATE WHERE / DELETE WHERE clauses are not supported"
                                    .to_string(),
                            );
                        }
                        let mut out = Vec::with_capacity(assignments.len());
                        for assignment in assignments {
                            let sqlast::AssignmentTarget::ColumnName(column_name) =
                                &assignment.target
                            else {
                                return Err(
                                    "only single-column MERGE UPDATE assignments are supported"
                                        .to_string(),
                                );
                            };
                            let column =
                                novarocks_sql::syntax::convert_object_name(column_name.clone())?;
                            if column.parts.len() != 1 {
                                return Err(format!(
                                    "MERGE UPDATE assignment must reference an unqualified target column, got `{column_name}`"
                                ));
                            }
                            out.push(UpdateAssignment {
                                column: column.parts[0].clone(),
                                value: assignment.value.clone(),
                            });
                        }
                        if out.is_empty() {
                            return Err(
                                "MERGE WHEN MATCHED UPDATE requires at least one assignment"
                                    .to_string(),
                            );
                        }
                        MergeMatchedAction::Update { assignments: out }
                    }
                    sqlast::MergeAction::Delete { .. } => MergeMatchedAction::Delete,
                    sqlast::MergeAction::Insert(_) => {
                        return Err(
                            "MERGE WHEN MATCHED INSERT is not valid; use UPDATE or DELETE"
                                .to_string(),
                        );
                    }
                };
                matched = Some(MergeWhenClause {
                    predicate: predicate.clone(),
                    action,
                });
            }
            sqlast::MergeClauseKind::NotMatched | sqlast::MergeClauseKind::NotMatchedByTarget => {
                if not_matched.is_some() {
                    return Err(
                        "MERGE supports at most one WHEN NOT MATCHED clause in this implementation"
                            .to_string(),
                    );
                }
                let action = match action {
                    sqlast::MergeAction::Insert(insert) => {
                        let sqlast::MergeInsertExpr {
                            insert_token,
                            columns,
                            kind_token,
                            kind,
                            insert_predicate,
                        } = insert;
                        let _ = (insert_token, kind_token);
                        if insert_predicate.is_some() {
                            return Err(
                                "MERGE WHEN NOT MATCHED INSERT WHERE clauses are not supported"
                                    .to_string(),
                            );
                        }
                        let columns_out: Vec<String> = columns
                            .iter()
                            .map(|name| {
                                let parts =
                                    novarocks_sql::syntax::convert_object_name(name.clone())?;
                                if parts.parts.len() != 1 {
                                    return Err(format!(
                                        "MERGE INSERT column must be unqualified, got `{name}`"
                                    ));
                                }
                                Ok::<_, String>(parts.parts[0].clone())
                            })
                            .collect::<Result<_, _>>()?;
                        let values = match kind {
                            sqlast::MergeInsertKind::Values(values) => {
                                if values.rows.len() != 1 {
                                    return Err(format!(
                                        "MERGE WHEN NOT MATCHED INSERT VALUES requires exactly one row tuple, got {}",
                                        values.rows.len()
                                    ));
                                }
                                values.rows[0].clone()
                            }
                            sqlast::MergeInsertKind::Row => {
                                return Err(
                                    "MERGE WHEN NOT MATCHED INSERT ROW shorthand is not supported; \
                                     spell out VALUES (...) explicitly"
                                        .to_string(),
                                );
                            }
                        };
                        if !columns_out.is_empty() && columns_out.len() != values.len() {
                            return Err(format!(
                                "MERGE INSERT column count {} does not match VALUES count {}",
                                columns_out.len(),
                                values.len()
                            ));
                        }
                        MergeNotMatchedAction {
                            columns: columns_out,
                            values,
                        }
                    }
                    sqlast::MergeAction::Update(_) | sqlast::MergeAction::Delete { .. } => {
                        return Err("MERGE WHEN NOT MATCHED action must be INSERT".to_string());
                    }
                };
                not_matched = Some(MergeWhenClause {
                    predicate: predicate.clone(),
                    action,
                });
            }
            sqlast::MergeClauseKind::NotMatchedBySource => {
                return Err(
                    "MERGE WHEN NOT MATCHED BY SOURCE is not supported in this implementation"
                        .to_string(),
                );
            }
        }
    }

    if matched.is_none() && not_matched.is_none() {
        return Err("MERGE requires at least one WHEN clause".to_string());
    }

    Ok(MergeStmt {
        table: target_name,
        target_alias,
        source,
        on: (**on).clone(),
        matched,
        not_matched,
    })
}

fn convert_update_from_source(
    from: &Option<sqlparser::ast::UpdateTableFromKind>,
) -> Result<Option<novarocks_sql::syntax::MutationSource>, String> {
    use novarocks_sql::syntax::MutationSource;
    use sqlparser::ast as sqlast;

    let Some(from) = from else {
        return Ok(None);
    };
    let tables = match from {
        sqlast::UpdateTableFromKind::BeforeSet(tables)
        | sqlast::UpdateTableFromKind::AfterSet(tables) => tables,
    };
    if tables.len() != 1 {
        return Err(format!(
            "UPDATE ... FROM supports exactly one source relation, got {}",
            tables.len()
        ));
    }
    if !tables[0].joins.is_empty() {
        return Err("UPDATE ... FROM joins must be wrapped in a subquery".to_string());
    }
    match &tables[0].relation {
        sqlast::TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => {
            reject_update_table_modifiers(
                args,
                with_hints,
                version,
                *with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
                "UPDATE ... FROM source",
            )?;
            Ok(Some(MutationSource::Table {
                name: novarocks_sql::syntax::convert_object_name(name.clone())?,
                alias: update_alias_name(alias, "UPDATE ... FROM source")?,
            }))
        }
        sqlast::TableFactor::Derived {
            lateral,
            subquery,
            alias,
            sample,
        } => {
            if *lateral {
                return Err(
                    "UPDATE ... FROM source lateral subqueries are not supported".to_string(),
                );
            }
            if sample.is_some() {
                return Err("UPDATE ... FROM source samples are not supported".to_string());
            }
            Ok(Some(MutationSource::Query {
                query: subquery.clone(),
                alias: update_alias_name(alias, "UPDATE ... FROM source")?,
            }))
        }
        sqlast::TableFactor::Pivot { .. } | sqlast::TableFactor::Unpivot { .. } => {
            Err("UPDATE ... FROM source pivot/unpivot are not supported".to_string())
        }
        other => Err(format!("unsupported UPDATE ... FROM source: {other:?}")),
    }
}

fn reject_update_table_modifiers(
    args: &Option<sqlparser::ast::TableFunctionArgs>,
    with_hints: &[sqlparser::ast::Expr],
    version: &Option<sqlparser::ast::TableVersion>,
    with_ordinality: bool,
    partitions: &[sqlparser::ast::Ident],
    json_path: &Option<sqlparser::ast::JsonPath>,
    sample: &Option<sqlparser::ast::TableSampleKind>,
    index_hints: &[sqlparser::ast::TableIndexHints],
    context: &str,
) -> Result<(), String> {
    if args.is_some() {
        return Err(format!("{context} table arguments are not supported"));
    }
    if !with_hints.is_empty() {
        return Err(format!("{context} table hints are not supported"));
    }
    if version.is_some() {
        return Err(format!("{context} time travel is not supported"));
    }
    if with_ordinality {
        return Err(format!("{context} WITH ORDINALITY is not supported"));
    }
    if !partitions.is_empty() {
        return Err(format!("{context} partitions are not supported"));
    }
    if json_path.is_some() {
        return Err(format!("{context} JSON paths are not supported"));
    }
    if sample.is_some() {
        return Err(format!("{context} samples are not supported"));
    }
    if !index_hints.is_empty() {
        return Err(format!("{context} index hints are not supported"));
    }
    Ok(())
}

fn update_alias_name(
    alias: &Option<sqlparser::ast::TableAlias>,
    context: &str,
) -> Result<Option<String>, String> {
    let Some(alias) = alias else {
        return Ok(None);
    };
    if !alias.columns.is_empty() {
        return Err(format!("{context} alias column lists are not supported"));
    }
    Ok(Some(alias.name.value.clone()))
}

// ---------------------------------------------------------------------------
// DDL handlers
// ---------------------------------------------------------------------------

/// The narrow catalog mutation surface shared by the legacy engine and the
/// explicit catalog command kernel.  Keep statement helpers on this port so
/// command routing cannot recover an application facade just to resolve a
/// catalog target or issue a provider-owned mutation.
pub trait CatalogMutationContext: crate::catalog_application::resolver::CatalogAdmission {
    fn connector_control(&self) -> &dyn ConnectorControlRegistry;
}

pub(crate) fn execute_create_database_statement(
    context: &impl CatalogMutationContext,
    name: &ObjectName,
    if_not_exists: bool,
    current_catalog: Option<&str>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target = crate::catalog_application::resolver::resolve_namespace_target(
        context,
        name,
        current_catalog,
    )?;
    let instance_id = mutation_instance_id(&target.catalog)?;
    crate::connector::mutation::execute_catalog_mutation(
        context.connector_control(),
        &instance_id,
        ConnectorCatalogMutationOperation::CreateNamespace {
            namespace: ConnectorNamespaceIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace),
            },
            policy: if if_not_exists {
                CreatePolicy::NoOpIfExists
            } else {
                CreatePolicy::FailIfExists
            },
        },
        connector_context.clone(),
    )?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_create_table_statement(
    context: &impl CatalogMutationContext,
    stmt: novarocks_sql::syntax::CreateTableStmt,
    current_catalog: Option<&str>,
    current_database: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let legacy_range_partitions = stmt.legacy_range_partitions.clone();
    if stmt.as_select.is_some() {
        return Err("CTAS must be routed by frontend DML service".to_string());
    }
    match stmt.kind {
        CreateTableKind::Iceberg {
            columns,
            key_desc,
            bucket_count,
            distribution_columns,
            partition_fields,
            properties,
        } => {
            // BITMAP / HLL columns cannot be used as distribution keys —
            // they are opaque blobs with no hash semantics that match a
            // scalar column. Reject the CREATE TABLE before any catalog
            // mutation. Column names are case-insensitive in StarRocks.
            for dist_col in &distribution_columns {
                let dist_lower = dist_col.to_ascii_lowercase();
                if let Some(column) = columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&dist_lower))
                    && matches!(
                        column.data_type,
                        novarocks_catalog::schema::SqlType::Bitmap
                            | novarocks_catalog::schema::SqlType::Hll
                    )
                {
                    return Err(format!(
                        "BITMAP/HLL columns cannot be used as distribution key (column `{}` has type {:?})",
                        column.name, column.data_type
                    ));
                }
            }
            // This validation must precede the connector mutation dispatcher:
            // its reconciliation path may inspect a not-yet-created table,
            // whereas an invalid partition source is a deterministic statement
            // error independent of catalog state.
            for partition_field in &partition_fields {
                let source_column = match partition_field {
                    novarocks_sql::syntax::IcebergPartitionFieldExpr::Identity { column }
                    | novarocks_sql::syntax::IcebergPartitionFieldExpr::Year { column }
                    | novarocks_sql::syntax::IcebergPartitionFieldExpr::Month { column }
                    | novarocks_sql::syntax::IcebergPartitionFieldExpr::Day { column }
                    | novarocks_sql::syntax::IcebergPartitionFieldExpr::Hour { column }
                    | novarocks_sql::syntax::IcebergPartitionFieldExpr::Bucket { column, .. }
                    | novarocks_sql::syntax::IcebergPartitionFieldExpr::Truncate {
                        column, ..
                    }
                    | novarocks_sql::syntax::IcebergPartitionFieldExpr::Void { column } => column,
                };
                if let Some(column) = columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(source_column))
                    && matches!(
                        column.data_type,
                        novarocks_catalog::schema::SqlType::Variant
                    )
                {
                    return Err(format!(
                        "iceberg table column `{}` is variant; variant columns cannot appear in the partition spec. Use a non-variant source column for partition transforms.",
                        column.name
                    ));
                }
            }

            let target = crate::catalog_application::resolver::resolve_table_target(
                context,
                &stmt.name,
                current_catalog,
                current_database,
            )?;
            let instance_id = mutation_instance_id(&target.catalog)?;
            let _ = bucket_count;
            crate::connector::mutation::execute_catalog_mutation(
                context.connector_control(),
                &instance_id,
                ConnectorCatalogMutationOperation::CreateTable {
                    table: ConnectorTableIdentity {
                        instance_id: instance_id.clone(),
                        namespace: Arc::from(target.namespace),
                        table: Arc::from(target.table),
                    },
                    columns: columns
                        .iter()
                        .map(connector_column)
                        .collect::<Result<_, _>>()?,
                    key: key_desc.as_ref().map(connector_table_key),
                    partitioning: partition_fields
                        .iter()
                        .map(connector_partition_transform)
                        .collect(),
                    properties: properties
                        .into_iter()
                        .map(|(key, value)| (Arc::from(key), Arc::from(value)))
                        .collect(),
                    policy: if stmt.if_not_exists {
                        CreatePolicy::NoOpIfExists
                    } else {
                        CreatePolicy::FailIfExists
                    },
                },
                connector_context.clone(),
            )?;
            let _ = legacy_range_partitions;
            Ok(StatementResult::Ok)
        }
    }
}

fn mutation_instance_id(catalog: &str) -> Result<ConnectorInstanceId, String> {
    ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())
}

pub fn connector_column(
    column: &novarocks_sql::syntax::TableColumnDef,
) -> Result<ConnectorColumnDefinition, String> {
    Ok(ConnectorColumnDefinition {
        name: Arc::from(column.name.as_str()),
        data_type: connector_data_type(&column.data_type)?,
        nullable: column.nullable,
        aggregation: column.aggregation.map(connector_column_aggregation),
        default: column.default.as_ref().map(connector_default).transpose()?,
    })
}

pub(crate) fn connector_data_type(data_type: &SqlType) -> Result<ConnectorDataType, String> {
    Ok(match data_type {
        SqlType::Boolean => ConnectorDataType::Boolean,
        SqlType::TinyInt => ConnectorDataType::TinyInt,
        SqlType::SmallInt => ConnectorDataType::SmallInt,
        SqlType::Int => ConnectorDataType::Int,
        SqlType::BigInt => ConnectorDataType::BigInt,
        SqlType::LargeInt => ConnectorDataType::LargeInt,
        SqlType::Float => ConnectorDataType::Float,
        SqlType::Double => ConnectorDataType::Double,
        SqlType::Decimal { precision, scale } => ConnectorDataType::Decimal {
            precision: *precision,
            scale: *scale,
        },
        SqlType::String => ConnectorDataType::String,
        SqlType::Json => ConnectorDataType::Json,
        SqlType::Binary => ConnectorDataType::Binary,
        SqlType::Bitmap => ConnectorDataType::Bitmap,
        SqlType::Hll => ConnectorDataType::Hll,
        SqlType::Date => ConnectorDataType::Date,
        SqlType::DateTime => ConnectorDataType::DateTime,
        SqlType::DateTimeNs => ConnectorDataType::DateTimeNs,
        SqlType::Time => ConnectorDataType::Time,
        SqlType::Array(element) => {
            ConnectorDataType::Array(Box::new(connector_data_type(element)?))
        }
        SqlType::Map(key, value) => ConnectorDataType::Map(
            Box::new(connector_data_type(key)?),
            Box::new(connector_data_type(value)?),
        ),
        SqlType::Struct(fields) => ConnectorDataType::Struct(
            fields
                .iter()
                .map(|(name, data_type)| {
                    Ok(novarocks_spi::connector::ConnectorStructField {
                        name: Arc::from(name.as_str()),
                        data_type: connector_data_type(data_type)?,
                        // SQL's current struct AST has no child-nullability bit.
                        nullable: true,
                    })
                })
                .collect::<Result<_, String>>()?,
        ),
        SqlType::Variant => ConnectorDataType::Variant,
    })
}

fn connector_default(value: &DefaultLiteral) -> Result<ConnectorDefaultValue, String> {
    Ok(match value {
        DefaultLiteral::Null => ConnectorDefaultValue::Null,
        DefaultLiteral::Bool(value) => ConnectorDefaultValue::Bool(*value),
        DefaultLiteral::Int(value) => ConnectorDefaultValue::Int(*value),
        DefaultLiteral::Float(value) => ConnectorDefaultValue::Float(*value),
        DefaultLiteral::Decimal { unscaled, scale } => ConnectorDefaultValue::Decimal {
            unscaled: *unscaled,
            scale: *scale,
        },
        DefaultLiteral::String(value) => ConnectorDefaultValue::String(Arc::from(value.as_str())),
        DefaultLiteral::Date(value) => ConnectorDefaultValue::Date(*value),
        DefaultLiteral::DateTime(value) => ConnectorDefaultValue::DateTime(*value),
        DefaultLiteral::Binary(value) => {
            ConnectorDefaultValue::Binary(Bytes::copy_from_slice(value))
        }
    })
}

fn connector_column_aggregation(
    aggregation: novarocks_sql::syntax::ColumnAggregation,
) -> ConnectorColumnAggregation {
    match aggregation {
        novarocks_sql::syntax::ColumnAggregation::Sum => ConnectorColumnAggregation::Sum,
        novarocks_sql::syntax::ColumnAggregation::Min => ConnectorColumnAggregation::Min,
        novarocks_sql::syntax::ColumnAggregation::Max => ConnectorColumnAggregation::Max,
        novarocks_sql::syntax::ColumnAggregation::Replace => ConnectorColumnAggregation::Replace,
        novarocks_sql::syntax::ColumnAggregation::ReplaceIfNotNull => {
            ConnectorColumnAggregation::ReplaceIfNotNull
        }
        novarocks_sql::syntax::ColumnAggregation::BitmapUnion => {
            ConnectorColumnAggregation::BitmapUnion
        }
        novarocks_sql::syntax::ColumnAggregation::HllUnion => ConnectorColumnAggregation::HllUnion,
    }
}

pub(crate) fn connector_table_key(key: &novarocks_sql::syntax::TableKeyDesc) -> ConnectorTableKey {
    ConnectorTableKey {
        kind: match key.kind {
            novarocks_sql::syntax::TableKeyKind::Duplicate => ConnectorTableKeyKind::Duplicate,
            novarocks_sql::syntax::TableKeyKind::Unique => ConnectorTableKeyKind::Unique,
            novarocks_sql::syntax::TableKeyKind::Aggregate => ConnectorTableKeyKind::Aggregate,
            novarocks_sql::syntax::TableKeyKind::Primary => ConnectorTableKeyKind::Primary,
        },
        columns: key
            .columns
            .iter()
            .map(|column| Arc::from(column.as_str()))
            .collect(),
    }
}

pub fn connector_partition_transform(
    field: &novarocks_sql::syntax::IcebergPartitionFieldExpr,
) -> ConnectorPartitionTransform {
    use novarocks_sql::syntax::IcebergPartitionFieldExpr;
    match field {
        IcebergPartitionFieldExpr::Identity { column } => ConnectorPartitionTransform::Identity {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Year { column } => ConnectorPartitionTransform::Year {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Month { column } => ConnectorPartitionTransform::Month {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Day { column } => ConnectorPartitionTransform::Day {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Hour { column } => ConnectorPartitionTransform::Hour {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Bucket {
            column,
            num_buckets,
        } => ConnectorPartitionTransform::Bucket {
            column: Arc::from(column.as_str()),
            num_buckets: *num_buckets,
        },
        IcebergPartitionFieldExpr::Truncate { column, width } => {
            ConnectorPartitionTransform::Truncate {
                column: Arc::from(column.as_str()),
                width: *width,
            }
        }
        IcebergPartitionFieldExpr::Void { column } => ConnectorPartitionTransform::Void {
            column: Arc::from(column.as_str()),
        },
    }
}

/// Typed-AST counterpart to [`connector_partition_transform`]. It keeps the
/// legacy partition grammar's single-column and positive-u32 constraints at
/// the semantic lowering boundary, without a source-text round trip.
pub(crate) fn connector_typed_partition_transform(
    field: &novarocks_parser::ast::IcebergPartitionField,
) -> Result<ConnectorPartitionTransform, String> {
    use novarocks_parser::ast::IcebergPartitionField;

    match field {
        IcebergPartitionField::Identity { column, .. } => {
            Ok(ConnectorPartitionTransform::Identity {
                column: typed_partition_column(column)?,
            })
        }
        IcebergPartitionField::Year { column, .. } => Ok(ConnectorPartitionTransform::Year {
            column: typed_partition_column(column)?,
        }),
        IcebergPartitionField::Month { column, .. } => Ok(ConnectorPartitionTransform::Month {
            column: typed_partition_column(column)?,
        }),
        IcebergPartitionField::Day { column, .. } => Ok(ConnectorPartitionTransform::Day {
            column: typed_partition_column(column)?,
        }),
        IcebergPartitionField::Hour { column, .. } => Ok(ConnectorPartitionTransform::Hour {
            column: typed_partition_column(column)?,
        }),
        IcebergPartitionField::Void { column, .. } => Ok(ConnectorPartitionTransform::Void {
            column: typed_partition_column(column)?,
        }),
        IcebergPartitionField::Bucket {
            column, buckets, ..
        } => Ok(ConnectorPartitionTransform::Bucket {
            column: typed_partition_column(column)?,
            num_buckets: typed_positive_u32(buckets, "bucket count")?,
        }),
        IcebergPartitionField::Truncate { column, width, .. } => {
            Ok(ConnectorPartitionTransform::Truncate {
                column: typed_partition_column(column)?,
                width: typed_positive_u32(width, "truncate width")?,
            })
        }
    }
}

fn typed_partition_column(path: &novarocks_parser::ast::ColumnPath) -> Result<Arc<str>, String> {
    let [column] = path.parts.as_slice() else {
        return Err("partition transform requires a single column identifier".to_string());
    };
    Ok(Arc::from(normalize_identifier(&column.value)?))
}

fn typed_positive_u32(
    literal: &novarocks_parser::ast::Literal,
    label: &str,
) -> Result<u32, String> {
    let novarocks_parser::ast::LiteralKind::Number(value) = &literal.kind else {
        return Err(format!("expected numeric {label}"));
    };
    let parsed = value
        .parse::<u32>()
        .map_err(|error| format!("invalid {label} `{value}`: {error}"))?;
    if parsed == 0 {
        return Err(format!("{label} must be positive"));
    }
    Ok(parsed)
}

// Ownership: `ColumnPath` and `AddPosition` are this module's own parsed
// schema-change AST types, so lowering them onto the connector SPI is catalog
// statement work, not query assembly. These two join the sibling converters
// above (`connector_partition_transform`, `connector_table_key`,
// `connector_column_aggregation`) that already own that lowering.
pub(crate) fn connector_schema_path(path: ColumnPath) -> ConnectorColumnPath {
    ConnectorColumnPath {
        segments: path
            .segments()
            .iter()
            .map(|segment| Arc::from(segment.as_str()))
            .collect(),
    }
}

pub(crate) fn connector_schema_position(position: AddPosition) -> ConnectorColumnPosition {
    match position {
        AddPosition::Default => ConnectorColumnPosition::Default,
        AddPosition::First => ConnectorColumnPosition::First,
        AddPosition::After(column) => ConnectorColumnPosition::After {
            column: Arc::from(column),
        },
        AddPosition::Before(column) => ConnectorColumnPosition::Before {
            column: Arc::from(column),
        },
    }
}

pub(crate) fn execute_drop_catalog_statement(
    context: &impl CatalogDropContext,
    catalog_name: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let normalized_catalog = normalize_identifier(catalog_name)?;
    let application = context.catalog_application().ok_or_else(|| {
        "catalog statements require a configured frontend catalog application".to_string()
    })?;
    let instance_id = ConnectorInstanceId::parse(&normalized_catalog)
        .map_err(|error| format!("invalid catalog connector instance ID: {error}"))?;
    // The Frontend application owns the exact-version delete and the MV
    // dependency scan that fences it, both inside one serializable StateStore
    // transaction. Core must not pre-check dependencies outside that fence.
    application
        .drop_catalog(crate::catalog_application::CatalogDropCommand {
            instance_id,
            if_exists,
        })
        .map_err(|error| error.to_string())?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_drop_database_statement(
    context: &impl CatalogDropContext,
    name: &ObjectName,
    current_catalog: Option<&str>,
    if_exists: bool,
    force: bool,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target = crate::catalog_application::resolver::resolve_namespace_target(
        context,
        name,
        current_catalog,
    )?;
    if target.backend_name == "iceberg" {
        ensure_no_iceberg_mv_targets_in_scope(context, &target.catalog, Some(&target.namespace))?;
        ensure_no_external_iceberg_dependents(context, &target.catalog, Some(&target.namespace))?;
    }
    let instance_id = mutation_instance_id(&target.catalog)?;
    if force {
        let lease = context
            .connector_control()
            .acquire_current(&instance_id)
            .map_err(|error| error.to_string())?;
        // `IF EXISTS` applies to the complete FORCE decomposition.  In
        // particular, do not ask a remote catalog to enumerate a namespace
        // which the final DropNamespace mutation would correctly treat as a
        // no-op.
        let namespace_identity = ConnectorNamespaceIdentity {
            instance_id: instance_id.clone(),
            namespace: Arc::from(target.namespace.as_str()),
        };
        let namespace_exists = lease
            .binding()
            .metadata()
            .namespace_exists(novarocks_spi::connector::ConnectorNamespaceRequest {
                namespace: namespace_identity.clone(),
                context: connector_context.clone(),
            })
            .map_err(|error| error.to_string())?;
        if !namespace_exists {
            if if_exists {
                return Ok(StatementResult::Ok);
            }
            return Err(format!("namespace `{}` does not exist", target.namespace));
        }
        let mut tables = lease
            .binding()
            .metadata()
            .list_tables(novarocks_spi::connector::ConnectorListTablesRequest {
                namespace: namespace_identity.clone(),
                context: connector_context.clone(),
            })
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|identity| identity.table.to_string())
            .collect::<Vec<_>>();
        tables.sort();
        let mut views = lease
            .binding()
            .view_metadata()
            .map(|view_metadata| {
                view_metadata.list_views(novarocks_spi::connector::ConnectorListViewsRequest {
                    namespace: namespace_identity,
                    context: connector_context.clone(),
                })
            })
            .transpose()
            .map_err(|error| error.to_string())?
            .unwrap_or_default()
            .into_iter()
            .map(|identity| identity.view.to_string())
            .collect::<Vec<_>>();
        views.sort();
        for table in tables {
            crate::connector::mutation::execute_catalog_mutation(
                context.connector_control(),
                &instance_id,
                ConnectorCatalogMutationOperation::DropTable {
                    table: ConnectorTableIdentity {
                        instance_id: instance_id.clone(),
                        namespace: Arc::from(target.namespace.as_str()),
                        table: Arc::from(table.as_str()),
                    },
                    // FORCE expands a namespace delete from a non-transactional
                    // listing. A child may disappear before its mutation starts,
                    // so every child delete is idempotent; the final namespace
                    // mutation retains the statement-level IF EXISTS contract.
                    policy: DropPolicy::NoOpIfMissing,
                    data_disposition: ConnectorDropTableDataDisposition::Purge,
                },
                connector_context.clone(),
            )?;
            context.catalog_service().invalidate_table(
                &target.catalog,
                &target.namespace,
                &table,
            )?;
            drop_local_table_registration_if_exists(context, &target.namespace, &table)?;
        }
        for view in views {
            crate::connector::mutation::execute_catalog_mutation(
                context.connector_control(),
                &instance_id,
                ConnectorCatalogMutationOperation::DropView {
                    view: ConnectorViewIdentity {
                        instance_id: instance_id.clone(),
                        namespace: Arc::from(target.namespace.as_str()),
                        view: Arc::from(view.as_str()),
                    },
                    policy: DropPolicy::FailIfMissing,
                },
                connector_context.clone(),
            )?;
        }
    }
    crate::connector::mutation::execute_catalog_mutation(
        context.connector_control(),
        &instance_id,
        ConnectorCatalogMutationOperation::DropNamespace {
            namespace: ConnectorNamespaceIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace),
            },
            policy: if if_exists {
                DropPolicy::NoOpIfMissing
            } else {
                DropPolicy::FailIfMissing
            },
        },
        connector_context.clone(),
    )?;
    Ok(StatementResult::Ok)
}

pub(crate) fn execute_drop_table_statement(
    context: &impl CatalogDropContext,
    name: &ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
    if_exists: bool,
    _force: bool,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let target = match crate::catalog_application::resolver::resolve_existing_table_target(
        context,
        name,
        current_catalog,
        current_database,
    ) {
        Ok(target) => target,
        Err(_) if current_catalog.is_none() && name.parts.len() <= 2 => {
            // External parquet tables registered through the embedding API are
            // still catalog-only entries. Dropping them does not involve a
            // connector backend.
            return drop_local_catalog_table(context, name, current_database, if_exists);
        }
        Err(err) => return Err(err),
    };
    let dependency_ref = if target.backend_name == "iceberg" {
        crate::mv::domain::dependency::model::iceberg_table_object_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        )
    } else {
        crate::mv::domain::dependency::model::external_table_object_ref(
            &target.catalog,
            &target.namespace,
            &target.table,
        )
    };
    match crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_ports(
        context.connector_control(),
        context.mv_storage_observation(),
        &target,
        crate::mv::domain::iceberg_guard::IcebergMvUserMutation::DropTable,
    ) {
        Ok(()) => {}
        Err(err)
            if if_exists
                && target.backend_name == "iceberg"
                && is_missing_table_guard_error(&err) =>
        {
            cleanup_iceberg_drop_table_registration_if_exists(context, &target)?;
            return Ok(StatementResult::Ok);
        }
        Err(err) => return Err(err),
    }
    context
        .mv_repository()
        .ensure_no_downstream_dependencies(&dependency_ref)
        .map_err(|error| error.to_string())?;
    let instance_id = mutation_instance_id(&target.catalog)?;
    match crate::connector::mutation::execute_catalog_mutation(
        context.connector_control(),
        &instance_id,
        ConnectorCatalogMutationOperation::DropTable {
            table: ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(target.namespace.as_str()),
                table: Arc::from(target.table.as_str()),
            },
            policy: if if_exists {
                DropPolicy::NoOpIfMissing
            } else {
                DropPolicy::FailIfMissing
            },
            data_disposition: ConnectorDropTableDataDisposition::Purge,
        },
        connector_context.clone(),
    ) {
        Ok(_) => {
            if target.backend_name == "iceberg" {
                context.catalog_service().invalidate_table(
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                )?;
                drop_local_table_registration_if_exists(context, &target.namespace, &target.table)?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) if if_exists && err.contains("NotFound") => {
            if target.backend_name == "iceberg" {
                cleanup_iceberg_drop_table_registration_if_exists(context, &target)?;
            }
            Ok(StatementResult::Ok)
        }
        Err(err) => {
            // A DROP TABLE aimed at a view must say so instead of "unknown
            // table" — views and tables are separate REST resources.
            if target.backend_name == "iceberg"
                && external_view_exists(
                    context,
                    &target.catalog,
                    &target.namespace,
                    &target.table,
                    connector_context,
                )?
            {
                return Err(format!(
                    "{}.{}.{} is a view, use DROP VIEW",
                    target.catalog, target.namespace, target.table
                ));
            }
            Err(err)
        }
    }
}

fn is_missing_table_guard_error(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    lower.contains("unknown table:")
        || lower.contains("table not found")
        || lower.contains("no metadata files")
        // Catalog backends normalize absence differently; the REST client
        // reports that the table does not exist.
        || lower.contains("does not exist")
}

fn cleanup_iceberg_drop_table_registration_if_exists(
    context: &impl CatalogDropContext,
    target: &crate::catalog_application::resolver::TargetBackend,
) -> Result<(), String> {
    context.catalog_service().invalidate_table(
        &target.catalog,
        &target.namespace,
        &target.table,
    )?;
    drop_local_table_registration_if_exists(context, &target.namespace, &target.table)
}

fn drop_local_catalog_table(
    context: &impl CatalogDropContext,
    name: &ObjectName,
    current_database: &str,
    if_exists: bool,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_table_name(name.parts.as_slice(), current_database)?;
    let mut guard = context
        .catalog_service()
        .local()
        .write()
        .expect("standalone catalog write lock");
    match guard.drop_table(&resolved.database, &resolved.table) {
        Ok(()) => Ok(StatementResult::Ok),
        Err(err) if if_exists && err.contains("unknown") => Ok(StatementResult::Ok),
        Err(err) => Err(err),
    }
}

fn ensure_no_iceberg_mv_targets_in_scope(
    context: &impl CatalogDropContext,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let definitions = context
        .mv_repository()
        .list_definitions()
        .map_err(|error| {
            format!("load MV definitions for drop target scope check failed: {error}")
        })?;
    let targets = definitions
        .iter()
        .filter_map(|definition| {
            definition
                .storage_engine
                .eq_ignore_ascii_case("iceberg")
                .then(|| {
                    crate::mv::domain::persistence::dependency::stored_definition_dependency_ref(
                        definition, None,
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    crate::mv::domain::dependency::scope::validate_no_iceberg_mv_targets_in_scope(
        scope_catalog,
        scope_namespace,
        &targets,
    )
}

fn ensure_no_external_iceberg_dependents(
    context: &impl CatalogDropContext,
    scope_catalog: &str,
    scope_namespace: Option<&str>,
) -> Result<(), String> {
    let definitions = context
        .mv_repository()
        .list_definitions()
        .map_err(|error| format!("load MV definitions for drop scope check failed: {error}"))?;
    let mut edges = Vec::with_capacity(definitions.len());
    for definition in definitions {
        let target = crate::mv::domain::persistence::dependency::stored_definition_dependency_ref(
            &definition,
            None,
        )?;
        let upstreams = context
            .mv_repository()
            .list_dependencies_by_downstream(definition.mv_id)
            .map_err(|error| format!("load MV dependencies for drop scope check failed: {error}"))?
            .into_iter()
            .map(|dependency| dependency.upstream)
            .collect();
        edges.push((target, upstreams));
    }
    crate::mv::domain::dependency::scope::validate_no_external_dependents_for_scope(
        scope_catalog,
        scope_namespace,
        &edges,
    )
}

fn external_view_exists(
    context: &impl CatalogDropContext,
    catalog: &str,
    namespace: &str,
    view: &str,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<bool, String> {
    let lease =
        crate::connector::acquire_metadata_planning_lease(context.connector_control(), catalog)?;
    let binding = lease.binding();
    let Some(view_metadata) = binding.view_metadata() else {
        return Ok(false);
    };
    let instance_id = binding.descriptor().instance_id.clone();
    match view_metadata.load_view(ConnectorViewRequest {
        view: ConnectorViewIdentity {
            instance_id,
            namespace: Arc::from(namespace),
            view: Arc::from(view),
        },
        context: connector_context.clone(),
    }) {
        Ok(_) => Ok(true),
        Err(error)
            if matches!(
                error.kind(),
                ConnectorErrorKind::NotFound | ConnectorErrorKind::Unsupported
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error.to_string()),
    }
}

// ---------------------------------------------------------------------------
// DML handlers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ADD FILES SQL parsing
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct AddEqualityDeleteStmt {
    pub table: ObjectName,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Literal>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AlterIcebergSchemaStmt {
    pub(crate) table: ObjectName,
    pub(crate) change: IcebergSchemaChange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AlterIcebergPropertiesStmt {
    pub(crate) table: ObjectName,
    pub(crate) op: PropertiesOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PropertiesOp {
    Set { entries: Vec<(String, String)> },
    Unset { keys: Vec<String>, if_exists: bool },
}

/// One typed partition-spec mutation after syntax has been lowered to the
/// connector-owned representation. Table resolution remains with command
/// execution, which owns the target catalog and mutation admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IcebergPartitionSpecChange {
    Add(ConnectorPartitionTransform),
    Drop(ConnectorPartitionTransform),
}

/// Lower typed Iceberg table-property syntax without reparsing SQL text.
/// The legacy path admits string-valued user properties only, so typed
/// literals retain that same semantic boundary here.
pub(crate) fn lower_typed_iceberg_properties_action(
    action: &novarocks_parser::ast::IcebergPropertiesAction,
) -> Result<PropertiesOp, String> {
    use novarocks_parser::ast::IcebergPropertiesAction;

    match action {
        IcebergPropertiesAction::Set { entries } => {
            if entries.is_empty() {
                return Err("SET TBLPROPERTIES requires at least one key=value pair".to_string());
            }
            let mut seen = std::collections::HashSet::new();
            let mut lowered = Vec::with_capacity(entries.len());
            for entry in entries {
                let key = entry.key.value.clone();
                if !seen.insert(key.clone()) {
                    return Err(format!("duplicate key '{key}' in SET TBLPROPERTIES"));
                }
                lowered.push((key, lower_typed_property_string(&entry.value)?));
            }
            Ok(PropertiesOp::Set { entries: lowered })
        }
        IcebergPropertiesAction::Unset { keys, if_exists } => {
            if keys.is_empty() {
                return Err("UNSET TBLPROPERTIES requires at least one key".to_string());
            }
            let mut seen = std::collections::HashSet::new();
            let mut lowered = Vec::with_capacity(keys.len());
            for key in keys {
                let key = key.key.value.clone();
                if !seen.insert(key.clone()) {
                    return Err(format!("duplicate key '{key}' in UNSET TBLPROPERTIES"));
                }
                lowered.push(key);
            }
            Ok(PropertiesOp::Unset {
                keys: lowered,
                if_exists: *if_exists,
            })
        }
        IcebergPropertiesAction::Comment { value } => Ok(PropertiesOp::Set {
            entries: vec![("comment".to_string(), lower_typed_property_string(value)?)],
        }),
    }
}

/// Lower a typed partition change directly to the connector representation.
pub(crate) fn lower_typed_iceberg_partition_change(
    change: &novarocks_parser::ast::IcebergPartitionChange,
) -> Result<IcebergPartitionSpecChange, String> {
    match change {
        novarocks_parser::ast::IcebergPartitionChange::Add { field } => Ok(
            IcebergPartitionSpecChange::Add(connector_typed_partition_transform(field)?),
        ),
        novarocks_parser::ast::IcebergPartitionChange::Drop { field } => Ok(
            IcebergPartitionSpecChange::Drop(connector_typed_partition_transform(field)?),
        ),
    }
}

fn lower_typed_property_string(literal: &novarocks_parser::ast::Literal) -> Result<String, String> {
    let novarocks_parser::ast::LiteralKind::String(value) = &literal.kind else {
        return Err("TBLPROPERTIES key/value must be a string literal".to_string());
    };
    Ok(value.clone())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnPath {
    segments: Vec<String>,
}

impl ColumnPath {
    pub(crate) fn root() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub(crate) fn parse(input: &str) -> Result<Self, String> {
        if input.is_empty() {
            return Err("column path is empty".to_string());
        }
        let mut segments = Vec::new();
        for raw in input.split('.') {
            if raw.is_empty() {
                return Err(format!("invalid column path '{input}': empty segment"));
            }
            segments.push(raw.to_ascii_lowercase());
        }
        Ok(Self { segments })
    }

    pub(crate) fn from_segments(segments: Vec<String>) -> Self {
        Self {
            segments: segments
                .into_iter()
                .map(|s| s.to_ascii_lowercase())
                .collect(),
        }
    }

    pub(crate) fn segments(&self) -> &[String] {
        &self.segments
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub(crate) fn last(&self) -> Option<&str> {
        self.segments.last().map(String::as_str)
    }

    pub(crate) fn parent(&self) -> ColumnPath {
        if self.segments.is_empty() {
            return ColumnPath::root();
        }
        Self {
            segments: self.segments[..self.segments.len() - 1].to_vec(),
        }
    }

    pub(crate) fn dotted(&self) -> String {
        self.segments.join(".")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AddPosition {
    Default,
    First,
    After(String),
    Before(String),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IcebergSchemaChange {
    AddColumn {
        parent: ColumnPath,
        name: String,
        data_type: SqlType,
        default: Option<DefaultLiteral>,
        position: AddPosition,
    },
    DropColumn {
        path: ColumnPath,
    },
    RenameColumn {
        path: ColumnPath,
        new_name: String,
    },
    ModifyColumn {
        path: ColumnPath,
        new_type: SqlType,
    },
    SetNullable {
        path: ColumnPath,
        nullable: bool,
    },
    Reorder {
        path: ColumnPath,
        position: AddPosition,
    },
    UpdateComment {
        path: ColumnPath,
        comment: String,
    },
}

/// Lower one parser-owned Iceberg schema change into the existing catalog
/// application DTO without reparsing SQL text. Catalog and provider checks
/// deliberately remain at the caller's admission boundary.
pub(crate) fn lower_typed_iceberg_schema_change(
    change: &novarocks_parser::ast::IcebergSchemaChange,
) -> Result<IcebergSchemaChange, String> {
    use novarocks_parser::ast::{IcebergColumnAction, IcebergSchemaChange as Typed};

    match change {
        Typed::AddColumn {
            path,
            data_type,
            nullable,
            default,
            position,
        } => {
            if matches!(nullable, Some(false)) {
                return Err(
                    "ADD COLUMN NOT NULL is not supported for Iceberg schema evolution".to_string(),
                );
            }
            let (parent, name) = lower_typed_add_column_path(path)?;
            let data_type = lower_typed_sql_type(data_type)?;
            Ok(IcebergSchemaChange::AddColumn {
                parent,
                name,
                data_type: data_type.clone(),
                default: default
                    .as_ref()
                    .map(|literal| lower_typed_default_literal(literal, &data_type))
                    .transpose()?,
                position: lower_typed_add_position(position, true)?,
            })
        }
        Typed::DropColumn { path } => Ok(IcebergSchemaChange::DropColumn {
            path: lower_typed_column_path(path)?,
        }),
        Typed::RenameColumn { from, to } => {
            let path = lower_typed_column_path(from)?;
            let target = lower_typed_column_path(to)?;
            if target.is_empty() {
                return Err("RENAME COLUMN target requires an identifier".to_string());
            }
            let source_parent = path.parent();
            let target_parent = target.parent();
            if !target_parent.is_empty() && target_parent != source_parent {
                return Err(
                    "RENAME COLUMN target must share the same parent path as the source"
                        .to_string(),
                );
            }
            Ok(IcebergSchemaChange::RenameColumn {
                path,
                new_name: target
                    .last()
                    .expect("non-empty typed rename target checked above")
                    .to_owned(),
            })
        }
        Typed::ModifyColumn { path, data_type } => Ok(IcebergSchemaChange::ModifyColumn {
            path: lower_typed_column_path(path)?,
            new_type: lower_typed_sql_type(data_type)?,
        }),
        Typed::AlterColumn { path, action } => {
            let path = lower_typed_column_path(path)?;
            match action {
                IcebergColumnAction::Reorder(position) => Ok(IcebergSchemaChange::Reorder {
                    path,
                    position: lower_typed_add_position(position, false)?,
                }),
                IcebergColumnAction::SetNullable(nullable) => {
                    Ok(IcebergSchemaChange::SetNullable {
                        path,
                        nullable: *nullable,
                    })
                }
                IcebergColumnAction::Comment(comment) => {
                    let novarocks_parser::ast::LiteralKind::String(comment) = &comment.kind else {
                        return Err("ALTER COLUMN COMMENT requires a string literal".to_string());
                    };
                    Ok(IcebergSchemaChange::UpdateComment {
                        path,
                        comment: comment.clone(),
                    })
                }
            }
        }
    }
}

/// Lower a parser-owned syntax type directly to the catalog's semantic type.
/// This is intentionally recursive so ARRAY/MAP/STRUCT never fall back to a
/// SQL text round-trip.
pub(crate) fn lower_typed_sql_type(
    type_name: &novarocks_parser::ast::TypeName,
) -> Result<SqlType, String> {
    use novarocks_parser::ast::TypeNameArgument;

    let name = type_name
        .name
        .parts
        .last()
        .ok_or_else(|| "type name is empty".to_string())?
        .value
        .to_ascii_lowercase();
    if type_name.name.parts.len() != 1 {
        return Err(format!("qualified type name `{name}` is not supported"));
    }

    match name.as_str() {
        "array" => Ok(SqlType::Array(Box::new(lower_array_element_type(
            type_name,
        )?))),
        "map" => {
            let (key, value) = lower_map_types(type_name)?;
            Ok(SqlType::Map(Box::new(key), Box::new(value)))
        }
        "struct" => Ok(SqlType::Struct(
            type_name
                .arguments
                .iter()
                .map(|argument| match argument {
                    TypeNameArgument::Field(field) => Ok((
                        field.name.value.clone(),
                        lower_typed_sql_type(&field.data_type)?,
                    )),
                    _ => Err("STRUCT type requires named fields".to_string()),
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        "decimal" | "dec" | "numeric" | "decimal32" | "decimal64" | "decimal128" => {
            let (precision, scale) = lower_decimal_arguments(type_name)?;
            Ok(SqlType::Decimal { precision, scale })
        }
        "tinyint" | "int8" => Ok(SqlType::TinyInt),
        "smallint" | "int16" => Ok(SqlType::SmallInt),
        "int" | "integer" | "int32" => Ok(SqlType::Int),
        "bigint" | "int64" => Ok(SqlType::BigInt),
        "largeint" | "int128" => Ok(SqlType::LargeInt),
        "float" | "float32" => Ok(SqlType::Float),
        "double" | "float64" | "double precision" => Ok(SqlType::Double),
        "boolean" | "bool" => Ok(SqlType::Boolean),
        "string" | "varchar" | "char" | "character" | "text" => Ok(SqlType::String),
        "date" => Ok(SqlType::Date),
        "datetime" | "timestamp" => Ok(SqlType::DateTime),
        "timestamp_ns" | "timestamptz_ns" | "datetime_ns" => Ok(SqlType::DateTimeNs),
        "time" => Ok(SqlType::Time),
        "binary" | "varbinary" => Ok(SqlType::Binary),
        "json" | "jsonb" => Ok(SqlType::Json),
        "bitmap" => Ok(SqlType::Bitmap),
        "hll" => Ok(SqlType::Hll),
        "variant" => Ok(SqlType::Variant),
        _ => Err(format!("unsupported Iceberg schema type `{name}`")),
    }
}

fn lower_typed_add_column_path(
    path: &novarocks_parser::ast::ColumnPath,
) -> Result<(ColumnPath, String), String> {
    let mut path = lower_typed_column_path(path)?;
    let name = path
        .segments
        .pop()
        .ok_or_else(|| "ADD COLUMN requires a column path".to_string())?;
    Ok((path, name))
}

fn lower_typed_column_path(path: &novarocks_parser::ast::ColumnPath) -> Result<ColumnPath, String> {
    if path.parts.is_empty() {
        return Err("column path is empty".to_string());
    }
    Ok(ColumnPath::from_segments(
        path.parts.iter().map(|part| part.value.clone()).collect(),
    ))
}

fn lower_typed_add_position(
    position: &novarocks_parser::ast::ColumnPosition,
    add_column: bool,
) -> Result<AddPosition, String> {
    use novarocks_parser::ast::ColumnPosition;

    match position {
        ColumnPosition::Default => Ok(AddPosition::Default),
        ColumnPosition::First => Ok(AddPosition::First),
        ColumnPosition::After(path) => {
            Ok(AddPosition::After(lower_position_target(path, add_column)?))
        }
        ColumnPosition::Before(path) => Ok(AddPosition::Before(lower_position_target(
            path, add_column,
        )?)),
    }
}

fn lower_position_target(
    path: &novarocks_parser::ast::ColumnPath,
    add_column: bool,
) -> Result<String, String> {
    let lowered = lower_typed_column_path(path)?;
    if add_column && lowered.segments.len() != 1 {
        return Err("ADD COLUMN position target must be a single column identifier".to_string());
    }
    lowered
        .last()
        .map(str::to_owned)
        .ok_or_else(|| "column position target is empty".to_string())
}

fn lower_array_element_type(
    type_name: &novarocks_parser::ast::TypeName,
) -> Result<SqlType, String> {
    use novarocks_parser::ast::TypeNameArgument;

    let [TypeNameArgument::Type(element)] = type_name.arguments.as_slice() else {
        return Err("ARRAY type requires one element type".to_string());
    };
    lower_typed_sql_type(element)
}

fn lower_map_types(
    type_name: &novarocks_parser::ast::TypeName,
) -> Result<(SqlType, SqlType), String> {
    use novarocks_parser::ast::TypeNameArgument;

    let [TypeNameArgument::Type(key), TypeNameArgument::Type(value)] =
        type_name.arguments.as_slice()
    else {
        return Err("MAP type requires key and value types".to_string());
    };
    Ok((lower_typed_sql_type(key)?, lower_typed_sql_type(value)?))
}

fn lower_decimal_arguments(
    type_name: &novarocks_parser::ast::TypeName,
) -> Result<(u8, i8), String> {
    use novarocks_parser::ast::{LiteralKind, TypeNameArgument};

    if type_name.arguments.len() > 2 {
        return Err("DECIMAL type accepts at most precision and scale".to_string());
    }
    let mut values = type_name.arguments.iter().map(|argument| match argument {
        TypeNameArgument::Literal(literal) => match &literal.kind {
            LiteralKind::Number(value) => Ok(value.as_str()),
            _ => Err("DECIMAL precision and scale must be numeric literals".to_string()),
        },
        _ => Err("DECIMAL precision and scale must be numeric literals".to_string()),
    });
    let precision = match values.next() {
        Some(value) => value?
            .parse::<u8>()
            .map_err(|_| "DECIMAL precision must fit u8".to_string())?,
        None => 38,
    };
    let scale = match values.next() {
        Some(value) => value?
            .parse::<i8>()
            .map_err(|_| "DECIMAL scale must fit i8".to_string())?,
        None => 0,
    };
    Ok((precision, scale))
}

fn lower_typed_default_literal(
    literal: &novarocks_parser::ast::Literal,
    data_type: &SqlType,
) -> Result<DefaultLiteral, String> {
    use novarocks_parser::ast::LiteralKind;

    let lowered = match &literal.kind {
        LiteralKind::Null => DefaultLiteral::Null,
        LiteralKind::Boolean(value) => DefaultLiteral::Bool(*value),
        LiteralKind::Number(value) => lower_typed_numeric_default(value, data_type)?,
        LiteralKind::String(value) => lower_typed_string_default(value, data_type)?,
        LiteralKind::HexString(value) => {
            if !matches!(data_type, SqlType::Binary) {
                return Err(format!(
                    "hex DEFAULT not supported for column type {data_type:?}"
                ));
            }
            let digits = value
                .strip_prefix("0x")
                .or_else(|| value.strip_prefix("0X"))
                .unwrap_or(value);
            DefaultLiteral::Binary(
                hex::decode(digits)
                    .map_err(|error| format!("invalid hex DEFAULT literal `{value}`: {error}"))?,
            )
        }
    };

    validate_typed_default_literal(&lowered, data_type)?;
    Ok(lowered)
}

fn lower_typed_numeric_default(text: &str, data_type: &SqlType) -> Result<DefaultLiteral, String> {
    match data_type {
        SqlType::TinyInt | SqlType::SmallInt | SqlType::Int | SqlType::BigInt => {
            let value = text
                .parse::<i64>()
                .map_err(|error| format!("invalid integer DEFAULT `{text}`: {error}"))?;
            Ok(DefaultLiteral::Int(value))
        }
        SqlType::Float | SqlType::Double => {
            let value = text
                .parse::<f64>()
                .map_err(|error| format!("invalid float DEFAULT `{text}`: {error}"))?;
            Ok(DefaultLiteral::Float(value))
        }
        SqlType::Decimal { scale, .. } => {
            let (unscaled, literal_scale) = typed_decimal_from_str(text)?;
            if literal_scale != *scale {
                return Err(format!(
                    "DEFAULT value scale {literal_scale} does not match column scale {scale}"
                ));
            }
            Ok(DefaultLiteral::Decimal {
                unscaled,
                scale: *scale,
            })
        }
        other => Err(format!(
            "numeric DEFAULT not supported for column type {other:?}"
        )),
    }
}

fn lower_typed_string_default(value: &str, data_type: &SqlType) -> Result<DefaultLiteral, String> {
    match data_type {
        SqlType::String => Ok(DefaultLiteral::String(value.to_string())),
        SqlType::TinyInt
        | SqlType::SmallInt
        | SqlType::Int
        | SqlType::BigInt
        | SqlType::Float
        | SqlType::Double
        | SqlType::Decimal { .. } => lower_typed_numeric_default(value.trim(), data_type),
        SqlType::Boolean => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(DefaultLiteral::Bool(true)),
            "false" | "0" => Ok(DefaultLiteral::Bool(false)),
            other => Err(format!(
                "invalid boolean DEFAULT `{other}` (expected true/false/0/1)"
            )),
        },
        SqlType::Json => {
            serde_json::from_str::<serde_json::Value>(value)
                .map_err(|error| format!("invalid JSON DEFAULT literal: {error}"))?;
            Ok(DefaultLiteral::String(value.to_string()))
        }
        SqlType::Date => Ok(DefaultLiteral::Date(
            novarocks_sql::syntax::parse_date_string_to_days(value)?,
        )),
        SqlType::DateTime => Ok(DefaultLiteral::DateTime(
            novarocks_sql::syntax::parse_datetime_string_to_micros(value)?,
        )),
        SqlType::DateTimeNs => Ok(DefaultLiteral::DateTime(typed_datetime_string_to_nanos(
            value,
        )?)),
        SqlType::Binary | SqlType::Bitmap | SqlType::Hll => {
            Ok(DefaultLiteral::Binary(value.as_bytes().to_vec()))
        }
        SqlType::Array(_) => {
            let json: serde_json::Value = serde_json::from_str(value)
                .map_err(|error| format!("invalid ARRAY DEFAULT literal: {error}"))?;
            if !json.is_array() {
                return Err(format!(
                    "ARRAY DEFAULT must be a JSON array literal (e.g. '[]'), got: {value:?}"
                ));
            }
            Ok(DefaultLiteral::String(value.to_string()))
        }
        SqlType::Map(_, _) => {
            let json: serde_json::Value = serde_json::from_str(value)
                .map_err(|error| format!("invalid MAP DEFAULT literal: {error}"))?;
            if !json.is_object() {
                return Err(format!(
                    "MAP DEFAULT must be a JSON object literal (e.g. '{{}}'), got: {value:?}"
                ));
            }
            Ok(DefaultLiteral::String(value.to_string()))
        }
        other => Err(format!(
            "string DEFAULT not supported for column type {other:?}"
        )),
    }
}

fn validate_typed_default_literal(
    literal: &DefaultLiteral,
    data_type: &SqlType,
) -> Result<(), String> {
    if matches!(literal, DefaultLiteral::Null) {
        return Ok(());
    }
    if let DefaultLiteral::Decimal { scale, .. } = literal
        && *scale < 0
    {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }
    if let SqlType::Decimal { scale, .. } = data_type
        && *scale < 0
    {
        return Err(format!("negative DECIMAL scale {scale} is not supported"));
    }

    match (literal, data_type) {
        (DefaultLiteral::String(value), SqlType::Array(_)) => {
            let elements = serde_json::from_str::<serde_json::Value>(value)
                .map_err(|error| format!("invalid ARRAY DEFAULT JSON: {error}"))?
                .as_array()
                .ok_or_else(|| format!("ARRAY DEFAULT must be a JSON array, got: {value:?}"))?
                .clone();
            if !elements.is_empty() {
                return Err(
                    "non-empty ARRAY DEFAULT literals are not yet supported; use '[]'".to_string(),
                );
            }
        }
        (DefaultLiteral::String(value), SqlType::Map(_, _)) => {
            let entries = serde_json::from_str::<serde_json::Value>(value)
                .map_err(|error| format!("invalid MAP DEFAULT JSON: {error}"))?
                .as_object()
                .ok_or_else(|| format!("MAP DEFAULT must be a JSON object, got: {value:?}"))?
                .clone();
            if !entries.is_empty() {
                return Err(
                    "non-empty MAP DEFAULT literals are not yet supported; use '{}'".to_string(),
                );
            }
        }
        (DefaultLiteral::Bool(_), SqlType::Boolean)
        | (DefaultLiteral::Int(_), SqlType::BigInt)
        | (DefaultLiteral::Float(_), SqlType::Float | SqlType::Double)
        | (DefaultLiteral::String(_), SqlType::String | SqlType::Json)
        | (DefaultLiteral::Binary(_), SqlType::Binary | SqlType::Bitmap | SqlType::Hll)
        | (DefaultLiteral::Date(_), SqlType::Date)
        | (DefaultLiteral::DateTime(_), SqlType::DateTime | SqlType::DateTimeNs) => {}
        (DefaultLiteral::Int(value), SqlType::TinyInt) => {
            i8::try_from(*value).map_err(|_| default_out_of_range("TINYINT", *value))?;
        }
        (DefaultLiteral::Int(value), SqlType::SmallInt) => {
            i16::try_from(*value).map_err(|_| default_out_of_range("SMALLINT", *value))?;
        }
        (DefaultLiteral::Int(value), SqlType::Int) => {
            i32::try_from(*value).map_err(|_| default_out_of_range("INT", *value))?;
        }
        (
            DefaultLiteral::Decimal { scale, .. },
            SqlType::Decimal {
                scale: column_scale,
                ..
            },
        ) if scale == column_scale => {}
        (
            DefaultLiteral::Decimal { scale, .. },
            SqlType::Decimal {
                scale: column_scale,
                ..
            },
        ) => {
            return Err(format!(
                "DEFAULT value scale {scale} does not match column scale {column_scale}"
            ));
        }
        (literal, column_type) => {
            return Err(format!(
                "DEFAULT value type does not match column type: literal={literal:?} column={column_type:?}"
            ));
        }
    }
    Ok(())
}

fn typed_decimal_from_str(text: &str) -> Result<(i128, i8), String> {
    let trimmed = text.trim();
    let (sign, body) = if let Some(rest) = trimmed.strip_prefix('-') {
        (-1_i128, rest)
    } else {
        (1_i128, trimmed)
    };
    let (whole, fraction) = match body.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (body, ""),
    };
    let combined: String = whole.chars().chain(fraction.chars()).collect();
    let unscaled = combined
        .parse::<i128>()
        .map_err(|error| format!("invalid decimal DEFAULT `{text}`: {error}"))?;
    let scale = i8::try_from(fraction.len()).map_err(|_| "decimal scale too large".to_string())?;
    Ok((sign * unscaled, scale))
}

fn typed_datetime_string_to_nanos(value: &str) -> Result<i64, String> {
    use chrono::{NaiveDate, NaiveDateTime};

    let value = value.trim();
    let date_time = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map(|date| date.and_hms_opt(0, 0, 0).expect("midnight"))
        })
        .map_err(|_| format!("invalid datetime literal `{value}`"))?;
    date_time
        .and_utc()
        .timestamp_nanos_opt()
        .ok_or_else(|| format!("DATETIME literal '{value}' out of nanosecond representable range"))
}

fn default_out_of_range(type_name: &str, value: i64) -> String {
    format!("DEFAULT value {value} out of range for {type_name}")
}

/// Detect `SHOW CREATE TABLE <name>` statements so the server layer can
/// route them to the engine instead of treating them as session noops.
pub(crate) fn looks_like_show_create_table(sql: &str) -> bool {
    let lower = sql.trim_start().to_ascii_lowercase();
    // Match "SHOW CREATE TABLE ..." quickly without full parsing.
    if !lower.starts_with("show") {
        return false;
    }
    let rest = lower["show".len()..].trim_start();
    if !rest.starts_with("create") {
        return false;
    }
    let rest2 = rest["create".len()..].trim_start();
    rest2.starts_with("table")
}

/// Parse `SHOW CREATE TABLE <catalog>.<db>.<table>` and return the parsed
/// `ObjectName`.  Returns `Err` if parsing fails.
pub(crate) fn parse_show_create_table(
    sql: &str,
) -> Result<novarocks_sql::syntax::ObjectName, String> {
    use sqlparser::keywords::Keyword;
    let normalized = novarocks_sql::syntax::normalize_for_raw_parse(sql)?;
    let mut parser = Parser::new(&StarRocksDialect)
        .try_with_sql(&normalized)
        .map_err(|e| format!("parse SHOW CREATE TABLE: {e}"))?;
    parser
        .expect_keyword(Keyword::SHOW)
        .map_err(|e| format!("parse SHOW CREATE TABLE: {e}"))?;
    parser
        .expect_keyword(Keyword::CREATE)
        .map_err(|e| format!("parse SHOW CREATE TABLE: {e}"))?;
    parser
        .expect_keyword(Keyword::TABLE)
        .map_err(|e| format!("parse SHOW CREATE TABLE: {e}"))?;
    let obj = parser
        .parse_object_name(false)
        .map_err(|e| format!("parse SHOW CREATE TABLE table name: {e}"))?;
    novarocks_sql::syntax::convert_object_name(obj)
}

pub fn looks_like_show_alter_table_optimize(sql: &str) -> bool {
    let Ok(normalized) = novarocks_sql::syntax::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    parser.parse_keyword(Keyword::SHOW)
        && parser.parse_keyword(Keyword::ALTER)
        && parser.parse_keyword(Keyword::TABLE)
        && peek_token_word_eq(&parser, "OPTIMIZE")
}

fn peek_token_word_eq(parser: &Parser<'_>, word: &str) -> bool {
    matches!(
        &parser.peek_token_ref().token,
        Token::Word(token_word) if token_word.value.eq_ignore_ascii_case(word)
    )
}

fn expect_word(parser: &mut Parser<'_>, word: &str) -> Result<(), String> {
    let token = parser.next_token();
    match token.token {
        Token::Word(token_word) if token_word.value.eq_ignore_ascii_case(word) => Ok(()),
        other => Err(format!("expected {word}, got {other}")),
    }
}

fn consume_optional_final_semicolon(parser: &mut Parser<'_>) -> Result<(), String> {
    if parser.consume_token(&Token::SemiColon) && parser.peek_token_ref().token == Token::SemiColon
    {
        return Err("only one final semicolon is allowed".to_string());
    }
    Ok(())
}

fn expect_parser_eof(parser: &Parser<'_>) -> Result<(), String> {
    match parser.peek_token_ref().token {
        Token::EOF => Ok(()),
        ref other => Err(format!("unexpected token after statement: {other}")),
    }
}

/// Check if SQL looks like ALTER TABLE ... ADD EQUALITY DELETE (...) VALUES ...
pub fn looks_like_add_equality_delete(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("ALTER TABLE") && upper.contains("ADD EQUALITY DELETE")
}

/// Parse: ALTER TABLE [catalog.db.]table ADD EQUALITY DELETE (k1, k2) VALUES (...)
pub fn parse_add_equality_delete_sql(sql: &str) -> Result<AddEqualityDeleteStmt, String> {
    const ALTER_TABLE: &str = "ALTER TABLE";
    const ADD_EQ_DELETE: &str = "ADD EQUALITY DELETE";
    const VALUES: &str = "VALUES";

    let upper = sql.to_ascii_uppercase();
    let alter_idx = upper.find(ALTER_TABLE).ok_or("missing ALTER TABLE")?;
    let add_idx = upper
        .find(ADD_EQ_DELETE)
        .ok_or("missing ADD EQUALITY DELETE")?;
    let values_idx = upper[add_idx + ADD_EQ_DELETE.len()..]
        .find(VALUES)
        .map(|idx| add_idx + ADD_EQ_DELETE.len() + idx)
        .ok_or("missing VALUES")?;

    let table_str = sql[alter_idx + ALTER_TABLE.len()..add_idx].trim();
    let table_parts = table_str
        .split('.')
        .map(normalize_identifier)
        .collect::<Result<Vec<_>, _>>()?;
    if table_parts.is_empty() {
        return Err("ADD EQUALITY DELETE requires a table name".to_string());
    }

    let columns_part = sql[add_idx + ADD_EQ_DELETE.len()..values_idx].trim();
    let columns_inner = columns_part
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| "ADD EQUALITY DELETE requires columns in parentheses".to_string())?;
    let columns = columns_inner
        .split(',')
        .map(normalize_identifier)
        .collect::<Result<Vec<_>, _>>()?;
    if columns.is_empty() {
        return Err("ADD EQUALITY DELETE requires at least one equality column".to_string());
    }

    let values_part = sql[values_idx + VALUES.len()..]
        .trim()
        .trim_end_matches(';');
    if values_part.is_empty() {
        return Err("ADD EQUALITY DELETE VALUES requires at least one row".to_string());
    }
    let fake_sql = format!(
        "INSERT INTO __eq_delete ({}) VALUES {values_part}",
        columns.join(", ")
    );
    let stmt = novarocks_sql::syntax::parse_normalized_sql_raw(&fake_sql)
        .map_err(|e| format!("parse ADD EQUALITY DELETE VALUES: {e}"))?;
    let insert = match stmt {
        sqlparser::ast::Statement::Insert(insert) => insert,
        other => {
            return Err(format!(
                "internal ADD EQUALITY DELETE VALUES parser expected INSERT, got {other:?}"
            ));
        }
    };
    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| "ADD EQUALITY DELETE requires a VALUES source".to_string())?;
    let rows = match source.body.as_ref() {
        sqlparser::ast::SetExpr::Values(values) => values
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(sqlparser_expr_to_literal)
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?,
        other => {
            return Err(format!(
                "ADD EQUALITY DELETE expects literal VALUES rows, got {other:?}"
            ));
        }
    };
    Ok(AddEqualityDeleteStmt {
        table: ObjectName { parts: table_parts },
        columns,
        rows,
    })
}

#[cfg(test)]
mod drop_table_if_exists_tests {
    #[test]
    fn guard_missing_table_error_is_soft_drop_candidate_but_mv_error_is_not() {
        assert!(super::is_missing_table_guard_error(
            "unknown table: db.missing"
        ));
        assert!(super::is_missing_table_guard_error(
            "load iceberg table db.missing: table not found: warehouse/db/missing"
        ));
        assert!(super::is_missing_table_guard_error(
            "no metadata files for db.missing"
        ));
        assert!(!super::is_missing_table_guard_error(
            "table ice.db.mv_orders is a materialized view; use DROP MATERIALIZED VIEW"
        ));
    }
}

#[cfg(test)]
mod tests {
    use novarocks_sql::syntax::Literal;

    #[test]
    fn convert_update_from_table_source() {
        let stmt = novarocks_sql::syntax::parse_sql_raw(
            "update ice.db1.t as t set v = s.v from staging.src as s where t.id = s.id",
        )
        .expect("parse");
        let sqlparser::ast::Statement::Update(_) = &stmt else {
            panic!("expected update statement: {stmt:?}");
        };
        let update = super::convert_sqlparser_update_to_custom(&stmt).expect("convert");
        assert_eq!(update.table.parts, vec!["ice", "db1", "t"]);
        assert_eq!(update.alias.as_deref(), Some("t"));
        assert_eq!(update.assignments.len(), 1);
        assert_eq!(update.assignments[0].column, "v");
        let Some(novarocks_sql::syntax::MutationSource::Table { name, alias }) = &update.source
        else {
            panic!("expected table source: {:?}", update.source);
        };
        assert_eq!(name.parts, vec!["staging", "src"]);
        assert_eq!(alias.as_deref(), Some("s"));
        assert!(update.where_clause.is_some());
    }

    #[test]
    fn convert_update_rejects_multi_column_assignment() {
        let stmt = novarocks_sql::syntax::parse_sql_raw(
            "update ice.db1.t set (v1, v2) = (1, 2) where id = 1",
        )
        .expect("parse");
        let err = super::convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
        assert!(err.contains("single-column UPDATE assignments"), "{err}");
    }

    #[test]
    fn convert_update_rejects_target_join() {
        let stmt = novarocks_sql::syntax::parse_sql_raw(
            "update ice.db1.t as t join staging.src as s on t.id = s.id set v = s.v",
        )
        .expect("parse");
        let err = super::convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
        assert!(
            err.contains("UPDATE target joins are not supported"),
            "{err}"
        );
    }

    #[test]
    fn convert_update_rejects_conflict_clause() {
        let stmt = novarocks_sql::syntax::parse_sql_raw("update or ignore ice.db1.t set v = 1")
            .expect("parse");
        let err = super::convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
        assert!(
            err.contains("UPDATE conflict clauses are not supported"),
            "{err}"
        );
    }

    #[test]
    fn convert_update_rejects_target_alias_column_list() {
        let stmt = novarocks_sql::syntax::parse_sql_raw("update ice.db1.t as t(c) set v = 1")
            .expect("parse");
        let err = super::convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
        assert!(
            err.contains("UPDATE target alias column lists are not supported"),
            "{err}"
        );
    }

    #[test]
    fn convert_update_rejects_source_alias_column_list() {
        let stmt = novarocks_sql::syntax::parse_sql_raw(
            "update ice.db1.t set v = s.v from staging.src as s(id)",
        )
        .expect("parse");
        let err = super::convert_sqlparser_update_to_custom(&stmt).expect_err("must fail");
        assert!(
            err.contains("UPDATE ... FROM source alias column lists are not supported"),
            "{err}"
        );
    }

    #[test]
    fn parse_add_equality_delete_values_statement() {
        let stmt = super::parse_add_equality_delete_sql(
            "ALTER TABLE ice.db.orders ADD EQUALITY DELETE (id, category) VALUES (2, 'B'), (4, 'A')",
        )
        .expect("parse");

        assert_eq!(stmt.table.parts, vec!["ice", "db", "orders"]);
        assert_eq!(stmt.columns, vec!["id", "category"]);
        assert_eq!(
            stmt.rows,
            vec![
                vec![Literal::Int(2), Literal::String("B".to_string())],
                vec![Literal::Int(4), Literal::String("A".to_string())],
            ]
        );
    }

    #[test]
    fn looks_like_show_alter_table_optimize_detects_only_live_show_route() {
        assert!(super::looks_like_show_alter_table_optimize(
            "SHOW ALTER TABLE OPTIMIZE"
        ));
        assert!(super::looks_like_show_alter_table_optimize(
            " show alter table optimize from db "
        ));
        assert!(!super::looks_like_show_alter_table_optimize(
            "ALTER TABLE ice.db.orders OPTIMIZE"
        ));
        assert!(!super::looks_like_show_alter_table_optimize(
            "SHOW CREATE TABLE ice.db.orders"
        ));
        assert!(!super::looks_like_show_alter_table_optimize(
            "SHOW ALTER TABLE orders OPTIMIZE"
        ));
    }

    fn typed_iceberg_action(sql: &str) -> novarocks_parser::ast::IcebergTableAction {
        let mut statements = novarocks_parser::parse(sql).expect("parse typed Iceberg command");
        let novarocks_parser::ast::Statement::Iceberg(
            novarocks_parser::ast::IcebergStatement::AlterTable(statement),
        ) = statements.remove(0)
        else {
            panic!("expected typed Iceberg ALTER TABLE statement");
        };
        statement.action
    }

    fn typed_schema_change(sql: &str) -> novarocks_parser::ast::IcebergSchemaChange {
        match typed_iceberg_action(sql) {
            novarocks_parser::ast::IcebergTableAction::Schema(change) => change,
            action => panic!("expected typed Iceberg schema action: {action:?}"),
        }
    }

    #[test]
    fn lower_typed_schema_add_column_preserves_nested_parameterized_type() {
        let change = typed_schema_change(
            "ALTER TABLE ice.db.orders ADD COLUMN profile STRUCT<name STRING, attributes MAP<STRING, ARRAY<DECIMAL(10, 2)>>> FIRST",
        );
        let lowered = super::lower_typed_iceberg_schema_change(&change).expect("lower typed add");

        assert_eq!(
            lowered,
            super::IcebergSchemaChange::AddColumn {
                parent: super::ColumnPath::root(),
                name: "profile".to_string(),
                data_type: super::SqlType::Struct(vec![
                    ("name".to_string(), super::SqlType::String),
                    (
                        "attributes".to_string(),
                        super::SqlType::Map(
                            Box::new(super::SqlType::String),
                            Box::new(super::SqlType::Array(Box::new(super::SqlType::Decimal {
                                precision: 10,
                                scale: 2,
                            }))),
                        ),
                    ),
                ]),
                default: None,
                position: super::AddPosition::First,
            }
        );
    }

    #[test]
    fn lower_typed_schema_defaults_without_sql_reparse() {
        let change = typed_schema_change("ALTER TABLE ice.db.orders ADD COLUMN d INT DEFAULT NULL");
        let lowered =
            super::lower_typed_iceberg_schema_change(&change).expect("lower null default");

        let super::IcebergSchemaChange::AddColumn { default, .. } = lowered else {
            panic!("expected AddColumn");
        };
        assert_eq!(default, Some(super::DefaultLiteral::Null));

        let integer = typed_schema_change("ALTER TABLE ice.db.orders ADD COLUMN d INT DEFAULT 7");
        let super::IcebergSchemaChange::AddColumn { default, .. } =
            super::lower_typed_iceberg_schema_change(&integer).expect("lower integer default")
        else {
            panic!("expected AddColumn");
        };
        assert_eq!(default, Some(super::DefaultLiteral::Int(7)));

        let decimal = typed_schema_change(
            "ALTER TABLE ice.db.orders ADD COLUMN d DECIMAL(8, 2) DEFAULT '12.34'",
        );
        let super::IcebergSchemaChange::AddColumn { default, .. } =
            super::lower_typed_iceberg_schema_change(&decimal).expect("lower decimal default")
        else {
            panic!("expected AddColumn");
        };
        assert_eq!(
            default,
            Some(super::DefaultLiteral::Decimal {
                unscaled: 1234,
                scale: 2,
            })
        );

        let boolean = typed_schema_change(
            "ALTER TABLE ice.db.orders ADD COLUMN enabled BOOLEAN DEFAULT TRUE",
        );
        let super::IcebergSchemaChange::AddColumn { default, .. } =
            super::lower_typed_iceberg_schema_change(&boolean).expect("lower boolean default")
        else {
            panic!("expected AddColumn");
        };
        assert_eq!(default, Some(super::DefaultLiteral::Bool(true)));
    }

    #[test]
    fn lower_typed_schema_defaults_reject_legacy_overflow_and_type_mismatch() {
        let overflow =
            typed_schema_change("ALTER TABLE ice.db.orders ADD COLUMN d TINYINT DEFAULT 200");
        assert!(
            super::lower_typed_iceberg_schema_change(&overflow)
                .expect_err("tinyint overflow")
                .contains("out of range for TINYINT")
        );

        let wrong_type =
            typed_schema_change("ALTER TABLE ice.db.orders ADD COLUMN d BOOLEAN DEFAULT 'maybe'");
        assert!(
            super::lower_typed_iceberg_schema_change(&wrong_type)
                .expect_err("boolean coercion must validate")
                .contains("invalid boolean DEFAULT")
        );

        let hex =
            typed_schema_change("ALTER TABLE ice.db.orders ADD COLUMN d BINARY DEFAULT 0xCAFE");
        let super::IcebergSchemaChange::AddColumn { default, .. } =
            super::lower_typed_iceberg_schema_change(&hex).expect("binary hex default")
        else {
            panic!("expected AddColumn");
        };
        assert_eq!(
            default,
            Some(super::DefaultLiteral::Binary(vec![0xCA, 0xFE]))
        );
    }

    #[test]
    fn lower_typed_schema_rename_and_comment_preserve_existing_dto_shape() {
        let rename = typed_schema_change(
            "ALTER TABLE ice.db.orders RENAME COLUMN Address.Zip TO Address.Postal_Code",
        );
        assert_eq!(
            super::lower_typed_iceberg_schema_change(&rename).expect("lower rename"),
            super::IcebergSchemaChange::RenameColumn {
                path: super::ColumnPath::from_segments(vec!["Address".into(), "Zip".into()]),
                new_name: "postal_code".to_string(),
            }
        );

        let comment = typed_schema_change(
            "ALTER TABLE ice.db.orders ALTER COLUMN address.zip COMMENT 'postal code'",
        );
        assert_eq!(
            super::lower_typed_iceberg_schema_change(&comment).expect("lower comment"),
            super::IcebergSchemaChange::UpdateComment {
                path: super::ColumnPath::from_segments(vec!["address".into(), "zip".into()]),
                comment: "postal code".to_string(),
            }
        );
    }

    #[test]
    fn lower_typed_properties_preserves_existing_property_dto_rules() {
        let action = typed_iceberg_action(
            "ALTER TABLE ice.db.orders SET TBLPROPERTIES ('format' = 'parquet', 'owner' = 'ops')",
        );
        let novarocks_parser::ast::IcebergTableAction::Properties(action) = action else {
            panic!("expected typed properties action");
        };
        assert_eq!(
            super::lower_typed_iceberg_properties_action(&action).expect("lower properties"),
            super::PropertiesOp::Set {
                entries: vec![
                    ("format".to_string(), "parquet".to_string()),
                    ("owner".to_string(), "ops".to_string()),
                ],
            }
        );

        let comment = typed_iceberg_action("ALTER TABLE ice.db.orders COMMENT 'order table'");
        let novarocks_parser::ast::IcebergTableAction::Properties(comment) = comment else {
            panic!("expected typed comment action");
        };
        assert_eq!(
            super::lower_typed_iceberg_properties_action(&comment).expect("lower comment"),
            super::PropertiesOp::Set {
                entries: vec![("comment".to_string(), "order table".to_string())],
            }
        );

        let non_string =
            typed_iceberg_action("ALTER TABLE ice.db.orders SET TBLPROPERTIES ('retention' = 7)");
        let novarocks_parser::ast::IcebergTableAction::Properties(non_string) = non_string else {
            panic!("expected typed properties action");
        };
        assert!(
            super::lower_typed_iceberg_properties_action(&non_string)
                .expect_err("legacy properties require string values")
                .contains("string literal")
        );
    }

    #[test]
    fn lower_typed_partition_change_preserves_transform_validation() {
        let action = typed_iceberg_action(
            "ALTER TABLE ice.db.orders ADD PARTITION COLUMN bucket(User_Id, 32)",
        );
        let novarocks_parser::ast::IcebergTableAction::Partition(change) = action else {
            panic!("expected typed partition action");
        };
        assert_eq!(
            super::lower_typed_iceberg_partition_change(&change).expect("lower partition"),
            super::IcebergPartitionSpecChange::Add(super::ConnectorPartitionTransform::Bucket {
                column: std::sync::Arc::from("user_id"),
                num_buckets: 32,
            },)
        );

        let zero = typed_iceberg_action(
            "ALTER TABLE ice.db.orders ADD PARTITION COLUMN bucket(user_id, 0)",
        );
        let novarocks_parser::ast::IcebergTableAction::Partition(zero) = zero else {
            panic!("expected typed partition action");
        };
        assert!(
            super::lower_typed_iceberg_partition_change(&zero)
                .expect_err("zero bucket count must fail")
                .contains("must be positive")
        );
    }
}

#[cfg(test)]
mod column_path_tests {
    use super::ColumnPath;

    #[test]
    fn column_path_parses_single_segment() {
        let p = ColumnPath::parse("address").unwrap();
        assert_eq!(p.segments(), &["address".to_string()]);
        assert!(!p.is_empty());
    }

    #[test]
    fn column_path_parses_dotted() {
        let p = ColumnPath::parse("address.street").unwrap();
        assert_eq!(p.segments(), &["address".to_string(), "street".to_string()]);
    }

    #[test]
    fn column_path_normalizes_case() {
        let p = ColumnPath::parse("Address.Street").unwrap();
        assert_eq!(p.segments(), &["address".to_string(), "street".to_string()]);
    }

    #[test]
    fn column_path_rejects_empty_segment() {
        assert!(ColumnPath::parse("address.").is_err());
        assert!(ColumnPath::parse(".street").is_err());
        assert!(ColumnPath::parse("").is_err());
        assert!(ColumnPath::parse("a..b").is_err());
    }

    #[test]
    fn column_path_root_is_empty() {
        assert!(ColumnPath::root().is_empty());
        assert!(ColumnPath::root().segments().is_empty());
    }

    #[test]
    fn add_position_default_constructed() {
        use super::AddPosition;
        let pos = AddPosition::Default;
        assert!(matches!(pos, AddPosition::Default));
    }

    #[test]
    fn add_position_variants_construct() {
        use super::AddPosition;
        let _ = AddPosition::First;
        let _ = AddPosition::After("col_a".to_string());
        let _ = AddPosition::Before("col_b".to_string());
    }
}
