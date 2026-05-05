#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::ast::{DefaultLiteral, SqlType};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::collections::HashMap;

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Float)).into(),
            ])
            .build()
            .expect("schema")
    }

    fn schema_with_identifier() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Float)).into(),
            ])
            .with_identifier_field_ids(vec![1])
            .build()
            .expect("schema")
    }

    fn props(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn sorted(mut values: Vec<String>) -> Vec<String> {
        values.sort();
        values
    }

    #[test]
    fn add_column_assigns_fresh_field_id() {
        let updated = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::AddColumn {
                name: "new_col".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Null),
            },
        )
        .expect("updated");
        let field = updated.field_by_name("new_col").expect("new field");
        assert_eq!(field.id, 3);
    }

    #[test]
    fn rename_and_modify_preserve_field_id() {
        let renamed = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::RenameColumn {
                old_name: "id".to_string(),
                new_name: "order_id".to_string(),
            },
        )
        .expect("renamed");
        assert_eq!(renamed.field_by_name("order_id").expect("renamed").id, 1);

        let modified = apply_change_to_schema_for_test(
            &renamed,
            2,
            &IcebergSchemaChange::ModifyColumn {
                name: "order_id".to_string(),
                new_type: SqlType::BigInt,
            },
        )
        .expect("modified");
        let field = modified.field_by_name("order_id").expect("modified");
        assert_eq!(field.id, 1);
        assert_eq!(
            field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
    }

    #[test]
    fn drop_removes_field_without_reusing_id() {
        let dropped = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::DropColumn {
                name: "v".to_string(),
            },
        )
        .expect("dropped");
        assert!(dropped.field_by_name("v").is_none());

        let added = apply_change_to_schema_for_test(
            &dropped,
            2,
            &IcebergSchemaChange::AddColumn {
                name: "later".to_string(),
                data_type: SqlType::Int,
                default: None,
            },
        )
        .expect("added");
        assert_eq!(added.field_by_name("later").expect("later").id, 3);
    }

    #[test]
    fn modify_rejects_unsafe_type_changes() {
        let err = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::ModifyColumn {
                name: "id".to_string(),
                new_type: SqlType::Double,
            },
        )
        .expect_err("unsafe change");
        assert!(err.contains("unsupported Iceberg type evolution"));
    }

    #[test]
    fn identifier_field_ids_survive_add_rename_modify() {
        let added = apply_change_to_schema_for_test(
            &schema_with_identifier(),
            2,
            &IcebergSchemaChange::AddColumn {
                name: "new_col".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Null),
            },
        )
        .expect("added");
        assert_eq!(added.identifier_field_ids().collect::<Vec<_>>(), vec![1]);

        let renamed = apply_change_to_schema_for_test(
            &added,
            3,
            &IcebergSchemaChange::RenameColumn {
                old_name: "id".to_string(),
                new_name: "order_id".to_string(),
            },
        )
        .expect("renamed");
        assert_eq!(renamed.identifier_field_ids().collect::<Vec<_>>(), vec![1]);
        assert_eq!(renamed.field_by_name("order_id").expect("renamed").id, 1);

        let modified = apply_change_to_schema_for_test(
            &renamed,
            3,
            &IcebergSchemaChange::ModifyColumn {
                name: "order_id".to_string(),
                new_type: SqlType::BigInt,
            },
        )
        .expect("modified");
        assert_eq!(modified.identifier_field_ids().collect::<Vec<_>>(), vec![1]);
        let field = modified.field_by_name("order_id").expect("modified");
        assert_eq!(
            field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
    }

    #[test]
    fn drop_identifier_field_is_rejected() {
        let err = apply_change_to_schema_for_test(
            &schema_with_identifier(),
            2,
            &IcebergSchemaChange::DropColumn {
                name: "id".to_string(),
            },
        )
        .expect_err("identifier drop");
        assert!(err.contains("identifier"));
    }

    #[test]
    fn reserved_column_changes_are_rejected() {
        let err = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::DropColumn {
                name: "_row_id".to_string(),
            },
        )
        .expect_err("reserved");
        assert!(err.contains("reserved column"));
    }

    #[test]
    fn drop_rejects_equality_delete_dependency_by_name() {
        let deps = vec!["id".to_string()];
        let err = reject_drop_dependencies_for_test("id", &deps, &[]).expect_err("drop dependency");
        assert!(err.contains("equality-delete"));
    }

    #[test]
    fn drop_rejects_managed_mv_explicit_column_dependency() {
        let mv_sqls = vec!["SELECT id FROM ice.ns.orders".to_string()];
        let err =
            reject_drop_dependencies_for_test("id", &[], &mv_sqls).expect_err("drop dependency");
        assert!(err.contains("materialized view"));
    }

    #[test]
    fn drop_allows_managed_mv_unrelated_column_dependency() {
        let mv_sqls = vec!["SELECT v FROM ice.ns.orders".to_string()];
        reject_drop_dependencies_for_test("id", &[], &mv_sqls).expect("unrelated column");
    }

    #[test]
    fn drop_rejects_managed_mv_select_wildcard_dependency() {
        let mv_sqls = vec!["SELECT * FROM ice.ns.orders".to_string()];
        let err =
            reject_drop_dependencies_for_test("id", &[], &mv_sqls).expect_err("drop dependency");
        assert!(err.contains("materialized view"));
    }

    #[test]
    fn drop_rejects_managed_mv_qualified_wildcard_dependency() {
        for sql in [
            "SELECT o.* FROM ice.ns.orders o",
            "SELECT orders.* FROM ice.ns.orders",
        ] {
            let mv_sqls = vec![sql.to_string()];
            let err = reject_drop_dependencies_for_test("id", &[], &mv_sqls)
                .expect_err("drop dependency");
            assert!(err.contains("materialized view"));
        }
    }

    #[test]
    fn drop_allows_managed_mv_count_star_without_column_token() {
        let mv_sqls = vec!["SELECT COUNT(*) FROM ice.ns.orders".to_string()];
        reject_drop_dependencies_for_test("id", &[], &mv_sqls).expect("count star");
    }

    #[test]
    fn add_column_sets_logical_type_property_only_when_needed() {
        let tinyint = build_property_updates_for_test(
            &HashMap::new(),
            &IcebergSchemaChange::AddColumn {
                name: "New_Col".to_string(),
                data_type: SqlType::TinyInt,
                default: Some(DefaultLiteral::Null),
            },
        )
        .expect("updates");
        assert_eq!(
            tinyint.sets.get("novarocks.logical_type.new_col"),
            Some(&"tinyint".to_string())
        );
        assert!(tinyint.removals.is_empty());

        let int = build_property_updates_for_test(
            &HashMap::new(),
            &IcebergSchemaChange::AddColumn {
                name: "new_col".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Null),
            },
        )
        .expect("updates");
        assert!(int.is_empty());
    }

    #[test]
    fn drop_column_removes_logical_and_aggregation_properties() {
        let changes = build_property_updates_for_test(
            &props(&[
                ("novarocks.logical_type.v", "smallint"),
                ("novarocks.column_agg.v", "sum"),
                ("novarocks.table.key_columns", "id"),
            ]),
            &IcebergSchemaChange::DropColumn {
                name: "V".to_string(),
            },
        )
        .expect("updates");

        assert!(changes.sets.is_empty());
        assert_eq!(
            sorted(changes.removals),
            vec![
                "novarocks.column_agg.v".to_string(),
                "novarocks.logical_type.v".to_string()
            ]
        );
    }

    #[test]
    fn drop_column_rejects_key_column() {
        let err = build_property_updates_for_test(
            &props(&[("novarocks.table.key_columns", "id,v")]),
            &IcebergSchemaChange::DropColumn {
                name: "V".to_string(),
            },
        )
        .expect_err("key column drop");
        assert!(err.contains("key column"));
    }

    #[test]
    fn rename_column_moves_properties_and_updates_key_columns() {
        let changes = build_property_updates_for_test(
            &props(&[
                ("novarocks.logical_type.old_col", "smallint"),
                ("novarocks.column_agg.old_col", "max"),
                ("novarocks.table.key_columns", "id,old_col"),
            ]),
            &IcebergSchemaChange::RenameColumn {
                old_name: "old_col".to_string(),
                new_name: "New_Col".to_string(),
            },
        )
        .expect("updates");

        assert_eq!(
            changes.sets.get("novarocks.logical_type.new_col"),
            Some(&"smallint".to_string())
        );
        assert_eq!(
            changes.sets.get("novarocks.column_agg.new_col"),
            Some(&"max".to_string())
        );
        assert_eq!(
            changes.sets.get("novarocks.table.key_columns"),
            Some(&"id,new_col".to_string())
        );
        assert_eq!(
            sorted(changes.removals),
            vec![
                "novarocks.column_agg.old_col".to_string(),
                "novarocks.logical_type.old_col".to_string()
            ]
        );
    }

    #[test]
    fn modify_column_updates_or_removes_logical_type_property() {
        let bigint = build_property_updates_for_test(
            &props(&[("novarocks.logical_type.id", "tinyint")]),
            &IcebergSchemaChange::ModifyColumn {
                name: "ID".to_string(),
                new_type: SqlType::BigInt,
            },
        )
        .expect("updates");
        assert!(bigint.sets.is_empty());
        assert_eq!(
            bigint.removals,
            vec!["novarocks.logical_type.id".to_string()]
        );

        let decimal = build_property_updates_for_test(
            &HashMap::new(),
            &IcebergSchemaChange::ModifyColumn {
                name: "amount".to_string(),
                new_type: SqlType::Decimal {
                    precision: 12,
                    scale: 2,
                },
            },
        )
        .expect("updates");
        assert_eq!(
            decimal.sets.get("novarocks.logical_type.amount"),
            Some(&"decimal(12,2)".to_string())
        );
        assert!(decimal.removals.is_empty());
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};

use crate::connector::iceberg::catalog::registry::{
    TABLE_KEY_COLUMNS_PROPERTY, column_aggregation_property_key, logical_type_property_key,
    logical_type_property_value,
};
use crate::engine::StandaloneState;
use crate::engine::backend_resolver::resolve_existing_table_target;
use crate::engine::catalog::normalize_identifier;
use crate::engine::statement::{AlterIcebergSchemaStmt, IcebergSchemaChange};
use crate::sql::parser::ast::SqlType;

#[cfg(test)]
pub(crate) fn apply_change_to_schema_for_test(
    current: &Schema,
    last_column_id: i32,
    change: &IcebergSchemaChange,
) -> Result<Schema, String> {
    build_updated_schema(current, last_column_id, change)
}

fn build_updated_schema(
    current: &Schema,
    last_column_id: i32,
    change: &IcebergSchemaChange,
) -> Result<Schema, String> {
    reject_reserved_change(change)?;
    let identifier_field_ids = current.identifier_field_ids().collect::<Vec<_>>();
    let mut fields = current
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<_>>();

    match change {
        IcebergSchemaChange::AddColumn {
            name, data_type, ..
        } => {
            reject_name_conflict(&fields, name)?;
            let mut next_nested_id = last_column_id
                .checked_add(2)
                .ok_or_else(|| "too many iceberg columns".to_string())?;
            let ty = crate::connector::iceberg::catalog::registry::iceberg_type_for_sql_type(
                data_type,
                &mut next_nested_id,
            )?;
            let id = last_column_id
                .checked_add(1)
                .ok_or_else(|| "too many iceberg columns".to_string())?;
            fields.push(NestedField::optional(id, name, ty));
        }
        IcebergSchemaChange::DropColumn { name } => {
            let normalized = normalize_identifier(name)?;
            let field_id = fields
                .iter()
                .find(|f| {
                    normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str())
                })
                .map(|f| f.id)
                .ok_or_else(|| format!("unknown Iceberg column `{name}`"))?;
            if identifier_field_ids.contains(&field_id) {
                return Err(format!(
                    "Iceberg schema evolution cannot drop identifier column `{name}`"
                ));
            }
            fields.retain(|f| {
                normalize_identifier(&f.name).ok().as_deref() != Some(normalized.as_str())
            });
        }
        IcebergSchemaChange::RenameColumn { old_name, new_name } => {
            reject_name_conflict(&fields, new_name)?;
            let normalized = normalize_identifier(old_name)?;
            let field = fields
                .iter_mut()
                .find(|f| {
                    normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str())
                })
                .ok_or_else(|| format!("unknown Iceberg column `{old_name}`"))?;
            field.name = new_name.clone();
        }
        IcebergSchemaChange::ModifyColumn { name, new_type } => {
            let normalized = normalize_identifier(name)?;
            let field = fields
                .iter_mut()
                .find(|f| {
                    normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str())
                })
                .ok_or_else(|| format!("unknown Iceberg column `{name}`"))?;
            field.field_type = Box::new(widen_type(field.field_type.as_ref(), new_type)?);
        }
    }

    Schema::builder()
        .with_fields(fields.into_iter().map(Arc::new).collect::<Vec<_>>())
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("build evolved iceberg schema failed: {e}"))
}

fn reject_name_conflict(fields: &[NestedField], name: &str) -> Result<(), String> {
    let normalized = normalize_identifier(name)?;
    if fields
        .iter()
        .any(|f| normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str()))
    {
        return Err(format!("Iceberg column `{name}` already exists"));
    }
    Ok(())
}

fn reject_reserved_change(change: &IcebergSchemaChange) -> Result<(), String> {
    let names: Vec<&str> = match change {
        IcebergSchemaChange::AddColumn { name, .. } => vec![name.as_str()],
        IcebergSchemaChange::DropColumn { name } => vec![name.as_str()],
        IcebergSchemaChange::RenameColumn { old_name, new_name } => {
            vec![old_name.as_str(), new_name.as_str()]
        }
        IcebergSchemaChange::ModifyColumn { name, .. } => vec![name.as_str()],
    };
    for name in names {
        if crate::exec::row_position::is_iceberg_row_id(name)
            || crate::exec::row_position::is_iceberg_last_updated_sequence_number(name)
        {
            return Err(format!(
                "Iceberg schema evolution cannot modify reserved column `{name}`"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
fn reject_drop_dependencies_for_test(
    column: &str,
    equality_delete_columns: &[String],
    mv_sqls: &[String],
) -> Result<(), String> {
    let target = ManagedMvTarget::new("ice", "ns", "orders")?;
    let mv_dependencies = mv_sqls
        .iter()
        .map(|sql| ManagedMvDependency {
            select_sql: sql.clone(),
            target: target.clone(),
        })
        .collect::<Vec<_>>();
    reject_drop_dependencies(column, equality_delete_columns, &mv_dependencies)
}

fn reject_drop_dependencies(
    column: &str,
    equality_delete_columns: &[String],
    mv_dependencies: &[ManagedMvDependency],
) -> Result<(), String> {
    let normalized = normalize_identifier(column)?;
    if equality_delete_columns
        .iter()
        .any(|c| normalize_identifier(c).ok().as_deref() == Some(normalized.as_str()))
    {
        return Err(format!(
            "DROP COLUMN `{column}` is blocked because an Iceberg equality-delete file references it"
        ));
    }
    for dependency in mv_dependencies {
        if managed_mv_depends_on_column(dependency, &normalized) {
            return Err(format!(
                "DROP COLUMN `{column}` is blocked because a managed materialized view references it"
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct ManagedMvDependency {
    select_sql: String,
    target: ManagedMvTarget,
}

#[derive(Clone, Debug)]
struct ManagedMvTarget {
    catalog: String,
    namespace: String,
    table: String,
}

impl ManagedMvTarget {
    fn new(catalog: &str, namespace: &str, table: &str) -> Result<Self, String> {
        Ok(Self {
            catalog: normalize_identifier(catalog)?,
            namespace: normalize_identifier(namespace)?,
            table: normalize_identifier(table)?,
        })
    }

    fn from_backend(
        target: &crate::engine::backend_resolver::TargetBackend,
    ) -> Result<Self, String> {
        Self::new(&target.catalog, &target.namespace, &target.table)
    }
}

fn managed_mv_depends_on_column(
    dependency: &ManagedMvDependency,
    normalized_identifier: &str,
) -> bool {
    sql_mentions_identifier(&dependency.select_sql, normalized_identifier)
        || sql_projects_target_wildcard(&dependency.select_sql, &dependency.target)
}

fn sql_mentions_identifier(sql: &str, normalized_identifier: &str) -> bool {
    sql.split(|ch: char| !(ch == '_' || ch.is_ascii_alphanumeric()))
        .filter(|token| !token.is_empty())
        .any(|token| token.eq_ignore_ascii_case(normalized_identifier))
}

fn sql_projects_target_wildcard(sql: &str, target: &ManagedMvTarget) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(statement) = crate::sql::parser::parse_normalized_sql_raw(&normalized) else {
        return false;
    };
    let sqlparser::ast::Statement::Query(query) = statement else {
        return false;
    };
    query_projects_target_wildcard(&query, target)
}

fn query_projects_target_wildcard(query: &sqlparser::ast::Query, target: &ManagedMvTarget) -> bool {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if query_projects_target_wildcard(&cte.query, target) {
                return true;
            }
        }
    }
    set_expr_projects_target_wildcard(query.body.as_ref(), target)
}

fn set_expr_projects_target_wildcard(
    set_expr: &sqlparser::ast::SetExpr,
    target: &ManagedMvTarget,
) -> bool {
    match set_expr {
        sqlparser::ast::SetExpr::Select(select) => select_projects_target_wildcard(select, target),
        sqlparser::ast::SetExpr::Query(query) => query_projects_target_wildcard(query, target),
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            set_expr_projects_target_wildcard(left, target)
                || set_expr_projects_target_wildcard(right, target)
        }
        _ => false,
    }
}

fn select_projects_target_wildcard(
    select: &sqlparser::ast::Select,
    target: &ManagedMvTarget,
) -> bool {
    let mut target_qualifiers = HashSet::new();
    for table_with_joins in &select.from {
        if collect_target_qualifiers_from_table_with_joins(
            table_with_joins,
            target,
            &mut target_qualifiers,
        ) {
            return true;
        }
    }

    select.projection.iter().any(|item| match item {
        sqlparser::ast::SelectItem::Wildcard(_) => !target_qualifiers.is_empty(),
        sqlparser::ast::SelectItem::QualifiedWildcard(kind, _) => {
            qualified_wildcard_matches_target(kind, &target_qualifiers)
        }
        _ => false,
    })
}

fn collect_target_qualifiers_from_table_with_joins(
    table_with_joins: &sqlparser::ast::TableWithJoins,
    target: &ManagedMvTarget,
    qualifiers: &mut HashSet<String>,
) -> bool {
    if collect_target_qualifiers_from_factor(&table_with_joins.relation, target, qualifiers) {
        return true;
    }
    for join in &table_with_joins.joins {
        if collect_target_qualifiers_from_factor(&join.relation, target, qualifiers) {
            return true;
        }
    }
    false
}

fn collect_target_qualifiers_from_factor(
    factor: &sqlparser::ast::TableFactor,
    target: &ManagedMvTarget,
    qualifiers: &mut HashSet<String>,
) -> bool {
    match factor {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => {
            if object_name_matches_target(name, target) {
                qualifiers.extend(object_name_qualifier_keys(name));
                if let Some(alias) = alias
                    && let Ok(normalized) = normalize_identifier(&alias.name.value)
                {
                    qualifiers.insert(normalized);
                }
            }
            false
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            query_projects_target_wildcard(subquery, target)
        }
        sqlparser::ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_target_qualifiers_from_table_with_joins(table_with_joins, target, qualifiers),
        sqlparser::ast::TableFactor::Pivot { table, .. }
        | sqlparser::ast::TableFactor::Unpivot { table, .. }
        | sqlparser::ast::TableFactor::MatchRecognize { table, .. } => {
            collect_target_qualifiers_from_factor(table, target, qualifiers)
        }
        _ => false,
    }
}

fn qualified_wildcard_matches_target(
    kind: &sqlparser::ast::SelectItemQualifiedWildcardKind,
    target_qualifiers: &HashSet<String>,
) -> bool {
    match kind {
        sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
            object_name_qualifier_keys(name)
                .into_iter()
                .any(|key| target_qualifiers.contains(&key))
        }
        sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr) => expr_qualifier_keys(expr)
            .into_iter()
            .any(|key| target_qualifiers.contains(&key)),
    }
}

fn expr_qualifier_keys(expr: &sqlparser::ast::Expr) -> Vec<String> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => normalize_identifier(&ident.value)
            .map(|name| vec![name])
            .unwrap_or_default(),
        sqlparser::ast::Expr::CompoundIdentifier(idents) => {
            let parts = idents
                .iter()
                .map(|ident| normalize_identifier(&ident.value))
                .collect::<Result<Vec<_>, _>>();
            parts
                .map(|parts| qualifier_keys_from_parts(&parts))
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

fn object_name_matches_target(name: &sqlparser::ast::ObjectName, target: &ManagedMvTarget) -> bool {
    let parts = normalized_object_name_parts(name);
    match parts.as_deref() {
        Some([catalog, namespace, table]) => {
            catalog == &target.catalog && namespace == &target.namespace && table == &target.table
        }
        Some([namespace, table]) => namespace == &target.namespace && table == &target.table,
        Some([table]) => table == &target.table,
        _ => false,
    }
}

fn object_name_qualifier_keys(name: &sqlparser::ast::ObjectName) -> Vec<String> {
    normalized_object_name_parts(name)
        .map(|parts| qualifier_keys_from_parts(&parts))
        .unwrap_or_default()
}

fn qualifier_keys_from_parts(parts: &[String]) -> Vec<String> {
    let mut keys = Vec::new();
    if !parts.is_empty() {
        keys.push(parts.join("."));
        if parts.len() >= 2 {
            keys.push(parts[parts.len() - 2..].join("."));
        }
        keys.push(parts[parts.len() - 1].clone());
    }
    keys
}

fn normalized_object_name_parts(name: &sqlparser::ast::ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => normalize_identifier(&ident.value),
            _ => Err("unsupported object name part".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()
        .ok()
}

fn widen_type(current: &Type, new_type: &SqlType) -> Result<Type, String> {
    match (current, new_type) {
        (Type::Primitive(PrimitiveType::Int), SqlType::BigInt) => {
            Ok(Type::Primitive(PrimitiveType::Long))
        }
        (Type::Primitive(PrimitiveType::Float), SqlType::Double) => {
            Ok(Type::Primitive(PrimitiveType::Double))
        }
        _ => Err(format!(
            "unsupported Iceberg type evolution: {current:?} -> {new_type:?}"
        )),
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SchemaPropertyUpdates {
    sets: HashMap<String, String>,
    removals: Vec<String>,
}

impl SchemaPropertyUpdates {
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.sets.is_empty() && self.removals.is_empty()
    }

    fn push_removal(&mut self, key: String) {
        if !self.removals.contains(&key) {
            self.removals.push(key);
        }
    }

    fn into_table_updates(self) -> Vec<TableUpdate> {
        let mut updates = Vec::new();
        if !self.sets.is_empty() {
            updates.push(TableUpdate::SetProperties { updates: self.sets });
        }
        if !self.removals.is_empty() {
            updates.push(TableUpdate::RemoveProperties {
                removals: self.removals,
            });
        }
        updates
    }
}

#[cfg(test)]
fn build_property_updates_for_test(
    properties: &HashMap<String, String>,
    change: &IcebergSchemaChange,
) -> Result<SchemaPropertyUpdates, String> {
    build_property_updates(properties, change)
}

fn build_property_updates(
    properties: &HashMap<String, String>,
    change: &IcebergSchemaChange,
) -> Result<SchemaPropertyUpdates, String> {
    let mut updates = SchemaPropertyUpdates::default();
    match change {
        IcebergSchemaChange::AddColumn {
            name, data_type, ..
        } => {
            if let Some(value) = logical_type_property_value(data_type) {
                updates.sets.insert(logical_type_property_key(name)?, value);
            }
        }
        IcebergSchemaChange::DropColumn { name } => {
            reject_key_column_drop(properties, name)?;
            let logical_key = logical_type_property_key(name)?;
            if properties.contains_key(&logical_key) {
                updates.push_removal(logical_key);
            }
            let aggregation_key = column_aggregation_property_key(name)?;
            if properties.contains_key(&aggregation_key) {
                updates.push_removal(aggregation_key);
            }
        }
        IcebergSchemaChange::RenameColumn { old_name, new_name } => {
            let old_logical_key = logical_type_property_key(old_name)?;
            if let Some(value) = properties.get(&old_logical_key) {
                updates
                    .sets
                    .insert(logical_type_property_key(new_name)?, value.clone());
                updates.push_removal(old_logical_key);
            }

            let old_aggregation_key = column_aggregation_property_key(old_name)?;
            if let Some(value) = properties.get(&old_aggregation_key) {
                updates
                    .sets
                    .insert(column_aggregation_property_key(new_name)?, value.clone());
                updates.push_removal(old_aggregation_key);
            }

            if let Some(key_columns) = rename_key_columns(properties, old_name, new_name)? {
                updates
                    .sets
                    .insert(TABLE_KEY_COLUMNS_PROPERTY.to_string(), key_columns);
            }
        }
        IcebergSchemaChange::ModifyColumn { name, new_type } => {
            let logical_key = logical_type_property_key(name)?;
            if let Some(value) = logical_type_property_value(new_type) {
                updates.sets.insert(logical_key, value);
            } else if properties.contains_key(&logical_key) {
                updates.push_removal(logical_key);
            }
        }
    }
    Ok(updates)
}

fn reject_key_column_drop(properties: &HashMap<String, String>, name: &str) -> Result<(), String> {
    let normalized = normalize_identifier(name)?;
    let Some(key_columns) = properties.get(TABLE_KEY_COLUMNS_PROPERTY) else {
        return Ok(());
    };
    for column in normalized_key_columns(key_columns)? {
        if column == normalized {
            return Err(format!(
                "Iceberg schema evolution cannot drop key column `{name}`"
            ));
        }
    }
    Ok(())
}

fn rename_key_columns(
    properties: &HashMap<String, String>,
    old_name: &str,
    new_name: &str,
) -> Result<Option<String>, String> {
    let Some(key_columns) = properties.get(TABLE_KEY_COLUMNS_PROPERTY) else {
        return Ok(None);
    };
    let old_normalized = normalize_identifier(old_name)?;
    let new_normalized = normalize_identifier(new_name)?;
    let mut renamed = false;
    let columns = normalized_key_columns(key_columns)?
        .into_iter()
        .map(|column| {
            if column == old_normalized {
                renamed = true;
                new_normalized.clone()
            } else {
                column
            }
        })
        .collect::<Vec<_>>();

    Ok(renamed.then(|| columns.join(",")))
}

fn normalized_key_columns(value: &str) -> Result<Vec<String>, String> {
    value
        .split(',')
        .filter(|column| !column.trim().is_empty())
        .map(|column| normalize_identifier(column.trim()))
        .collect()
}

struct SchemaUpdateTxnAction {
    change: IcebergSchemaChange,
}

#[async_trait]
impl TransactionAction for SchemaUpdateTxnAction {
    async fn commit(
        self: Arc<Self>,
        table: &iceberg::table::Table,
    ) -> iceberg::Result<ActionCommit> {
        let metadata = table.metadata();
        let current_schema = metadata.current_schema();
        let new_schema =
            build_updated_schema(current_schema, metadata.last_column_id(), &self.change)
                .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e))?;
        let property_updates = build_property_updates(metadata.properties(), &self.change)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e))?;
        let mut updates = vec![
            TableUpdate::AddSchema { schema: new_schema },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];
        updates.extend(property_updates.into_table_updates());

        Ok(ActionCommit::new(
            updates,
            vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: metadata.current_schema_id(),
                },
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id: metadata.last_column_id(),
                },
            ],
        ))
    }
}

pub(crate) fn alter_table_schema(
    state: &Arc<StandaloneState>,
    stmt: &AlterIcebergSchemaStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), String> {
    let target =
        resolve_existing_table_target(state, &stmt.table, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        return Err(
            "Iceberg schema evolution only supports standalone iceberg catalogs".to_string(),
        );
    }

    protect_schema_change(state, &target, &stmt.change)?;

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let loaded = crate::connector::iceberg::catalog::registry::load_table(
        &entry,
        &target.namespace,
        &target.table,
    )?;

    let commit_result = (|| {
        let catalog = crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)?;
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            let tx = Transaction::new(&loaded.table);
            let tx = SchemaUpdateTxnAction {
                change: stmt.change.clone(),
            }
            .apply(tx)?;
            tx.commit(&catalog).await
        })
        .map_err(|e| format!("alter iceberg schema runtime failed: {e}"))?
        .map_err(|e| format!("alter iceberg schema failed: {e}"))?;
        Ok::<(), String>(())
    })();

    entry.invalidate_table_cache(&target.namespace, &target.table);
    commit_result?;

    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, &target)?;
    Ok(())
}

fn protect_schema_change(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    change: &IcebergSchemaChange,
) -> Result<(), String> {
    reject_reserved_change(change)?;
    let IcebergSchemaChange::DropColumn { name } = change else {
        return Ok(());
    };

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let loaded = crate::connector::iceberg::catalog::registry::load_table(
        &entry,
        &target.namespace,
        &target.table,
    )?;
    let metadata = loaded.table.metadata();
    build_updated_schema(metadata.current_schema(), metadata.last_column_id(), change)?;
    build_property_updates(metadata.properties(), change)?;

    let equality_delete_columns =
        crate::connector::iceberg::catalog::registry::current_equality_delete_column_names(
            &loaded.table,
        )?;

    let mv_dependencies = managed_mv_dependencies_for_target(state, target)?;
    reject_drop_dependencies(name, &equality_delete_columns, &mv_dependencies)
}

fn managed_mv_dependencies_for_target(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<Vec<ManagedMvDependency>, String> {
    let Some(store) = state.metadata_store.as_ref() else {
        return Ok(Vec::new());
    };
    let snapshot = store.load_snapshot()?.managed;
    let target_key = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
    let target_key_lower = target_key.to_ascii_lowercase();
    let target = ManagedMvTarget::from_backend(target)?;
    Ok(snapshot
        .materialized_views
        .into_iter()
        .filter(|mv| {
            mv.base_table_refs.iter().any(|base| {
                base.catalog.eq_ignore_ascii_case(&target.catalog)
                    && base.namespace.eq_ignore_ascii_case(&target.namespace)
                    && base.table.eq_ignore_ascii_case(&target.table)
            }) || mv
                .select_sql
                .to_ascii_lowercase()
                .contains(&target_key_lower)
        })
        .map(|mv| ManagedMvDependency {
            select_sql: mv.select_sql,
            target: target.clone(),
        })
        .collect())
}
