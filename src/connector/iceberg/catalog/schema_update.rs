#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::ast::SqlType;
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
                default_null: true,
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
                default_null: false,
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
                default_null: true,
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
    fn add_column_sets_logical_type_property_only_when_needed() {
        let tinyint = build_property_updates_for_test(
            &HashMap::new(),
            &IcebergSchemaChange::AddColumn {
                name: "New_Col".to_string(),
                data_type: SqlType::TinyInt,
                default_null: true,
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
                default_null: true,
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

use std::collections::HashMap;
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
    _state: &Arc<StandaloneState>,
    _target: &crate::engine::backend_resolver::TargetBackend,
    change: &IcebergSchemaChange,
) -> Result<(), String> {
    reject_reserved_change(change)
}
