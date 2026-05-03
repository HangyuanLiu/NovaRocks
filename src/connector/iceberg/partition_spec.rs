use iceberg::spec::{
    PartitionSpecRef, PrimitiveType, Schema, Transform, Type, UnboundPartitionField,
    UnboundPartitionSpec, UnboundPartitionSpecBuilder,
};

use crate::engine::catalog::normalize_identifier;
use crate::sql::parser::ast::IcebergPartitionFieldExpr;

pub(crate) fn build_initial_partition_spec(
    schema: &Schema,
    fields: &[IcebergPartitionFieldExpr],
) -> Result<Option<UnboundPartitionSpec>, String> {
    if fields.is_empty() {
        return Ok(None);
    }

    let mut builder = UnboundPartitionSpec::builder();
    for field in fields {
        let source_id = source_field_id(schema, field)?;
        validate_transform(schema, source_id, field)?;
        builder = builder
            .add_partition_field(source_id, stable_field_name(field), to_transform(field))
            .map_err(|e| format!("build iceberg partition spec failed: {e}"))?;
    }
    Ok(Some(builder.build()))
}

#[allow(dead_code)]
pub(crate) fn build_evolved_partition_spec(
    schema: &Schema,
    current: &PartitionSpecRef,
    change: PartitionSpecChange<'_>,
) -> Result<UnboundPartitionSpec, String> {
    let mut fields: Vec<UnboundPartitionField> =
        current.fields().iter().cloned().map(Into::into).collect();

    match change {
        PartitionSpecChange::Add(expr) => {
            let source_id = source_field_id(schema, expr)?;
            validate_transform(schema, source_id, expr)?;
            let transform = to_transform(expr);
            if fields
                .iter()
                .any(|field| field.source_id == source_id && field.transform == transform)
            {
                return Err(format!(
                    "partition field `{}` already exists in current default spec {}",
                    stable_field_name(expr),
                    current.spec_id()
                ));
            }
            fields.push(UnboundPartitionField {
                source_id,
                field_id: None,
                name: stable_field_name(expr),
                transform,
            });
        }
        PartitionSpecChange::Drop(expr) => {
            let source_id = source_field_id(schema, expr)?;
            let transform = to_transform(expr);
            let before = fields.len();
            fields.retain(|field| !(field.source_id == source_id && field.transform == transform));
            if fields.len() == before {
                return Err(format!(
                    "partition field `{}` is not present in current default spec {}",
                    stable_field_name(expr),
                    current.spec_id()
                ));
            }
        }
    }

    let mut builder = UnboundPartitionSpecBuilder::new();
    for field in fields {
        builder = builder
            .add_partition_fields([field])
            .map_err(|e| format!("build evolved iceberg partition spec failed: {e}"))?;
    }
    Ok(builder.build())
}

#[allow(dead_code)]
pub(crate) enum PartitionSpecChange<'a> {
    Add(&'a IcebergPartitionFieldExpr),
    Drop(&'a IcebergPartitionFieldExpr),
}

#[allow(dead_code)]
pub(crate) fn spec_count(table: &iceberg::table::Table) -> usize {
    table.metadata().partition_specs_iter().count()
}

#[allow(dead_code)]
pub(crate) fn partition_spec_by_id(
    table: &iceberg::table::Table,
    spec_id: i32,
) -> Result<PartitionSpecRef, String> {
    table
        .metadata()
        .partition_spec_by_id(spec_id)
        .cloned()
        .ok_or_else(|| format!("iceberg table metadata missing partition spec id {spec_id}"))
}

fn source_field_id(schema: &Schema, expr: &IcebergPartitionFieldExpr) -> Result<i32, String> {
    let column = normalize_identifier(source_column(expr))?;
    schema
        .field_by_name_case_insensitive(&column)
        .map(|field| field.id)
        .ok_or_else(|| format!("partition source column `{column}` does not exist"))
}

fn source_column(expr: &IcebergPartitionFieldExpr) -> &str {
    match expr {
        IcebergPartitionFieldExpr::Identity { column }
        | IcebergPartitionFieldExpr::Year { column }
        | IcebergPartitionFieldExpr::Month { column }
        | IcebergPartitionFieldExpr::Day { column }
        | IcebergPartitionFieldExpr::Hour { column }
        | IcebergPartitionFieldExpr::Bucket { column, .. }
        | IcebergPartitionFieldExpr::Truncate { column, .. }
        | IcebergPartitionFieldExpr::Void { column } => column,
    }
}

fn to_transform(expr: &IcebergPartitionFieldExpr) -> Transform {
    match expr {
        IcebergPartitionFieldExpr::Identity { .. } => Transform::Identity,
        IcebergPartitionFieldExpr::Year { .. } => Transform::Year,
        IcebergPartitionFieldExpr::Month { .. } => Transform::Month,
        IcebergPartitionFieldExpr::Day { .. } => Transform::Day,
        IcebergPartitionFieldExpr::Hour { .. } => Transform::Hour,
        IcebergPartitionFieldExpr::Bucket { num_buckets, .. } => Transform::Bucket(*num_buckets),
        IcebergPartitionFieldExpr::Truncate { width, .. } => Transform::Truncate(*width),
        IcebergPartitionFieldExpr::Void { .. } => Transform::Void,
    }
}

fn stable_field_name(expr: &IcebergPartitionFieldExpr) -> String {
    let normalized = normalize_identifier(source_column(expr))
        .unwrap_or_else(|_| source_column(expr).to_string());
    match expr {
        IcebergPartitionFieldExpr::Identity { .. } => normalized,
        IcebergPartitionFieldExpr::Year { .. } => format!("{normalized}_year"),
        IcebergPartitionFieldExpr::Month { .. } => format!("{normalized}_month"),
        IcebergPartitionFieldExpr::Day { .. } => format!("{normalized}_day"),
        IcebergPartitionFieldExpr::Hour { .. } => format!("{normalized}_hour"),
        IcebergPartitionFieldExpr::Bucket { num_buckets, .. } => {
            format!("{normalized}_bucket_{num_buckets}")
        }
        IcebergPartitionFieldExpr::Truncate { width, .. } => {
            format!("{normalized}_truncate_{width}")
        }
        IcebergPartitionFieldExpr::Void { .. } => format!("{normalized}_void"),
    }
}

fn validate_transform(
    schema: &Schema,
    source_id: i32,
    expr: &IcebergPartitionFieldExpr,
) -> Result<(), String> {
    let field = schema
        .field_by_id(source_id)
        .ok_or_else(|| format!("partition source field id {source_id} is missing"))?;
    let source_type = field.field_type.as_ref();
    match expr {
        IcebergPartitionFieldExpr::Year { .. }
        | IcebergPartitionFieldExpr::Month { .. }
        | IcebergPartitionFieldExpr::Day { .. } => {
            if !matches!(
                source_type,
                Type::Primitive(
                    PrimitiveType::Date
                        | PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs
                )
            ) {
                return Err(format!(
                    "temporal partition transform requires date/timestamp source, got {source_type}"
                ));
            }
        }
        IcebergPartitionFieldExpr::Hour { .. } => {
            if !matches!(
                source_type,
                Type::Primitive(
                    PrimitiveType::Timestamp
                        | PrimitiveType::Timestamptz
                        | PrimitiveType::TimestampNs
                        | PrimitiveType::TimestamptzNs
                )
            ) {
                return Err(format!(
                    "temporal partition transform requires timestamp source, got {source_type}"
                ));
            }
        }
        IcebergPartitionFieldExpr::Bucket { .. }
        | IcebergPartitionFieldExpr::Truncate { .. }
        | IcebergPartitionFieldExpr::Identity { .. }
        | IcebergPartitionFieldExpr::Void { .. } => {
            to_transform(expr)
                .result_type(source_type)
                .map_err(|e| format!("invalid iceberg partition transform: {e}"))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Transform, Type};

    use super::*;

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "ts",
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn initial_spec_builds_expected_transforms() {
        let spec = build_initial_partition_spec(
            &schema(),
            &[
                IcebergPartitionFieldExpr::Month {
                    column: "ts".to_string(),
                },
                IcebergPartitionFieldExpr::Bucket {
                    column: "id".to_string(),
                    num_buckets: 16,
                },
                IcebergPartitionFieldExpr::Truncate {
                    column: "name".to_string(),
                    width: 8,
                },
            ],
        )
        .unwrap()
        .unwrap()
        .bind(Arc::new(schema()))
        .unwrap();

        assert_eq!(spec.fields().len(), 3);
        assert_eq!(spec.fields()[0].name, "ts_month");
        assert_eq!(spec.fields()[0].transform, Transform::Month);
        assert_eq!(spec.fields()[1].name, "id_bucket_16");
        assert_eq!(spec.fields()[1].transform, Transform::Bucket(16));
        assert_eq!(spec.fields()[2].name, "name_truncate_8");
        assert_eq!(spec.fields()[2].transform, Transform::Truncate(8));
    }

    #[test]
    fn temporal_transform_rejects_non_temporal_source() {
        let err = build_initial_partition_spec(
            &schema(),
            &[IcebergPartitionFieldExpr::Month {
                column: "name".to_string(),
            }],
        )
        .unwrap_err();
        assert!(err.contains("date/timestamp"), "{err}");
    }
}
