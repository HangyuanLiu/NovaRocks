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

use std::collections::HashMap;

use arrow::datatypes::DataType;
use iceberg::spec::TableMetadata;

use crate::exec::expr::function::lookup_function;
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use crate::proto::plan;
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

type PartitionMetadataInfo = (Vec<String>, Vec<String>, Vec<String>);

pub(crate) fn partition_info_from_metadata(
    metadata: Option<&TableMetadata>,
    target_partition_spec_id: i32,
) -> Result<PartitionMetadataInfo, NativeFragmentLeafDecodeError> {
    let Some(metadata) = metadata else {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    };
    let spec = metadata
        .partition_spec_by_id(target_partition_spec_id)
        .ok_or_else(|| {
            format!(
                "native Iceberg write sink target partition spec id {target_partition_spec_id} not found"
            )
        })?;
    let schema = metadata.current_schema();
    let mut source_names = Vec::with_capacity(spec.fields().len());
    let mut partition_names = Vec::with_capacity(spec.fields().len());
    let mut transforms = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "native Iceberg write sink partition source field id {} not found",
                field.source_id
            )
        })?;
        source_names.push(source.name.clone());
        partition_names.push(field.name.clone());
        transforms.push(field.transform.to_string());
    }
    Ok((source_names, partition_names, transforms))
}

pub(crate) fn build_partition_exprs_from_output_exprs(
    partition_source_column_names: &[String],
    transform_exprs: &[String],
    target_columns: &[plan::ColumnDef],
    output_exprs: &[ExprId],
    arena: &mut ExprArena,
) -> Result<Vec<ExprId>, NativeFragmentLeafDecodeError> {
    if partition_source_column_names.len() != transform_exprs.len() {
        return Err(format!(
            "native Iceberg write sink partition metadata mismatch: sources={} transforms={}",
            partition_source_column_names.len(),
            transform_exprs.len()
        )
        .into());
    }
    if target_columns.len() != output_exprs.len() {
        return Err(format!(
            "native Iceberg write sink partition expr source mismatch: columns={} output_exprs={}",
            target_columns.len(),
            output_exprs.len()
        )
        .into());
    }

    let mut expr_by_column_name = HashMap::with_capacity(target_columns.len());
    for (column, expr) in target_columns.iter().zip(output_exprs.iter().copied()) {
        expr_by_column_name.insert(column.name.to_ascii_lowercase(), expr);
    }

    partition_source_column_names
        .iter()
        .zip(transform_exprs.iter())
        .enumerate()
        .map(|(idx, (source_name, transform))| {
            let source_expr = expr_by_column_name
                .get(&source_name.to_ascii_lowercase())
                .copied()
                .ok_or_else(|| {
                    format!(
                        "native Iceberg write sink partition source column {} is not in target output columns",
                        source_name
                    )
                })?;
            build_partition_expr_from_transform(transform, source_expr, arena)
                .map_err(|err| NativeFragmentLeafDecodeError::new(format!("native Iceberg write sink partition expr[{idx}]: {err}")))
        })
        .collect()
}

fn build_partition_expr_from_transform(
    transform: &str,
    source_expr: ExprId,
    arena: &mut ExprArena,
) -> Result<ExprId, String> {
    let normalized = transform.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "identity" => Ok(source_expr),
        "void" => push_partition_transform_call(
            "__iceberg_transform_void",
            vec![source_expr],
            DataType::Null,
            arena,
        ),
        "year" => push_partition_transform_call(
            "__iceberg_transform_year",
            vec![source_expr],
            DataType::Int64,
            arena,
        ),
        "month" => push_partition_transform_call(
            "__iceberg_transform_month",
            vec![source_expr],
            DataType::Int64,
            arena,
        ),
        "day" => push_partition_transform_call(
            "__iceberg_transform_day",
            vec![source_expr],
            DataType::Int64,
            arena,
        ),
        "hour" => push_partition_transform_call(
            "__iceberg_transform_hour",
            vec![source_expr],
            DataType::Int64,
            arena,
        ),
        value if value.starts_with("bucket[") && value.ends_with(']') => {
            let width = parse_transform_width(value, "bucket")?;
            let width_expr = arena.push_typed(
                ExprNode::Literal(LiteralValue::Int64(width)),
                DataType::Int64,
            );
            push_partition_transform_call(
                "__iceberg_transform_bucket",
                vec![source_expr, width_expr],
                DataType::Int32,
                arena,
            )
        }
        value if value.starts_with("truncate[") && value.ends_with(']') => {
            let width = parse_transform_width(value, "truncate")?;
            let width_expr = arena.push_typed(
                ExprNode::Literal(LiteralValue::Int64(width)),
                DataType::Int64,
            );
            let source_type = arena
                .data_type(source_expr)
                .cloned()
                .ok_or_else(|| "partition source expression missing data type".to_string())?;
            push_partition_transform_call(
                "__iceberg_transform_truncate",
                vec![source_expr, width_expr],
                source_type,
                arena,
            )
        }
        other => Err(format!("unsupported Iceberg partition transform {other}")),
    }
}

fn parse_transform_width(transform: &str, name: &str) -> Result<i64, String> {
    let prefix = format!("{name}[");
    let raw = transform
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| format!("invalid Iceberg {name} transform syntax: {transform}"))?;
    let width = raw
        .parse::<i64>()
        .map_err(|e| format!("invalid Iceberg {name} transform width {raw}: {e}"))?;
    if width <= 0 {
        return Err(format!(
            "Iceberg {name} transform width must be positive, got {width}"
        ));
    }
    Ok(width)
}

fn push_partition_transform_call(
    name: &str,
    args: Vec<ExprId>,
    data_type: DataType,
    arena: &mut ExprArena,
) -> Result<ExprId, String> {
    let kind = lookup_function(name)
        .ok_or_else(|| format!("native Iceberg partition transform function {name} is missing"))?;
    Ok(arena.push_typed(ExprNode::FunctionCall { kind, args }, data_type))
}

pub(crate) fn partition_source_field_ids_from_metadata(
    metadata: &TableMetadata,
    source_column_names: &[String],
) -> Result<Vec<i32>, NativeFragmentLeafDecodeError> {
    let target_schema = metadata.current_schema();
    source_column_names
        .iter()
        .map(|source_name| {
            target_schema
                .field_by_name_case_insensitive(source_name)
                .map(|field| field.id)
                .ok_or_else(|| {
                    NativeFragmentLeafDecodeError::new(format!(
                        "native Iceberg sink partition source column {source_name} missing from target metadata schema"
                    ))
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::expr::function::FunctionKind;

    fn column_def(name: &str) -> plan::ColumnDef {
        plan::ColumnDef {
            name: name.to_string(),
            data_type: None,
            nullable: true,
            write_default_json: None,
            logical_type: None,
        }
    }

    #[test]
    fn partition_expr_identity_reuses_source_expr() {
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(7)), DataType::Int64);

        let exprs = build_partition_exprs_from_output_exprs(
            &[String::from("id")],
            &[String::from("identity")],
            &[column_def("id")],
            &[source],
            &mut arena,
        )
        .expect("partition expr");

        assert_eq!(exprs, vec![source]);
    }

    #[test]
    fn partition_expr_bucket_and_truncate_build_transform_calls() {
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(7)), DataType::Int64);

        let bucket = build_partition_expr_from_transform("bucket[16]", source, &mut arena)
            .expect("bucket expr");
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(bucket) else {
            panic!("expected bucket transform function call");
        };
        assert_eq!(*kind, FunctionKind::IcebergTransformBucket);
        assert_eq!(args[0], source);
        assert!(matches!(
            arena.node(args[1]),
            Some(ExprNode::Literal(LiteralValue::Int64(16)))
        ));
        assert_eq!(arena.data_type(bucket), Some(&DataType::Int32));

        let truncate = build_partition_expr_from_transform("truncate[4]", source, &mut arena)
            .expect("truncate expr");
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(truncate) else {
            panic!("expected truncate transform function call");
        };
        assert_eq!(*kind, FunctionKind::IcebergTransformTruncate);
        assert_eq!(args[0], source);
        assert!(matches!(
            arena.node(args[1]),
            Some(ExprNode::Literal(LiteralValue::Int64(4)))
        ));
        assert_eq!(arena.data_type(truncate), Some(&DataType::Int64));
    }

    #[test]
    fn partition_expr_rejects_missing_source_column() {
        let mut arena = ExprArena::default();
        let source = arena.push_typed(ExprNode::SlotId(SlotId::new(7)), DataType::Int64);

        let err = build_partition_exprs_from_output_exprs(
            &[String::from("missing")],
            &[String::from("identity")],
            &[column_def("id")],
            &[source],
            &mut arena,
        )
        .unwrap_err();

        assert!(err.contains("partition source column missing"), "{err}");
    }
}
