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
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::thrift::{data_sinks, exprs, types};

pub(crate) const ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID: i32 = 2_147_483_546;
pub(crate) const ICEBERG_POSITION_DELETE_POS_FIELD_ID: i32 = 2_147_483_545;
pub(crate) const ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN: &str = "file_path";
pub(crate) const ICEBERG_POSITION_DELETE_POS_COLUMN: &str = "pos";

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct PositionDeleteDescriptorBinding {
    pub(crate) output_schema: SchemaRef,
    pub(crate) output_column_names: Vec<String>,
    pub(crate) partition_source_column_names: Vec<String>,
    pub(crate) partition_column_names: Vec<String>,
}

fn descriptor_error(message: impl Into<String>) -> crate::common::engine_error::EngineError {
    crate::common::engine_error::EngineError::unsupported_position_delete_descriptor(message)
}

fn primitive_type(type_desc: &types::TTypeDesc) -> Option<types::TPrimitiveType> {
    crate::types::arrow_thrift::thrift_desc_to_primitive(type_desc)
}

fn validate_output_field(
    label: &str,
    field: Option<&data_sinks::TIcebergPositionDeleteOutputField>,
    expected_index: i32,
    expected_name: &str,
    expected_primitive: types::TPrimitiveType,
    expected_field_id: i32,
) -> Result<(), crate::common::engine_error::EngineError> {
    let field = field.ok_or_else(|| descriptor_error(format!("{label} descriptor is missing")))?;
    if field.output_expr_index != Some(expected_index) {
        return Err(descriptor_error(format!(
            "{label} output_expr_index mismatch: expected {expected_index}, got {:?}",
            field.output_expr_index
        )));
    }
    if field.name.as_deref() != Some(expected_name) {
        return Err(descriptor_error(format!(
            "{label} name mismatch: expected {expected_name}, got {:?}",
            field.name
        )));
    }
    let actual_primitive = field.type_desc.as_ref().and_then(primitive_type);
    if actual_primitive != Some(expected_primitive) {
        return Err(descriptor_error(format!(
            "{label} type mismatch: expected {expected_primitive:?}, got {actual_primitive:?}"
        )));
    }
    if field.field_id != Some(expected_field_id) {
        return Err(descriptor_error(format!(
            "{label} field_id mismatch: expected {expected_field_id}, got {:?}",
            field.field_id
        )));
    }
    Ok(())
}

fn output_expr_root_primitive(
    label: &str,
    output_exprs: &[exprs::TExpr],
    output_expr_index: i32,
) -> Result<Option<types::TPrimitiveType>, crate::common::engine_error::EngineError> {
    let index = usize::try_from(output_expr_index).map_err(|_| {
        descriptor_error(format!(
            "{label} output expr index is negative: {output_expr_index}"
        ))
    })?;
    let expr = output_exprs.get(index).ok_or_else(|| {
        descriptor_error(format!(
            "{label} output expr index out of bounds: index={output_expr_index}, exprs={}",
            output_exprs.len()
        ))
    })?;
    let root = expr.nodes.first().ok_or_else(|| {
        descriptor_error(format!("{label} output expr {output_expr_index} is empty"))
    })?;
    Ok(primitive_type(&root.type_))
}

fn validate_output_expr_root_type(
    label: &str,
    output_exprs: &[exprs::TExpr],
    output_expr_index: i32,
    expected_primitive: types::TPrimitiveType,
) -> Result<(), crate::common::engine_error::EngineError> {
    let actual_primitive = output_expr_root_primitive(label, output_exprs, output_expr_index)?;
    if actual_primitive != Some(expected_primitive) {
        return Err(descriptor_error(format!(
            "{label} output expr type mismatch: expected {expected_primitive:?}, got {actual_primitive:?}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_required_fields(
    desc: &data_sinks::TIcebergPositionDeleteOutputDescriptor,
) -> Result<(), crate::common::engine_error::EngineError> {
    validate_output_field(
        "file_path",
        desc.file_path.as_ref(),
        0,
        ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN,
        types::TPrimitiveType::VARCHAR,
        ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
    )?;
    validate_output_field(
        "pos",
        desc.pos.as_ref(),
        1,
        ICEBERG_POSITION_DELETE_POS_COLUMN,
        types::TPrimitiveType::BIGINT,
        ICEBERG_POSITION_DELETE_POS_FIELD_ID,
    )
}

pub(crate) fn output_schema_from_descriptor(
    desc: &data_sinks::TIcebergPositionDeleteOutputDescriptor,
) -> Result<SchemaRef, crate::common::engine_error::EngineError> {
    validate_required_fields(desc)?;
    let file_path = Field::new(
        ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN,
        DataType::Utf8,
        false,
    )
    .with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_string(),
        ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID.to_string(),
    )]));
    let pos = Field::new(ICEBERG_POSITION_DELETE_POS_COLUMN, DataType::Int64, false).with_metadata(
        HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            ICEBERG_POSITION_DELETE_POS_FIELD_ID.to_string(),
        )]),
    );
    Ok(Arc::new(Schema::new(vec![file_path, pos])))
}

#[cfg(test)]
pub(crate) fn required_position_delete_descriptor_for_tests(
    target_partition_spec_id: i32,
) -> data_sinks::TIcebergPositionDeleteOutputDescriptor {
    data_sinks::TIcebergPositionDeleteOutputDescriptor::new(
        Some(data_sinks::TIcebergPositionDeleteOutputField::new(
            Some(0),
            Some(ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN.to_string()),
            Some(crate::lower::type_lowering::scalar_type_desc(
                types::TPrimitiveType::VARCHAR,
            )),
            Some(ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID),
        )),
        Some(data_sinks::TIcebergPositionDeleteOutputField::new(
            Some(1),
            Some(ICEBERG_POSITION_DELETE_POS_COLUMN.to_string()),
            Some(crate::lower::type_lowering::scalar_type_desc(
                types::TPrimitiveType::BIGINT,
            )),
            Some(ICEBERG_POSITION_DELETE_POS_FIELD_ID),
        )),
        Some(Vec::new()),
        Some(target_partition_spec_id),
    )
}

#[allow(dead_code)]
pub(crate) fn bind_position_delete_descriptor(
    desc: Option<&data_sinks::TIcebergPositionDeleteOutputDescriptor>,
    output_exprs: &[exprs::TExpr],
    target_partition_spec_id: i32,
    expected_partition_source_column_names: &[String],
    expected_partition_column_names: &[String],
    expected_partition_transform_exprs: &[String],
    expected_partition_source_field_ids: &[i32],
) -> Result<PositionDeleteDescriptorBinding, crate::common::engine_error::EngineError> {
    let desc =
        desc.ok_or_else(|| descriptor_error("position delete output descriptor is missing"))?;
    validate_required_fields(desc)?;
    if desc.target_partition_spec_id != Some(target_partition_spec_id) {
        return Err(descriptor_error(format!(
            "target partition spec id mismatch: sink={target_partition_spec_id}, descriptor={:?}",
            desc.target_partition_spec_id
        )));
    }
    if expected_partition_source_column_names.len() != expected_partition_column_names.len()
        || expected_partition_source_column_names.len() != expected_partition_transform_exprs.len()
        || expected_partition_source_column_names.len() != expected_partition_source_field_ids.len()
    {
        return Err(descriptor_error(format!(
            "partition metadata count mismatch: source columns={}, partition columns={}, transforms={}, source field ids={}",
            expected_partition_source_column_names.len(),
            expected_partition_column_names.len(),
            expected_partition_transform_exprs.len(),
            expected_partition_source_field_ids.len()
        )));
    }
    let partition_fields = desc.partition_source_fields.as_deref().unwrap_or_default();
    if partition_fields.len() != expected_partition_source_column_names.len() {
        return Err(descriptor_error(format!(
            "partition source field count mismatch: expected {}, got {}",
            expected_partition_source_column_names.len(),
            partition_fields.len()
        )));
    }
    let expected_exprs = 2 + partition_fields.len();
    if output_exprs.len() != expected_exprs {
        return Err(descriptor_error(format!(
            "output expr count mismatch: expected {expected_exprs}, got {}",
            output_exprs.len()
        )));
    }
    validate_output_expr_root_type(
        "file_path",
        output_exprs,
        desc.file_path
            .as_ref()
            .and_then(|field| field.output_expr_index)
            .ok_or_else(|| descriptor_error("file_path output_expr_index is missing"))?,
        types::TPrimitiveType::VARCHAR,
    )?;
    validate_output_expr_root_type(
        "pos",
        output_exprs,
        desc.pos
            .as_ref()
            .and_then(|field| field.output_expr_index)
            .ok_or_else(|| descriptor_error("pos output_expr_index is missing"))?,
        types::TPrimitiveType::BIGINT,
    )?;
    for (idx, field) in partition_fields.iter().enumerate() {
        let output_expr_index = i32::try_from(idx + 2)
            .map_err(|_| descriptor_error("partition source output index overflow"))?;
        if field.output_expr_index != Some(output_expr_index) {
            return Err(descriptor_error(format!(
                "partition source {} output_expr_index mismatch: expected {output_expr_index}, got {:?}",
                expected_partition_source_column_names[idx], field.output_expr_index
            )));
        }
        let partition_source_primitive = output_expr_root_primitive(
            expected_partition_source_column_names[idx].as_str(),
            output_exprs,
            output_expr_index,
        )?;
        if partition_source_primitive.is_none() {
            return Err(descriptor_error(format!(
                "partition source {} output expr type is not scalar",
                expected_partition_source_column_names[idx]
            )));
        }
        if field.source_column_name.as_deref()
            != Some(expected_partition_source_column_names[idx].as_str())
        {
            return Err(descriptor_error(format!(
                "partition source column mismatch: expected {}, got {:?}",
                expected_partition_source_column_names[idx], field.source_column_name
            )));
        }
        if field.partition_field_name.as_deref()
            != Some(expected_partition_column_names[idx].as_str())
        {
            return Err(descriptor_error(format!(
                "partition field name mismatch: expected {}, got {:?}",
                expected_partition_column_names[idx], field.partition_field_name
            )));
        }
        if field.transform_expr.as_deref() != Some(expected_partition_transform_exprs[idx].as_str())
        {
            return Err(descriptor_error(format!(
                "partition transform mismatch for {}: expected {}, got {:?}",
                expected_partition_source_column_names[idx],
                expected_partition_transform_exprs[idx],
                field.transform_expr
            )));
        }
        if field.source_field_id != Some(expected_partition_source_field_ids[idx]) {
            return Err(descriptor_error(format!(
                "partition source field id mismatch for {}: expected {}, got {:?}",
                expected_partition_source_column_names[idx],
                expected_partition_source_field_ids[idx],
                field.source_field_id
            )));
        }
    }
    let mut output_column_names = vec![
        ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN.to_string(),
        ICEBERG_POSITION_DELETE_POS_COLUMN.to_string(),
    ];
    output_column_names.extend(expected_partition_source_column_names.iter().cloned());
    Ok(PositionDeleteDescriptorBinding {
        output_schema: output_schema_from_descriptor(desc)?,
        output_column_names,
        partition_source_column_names: expected_partition_source_column_names.to_vec(),
        partition_column_names: expected_partition_column_names.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lower::type_lowering::scalar_type_desc;

    fn field(
        index: i32,
        name: &str,
        primitive: crate::thrift::types::TPrimitiveType,
        field_id: i32,
    ) -> crate::thrift::data_sinks::TIcebergPositionDeleteOutputField {
        crate::thrift::data_sinks::TIcebergPositionDeleteOutputField::new(
            Some(index),
            Some(name.to_string()),
            Some(scalar_type_desc(primitive)),
            Some(field_id),
        )
    }

    fn partition_source(
        index: i32,
    ) -> crate::thrift::data_sinks::TIcebergPositionDeletePartitionSourceField {
        crate::thrift::data_sinks::TIcebergPositionDeletePartitionSourceField::new(
            Some(index),
            Some("id".to_string()),
            Some("id_bucket".to_string()),
            Some("bucket[8]".to_string()),
            Some(42),
        )
    }

    fn valid_descriptor() -> crate::thrift::data_sinks::TIcebergPositionDeleteOutputDescriptor {
        crate::thrift::data_sinks::TIcebergPositionDeleteOutputDescriptor::new(
            Some(field(
                0,
                ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN,
                crate::thrift::types::TPrimitiveType::VARCHAR,
                ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            )),
            Some(field(
                1,
                ICEBERG_POSITION_DELETE_POS_COLUMN,
                crate::thrift::types::TPrimitiveType::BIGINT,
                ICEBERG_POSITION_DELETE_POS_FIELD_ID,
            )),
            Some(vec![partition_source(2)]),
            Some(7),
        )
    }

    fn empty_output_exprs(count: usize) -> Vec<crate::thrift::exprs::TExpr> {
        (0..count)
            .map(|_| crate::thrift::exprs::TExpr::new(Vec::new()))
            .collect()
    }

    fn typed_expr(primitive: crate::thrift::types::TPrimitiveType) -> crate::thrift::exprs::TExpr {
        let (node_type, int_literal, string_literal) =
            if primitive == crate::thrift::types::TPrimitiveType::VARCHAR {
                (
                    crate::thrift::exprs::TExprNodeType::STRING_LITERAL,
                    None,
                    Some(crate::thrift::exprs::TStringLiteral {
                        value: "value".to_string(),
                    }),
                )
            } else {
                (
                    crate::thrift::exprs::TExprNodeType::INT_LITERAL,
                    Some(crate::thrift::exprs::TIntLiteral { value: 1 }),
                    None,
                )
            };
        crate::thrift::exprs::TExpr::new(vec![crate::thrift::exprs::TExprNode {
            node_type,
            type_: scalar_type_desc(primitive),
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: 0,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: None,
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }])
    }

    fn valid_output_exprs() -> Vec<crate::thrift::exprs::TExpr> {
        vec![
            typed_expr(crate::thrift::types::TPrimitiveType::VARCHAR),
            typed_expr(crate::thrift::types::TPrimitiveType::BIGINT),
            typed_expr(crate::thrift::types::TPrimitiveType::INT),
        ]
    }

    fn malformed_non_scalar_type_desc(
        primitive: crate::thrift::types::TPrimitiveType,
    ) -> crate::thrift::types::TTypeDesc {
        crate::thrift::types::TTypeDesc::new(vec![crate::thrift::types::TTypeNode::new(
            crate::thrift::types::TTypeNodeType::ARRAY,
            crate::thrift::types::TScalarType::new(primitive, None, None, None, None),
            None,
            None,
        )])
    }

    #[test]
    fn descriptor_missing_file_path_field_id_fails() {
        let mut desc = valid_descriptor();
        desc.file_path.as_mut().unwrap().field_id = None;
        let err = validate_required_fields(&desc).unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("file_path field_id"));
    }

    #[test]
    fn descriptor_output_order_mismatch_fails() {
        let mut desc = valid_descriptor();
        desc.pos.as_mut().unwrap().output_expr_index = Some(0);
        let err = validate_required_fields(&desc).unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("pos output_expr_index"));
    }

    #[test]
    fn descriptor_non_scalar_type_desc_fails() {
        let mut desc = valid_descriptor();
        desc.file_path.as_mut().unwrap().type_desc = Some(malformed_non_scalar_type_desc(
            crate::thrift::types::TPrimitiveType::VARCHAR,
        ));
        let err = validate_required_fields(&desc).unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("type"));
    }

    #[test]
    fn descriptor_builds_required_arrow_schema() {
        let schema = output_schema_from_descriptor(&valid_descriptor()).expect("schema");
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "file_path");
        assert_eq!(schema.field(1).name(), "pos");
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY),
            Some(&ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID.to_string())
        );
        assert_eq!(
            schema
                .field(1)
                .metadata()
                .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY),
            Some(&ICEBERG_POSITION_DELETE_POS_FIELD_ID.to_string())
        );
    }

    #[test]
    fn descriptor_missing_descriptor_fails() {
        let output_exprs = valid_output_exprs();
        let err = bind_position_delete_descriptor(
            None,
            &output_exprs,
            7,
            &[String::from("id")],
            &[String::from("id_bucket")],
            &[String::from("bucket[8]")],
            &[42],
        )
        .unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("descriptor is missing"));
    }

    #[test]
    fn descriptor_target_spec_mismatch_fails() {
        let desc = valid_descriptor();
        let output_exprs = valid_output_exprs();
        let err = bind_position_delete_descriptor(
            Some(&desc),
            &output_exprs,
            8,
            &[String::from("id")],
            &[String::from("id_bucket")],
            &[String::from("bucket[8]")],
            &[42],
        )
        .unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(
            err.to_user_message()
                .contains("target partition spec id mismatch")
        );
    }

    #[test]
    fn descriptor_output_expr_count_mismatch_fails() {
        let desc = valid_descriptor();
        let output_exprs = valid_output_exprs();
        let err = bind_position_delete_descriptor(
            Some(&desc),
            &output_exprs[..2],
            7,
            &[String::from("id")],
            &[String::from("id_bucket")],
            &[String::from("bucket[8]")],
            &[42],
        )
        .unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("output expr count"));
    }

    #[test]
    fn descriptor_partition_metadata_count_mismatch_fails() {
        let desc = valid_descriptor();
        let output_exprs = empty_output_exprs(3);
        let err = bind_position_delete_descriptor(
            Some(&desc),
            &output_exprs,
            7,
            &[String::from("id")],
            &[],
            &[String::from("bucket[8]")],
            &[42],
        )
        .unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(
            err.to_user_message()
                .contains("partition metadata count mismatch")
        );
    }

    #[test]
    fn descriptor_empty_required_output_expr_fails() {
        let desc = valid_descriptor();
        let mut output_exprs = valid_output_exprs();
        output_exprs[0] = crate::thrift::exprs::TExpr::new(Vec::new());
        let err = bind_position_delete_descriptor(
            Some(&desc),
            &output_exprs,
            7,
            &[String::from("id")],
            &[String::from("id_bucket")],
            &[String::from("bucket[8]")],
            &[42],
        )
        .unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("output expr"));
    }

    #[test]
    fn descriptor_wrong_required_output_expr_type_fails() {
        let desc = valid_descriptor();
        let mut output_exprs = valid_output_exprs();
        output_exprs[1] = typed_expr(crate::thrift::types::TPrimitiveType::VARCHAR);
        let err = bind_position_delete_descriptor(
            Some(&desc),
            &output_exprs,
            7,
            &[String::from("id")],
            &[String::from("id_bucket")],
            &[String::from("bucket[8]")],
            &[42],
        )
        .unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("output expr"));
    }

    #[test]
    fn descriptor_partition_source_non_scalar_output_expr_fails() {
        let desc = valid_descriptor();
        let mut output_exprs = valid_output_exprs();
        output_exprs[2].nodes[0].type_ =
            malformed_non_scalar_type_desc(crate::thrift::types::TPrimitiveType::INT);
        let err = bind_position_delete_descriptor(
            Some(&desc),
            &output_exprs,
            7,
            &[String::from("id")],
            &[String::from("id_bucket")],
            &[String::from("bucket[8]")],
            &[42],
        )
        .unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        let message = err.to_user_message();
        assert!(message.contains("partition source"));
        assert!(message.contains("type"));
    }

    #[test]
    fn descriptor_partition_transform_mismatch_fails() {
        let desc = valid_descriptor();
        let output_exprs = valid_output_exprs();
        let err = bind_position_delete_descriptor(
            Some(&desc),
            &output_exprs,
            7,
            &[String::from("id")],
            &[String::from("id_bucket")],
            &[String::from("identity")],
            &[42],
        )
        .unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("transform"));
    }

    #[test]
    fn descriptor_partition_source_field_id_mismatch_fails() {
        let desc = valid_descriptor();
        let output_exprs = valid_output_exprs();
        let err = bind_position_delete_descriptor(
            Some(&desc),
            &output_exprs,
            7,
            &[String::from("id")],
            &[String::from("id_bucket")],
            &[String::from("bucket[8]")],
            &[43],
        )
        .unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("source field id"));
    }

    #[test]
    fn descriptor_bind_returns_expected_metadata() {
        let desc = valid_descriptor();
        let output_exprs = valid_output_exprs();
        let binding = bind_position_delete_descriptor(
            Some(&desc),
            &output_exprs,
            7,
            &[String::from("id")],
            &[String::from("id_bucket")],
            &[String::from("bucket[8]")],
            &[42],
        )
        .expect("binding");
        assert_eq!(
            binding.output_column_names,
            vec!["file_path".to_string(), "pos".to_string(), "id".to_string()]
        );
        assert_eq!(
            binding.partition_source_column_names,
            vec!["id".to_string()]
        );
        assert_eq!(
            binding.partition_column_names,
            vec!["id_bucket".to_string()]
        );
        assert_eq!(binding.output_schema.fields().len(), 2);
        assert_eq!(binding.output_schema.field(0).name(), "file_path");
        assert_eq!(binding.output_schema.field(1).name(), "pos");
    }
}
