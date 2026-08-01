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

pub(crate) const ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID: i32 = 2_147_483_546;
pub(crate) const ICEBERG_POSITION_DELETE_POS_FIELD_ID: i32 = 2_147_483_545;
pub(crate) const ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN: &str = "file_path";
pub(crate) const ICEBERG_POSITION_DELETE_POS_COLUMN: &str = "pos";

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub struct PositionDeleteDescriptorBinding {
    pub output_schema: SchemaRef,
    pub output_column_names: Vec<String>,
    pub partition_source_column_names: Vec<String>,
    pub partition_column_names: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositionDeleteOutputField {
    pub output_expr_index: usize,
    pub name: String,
    pub data_type: DataType,
    pub field_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositionDeletePartitionSourceField {
    pub output_expr_index: usize,
    pub source_column_name: String,
    pub partition_field_name: String,
    pub transform_expr: String,
    pub source_field_id: i32,
    pub data_type: DataType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositionDeleteDescriptorInput {
    pub file_path: PositionDeleteOutputField,
    pub pos: PositionDeleteOutputField,
    pub partition_source_fields: Vec<PositionDeletePartitionSourceField>,
    pub target_partition_spec_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositionDeleteExpectedBinding {
    pub target_partition_spec_id: i32,
    pub partition_source_column_names: Vec<String>,
    pub partition_column_names: Vec<String>,
    pub partition_transform_exprs: Vec<String>,
    pub partition_source_field_ids: Vec<i32>,
    pub output_expr_count: usize,
}

fn descriptor_error(message: impl Into<String>) -> crate::common::engine_error::EngineError {
    crate::common::engine_error::EngineError::unsupported_position_delete_descriptor(message)
}

fn validate_output_field(
    label: &str,
    field: &PositionDeleteOutputField,
    expected_index: usize,
    expected_name: &str,
    expected_data_type: &DataType,
    expected_field_id: i32,
) -> Result<(), crate::common::engine_error::EngineError> {
    if field.output_expr_index != expected_index {
        return Err(descriptor_error(format!(
            "{label} output_expr_index mismatch: expected {expected_index}, got {}",
            field.output_expr_index
        )));
    }
    if field.name != expected_name {
        return Err(descriptor_error(format!(
            "{label} name mismatch: expected {expected_name}, got {}",
            field.name
        )));
    }
    if field.data_type.ne(expected_data_type) {
        return Err(descriptor_error(format!(
            "{label} type mismatch: expected {expected_data_type:?}, got {:?}",
            field.data_type
        )));
    }
    if field.field_id != expected_field_id {
        return Err(descriptor_error(format!(
            "{label} field_id mismatch: expected {expected_field_id}, got {}",
            field.field_id
        )));
    }
    Ok(())
}

fn is_scalar_partition_source_type(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Struct(_)
            | DataType::Map(_, _)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _)
    )
}

pub(crate) fn validate_required_fields(
    desc: &PositionDeleteDescriptorInput,
) -> Result<(), crate::common::engine_error::EngineError> {
    validate_output_field(
        "file_path",
        &desc.file_path,
        0,
        ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN,
        &DataType::Utf8,
        ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
    )?;
    validate_output_field(
        "pos",
        &desc.pos,
        1,
        ICEBERG_POSITION_DELETE_POS_COLUMN,
        &DataType::Int64,
        ICEBERG_POSITION_DELETE_POS_FIELD_ID,
    )
}

pub(crate) fn output_schema_from_descriptor(
    desc: &PositionDeleteDescriptorInput,
) -> Result<SchemaRef, crate::common::engine_error::EngineError> {
    validate_required_fields(desc)?;
    Ok(canonical_output_schema())
}

/// The on-file schema for an Iceberg position-delete Parquet file.  The
/// distributed sink receives the internal row-identity columns (`_file`,
/// `_pos`), but its staged artifact must use Iceberg's canonical physical
/// field names and IDs.
pub(crate) fn canonical_output_schema() -> SchemaRef {
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
    Arc::new(Schema::new(vec![file_path, pos]))
}

#[allow(dead_code)]
pub fn bind_position_delete_descriptor(
    desc: &PositionDeleteDescriptorInput,
    expected: &PositionDeleteExpectedBinding,
) -> Result<PositionDeleteDescriptorBinding, crate::common::engine_error::EngineError> {
    validate_required_fields(desc)?;
    if desc.target_partition_spec_id != expected.target_partition_spec_id {
        return Err(descriptor_error(format!(
            "target partition spec id mismatch: expected={}, descriptor={}",
            expected.target_partition_spec_id, desc.target_partition_spec_id
        )));
    }
    if expected.partition_source_column_names.len() != expected.partition_column_names.len()
        || expected.partition_source_column_names.len() != expected.partition_transform_exprs.len()
        || expected.partition_source_column_names.len() != expected.partition_source_field_ids.len()
    {
        return Err(descriptor_error(format!(
            "partition metadata count mismatch: source columns={}, partition columns={}, transforms={}, source field ids={}",
            expected.partition_source_column_names.len(),
            expected.partition_column_names.len(),
            expected.partition_transform_exprs.len(),
            expected.partition_source_field_ids.len()
        )));
    }
    let partition_fields = &desc.partition_source_fields;
    if partition_fields.len() != expected.partition_source_column_names.len() {
        return Err(descriptor_error(format!(
            "partition source field count mismatch: expected {}, got {}",
            expected.partition_source_column_names.len(),
            partition_fields.len()
        )));
    }
    let expected_exprs = 2 + partition_fields.len();
    if expected.output_expr_count != expected_exprs {
        return Err(descriptor_error(format!(
            "output expr count mismatch: expected {expected_exprs}, got {}",
            expected.output_expr_count
        )));
    }
    for (idx, field) in partition_fields.iter().enumerate() {
        let output_expr_index = idx + 2;
        if field.output_expr_index != output_expr_index {
            return Err(descriptor_error(format!(
                "partition source {} output_expr_index mismatch: expected {output_expr_index}, got {}",
                expected.partition_source_column_names[idx], field.output_expr_index
            )));
        }
        if !is_scalar_partition_source_type(&field.data_type) {
            return Err(descriptor_error(format!(
                "partition source {} type is not scalar: {:?}",
                expected.partition_source_column_names[idx], field.data_type
            )));
        }
        if field.source_column_name != expected.partition_source_column_names[idx] {
            return Err(descriptor_error(format!(
                "partition source column mismatch: expected {}, got {}",
                expected.partition_source_column_names[idx], field.source_column_name
            )));
        }
        if field.partition_field_name != expected.partition_column_names[idx] {
            return Err(descriptor_error(format!(
                "partition field name mismatch: expected {}, got {}",
                expected.partition_column_names[idx], field.partition_field_name
            )));
        }
        if field.transform_expr != expected.partition_transform_exprs[idx] {
            return Err(descriptor_error(format!(
                "partition transform mismatch for {}: expected {}, got {}",
                expected.partition_source_column_names[idx],
                expected.partition_transform_exprs[idx],
                field.transform_expr
            )));
        }
        if field.source_field_id != expected.partition_source_field_ids[idx] {
            return Err(descriptor_error(format!(
                "partition source field id mismatch for {}: expected {}, got {}",
                expected.partition_source_column_names[idx],
                expected.partition_source_field_ids[idx],
                field.source_field_id
            )));
        }
    }
    let mut output_column_names = vec![
        ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN.to_string(),
        ICEBERG_POSITION_DELETE_POS_COLUMN.to_string(),
    ];
    output_column_names.extend(expected.partition_source_column_names.iter().cloned());
    Ok(PositionDeleteDescriptorBinding {
        output_schema: output_schema_from_descriptor(desc)?,
        output_column_names,
        partition_source_column_names: expected.partition_source_column_names.clone(),
        partition_column_names: expected.partition_column_names.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output_field(
        index: usize,
        name: &str,
        data_type: DataType,
        field_id: i32,
    ) -> PositionDeleteOutputField {
        PositionDeleteOutputField {
            output_expr_index: index,
            name: name.to_string(),
            data_type,
            field_id,
        }
    }

    fn partition_source(index: usize) -> PositionDeletePartitionSourceField {
        PositionDeletePartitionSourceField {
            output_expr_index: index,
            source_column_name: "id".to_string(),
            partition_field_name: "id_bucket".to_string(),
            transform_expr: "bucket[8]".to_string(),
            source_field_id: 42,
            data_type: DataType::Int32,
        }
    }

    fn valid_descriptor() -> PositionDeleteDescriptorInput {
        PositionDeleteDescriptorInput {
            file_path: output_field(
                0,
                ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN,
                DataType::Utf8,
                ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
            ),
            pos: output_field(
                1,
                ICEBERG_POSITION_DELETE_POS_COLUMN,
                DataType::Int64,
                ICEBERG_POSITION_DELETE_POS_FIELD_ID,
            ),
            partition_source_fields: vec![partition_source(2)],
            target_partition_spec_id: 7,
        }
    }

    fn expected_binding() -> PositionDeleteExpectedBinding {
        PositionDeleteExpectedBinding {
            target_partition_spec_id: 7,
            partition_source_column_names: vec!["id".to_string()],
            partition_column_names: vec!["id_bucket".to_string()],
            partition_transform_exprs: vec!["bucket[8]".to_string()],
            partition_source_field_ids: vec![42],
            output_expr_count: 3,
        }
    }

    #[test]
    fn descriptor_missing_file_path_field_id_fails() {
        let mut desc = valid_descriptor();
        desc.file_path.field_id = 0;
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
        desc.pos.output_expr_index = 0;
        let err = validate_required_fields(&desc).unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("pos output_expr_index"));
    }

    #[test]
    fn descriptor_file_path_type_mismatch_fails() {
        let mut desc = valid_descriptor();
        desc.file_path.data_type = DataType::Int64;
        let err = validate_required_fields(&desc).unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("file_path type mismatch"));
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
    fn descriptor_target_spec_mismatch_fails() {
        let desc = valid_descriptor();
        let mut expected = expected_binding();
        expected.target_partition_spec_id = 8;
        let err = bind_position_delete_descriptor(&desc, &expected).unwrap_err();
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
        let mut expected = expected_binding();
        expected.output_expr_count = 2;
        let err = bind_position_delete_descriptor(&desc, &expected).unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("output expr count"));
    }

    #[test]
    fn descriptor_partition_metadata_count_mismatch_fails() {
        let desc = valid_descriptor();
        let mut expected = expected_binding();
        expected.partition_column_names = Vec::new();
        let err = bind_position_delete_descriptor(&desc, &expected).unwrap_err();
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
    fn descriptor_partition_source_complex_type_fails() {
        let mut desc = valid_descriptor();
        desc.partition_source_fields[0].data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let expected = expected_binding();

        let err = bind_position_delete_descriptor(&desc, &expected).unwrap_err();

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
        let mut expected = expected_binding();
        expected.partition_transform_exprs = vec!["identity".to_string()];
        let err = bind_position_delete_descriptor(&desc, &expected).unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("transform"));
    }

    #[test]
    fn descriptor_partition_source_field_id_mismatch_fails() {
        let desc = valid_descriptor();
        let mut expected = expected_binding();
        expected.partition_source_field_ids = vec![43];
        let err = bind_position_delete_descriptor(&desc, &expected).unwrap_err();
        assert_eq!(
            err.code(),
            crate::common::engine_error_codes::EngineErrorCode::UnsupportedPositionDeleteDescriptor
        );
        assert!(err.to_user_message().contains("source field id"));
    }

    #[test]
    fn descriptor_bind_returns_expected_metadata() {
        let desc = valid_descriptor();
        let expected = expected_binding();
        let binding = bind_position_delete_descriptor(&desc, &expected).expect("binding");
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
