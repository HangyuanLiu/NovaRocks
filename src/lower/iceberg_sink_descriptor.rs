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

use arrow::datatypes::DataType;

use crate::connector::iceberg::position_delete_descriptor::{
    PositionDeleteDescriptorInput, PositionDeleteOutputField, PositionDeletePartitionSourceField,
};
use crate::thrift::{data_sinks, exprs, types};

fn descriptor_error(message: impl Into<String>) -> crate::common::engine_error::EngineError {
    crate::common::engine_error::EngineError::unsupported_position_delete_descriptor(message)
}

fn required_output_index(
    label: &str,
    index: Option<i32>,
) -> Result<usize, crate::common::engine_error::EngineError> {
    let index =
        index.ok_or_else(|| descriptor_error(format!("{label} output_expr_index is missing")))?;
    usize::try_from(index)
        .map_err(|_| descriptor_error(format!("{label} output expr index is negative: {index}")))
}

fn arrow_data_type_from_type_desc(
    label: &str,
    type_desc: Option<&types::TTypeDesc>,
) -> Result<DataType, crate::common::engine_error::EngineError> {
    type_desc
        .and_then(crate::types::arrow_thrift::thrift_desc_to_arrow_type)
        .ok_or_else(|| descriptor_error(format!("{label} type_desc is missing")))
}

fn output_expr_root_data_type(
    label: &str,
    output_exprs: &[exprs::TExpr],
    output_expr_index: usize,
) -> Result<DataType, crate::common::engine_error::EngineError> {
    let expr = output_exprs.get(output_expr_index).ok_or_else(|| {
        descriptor_error(format!(
            "{label} output expr index out of bounds: index={output_expr_index}, exprs={}",
            output_exprs.len()
        ))
    })?;
    let root = expr.nodes.first().ok_or_else(|| {
        descriptor_error(format!("{label} output expr {output_expr_index} is empty"))
    })?;
    arrow_data_type_from_type_desc(label, Some(&root.type_))
}

fn output_field_from_thrift(
    label: &str,
    field: Option<&data_sinks::TIcebergPositionDeleteOutputField>,
    output_exprs: &[exprs::TExpr],
) -> Result<PositionDeleteOutputField, crate::common::engine_error::EngineError> {
    let field = field.ok_or_else(|| descriptor_error(format!("{label} descriptor is missing")))?;
    let output_expr_index = required_output_index(label, field.output_expr_index)?;
    Ok(PositionDeleteOutputField {
        output_expr_index,
        name: field
            .name
            .clone()
            .ok_or_else(|| descriptor_error(format!("{label} name is missing")))?,
        data_type: output_expr_root_data_type(label, output_exprs, output_expr_index)?,
        field_id: field
            .field_id
            .ok_or_else(|| descriptor_error(format!("{label} field_id is missing")))?,
    })
}

pub(crate) fn position_delete_descriptor_input_from_thrift(
    desc: Option<&data_sinks::TIcebergPositionDeleteOutputDescriptor>,
    output_exprs: &[exprs::TExpr],
) -> Result<PositionDeleteDescriptorInput, crate::common::engine_error::EngineError> {
    let desc =
        desc.ok_or_else(|| descriptor_error("position delete output descriptor is missing"))?;
    let partition_source_fields = desc
        .partition_source_fields
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|field| {
            let output_expr_index =
                required_output_index("partition source", field.output_expr_index)?;
            let source_column_name = field
                .source_column_name
                .clone()
                .ok_or_else(|| descriptor_error("partition source column name is missing"))?;
            Ok(PositionDeletePartitionSourceField {
                output_expr_index,
                source_column_name: source_column_name.clone(),
                partition_field_name: field.partition_field_name.clone().ok_or_else(|| {
                    descriptor_error(format!(
                        "partition field name is missing for {source_column_name}"
                    ))
                })?,
                transform_expr: field.transform_expr.clone().ok_or_else(|| {
                    descriptor_error(format!(
                        "partition transform is missing for {source_column_name}"
                    ))
                })?,
                source_field_id: field.source_field_id.ok_or_else(|| {
                    descriptor_error(format!(
                        "partition source field id is missing for {source_column_name}"
                    ))
                })?,
                data_type: output_expr_root_data_type(
                    source_column_name.as_str(),
                    output_exprs,
                    output_expr_index,
                )?,
            })
        })
        .collect::<Result<Vec<_>, crate::common::engine_error::EngineError>>()?;

    Ok(PositionDeleteDescriptorInput {
        file_path: output_field_from_thrift("file_path", desc.file_path.as_ref(), output_exprs)?,
        pos: output_field_from_thrift("pos", desc.pos.as_ref(), output_exprs)?,
        partition_source_fields,
        target_partition_spec_id: desc
            .target_partition_spec_id
            .ok_or_else(|| descriptor_error("target partition spec id is missing"))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::iceberg::position_delete_descriptor::{
        ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
        PositionDeleteExpectedBinding, bind_position_delete_descriptor,
    };

    fn thrift_type_desc(primitive: types::TPrimitiveType) -> types::TTypeDesc {
        crate::types::arrow_thrift::thrift_type_desc_from_primitive(primitive)
    }

    fn slot_expr(slot_id: i32, primitive: types::TPrimitiveType) -> exprs::TExpr {
        crate::sql::codegen::expr_compiler::build_slot_ref_texpr(
            slot_id,
            1,
            thrift_type_desc(primitive),
        )
    }

    fn required_descriptor() -> data_sinks::TIcebergPositionDeleteOutputDescriptor {
        data_sinks::TIcebergPositionDeleteOutputDescriptor::new(
            Some(data_sinks::TIcebergPositionDeleteOutputField::new(
                Some(0),
                Some("file_path".to_string()),
                Some(thrift_type_desc(types::TPrimitiveType::VARCHAR)),
                Some(ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID),
            )),
            Some(data_sinks::TIcebergPositionDeleteOutputField::new(
                Some(1),
                Some("pos".to_string()),
                Some(thrift_type_desc(types::TPrimitiveType::BIGINT)),
                Some(ICEBERG_POSITION_DELETE_POS_FIELD_ID),
            )),
            Some(Vec::new()),
            Some(7),
        )
    }

    #[test]
    fn required_field_data_type_comes_from_output_expr_root_type() {
        let desc = required_descriptor();
        let output_exprs = vec![
            slot_expr(1, types::TPrimitiveType::BIGINT),
            slot_expr(2, types::TPrimitiveType::BIGINT),
        ];
        let domain = position_delete_descriptor_input_from_thrift(Some(&desc), &output_exprs)
            .expect("domain descriptor");
        let expected = PositionDeleteExpectedBinding {
            target_partition_spec_id: 7,
            partition_source_column_names: Vec::new(),
            partition_column_names: Vec::new(),
            partition_transform_exprs: Vec::new(),
            partition_source_field_ids: Vec::new(),
            output_expr_count: output_exprs.len(),
        };

        let err = bind_position_delete_descriptor(&domain, &expected).unwrap_err();

        assert!(
            err.to_user_message().contains("file_path type mismatch"),
            "unexpected error: {}",
            err.to_user_message()
        );
    }
}
