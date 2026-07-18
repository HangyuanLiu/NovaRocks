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

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::datatypes::DataType;

use super::super::expr::decode_expr;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::fs::object_store::{ObjectStoreConfig, apply_object_store_runtime_defaults};
use crate::fs::object_store_credentials::{ObjectStoreCredentials, ObjectStoreCredentialsSource};
use crate::proto::{common, plan};
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

pub(super) fn scan_output_columns(
    scan: &plan::ScanNode,
) -> Result<Vec<common::OutputColumn>, NativeFragmentLeafDecodeError> {
    if scan.columns.is_empty() {
        return Err("ScanNode columns are empty".into());
    }
    if scan.required_columns.is_empty() {
        return Ok(scan.columns.clone());
    }

    let required = scan
        .required_columns
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let output_columns = scan
        .columns
        .iter()
        .filter(|column| required.contains(&column.name.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    if output_columns.is_empty() {
        return Err(NativeFragmentLeafDecodeError::new(format!(
            "ScanNode required_columns {:?} do not match any scan columns",
            scan.required_columns
        )));
    }
    Ok(output_columns)
}

pub(super) fn column_def_data_type(
    column: &plan::ColumnDef,
) -> Result<DataType, NativeFragmentLeafDecodeError> {
    let desc = column
        .logical_type
        .as_ref()
        .or(column.data_type.as_ref())
        .ok_or_else(|| format!("column {} type missing", column.name))?;
    Ok(super::super::decode_type(desc)?)
}

pub(super) fn output_column_data_type(
    column: &common::OutputColumn,
) -> Result<DataType, NativeFragmentLeafDecodeError> {
    let desc = column
        .r#type
        .as_ref()
        .ok_or_else(|| format!("output column {} type missing", column.name))?;
    Ok(super::super::decode_type(desc)?)
}

pub(super) fn scan_batch_size(
    query_options: Option<&crate::runtime::query_options::QueryOptions>,
) -> Result<usize, String> {
    let Some(value) = query_options.and_then(|opts| opts.batch_size) else {
        return Ok(4096);
    };
    let batch_size = usize::try_from(value).map_err(|_| {
        format!("native ScanNode query_options.batch_size must be positive, got {value}")
    })?;
    if batch_size == 0 {
        return Err("native ScanNode query_options.batch_size must be positive".to_string());
    }
    Ok(batch_size)
}

pub(super) fn lower_scan_predicate(
    scan: &plan::ScanNode,
    arena: &mut ExprArena,
    layout: &super::super::layout::Layout,
) -> Result<Option<crate::exec::expr::ExprId>, NativeFragmentLeafDecodeError> {
    let mut predicate = None;
    for (idx, expr) in scan.predicates.iter().enumerate() {
        let expr_id = decode_expr(expr, arena, layout).map_err(|err| {
            NativeFragmentLeafDecodeError::new(format!("ScanNode predicate {idx}: {err}"))
        })?;
        predicate = Some(match predicate {
            Some(prev) => arena.push_typed(ExprNode::And(prev, expr_id), DataType::Boolean),
            None => expr_id,
        });
    }
    Ok(predicate)
}

pub(super) fn parse_scan_limit(limit: i64) -> Result<Option<usize>, String> {
    if limit == -1 {
        Ok(None)
    } else if limit < 0 {
        Err(format!("ScanNode limit must be -1 or >= 0, got {limit}"))
    } else {
        Ok(Some(limit as usize))
    }
}

pub(super) fn resolve_cloud_object_store_config(
    cloud_properties: &HashMap<String, String>,
) -> Result<Option<ObjectStoreConfig>, String> {
    let props = cloud_properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let Some(credentials) = ObjectStoreCredentials::optional_from_aws_s3_properties(
        ObjectStoreCredentialsSource::AwsS3Properties,
        &props,
    )?
    else {
        return Ok(None);
    };
    let mut cfg = credentials.to_object_store_config();
    apply_object_store_runtime_defaults(&mut cfg);
    Ok(Some(cfg))
}

pub(super) fn table_location_map(table: &plan::IcebergTableInfo) -> HashMap<i64, String> {
    let mut locations = HashMap::new();
    if !table.location.is_empty() {
        locations.insert(i64::from(table.schema_id), table.location.clone());
    }
    locations
}
