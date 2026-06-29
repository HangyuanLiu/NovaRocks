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

use arrow::datatypes::SchemaRef;
use iceberg::spec::{Struct, TableMetadata};
use parquet::basic::Compression;

use crate::connector::iceberg::commit::EqualityDeleteColumn;
use crate::connector::iceberg::delete_file::IcebergFileFormat;
use crate::connector::iceberg::position_delete_descriptor::PositionDeleteDescriptorBinding;
use crate::exec::expr::{ExprArena, ExprId};
use crate::runtime::starlet_shard_registry::S3StoreConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergSinkMode {
    Data,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

#[derive(Clone, Debug)]
pub(crate) struct PositionDeleteDataFilePartition {
    pub(crate) partition_spec_id: i32,
    pub(crate) partition_values: Struct,
}

#[derive(Clone)]
pub(crate) struct IcebergSinkPlan {
    pub(crate) mode: IcebergSinkMode,
    pub(crate) table_location: String,
    pub(crate) data_location: String,
    pub(crate) target_partition_spec_id: i32,
    pub(crate) target_table_metadata: Option<TableMetadata>,
    pub(crate) target_snapshot_id: Option<i64>,
    pub(crate) position_delete_data_file_partitions:
        HashMap<String, PositionDeleteDataFilePartition>,
    pub(crate) object_store_s3: Option<S3StoreConfig>,
    pub(crate) file_format: IcebergFileFormat,
    pub(crate) report_file_format: String,
    pub(crate) compression: Compression,
    pub(crate) output_schema: SchemaRef,
    pub(crate) target_schema: SchemaRef,
    pub(crate) equality_delete_columns: Vec<EqualityDeleteColumn>,
    pub(crate) row_lineage_data: bool,
    pub(crate) output_exprs: Vec<ExprId>,
    pub(crate) partition_exprs: Vec<ExprId>,
    pub(crate) partition_source_column_names: Vec<String>,
    pub(crate) partition_column_names: Vec<String>,
    pub(crate) transform_exprs: Vec<String>,
    pub(crate) position_delete_binding: Option<PositionDeleteDescriptorBinding>,
}

pub(crate) struct IcebergSinkFactoryInput {
    pub(crate) name: String,
    pub(crate) arena: ExprArena,
    pub(crate) plan: IcebergSinkPlan,
}
