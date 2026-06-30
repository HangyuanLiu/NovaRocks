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
use crate::fs::object_store::ObjectStoreConfig;
use crate::fs::object_store_credentials::ObjectStoreCredentials;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IcebergSinkObjectStoreConfig {
    pub(crate) endpoint: String,
    pub(crate) bucket: String,
    pub(crate) access_key_id: String,
    pub(crate) access_key_secret: String,
    pub(crate) session_token: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) enable_path_style_access: Option<bool>,
    pub(crate) retry_max_times: Option<usize>,
    pub(crate) retry_min_delay_ms: Option<u64>,
    pub(crate) retry_max_delay_ms: Option<u64>,
    pub(crate) timeout_ms: Option<u64>,
    pub(crate) io_timeout_ms: Option<u64>,
}

impl IcebergSinkObjectStoreConfig {
    pub(crate) fn from_credentials(bucket: String, credentials: ObjectStoreCredentials) -> Self {
        Self {
            endpoint: credentials.endpoint,
            bucket,
            access_key_id: credentials.access_key_id,
            access_key_secret: credentials.access_key_secret,
            session_token: credentials.session_token,
            region: credentials.region,
            enable_path_style_access: credentials.enable_path_style_access,
            retry_max_times: credentials.retry_max_times,
            retry_min_delay_ms: credentials.retry_min_delay_ms,
            retry_max_delay_ms: credentials.retry_max_delay_ms,
            timeout_ms: credentials.timeout_ms,
            io_timeout_ms: credentials.io_timeout_ms,
        }
    }

    pub(crate) fn to_object_store_config(&self) -> ObjectStoreConfig {
        ObjectStoreConfig {
            endpoint: self.endpoint.clone(),
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
            session_token: self.session_token.clone(),
            enable_path_style_access: self.enable_path_style_access,
            region: self.region.clone(),
            retry_max_times: self.retry_max_times,
            retry_min_delay_ms: self.retry_min_delay_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            timeout_ms: self.timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
        }
    }

    pub(crate) fn to_s3_storage_factory(
        &self,
    ) -> crate::connector::iceberg::catalog::s3_storage::S3StorageFactory {
        crate::connector::iceberg::catalog::s3_storage::S3StorageFactory {
            endpoint: self.endpoint.clone(),
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
            session_token: self.session_token.clone(),
            region: self
                .region
                .clone()
                .unwrap_or_else(|| "us-east-1".to_string()),
            enable_path_style: self.enable_path_style_access.unwrap_or(false),
            retry_max_times: self.retry_max_times,
            retry_min_delay_ms: self.retry_min_delay_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            timeout_ms: self.timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
        }
    }
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
    pub(crate) object_store_s3: Option<IcebergSinkObjectStoreConfig>,
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
