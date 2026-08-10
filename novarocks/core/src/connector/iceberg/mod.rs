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

pub mod catalog;
pub(crate) mod change_stream_routing;
pub(crate) mod change_stream_write;
pub mod changes;
pub(crate) mod cleanup_maintenance;
pub mod commit;
#[cfg_attr(test, allow(dead_code))]
pub(crate) mod compact;
pub(crate) mod data_mutation;
#[cfg(test)]
mod data_writer_tests;
pub(crate) mod delete_visibility;
pub(crate) mod distributed_rewrite;
pub(crate) mod distributed_rewrite_execution;
pub mod equality_delete;
pub(crate) mod file_pruning;
pub mod metadata;
pub(crate) mod metadata_maintenance;
pub(crate) mod operation_lifecycle;
pub(crate) mod partition_spec;
pub(crate) mod planning;
pub mod position_delete;
/// Provider execution/control adapters consumed by the process composition
/// root. The provider crate owns the external Iceberg dependency boundary;
/// these adapters only bind frozen SPI payloads to Core's runtime.
pub mod provider;

pub mod scan_deletes;
pub mod schema;
pub mod sink;
pub mod sink_plan;
pub(crate) mod staged_create;
pub(crate) mod stats;
#[cfg(test)]
pub(crate) mod test_metadata;
pub(crate) mod write_commit;
pub(crate) mod write_contract;
pub(crate) mod write_control;
pub(crate) mod write_service;

pub use metadata::{
    IcebergMetadataOutputColumn, IcebergMetadataScanConfig, IcebergMetadataScanRange,
    IcebergMetadataTableType,
};
pub use metadata::{
    plan_native_iceberg_metadata_read_source,
    plan_native_iceberg_metadata_read_source_with_cancellation,
};
pub(crate) use schema::build_projected_output_schema_from_scan_model;
pub use schema::{
    IcebergArrowColumn, IcebergPartitionInfo, IcebergSchemaDescriptor,
    IcebergSchemaFieldDescriptor, IcebergTableColumn, IcebergTableDescriptor,
    apply_field_id_recursive, build_full_output_schema, build_projected_output_schema,
};
pub use sink_plan::IcebergSinkMode;
