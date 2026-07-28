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

pub(crate) mod analyze;
pub mod catalog;
pub(crate) mod change_stream_routing;
pub mod changes;
pub mod commit;
#[cfg_attr(test, allow(dead_code))]
pub(crate) mod compact;
pub(crate) mod data_writer;
pub(crate) mod default_value;
pub mod delete_file;
pub mod equality_delete;
pub(crate) mod file_pruning;
#[cfg(feature = "compat")]
pub(crate) mod file_pruning_wire;
pub(crate) mod fs_io;
pub mod metadata;
pub(crate) mod metadata_read;
pub(crate) mod operation_lifecycle;
pub(crate) mod partition_spec;
pub mod position_delete;
pub(crate) mod position_delete_descriptor;
pub(crate) mod provider;
pub(crate) mod read;
pub(crate) mod report;
pub(crate) mod row_lineage_synth;
pub mod scan_deletes;
pub mod scan_model;
pub(crate) mod scan_planner;
pub(crate) mod scan_range;
pub mod schema;
pub mod sink;
pub(crate) mod sink_plan;
pub(crate) mod stats;
pub(crate) mod stats_assembler;
pub(crate) mod stats_loader;
pub(crate) mod theta_sketch;
pub(crate) mod variant_write;
pub(crate) mod write_descriptor;

pub use metadata::{
    IcebergMetadataOutputColumn, IcebergMetadataScanConfig, IcebergMetadataScanRange,
    IcebergMetadataTableType,
};
pub(crate) use metadata::{
    plan_compat_iceberg_metadata_read_source, plan_native_iceberg_metadata_read_source,
};
pub(crate) use scan_planner::IcebergConnectorScanPlanner;
pub(crate) use schema::build_projected_output_schema_from_descriptor;
pub use schema::{
    IcebergArrowColumn, IcebergPartitionInfo, IcebergSchemaDescriptor,
    IcebergSchemaFieldDescriptor, IcebergTableColumn, IcebergTableDescriptor,
    apply_field_id_recursive, build_full_output_schema, build_projected_output_schema,
};
pub use sink::IcebergTableSinkFactory;
pub use sink_plan::IcebergSinkMode;
