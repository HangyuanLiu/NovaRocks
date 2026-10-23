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

//! Shared Arrow IPC construction geometry. These numbers describe the locked
//! implementation's requests and opaque operation bounds, never memory grants
//! or semantic declarations. Caller source provenance and receiving policy stay
//! at their original boundary.

mod schema_geometry;
mod stream_geometry;
pub use schema_geometry::{
    LockedFieldLocation, SchemaAllocationMetrics, SchemaGeometry, SchemaRequestAccumulator,
    SchemaResourceSource, TypeHeaderGeometry, locked_field_location_layout,
    schema_backing_upper_bound, schema_writer_allocation_metrics, type_header_geometry,
};
pub use stream_geometry::{
    IpcConstructionError, IpcStreamBatchFacts, IpcStreamSchemaFacts, IpcStreamWireFacts,
    observe_stream_logical_size_construction, observe_stream_writer_batch_construction,
    observe_stream_writer_schema_construction,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IpcGeometryError {
    Overflow,
    SourceModel,
}
impl std::fmt::Display for IpcGeometryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overflow => f.write_str("Arrow IPC construction geometry overflow"),
            Self::SourceModel => f.write_str("Arrow IPC construction source model changed"),
        }
    }
}
impl std::error::Error for IpcGeometryError {}

mod schema_verification;
pub use schema_verification::{
    FieldVerificationFailure, FieldVerificationPurpose, IpcFieldRole, verify_field,
};

mod reader_geometry;
pub use reader_geometry::{ReaderStructureClass, ReaderStructureFailure, ReaderStructureObserver};

mod reader_array_geometry;
pub use reader_array_geometry::{
    ReaderArrayHeaderClass, ReaderArrayStructureObserver, original_reader_buffer_header_upper_bound,
};
