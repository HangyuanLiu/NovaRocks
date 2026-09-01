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

//! The Trino-aligned connector write stack.
//!
//! This module owns the transport-neutral vocabulary every provider write uses:
//! a frontend-only commit session, immutable logical writer recipes, per-driver
//! writer execution, independent commit fragments, and the bounded prepared
//! write set the frontend commits.
//!
//! The production flow it describes has exactly one shape:
//!
//! ```text
//! FE begin_write
//!   -> BE TableWriterOperator (one writer per driver)
//!   -> Exchange
//!   -> one Root BE TableFinishOperator (bounded aggregation only)
//!   -> RESULT_SINK / FetchResult
//!   -> FE finish_write (the only external commit)
//! ```
//!
//! Like the read stack, it deliberately contains no provider name, no generated
//! wire DTO, no opaque byte payload, and no downcast. Concrete provider facts
//! that cross the FE/BE boundary are carried by the central IDL's closed
//! per-category `oneof`s, validated by `novarocks-proto-codec`, and converted to
//! concrete domain types only inside the provider that produced them.
//!
//! Three identity layers exist and are never fused into one universal writer
//! id:
//!
//! | layer | value | lifetime |
//! |---|---|---|
//! | exact provider generation | [`ConnectorWriteBinding`] | query / commit session |
//! | logical target association | [`WriteTargetOrdinal`] | one sealed query plan |
//! | physical execution context | [`ConnectorWriterPhysicalContext`] | one execution attempt |

pub mod adapter;
pub mod budget;
pub mod limits;
pub mod prepared;
pub mod relation;
pub mod runtime;
pub mod session;
pub mod target;
pub mod writer;

pub use adapter::{ProviderWriteRuntime, WriteRuntimeAdapter};
pub use budget::{UniqueWriterHandleLedger, validate_writer_handle_bytes};
pub use limits::{
    MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_PREPARED_WRITE_SET_BYTES,
    MAX_CONNECTOR_PREPARED_WRITE_SET_ENTRIES, MAX_CONNECTOR_UNIQUE_WRITER_HANDLE_BYTES,
    MAX_CONNECTOR_WRITE_TARGETS, MAX_CONNECTOR_WRITER_HANDLE_BYTES,
};
pub use prepared::{ConnectorPreparedWriteSet, PreparedWriteSetLedger, WriteRowCountAccumulator};
pub use relation::{
    RootRowKind, WRITE_RELATION_COLUMN_COUNT, WRITE_RELATION_FRAGMENT_COLUMN,
    WRITE_RELATION_FRAGMENT_INDEX, WRITE_RELATION_KIND_COLUMN, WRITE_RELATION_KIND_INDEX,
    WRITE_RELATION_ROW_COUNT_COLUMN, WRITE_RELATION_ROW_COUNT_INDEX, WRITE_RELATION_TARGET_COLUMN,
    WRITE_RELATION_TARGET_INDEX, WriterRowKind, root_output_schema, row_count_from_wire,
    row_count_to_wire, target_ordinal_from_wire, target_ordinal_to_wire, validate_root_row,
    validate_writer_row, writer_output_schema,
};
pub use runtime::{
    ConnectorCommitFragment, ConnectorWriteBinding, ConnectorWriteCommitHandle,
    ConnectorWriterHandle,
};
pub use session::{
    ConnectorWriteBeginRequest, ConnectorWriteControl, ConnectorWriteFinishRequest,
    ConnectorWriteRouteFacts, ConnectorWriteSessionAbortRequest, ConnectorWriteSessionFlavor,
    ConnectorWriteSessionPlan, ConnectorWriteSessionReconcileRequest, ConnectorWriteTargetPlan,
};
pub use target::{WriteTargetOrdinal, validate_dense_target_ordinals};
pub use writer::{
    ConnectorBatchWriter, ConnectorOpenWriterRequest, ConnectorWriteExecution,
    ConnectorWriteExecutionFactory, ConnectorWriterPhysicalContext,
};
