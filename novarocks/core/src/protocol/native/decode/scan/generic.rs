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

//! Native decoding for the provider-neutral SPI read carrier.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorCancellation, ConnectorInstanceId, ConnectorOpenReaderRequest,
    ConnectorProviderId, ConnectorRequestContext, ConnectorScanHandle, ConnectorSplit,
};

use crate::connector::file_execution::FileScanRange;
use crate::connector::runtime::{ConnectorReadScanSource, ConnectorScheduledSplit};
use crate::exec::expr::ExprArena;
use crate::exec::node::scan::BoundScanRanges;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::plan;
use crate::protocol::common::error::ProtocolErrorKind;
use crate::runtime::query_context::{QueryId, query_context_manager};
use crate::runtime::query_options::query_expire_durations;

use super::super::node::{DecodedNode, NativePlanDecodeContext};
use super::common::{DecodedScanOutputColumns, lower_scan_predicate, parse_scan_limit};
use crate::protocol::native::decode::error::NativeFragmentLeafDecodeError;

struct NativeQueryCancellation {
    query_id: QueryId,
}

impl ConnectorCancellation for NativeQueryCancellation {
    fn is_cancelled(&self) -> bool {
        query_context_manager().is_query_canceled(self.query_id)
    }
}

pub(super) fn lower_connector_read_scan(
    node: &plan::DistributedNode,
    scan: &plan::ScanNode,
    source: &plan::ConnectorReadSource,
    output_columns: &DecodedScanOutputColumns,
    ctx: &NativePlanDecodeContext,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentLeafDecodeError> {
    let instance_id = ConnectorInstanceId::parse(&source.instance_id).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "instance_id",
            error.to_string(),
        )
    })?;
    let provider_id = ConnectorProviderId::parse(&source.provider_id).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "provider_id",
            error.to_string(),
        )
    })?;
    let batch = ConnectorBatchBudget {
        max_rows: required_nonzero_usize(source.max_batch_rows, "max_batch_rows")?,
        max_bytes: required_nonzero_usize(source.max_batch_bytes, "max_batch_bytes")?,
    };
    let query_id = ctx.query_id().ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "query_id",
            "ConnectorReadSource requires a native query identity",
        )
    })?;
    let (_, query_expire) = query_expire_durations(ctx.query_options());
    let request_context = ConnectorRequestContext::try_new(
        Instant::now() + query_expire,
        Arc::new(NativeQueryCancellation { query_id }),
        required_usize(source.max_handle_payload_bytes, "max_handle_payload_bytes")?,
        required_usize(source.max_total_payload_bytes, "max_total_payload_bytes")?,
    )
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "payload_budgets",
            error.to_string(),
        )
    })?;
    let _scan = ConnectorScanHandle::try_new(
        instance_id.clone(),
        Bytes::copy_from_slice(&source.scan_payload),
    )
    .map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "scan_payload",
            error.to_string(),
        )
    })?;
    let mut total_payload_bytes = source.scan_payload.len();
    let mut split_ids = BTreeSet::new();
    let scheduled = source
        .splits
        .iter()
        .enumerate()
        .map(|(index, wire_split)| {
            let split = ConnectorSplit::try_new(
                instance_id.clone(),
                wire_split.split_id.clone(),
                Bytes::copy_from_slice(&wire_split.split_payload),
                wire_split.estimated_bytes,
            )
            .map_err(|error| {
                NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InvalidValue,
                    "splits",
                    error.to_string(),
                )
                .append_index(index)
            })?;
            if !split_ids.insert(split.split_id().to_string()) {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "splits",
                    "ConnectorReadSource has duplicate split_id values",
                )
                .append_index(index));
            }
            if split.payload().len() > request_context.max_handle_payload_bytes() {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::OutOfRange,
                    "splits",
                    "connector split payload exceeds its request handle budget",
                )
                .append_index(index));
            }
            total_payload_bytes = total_payload_bytes.saturating_add(split.payload().len());
            let file_range = wire_split
                .file_execution
                .as_ref()
                .map(|sidecar| decode_file_execution_sidecar(sidecar, index))
                .transpose()?;
            Ok(match file_range {
                Some(file_range) => ConnectorScheduledSplit::file(split, file_range),
                None => ConnectorScheduledSplit::plain(split),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if source.scan_payload.len() > request_context.max_handle_payload_bytes() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "scan_payload",
            "connector scan payload exceeds its request handle budget",
        ));
    }
    if total_payload_bytes > request_context.max_total_payload_bytes() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "splits",
            "connector read payloads exceed their request total budget",
        ));
    }

    let layout = output_columns.layout();
    let output_schema = output_columns.output_schema();
    let file_ranges = scheduled
        .iter()
        .filter_map(|scheduled| scheduled.file_range().cloned())
        .collect::<Vec<_>>();
    let connectors = ctx.connectors()?;
    let (instance, lifecycle) = match connectors.connector_instance(&instance_id) {
        Ok(instance) => {
            if instance.descriptor().provider_id != provider_id {
                return Err(NativeFragmentLeafDecodeError::at_field(
                    ProtocolErrorKind::InconsistentFields,
                    "provider_id",
                    format!(
                        "ConnectorReadSource provider_id `{}` does not match registered instance provider `{}`",
                        provider_id.as_str(),
                        instance.descriptor().provider_id.as_str(),
                    ),
                ));
            }
            (instance, None)
        }
        Err(_) => {
            let native_instance = connectors
                .materialize_transport_connector_instance(
                    &provider_id,
                    instance_id,
                    Bytes::copy_from_slice(&source.scan_payload),
                    &file_ranges,
                    output_schema.clone(),
                )
                .map_err(|error| {
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::InvalidValue,
                        "provider_id",
                        error.to_string(),
                    )
                })?;
            let (instance, lifecycle) = connectors
                .register_ephemeral_connector_instance(native_instance)
                .map_err(|error| {
                    NativeFragmentLeafDecodeError::at_field(
                        ProtocolErrorKind::InvalidValue,
                        "instance_id",
                        error.to_string(),
                    )
                })?;
            (instance, Some(lifecycle))
        }
    };
    let request = ConnectorOpenReaderRequest {
        expected_schema: output_schema.arrow_schema_ref(),
        batch,
        context: request_context,
    };
    let predicate = lower_scan_predicate(scan, arena, &layout)?;
    let source = Arc::new(match lifecycle {
        Some(lifecycle) => ConnectorReadScanSource::new_scheduled_ephemeral(
            instance,
            scheduled,
            request,
            output_schema.clone(),
            lifecycle,
            None,
        ),
        None => ConnectorReadScanSource::new_scheduled(
            instance,
            scheduled,
            request,
            output_schema.clone(),
        ),
    });
    ctx.capture_scan_ranges(node.node_id, BoundScanRanges::None);
    let scan_node = crate::exec::node::scan::ScanNode::new(source)
        .with_node_id(node.node_id)
        .with_output_chunk_schema(output_schema.clone())
        .with_limit(parse_scan_limit(node.limit)?)
        .with_conjunct_predicate(predicate)
        .with_accept_empty_scan_ranges(true);
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Scan(scan_node),
        },
        layout,
        output_schema,
    })
}

fn decode_file_execution_sidecar(
    sidecar: &plan::FileExecutionSidecar,
    split_index: usize,
) -> Result<FileScanRange, NativeFragmentLeafDecodeError> {
    let error = |field: &'static str, detail: String| {
        NativeFragmentLeafDecodeError::at_field(ProtocolErrorKind::InvalidValue, "splits", detail)
            .append_index(split_index)
            .append_field("file_execution")
            .append_field(field)
    };
    if sidecar.version != 1 {
        return Err(error(
            "version",
            format!(
                "unsupported core file execution sidecar version {}",
                sidecar.version
            ),
        ));
    }
    if sidecar.path.is_empty() {
        return Err(error(
            "path",
            "file execution sidecar path is empty".to_string(),
        ));
    }
    if !matches!(
        plan::FileExecutionFormat::try_from(sidecar.file_format),
        Ok(plan::FileExecutionFormat::Parquet) | Ok(plan::FileExecutionFormat::Orc)
    ) {
        return Err(error(
            "file_format",
            "unsupported file execution format".to_string(),
        ));
    }
    if !sidecar.delete_files.is_empty()
        || sidecar.deletion_vector.is_some()
        || !sidecar.file_pruning_min_max_values.is_empty()
    {
        return Err(error(
            "file_execution",
            "delete, deletion-vector, and pruning sidecars require an authenticated connector binding".to_string(),
        ));
    }
    if sidecar.offset > sidecar.file_length {
        return Err(error("offset", "offset exceeds file length".to_string()));
    }
    let ivm_change_op = sidecar
        .change_op
        .map(|value| {
            i8::try_from(value).map_err(|_| error("change_op", "change op exceeds i8".to_string()))
        })
        .transpose()?;
    Ok(FileScanRange {
        path: sidecar.path.clone(),
        file_len: sidecar.file_length,
        offset: sidecar.offset,
        length: sidecar.length,
        scan_range_id: i32::try_from(split_index)
            .map_err(|_| error("split_id", "split index exceeds i32".to_string()))?,
        first_row_id: sidecar.first_row_id,
        data_sequence_number: sidecar.data_sequence_number,
        ivm_change_op,
        included_positions: (!sidecar.included_positions.is_empty())
            .then(|| sidecar.included_positions.clone()),
        external_datacache: None,
        delete_files: Vec::new(),
        iceberg_file_pruning: None,
    })
}

fn required_nonzero_usize(
    value: u64,
    field: &'static str,
) -> Result<NonZeroUsize, NativeFragmentLeafDecodeError> {
    NonZeroUsize::new(required_usize(value, field)?).ok_or_else(|| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            field,
            format!("ConnectorReadSource {field} must be nonzero"),
        )
    })
}

fn required_usize(value: u64, field: &'static str) -> Result<usize, NativeFragmentLeafDecodeError> {
    usize::try_from(value).map_err(|_| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            field,
            format!("ConnectorReadSource {field} does not fit usize"),
        )
    })
}
