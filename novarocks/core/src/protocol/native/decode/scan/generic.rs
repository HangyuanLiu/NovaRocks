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
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use arrow::ipc::reader::StreamReader;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorCancellation, ConnectorInstanceId, ConnectorOpenReaderRequest,
    ConnectorRequestContext, ConnectorScanHandle, ConnectorSplit,
};

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
            Ok(ConnectorScheduledSplit::plain(split))
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
    validate_expected_schema_ipc(
        &source.expected_schema_ipc,
        output_schema.arrow_schema_ref().as_ref(),
        request_context.max_handle_payload_bytes(),
    )?;
    let connectors = ctx.connectors()?;
    let instance = connectors
        .connector_instance(&instance_id)
        .map_err(|error| {
            NativeFragmentLeafDecodeError::at_field(
                ProtocolErrorKind::InvalidValue,
                "instance_id",
                error.to_string(),
            )
        })?;
    let request = ConnectorOpenReaderRequest {
        expected_schema: output_schema.arrow_schema_ref(),
        batch,
        context: request_context,
    };
    let predicate = lower_scan_predicate(scan, arena, &layout)?;
    let source = Arc::new(ConnectorReadScanSource::new_scheduled(
        instance,
        scheduled,
        request,
        output_schema.clone(),
    ));
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

fn validate_expected_schema_ipc(
    encoded: &[u8],
    expected: &arrow::datatypes::Schema,
    max_bytes: usize,
) -> Result<(), NativeFragmentLeafDecodeError> {
    if encoded.is_empty() {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::MissingField,
            "expected_schema_ipc",
            "ConnectorReadSource requires an expected Arrow schema",
        ));
    }
    if encoded.len() > max_bytes {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::OutOfRange,
            "expected_schema_ipc",
            format!("ConnectorReadSource expected Arrow schema exceeds handle budget {max_bytes}"),
        ));
    }
    let reader = StreamReader::try_new(Cursor::new(encoded), None).map_err(|error| {
        NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InvalidValue,
            "expected_schema_ipc",
            format!("decode ConnectorReadSource expected Arrow schema: {error}"),
        )
    })?;
    if reader.schema().as_ref() != expected {
        return Err(NativeFragmentLeafDecodeError::at_field(
            ProtocolErrorKind::InconsistentFields,
            "expected_schema_ipc",
            "ConnectorReadSource expected Arrow schema does not match scan output columns",
        ));
    }
    Ok(())
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
