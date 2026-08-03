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

use super::type_mapping::{encode_change_stream_branch_kind, encode_data_partition, usize_to_u64};
use super::{NativePlanEncodeContext, required_context_ref};
use crate::proto::{common, plan};
use crate::sql::planner::distributed::FragmentId;
use crate::sql::planner::distributed::write::change_stream::ChangeStreamRouterSink;
use crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding;
use crate::sql::planner::distributed::write::sink::ConnectorWriteFragmentSink;

/// Encode the generic writer envelope without inspecting the provider payload.
/// The payload is authenticated by its SPI digest and is interpreted only by
/// the exact execution binding on the backend.
pub(super) fn encode_connector_write_fragment_sink(
    src: &ConnectorWriteFragmentSink,
) -> plan::ConnectorWriteFragmentSink {
    plan::ConnectorWriteFragmentSink {
        handle: src.handle.as_ref().map(|handle| {
            let writer = handle.writer();
            plan::ConnectorWriterHandleEnvelope {
                contract_version: handle.version(),
                writer: Some(plan::ConnectorWriterIdentity {
                    operation_id: writer.operation_id().to_bytes().to_vec(),
                    cohort_id: writer.cohort_id().to_bytes().to_vec(),
                    execution_query_id: writer.execution_id().query_id().to_vec(),
                    execution_attempt_id: writer.execution_id().attempt_id(),
                    fragment_instance_id: Some(encode_writer_unique_id(
                        writer.fragment_instance_id(),
                    )),
                    fragment_id: writer.fragment_id(),
                    backend_num: writer.backend_num(),
                    sink_ordinal: writer.sink_ordinal(),
                    connector_instance_id: writer.binding_key().instance_id.as_str().to_string(),
                    connector_incarnation: writer.binding_key().incarnation.to_bytes().to_vec(),
                }),
                payload: handle.payload().to_vec(),
                payload_sha256: handle.payload_digest().to_vec(),
            }
        }),
        input: Some(encode_connector_write_input_binding(&src.input)),
    }
}

fn encode_writer_unique_id(value: [u8; 16]) -> common::UniqueId {
    common::UniqueId {
        hi: i64::from_be_bytes(value[..8].try_into().expect("fixed UUID prefix")),
        lo: i64::from_be_bytes(value[8..].try_into().expect("fixed UUID suffix")),
    }
}

fn encode_connector_write_input_binding(
    src: &ConnectorWriteInputBinding,
) -> plan::ConnectorWriteInputBinding {
    use plan::connector_write_input_binding::Kind;

    plan::ConnectorWriteInputBinding {
        kind: Some(match src {
            ConnectorWriteInputBinding::RootOutputByOrdinal => Kind::RootOutputByOrdinal(true),
            ConnectorWriteInputBinding::OutputOrdinals(ordinals) => {
                Kind::OutputOrdinals(plan::UInt64List {
                    values: ordinals.iter().map(|value| *value as u64).collect(),
                })
            }
        }),
    }
}

pub(super) fn encode_change_stream_router_sink(
    src: &ChangeStreamRouterSink,
    fragment_id: FragmentId,
    ctx: &NativePlanEncodeContext<'_>,
) -> Result<plan::ChangeStreamRouterSink, String> {
    Ok(plan::ChangeStreamRouterSink {
        group_id: src.group_id,
        change_op_output_ordinal: usize_to_u64(src.change_op_output_ordinal),
        data_route_output_ordinal: src.data_route_output_ordinal.map(usize_to_u64),
        branches: src
            .branches
            .iter()
            .map(|branch| {
                Ok(plan::ChangeStreamBranchRoute {
                    branch_id: branch.branch_id,
                    branch_kind: encode_change_stream_branch_kind(branch.branch_kind),
                    target_fragment_id: branch.target_fragment_id,
                    target_exchange_node_id: branch.target_exchange_node_id,
                    output_ordinals: branch
                        .output_ordinals
                        .iter()
                        .map(|value| usize_to_u64(*value))
                        .collect(),
                    // The ordinals still travel on the wire 1:1, but
                    // the runtime consumes `output_partition` below;
                    // the encoder no longer reconstructs the partition
                    // expression from them (CGO-9C Task 3).
                    output_partition_ordinals: branch
                        .output_partition_ordinals
                        .iter()
                        .map(|value| usize_to_u64(*value))
                        .collect(),
                    output_partition: Some(encode_finalized_router_branch_partition(
                        ctx,
                        fragment_id,
                        branch.branch_id,
                    )?),
                    destinations: None,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
    })
}

/// Map a change-stream router branch's finalized partition from the sealed write
/// contract (CGO-9C Task 3). The planner already reconstructed the partition
/// expression from the branch's ordinals against the router fragment's output
/// columns at seal; the encoder maps the typed result 1:1.
fn encode_finalized_router_branch_partition(
    ctx: &NativePlanEncodeContext<'_>,
    fragment_id: FragmentId,
    branch_id: i32,
) -> Result<plan::DataPartition, String> {
    let partition = required_context_ref(ctx.write_contracts, || {
        format!("native change-stream router fragment {fragment_id} has no sealed write contract")
    })?
    .router_branch_partition(fragment_id, branch_id)
    .ok_or_else(|| {
        format!(
            "native change-stream router fragment {fragment_id} branch {branch_id} is missing from the sealed write contract"
        )
    })?;
    encode_data_partition(partition)
}
