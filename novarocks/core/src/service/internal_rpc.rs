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

#[cfg(feature = "compat")]
use crate::common::failpoint::{self, FailPointMode};
use crate::common::ids::SlotId;
use crate::proto;
use crate::runtime::exchange;
use crate::runtime::lookup::{
    decode_column_ipc, encode_column_ipc, execute_position_lookup_request,
};
use crate::runtime::query_context::{QueryId, query_context_manager, query_expire_durations};
use crate::runtime::runtime_filter_transmission::RuntimeFilterTransmission;
use crate::service::grpc_runtime_filter_adapter::{
    NativeRuntimeFilterRequest, NativeRuntimeFilterResponse,
    decode_runtime_filter_transmission as decode_native_runtime_filter_transmission,
    encode_runtime_filter_result as encode_native_runtime_filter_result,
};

#[cfg(feature = "compat")]
type CompatTransmitChunkRequest = proto::starrocks::PTransmitChunkParams; // cfg(feature = "compat")
#[cfg(feature = "compat")]
type CompatTransmitChunkResponse = proto::starrocks::PTransmitChunkResult; // cfg(feature = "compat")
#[cfg(feature = "compat")]
type CompatLookupRequest = proto::starrocks::PLookUpRequest; // cfg(feature = "compat")
#[cfg(feature = "compat")]
type CompatLookupResponse = proto::starrocks::PLookUpResponse; // cfg(feature = "compat")
#[cfg(feature = "compat")]
type CompatLookupCloseRequest = proto::starrocks::PLookUpCloseRequest; // cfg(feature = "compat")
#[cfg(feature = "compat")]
type CompatLookupCloseResponse = proto::starrocks::PLookUpCloseResponse; // cfg(feature = "compat")
#[cfg(feature = "compat")]
type CompatColumn = proto::starrocks::PColumn; // cfg(feature = "compat")

#[cfg(feature = "compat")]
fn ok_status() -> proto::starrocks::StatusPb {
    proto::starrocks::StatusPb {
        status_code: 0,
        error_msgs: Vec::new(),
    }
}

#[cfg(feature = "compat")]
fn error_status(message: impl Into<String>) -> proto::starrocks::StatusPb {
    proto::starrocks::StatusPb {
        status_code: 1,
        error_msgs: vec![message.into()],
    }
}

fn ok_common_status() -> proto::common::Status {
    proto::common::Status {
        code: 0,
        message: String::new(),
    }
}

fn error_common_status(message: impl Into<String>) -> proto::common::Status {
    proto::common::Status {
        code: 1,
        message: message.into(),
    }
}

#[cfg(feature = "compat")]
fn common_status_to_compat(status: proto::common::Status) -> proto::starrocks::StatusPb {
    proto::starrocks::StatusPb {
        status_code: status.code,
        error_msgs: if status.message.is_empty() {
            Vec::new()
        } else {
            vec![status.message]
        },
    }
}

// Used by engine_ffi.rs when feature = "compat" is enabled.
#[cfg(feature = "compat")]
#[allow(dead_code)]
pub(crate) fn handle_update_fail_point_status(
    request: proto::starrocks::PUpdateFailPointStatusRequest,
) -> proto::starrocks::PUpdateFailPointStatusResponse {
    let mut response = proto::starrocks::PUpdateFailPointStatusResponse {
        status: Some(ok_status()),
    };

    let Some(name) = request.fail_point_name.as_deref() else {
        response.status = Some(error_status("missing fail_point_name"));
        return response;
    };
    let Some(trigger_mode) = request.trigger_mode.as_ref() else {
        response.status = Some(error_status("missing trigger_mode"));
        return response;
    };
    let Some(mode) = trigger_mode.mode else {
        response.status = Some(error_status("missing trigger_mode.mode"));
        return response;
    };

    let mode = match proto::starrocks::FailPointTriggerModeType::try_from(mode) {
        Ok(proto::starrocks::FailPointTriggerModeType::Enable) => FailPointMode::Enable,
        Ok(proto::starrocks::FailPointTriggerModeType::Disable) => FailPointMode::Disable,
        Ok(proto::starrocks::FailPointTriggerModeType::ProbabilityEnable) => {
            let Some(probability) = trigger_mode.probability else {
                response.status = Some(error_status("missing trigger_mode.probability"));
                return response;
            };
            FailPointMode::Probability(probability)
        }
        Ok(proto::starrocks::FailPointTriggerModeType::EnableNTimes) => {
            let Some(n_times) = trigger_mode.n_times else {
                response.status = Some(error_status("missing trigger_mode.n_times"));
                return response;
            };
            FailPointMode::EnableNTimes(n_times)
        }
        Err(_) => {
            response.status = Some(error_status(format!("invalid trigger_mode.mode={mode}")));
            return response;
        }
    };

    if let Err(err) = failpoint::update(name, mode) {
        response.status = Some(error_status(err));
    }
    response
}

pub(crate) fn handle_transmit_chunk(
    params: proto::novarocks::ExchangeRequest,
) -> proto::novarocks::ExchangeResponse {
    let mut response = proto::novarocks::ExchangeResponse {
        ack_sequence: params.sequence,
        status: Some(ok_common_status()),
    };

    let decode_start = std::time::Instant::now();
    let key = exchange::ExchangeKey {
        finst_id_hi: params.finst_id_hi,
        finst_id_lo: params.finst_id_lo,
        node_id: params.node_id,
    };
    let chunks = match exchange::decode_chunks_for_sender(
        key,
        params.sender_id,
        params.be_number,
        &params.payload,
    ) {
        Ok(v) => v,
        Err(err) => {
            response.status = Some(error_common_status(format!(
                "exchange decode failed: {err}"
            )));
            return response;
        }
    };
    let decode_ns = decode_start.elapsed().as_nanos();

    exchange::push_chunks_with_stats(
        key,
        params.sender_id,
        params.be_number,
        chunks,
        params.eos,
        params.payload.len(),
        decode_ns,
    );
    response
}

#[cfg(feature = "compat")]
pub(crate) fn handle_transmit_chunk_compat(
    params: CompatTransmitChunkRequest,
) -> CompatTransmitChunkResponse {
    let mut response = CompatTransmitChunkResponse {
        status: Some(ok_status()),
        receive_timestamp: None,
        receiver_post_process_time: None,
    };

    let Some(finst_id) = params.finst_id.as_ref() else {
        response.status = Some(error_status("missing finst_id for transmit_chunk"));
        return response;
    };
    let Some(node_id) = params.node_id else {
        response.status = Some(error_status("missing node_id for transmit_chunk"));
        return response;
    };
    let Some(sender_id) = params.sender_id else {
        response.status = Some(error_status("missing sender_id for transmit_chunk"));
        return response;
    };
    let Some(be_number) = params.be_number else {
        response.status = Some(error_status("missing be_number for transmit_chunk"));
        return response;
    };
    let Some(eos) = params.eos else {
        response.status = Some(error_status("missing eos for transmit_chunk"));
        return response;
    };
    let Some(sequence) = params.sequence else {
        response.status = Some(error_status("missing sequence for transmit_chunk"));
        return response;
    };
    let Some(payload) = params.chunks.first().and_then(|chunk| chunk.data.as_ref()) else {
        response.status = Some(error_status("missing chunks[0].data for transmit_chunk"));
        return response;
    };

    let native = proto::novarocks::ExchangeRequest {
        finst_id_hi: finst_id.hi,
        finst_id_lo: finst_id.lo,
        node_id,
        sender_id,
        be_number,
        eos,
        sequence,
        payload: payload.clone(),
    };
    let native_response = handle_transmit_chunk(native);
    response.status = native_response.status.map(common_status_to_compat);
    response
}

pub(crate) fn handle_transmit_runtime_filter(
    params: NativeRuntimeFilterRequest,
) -> NativeRuntimeFilterResponse {
    let filter_id = params.filter_id;
    let result = decode_native_runtime_filter_transmission(params).and_then(|params| {
        handle_runtime_filter_transmission(params, RuntimeFilterIngress::Native)
    });
    encode_native_runtime_filter_result(filter_id, result)
}

enum RuntimeFilterIngress {
    Native,
}

fn handle_runtime_filter_transmission(
    params: RuntimeFilterTransmission,
    ingress: RuntimeFilterIngress,
) -> Result<(), String> {
    let filter_id = params.filter_id;
    let query_id = QueryId {
        hi: params.query_id.hi,
        lo: params.query_id.lo,
    };

    let (delivery_expire, query_expire) = query_expire_durations(None);
    match ingress {
        RuntimeFilterIngress::Native => {
            query_context_manager().ensure_native_context(
                query_id,
                false,
                delivery_expire,
                query_expire,
            )?;
            Err(format!(
                "legacy runtime-filter RPC is disabled for native query_id={query_id} filter_id={filter_id}"
            ))
        }
    }
}

pub(crate) fn handle_lookup(req: proto::filter::LookupRequest) -> proto::filter::LookupResponse {
    let mut response = proto::filter::LookupResponse {
        status: Some(ok_common_status()),
        columns: Vec::new(),
    };

    let Some(query_id) = req.query_id.as_ref() else {
        response.status = Some(error_common_status("missing query_id for lookup"));
        return response;
    };
    let query_id = QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };
    let tuple_id = req.request_tuple_id;

    let mut request_columns = HashMap::new();
    for col in req.request_columns {
        let slot_id = col.slot_id;
        if col.data.is_empty() {
            response.status = Some(error_common_status(format!(
                "lookup request column {} missing data",
                slot_id
            )));
            return response;
        }
        let slot_id = match SlotId::try_from(slot_id) {
            Ok(v) => v,
            Err(err) => {
                response.status = Some(error_common_status(err));
                return response;
            }
        };
        let array = match decode_column_ipc(&col.data) {
            Ok(arr) => arr,
            Err(err) => {
                response.status = Some(error_common_status(err));
                return response;
            }
        };
        request_columns.insert(slot_id, array);
    }

    match execute_position_lookup_request(query_id, tuple_id, request_columns) {
        Ok(columns) => {
            for (slot_id, array) in columns {
                let data = match encode_column_ipc(&array) {
                    Ok(v) => v,
                    Err(err) => {
                        response.status = Some(error_common_status(err));
                        return response;
                    }
                };
                response.columns.push(proto::filter::Column {
                    slot_id: slot_id.as_u32() as i32,
                    data_size: data.len() as i64,
                    data,
                });
            }
        }
        Err(err) => {
            response.status = Some(error_common_status(err));
        }
    }
    response
}

#[cfg(feature = "compat")]
pub(crate) fn handle_lookup_compat(req: CompatLookupRequest) -> CompatLookupResponse {
    let mut request_columns = Vec::with_capacity(req.request_columns.len());
    let Some(tuple_id) = req.request_tuple_id else {
        return CompatLookupResponse {
            status: Some(error_status("missing request_tuple_id for lookup")),
            columns: Vec::new(),
        };
    };

    for col in req.request_columns {
        let Some(slot_id) = col.slot_id else {
            return CompatLookupResponse {
                status: Some(error_status("lookup request column missing slot_id")),
                columns: Vec::new(),
            };
        };
        let data = col.data.unwrap_or_default();
        if data.is_empty() {
            return CompatLookupResponse {
                status: Some(error_status(format!(
                    "lookup request column {} missing data",
                    slot_id
                ))),
                columns: Vec::new(),
            };
        }
        let data_size = col.data_size.unwrap_or(data.len() as i64);
        request_columns.push(proto::filter::Column {
            slot_id,
            data_size,
            data,
        });
    }

    let native = proto::filter::LookupRequest {
        query_id: req
            .query_id
            .as_ref()
            .map(|query_id| proto::common::UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
        lookup_node_id: req.lookup_node_id.unwrap_or_default(),
        request_tuple_id: tuple_id,
        request_columns,
    };
    let response = handle_lookup(native);
    let status = response.status.map(|status| proto::starrocks::StatusPb {
        status_code: status.code,
        error_msgs: if status.message.is_empty() {
            Vec::new()
        } else {
            vec![status.message]
        },
    });
    CompatLookupResponse {
        status,
        columns: response
            .columns
            .into_iter()
            .map(|col| CompatColumn {
                slot_id: Some(col.slot_id),
                data_size: Some(col.data_size),
                data: Some(col.data),
            })
            .collect(),
    }
}

#[cfg(feature = "compat")]
pub(crate) fn handle_lookup_close_compat(
    req: CompatLookupCloseRequest,
) -> CompatLookupCloseResponse {
    let Some(query_id) = req.query_id else {
        return CompatLookupCloseResponse {
            status: Some(error_status("missing query_id for lookup_close")),
        };
    };
    let Some(lookup_node_id) = req.lookup_node_id else {
        return CompatLookupCloseResponse {
            status: Some(error_status("missing lookup_node_id for lookup_close")),
        };
    };
    let query_id = QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };
    let status = match query_context_manager().complete_lookup_fetcher(query_id, lookup_node_id) {
        Ok(()) => ok_status(),
        Err(err) => error_status(err),
    };
    CompatLookupCloseResponse {
        status: Some(status),
    }
}

#[cfg(test)]
mod native_runtime_filter_mode_tests {
    use super::*;

    fn submit_native_fragment(query_id: QueryId) -> Result<(), String> {
        let fragment_id = 8;
        crate::service::native_fragment_service::submit_exec_plan_fragment_native(
            crate::proto::plan::PlanFragment {
                fragment_id,
                root: Some(crate::proto::plan::DistributedNode {
                    node_id: 81,
                    fragment_id,
                    limit: -1,
                    payload: Some(crate::proto::plan::distributed_node::Payload::Physical(
                        crate::proto::plan::PlanNode {
                            output_columns: Vec::new(),
                            kind: Some(crate::proto::plan::plan_node::Kind::Values(
                                crate::proto::plan::ValuesNode {
                                    rows: Vec::new(),
                                    columns: Vec::new(),
                                },
                            )),
                        },
                    )),
                    ..Default::default()
                }),
                sink: Some(crate::proto::plan::DataSink {
                    kind: Some(crate::proto::plan::data_sink::Kind::Noop(true)),
                }),
                output_columns: Vec::new(),
                runtime_filter_bindings: Some(crate::proto::plan::RuntimeFilterBindingTable {
                    fragment_id,
                    bindings: Vec::new(),
                }),
                ..Default::default()
            },
            crate::proto::novarocks::InstanceParams {
                query_id: Some(proto::common::UniqueId {
                    hi: query_id.hi,
                    lo: query_id.lo,
                }),
                fragment_instance_id: Some(proto::common::UniqueId {
                    hi: query_id.hi + 1,
                    lo: query_id.lo + 1,
                }),
                query_options: Some(crate::proto::novarocks::QueryOptions {
                    pipeline_dop: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
    }

    #[test]
    fn native_rf_rpc_before_fragment_claims_disabled_and_queues_nothing() {
        let query_id = QueryId {
            hi: 71_001,
            lo: 71_002,
        };
        let response =
            handle_transmit_runtime_filter(proto::filter::TransmitRuntimeFilterRequest {
                is_partial: true,
                query_id: Some(proto::common::UniqueId {
                    hi: query_id.hi,
                    lo: query_id.lo,
                }),
                filter_id: 9,
                data: vec![1, 2, 3],
                build_be_number: 0,
                column_type: None,
            });

        let status = response.status.expect("status");
        assert_ne!(status.code, 0);
        assert!(status.message.contains("disabled"), "{}", status.message);
    }

    #[test]
    fn native_rf_rpc_before_fragment_claim_only_context_expires() {
        let query_id = QueryId {
            hi: 71_101,
            lo: 71_102,
        };
        let query_key = crate::runtime::runtime_filter_observability::QueryKey::from_hi_lo(
            query_id.hi,
            query_id.lo,
        );
        let registry =
            crate::runtime::runtime_filter_observability::RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);

        let response =
            handle_transmit_runtime_filter(proto::filter::TransmitRuntimeFilterRequest {
                is_partial: true,
                query_id: Some(proto::common::UniqueId {
                    hi: query_id.hi,
                    lo: query_id.lo,
                }),
                filter_id: 9,
                data: vec![1, 2, 3],
                build_be_number: 0,
                column_type: None,
            });
        assert_ne!(response.status.expect("status").code, 0);

        let manager = query_context_manager();
        assert!(registry.snapshot(query_key).is_some());
        manager
            .with_context_mut(query_id, |context| {
                assert_eq!(context.num_active_fragments, 0);
                context.query_deadline =
                    std::time::Instant::now() - std::time::Duration::from_millis(1);
                Ok(())
            })
            .expect("claim-only active context");

        manager.clean_expired_for_test();

        assert!(manager.query_mem_tracker(query_id).is_none());
        assert!(registry.snapshot(query_key).is_none());
    }

    #[test]
    fn native_fragment_before_rf_rpc_claims_disabled_and_queues_nothing() {
        let query_id = QueryId {
            hi: 71_003,
            lo: 71_004,
        };
        submit_native_fragment(query_id).expect("native fragment submission");

        let response =
            handle_transmit_runtime_filter(proto::filter::TransmitRuntimeFilterRequest {
                is_partial: true,
                query_id: Some(proto::common::UniqueId {
                    hi: query_id.hi,
                    lo: query_id.lo,
                }),
                filter_id: 9,
                data: vec![1, 2, 3],
                build_be_number: 0,
                column_type: None,
            });

        let status = response.status.expect("status");
        assert_ne!(status.code, 0);
        assert!(status.message.contains("disabled"), "{}", status.message);
    }
}
