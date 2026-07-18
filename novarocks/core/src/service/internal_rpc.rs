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
use crate::exec::runtime_filter::arrow_type_from_common_type_desc;
use crate::novarocks_logging::warn;
use crate::proto;
use crate::runtime::exchange;
use crate::runtime::lookup::{
    decode_column_ipc, encode_column_ipc, execute_position_lookup_request,
};
use crate::runtime::query_context::{QueryId, query_context_manager, query_expire_durations};

#[cfg(feature = "compat")]
type CompatTransmitChunkRequest = proto::starrocks::PTransmitChunkParams; // cfg(feature = "compat")
#[cfg(feature = "compat")]
type CompatTransmitChunkResponse = proto::starrocks::PTransmitChunkResult; // cfg(feature = "compat")
#[cfg(feature = "compat")]
type CompatTransmitRuntimeFilterRequest = proto::starrocks::PTransmitRuntimeFilterParams; // cfg(feature = "compat")
#[cfg(feature = "compat")]
type CompatTransmitRuntimeFilterResponse = proto::starrocks::PTransmitRuntimeFilterResult; // cfg(feature = "compat")
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
    params: proto::filter::TransmitRuntimeFilterRequest,
) -> proto::filter::TransmitRuntimeFilterResponse {
    let filter_id = params.filter_id;
    let mut response = proto::filter::TransmitRuntimeFilterResponse {
        status: Some(ok_common_status()),
        filter_id,
    };

    let Some(query_id) = params.query_id.as_ref() else {
        response.status = Some(error_common_status(
            "missing query_id for transmit_runtime_filter",
        ));
        return response;
    };
    let query_id = QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };

    let (delivery_expire, query_expire) = query_expire_durations(None);
    if let Err(err) = query_context_manager().ensure_native_context(
        query_id,
        false,
        delivery_expire,
        query_expire,
    ) {
        response.status = Some(error_common_status(err));
        return response;
    }
    response.status = Some(error_common_status(format!(
        "legacy runtime-filter RPC is disabled for native query_id={query_id} filter_id={filter_id}"
    )));
    response
}

#[cfg(feature = "compat")]
fn record_pending_runtime_filter_enqueue_result(
    response: &mut proto::filter::TransmitRuntimeFilterResponse,
    result: Result<(), String>,
) {
    if let Err(err) = result {
        response.status = Some(error_common_status(err));
    }
}

#[cfg(feature = "compat")]
fn handle_transmit_runtime_filter_compat_common(
    params: proto::filter::TransmitRuntimeFilterRequest,
) -> proto::filter::TransmitRuntimeFilterResponse {
    let filter_id = params.filter_id;
    let mut response = proto::filter::TransmitRuntimeFilterResponse {
        status: Some(ok_common_status()),
        filter_id,
    };
    let Some(query_id) = params.query_id.as_ref() else {
        response.status = Some(error_common_status(
            "missing query_id for transmit_runtime_filter",
        ));
        return response;
    };
    let query_id = QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };

    let (delivery_expire, query_expire) = query_expire_durations(None);
    if let Err(err) = query_context_manager().ensure_compat_context(
        query_id,
        false,
        delivery_expire,
        query_expire,
    ) {
        response.status = Some(error_common_status(err));
        return response;
    }

    let payload = params.data.as_slice();
    if payload.is_empty() {
        response.status = Some(error_common_status(format!(
            "runtime filter payload is empty: query_id={} filter_id={}",
            query_id, filter_id
        )));
        return response;
    }

    let build_data_type = params
        .column_type
        .as_ref()
        .and_then(arrow_type_from_common_type_desc);

    if params.is_partial {
        let worker = match query_context_manager().get_or_create_runtime_filter_worker(query_id) {
            Ok(worker) => worker,
            Err(err) => {
                response.status = Some(error_common_status(err));
                return response;
            }
        };
        let Some(worker) = worker else {
            record_pending_runtime_filter_enqueue_result(
                &mut response,
                query_context_manager().enqueue_pending_runtime_filter(
                    query_id,
                    filter_id,
                    params.build_be_number,
                    params.data,
                    build_data_type,
                ),
            );
            return response;
        };
        let build_be_number = params.build_be_number;
        if let Err(err) =
            worker.receive_partial(filter_id, payload, build_be_number, build_data_type)
        {
            warn!(
                "receive_partial_runtime_filter failed: query_id={} filter_id={} err={}",
                query_id, filter_id, err
            );
            response.status = Some(error_common_status(err));
        }
        return response;
    }

    if let Err(err) = receive_total_runtime_filter(query_id, filter_id, params.is_partial, payload)
    {
        warn!(
            "receive_total_runtime_filter failed: query_id={} filter_id={} err={}",
            query_id, filter_id, err
        );
        response.status = Some(error_common_status(err));
    }
    response
}

#[cfg(feature = "compat")]
pub(crate) fn handle_transmit_runtime_filter_compat(
    params: CompatTransmitRuntimeFilterRequest,
) -> CompatTransmitRuntimeFilterResponse {
    let Some(filter_id) = params.filter_id else {
        return CompatTransmitRuntimeFilterResponse {
            status: Some(error_status(
                "missing filter_id for transmit_runtime_filter",
            )),
            filter_id: None,
        };
    };
    let column_type = params.column_type.as_ref().and_then(|desc| {
        crate::exec::runtime_filter::arrow_type_from_proto_type_desc(desc).and_then(|data_type| {
            crate::exec::runtime_filter::arrow_type_to_common_type_desc(&data_type)
        })
    });
    let native = proto::filter::TransmitRuntimeFilterRequest {
        is_partial: params.is_partial.unwrap_or(false),
        query_id: params
            .query_id
            .as_ref()
            .map(|query_id| proto::common::UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
        filter_id,
        data: params.data.unwrap_or_default(),
        build_be_number: params.build_be_number.unwrap_or_default(),
        column_type,
    };
    let response = handle_transmit_runtime_filter_compat_common(native);
    let status = response.status.map(|status| proto::starrocks::StatusPb {
        status_code: status.code,
        error_msgs: if status.message.is_empty() {
            Vec::new()
        } else {
            vec![status.message]
        },
    });
    CompatTransmitRuntimeFilterResponse {
        status,
        filter_id: Some(response.filter_id),
    }
}

fn receive_total_runtime_filter(
    query_id: QueryId,
    filter_id: i32,
    is_partial: bool,
    payload: &[u8],
) -> Result<(), String> {
    // Complete-only contract (P0a section 5.4): the prober install path only handles
    // TOTAL filters. Partial filters must be routed to the merge node above.
    if is_partial {
        return Err(
            "runtime filter contract violation: partial filter reached prober install path"
                .to_string(),
        );
    }
    let Some(hub) = query_context_manager().get_runtime_filter_hub(query_id)? else {
        return Err(format!("runtime filter hub not found: query_id={query_id}"));
    };
    hub.receive_remote_filter(filter_id, payload)
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

    fn submit_native_fragment_with_legacy_runtime_filter(query_id: QueryId) -> Result<(), String> {
        crate::service::native_fragment_service::submit_exec_plan_fragment_native(
            crate::proto::plan::PlanFragment::default(),
            crate::proto::novarocks::InstanceParams {
                query_id: Some(proto::common::UniqueId {
                    hi: query_id.hi,
                    lo: query_id.lo,
                }),
                fragment_instance_id: Some(proto::common::UniqueId {
                    hi: query_id.hi + 1,
                    lo: query_id.lo + 1,
                }),
                runtime_filter_params: Some(crate::proto::novarocks::RuntimeFilterParams {
                    runtime_filter_builder_number: HashMap::from([(9, 1)]),
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
        let manager = query_context_manager();
        assert!(manager.get_runtime_filter_hub(query_id).is_err());
        assert!(
            manager
                .enqueue_pending_runtime_filter(query_id, 9, 0, vec![1], None)
                .is_err()
        );
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
        let fragment_error = submit_native_fragment_with_legacy_runtime_filter(query_id)
            .expect_err("native fragment must reject legacy runtime-filter params");
        assert!(
            fragment_error.contains("contains legacy runtime-filter params"),
            "{fragment_error}"
        );

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
        let manager = query_context_manager();
        assert!(manager.get_runtime_filter_hub(query_id).is_err());
        assert!(manager.get_runtime_filter_worker(query_id).is_err());
        assert!(
            manager
                .enqueue_pending_runtime_filter(query_id, 9, 0, vec![1], None)
                .is_err()
        );
    }
}
