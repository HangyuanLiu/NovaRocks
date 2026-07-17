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

#[cfg(all(test, feature = "compat"))]
mod pending_runtime_filter_enqueue_error_tests {
    use super::*;

    #[test]
    fn pending_enqueue_error_is_returned_in_rpc_status() {
        let mut response = proto::filter::TransmitRuntimeFilterResponse {
            status: Some(ok_common_status()),
            filter_id: 17,
        };

        record_pending_runtime_filter_enqueue_result(
            &mut response,
            Err("injected pending enqueue failure".to_string()),
        );

        let status = response.status.expect("status");
        assert_ne!(status.code, 0);
        assert!(status.message.contains("injected pending enqueue failure"));
     }
 }

#[cfg(all(test, feature = "compat"))]
mod tests {
    #[cfg(feature = "compat")]
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::sync::Arc;
    #[cfg(feature = "compat")]
    use std::sync::Mutex;
    use std::time::Duration;

    use arrow::array::{ArrayRef, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;

    #[cfg(feature = "compat")]
    use super::{
        CompatColumn, CompatLookupRequest, CompatTransmitChunkRequest,
        CompatTransmitRuntimeFilterRequest, handle_lookup_compat, handle_transmit_chunk_compat,
        handle_transmit_runtime_filter_compat,
    };
    use super::{
        decode_column_ipc, encode_column_ipc, handle_lookup, handle_transmit_chunk,
        handle_transmit_runtime_filter, receive_total_runtime_filter,
    };
    use crate::cache::CacheOptions;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::ExprId;
    use crate::exec::node::RuntimeFilterProbeSpec;
    use crate::exec::node::scan::{HdfsScanFileFormat, RowPositionScanConfig};
    use crate::exec::row_position::{RowPositionDescriptor, RowPositionType};
    use crate::exec::runtime_filter::{RuntimeInFilter, encode_starrocks_in_filter};
    #[cfg(feature = "compat")]
    use crate::exec::runtime_filter::{arrow_type_to_proto_type_desc, decode_starrocks_in_filter};
    use crate::fs::scan_context::FileScanRange;
    use crate::proto;
    use crate::runtime::descriptor_snapshot_thrift::descriptor_snapshot_from_thrift;
    use crate::runtime::exchange;
    use crate::runtime::query_context::{QueryId, query_context_manager};
    use crate::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};
    #[cfg(feature = "compat")]
    use crate::runtime::runtime_filter_params::RuntimeFilterParams;
    #[cfg(feature = "compat")]
    use crate::service::internal_rpc_client;
    use crate::thrift::descriptors;
    use crate::thrift::types;
    #[cfg(feature = "compat")]
    use crate::thrift::{
        internal_service as thrift_internal_service, partitions, planner, runtime_filter,
    };

    fn unique_id(hi: i64, lo: i64) -> proto::starrocks::PUniqueId {
        proto::starrocks::PUniqueId { hi, lo }
    }

    fn common_unique_id(hi: i64, lo: i64) -> proto::common::UniqueId {
        proto::common::UniqueId { hi, lo }
    }

    #[cfg(feature = "compat")]
    fn runtime_filter_params_from_thrift_fixture(
        params: crate::thrift::runtime_filter::TRuntimeFilterParams,
    ) -> RuntimeFilterParams {
        RuntimeFilterParams::from_thrift(&params).expect("runtime filter params")
    }

    #[cfg(feature = "compat")]
    fn submit_compat_fragment_with_runtime_filter_params(
        query_id: QueryId,
        runtime_filter_params: runtime_filter::TRuntimeFilterParams,
    ) -> Result<(), String> {
        let fragment = planner::TPlanFragment {
            plan: None,
            output_exprs: None,
            output_sink: None,
            partition: partitions::TDataPartition::new(
                partitions::TPartitionType::UNPARTITIONED,
                None::<Vec<crate::thrift::exprs::TExpr>>,
                None::<Vec<partitions::TRangePartition>>,
                None::<Vec<partitions::TBucketProperty>>,
            ),
            min_reservation_bytes: None,
            initial_reservation_total_claims: None,
            query_global_dicts: None,
            load_global_dicts: None,
            cache_param: None,
            query_global_dict_exprs: None,
            group_execution_param: None,
        };
        let exec_params = thrift_internal_service::TPlanFragmentExecParams {
            query_id: types::TUniqueId::new(query_id.hi, query_id.lo),
            fragment_instance_id: types::TUniqueId::new(query_id.hi + 1, query_id.lo + 1),
            per_node_scan_ranges: BTreeMap::new(),
            per_exch_num_senders: BTreeMap::new(),
            destinations: None,
            sender_id: None,
            num_senders: None,
            send_query_statistics_with_every_batch: None,
            use_vectorized: None,
            runtime_filter_params: Some(runtime_filter_params),
            instances_number: None,
            enable_exchange_pass_through: None,
            node_to_per_driver_seq_scan_ranges: None,
            enable_exchange_perf: None,
            pipeline_sink_dop: None,
            report_when_finish: None,
            exec_debug_options: None,
        };
        let request = thrift_internal_service::TExecPlanFragmentParams {
            protocol_version: thrift_internal_service::InternalServiceVersion::V1,
            fragment: Some(fragment),
            desc_tbl: None,
            params: Some(exec_params),
            coord: None,
            backend_num: None,
            query_globals: None,
            query_options: None,
            enable_profile: None,
            resource_info: None,
            import_label: None,
            db_name: None,
            load_job_id: None,
            load_error_hub_info: None,
            is_pipeline: None,
            pipeline_dop: None,
            per_scan_node_dop: None,
            workgroup: None,
            enable_resource_group: None,
            func_version: None,
            enable_shared_scan: None,
            is_stream_pipeline: None,
            adaptive_dop_param: None,
            group_execution_scan_dop: None,
            pred_tree_params: None,
            exec_stats_node_ids: None,
            arrow_flight_sql_version: None,
            novarocks_report_addr: None,
            novarocks_typed_result_sink: None,
        };
        let bytes = crate::common::thrift::thrift_binary_serialize(&request)?;
        crate::service::internal_service::submit_exec_plan_fragment(&bytes)
    }

    #[cfg(feature = "compat")]
    fn submit_native_fragment(query_id: QueryId) -> Result<(), String> {
        crate::service::native_fragment_service::submit_exec_plan_fragment_native(
            crate::proto::plan::PlanFragment::default(),
            crate::proto::novarocks::InstanceParams {
                query_id: Some(common_unique_id(query_id.hi, query_id.lo)),
                fragment_instance_id: Some(common_unique_id(query_id.hi + 1, query_id.lo + 1)),
                ..Default::default()
            },
        )
    }

    fn ok_status(status: Option<&proto::starrocks::StatusPb>) -> bool {
        status.map(|s| s.status_code).unwrap_or_default() == 0
    }

    fn ok_common_status(status: Option<&proto::common::Status>) -> bool {
        status.map(|s| s.code).unwrap_or_default() == 0
    }

    #[cfg(feature = "compat")]
    fn error_status_message(status: Option<&proto::starrocks::StatusPb>) -> String {
        status
            .and_then(|s| s.error_msgs.first())
            .cloned()
            .unwrap_or_default()
    }

    fn int_type_desc() -> types::TTypeDesc {
        types::TTypeDesc {
            types: Some(vec![types::TTypeNode {
                type_: types::TTypeNodeType::SCALAR,
                scalar_type: Some(types::TScalarType {
                    type_: types::TPrimitiveType::INT,
                    len: None,
                    precision: None,
                    scale: None,
                    time_unit: None,
                }),
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn bigint_type_desc() -> types::TTypeDesc {
        types::TTypeDesc {
            types: Some(vec![types::TTypeNode {
                type_: types::TTypeNodeType::SCALAR,
                scalar_type: Some(types::TScalarType {
                    type_: types::TPrimitiveType::BIGINT,
                    len: None,
                    precision: None,
                    scale: None,
                    time_unit: None,
                }),
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn lookup_desc_tbl(tuple_id: i32) -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable {
            slot_descriptors: Some(vec![
                descriptors::TSlotDescriptor {
                    id: Some(1),
                    parent: Some(tuple_id),
                    slot_type: Some(int_type_desc()),
                    column_pos: None,
                    byte_offset: None,
                    null_indicator_byte: None,
                    null_indicator_bit: None,
                    col_name: Some("_row_source_id".to_string()),
                    slot_idx: None,
                    is_materialized: Some(true),
                    is_output_column: Some(true),
                    is_nullable: Some(false),
                    col_unique_id: None,
                    col_physical_name: None,
                    is_virtual_column: None,
                },
                descriptors::TSlotDescriptor {
                    id: Some(2),
                    parent: Some(tuple_id),
                    slot_type: Some(int_type_desc()),
                    column_pos: None,
                    byte_offset: None,
                    null_indicator_byte: None,
                    null_indicator_bit: None,
                    col_name: Some("_scan_range_id".to_string()),
                    slot_idx: None,
                    is_materialized: Some(true),
                    is_output_column: Some(true),
                    is_nullable: Some(false),
                    col_unique_id: None,
                    col_physical_name: None,
                    is_virtual_column: None,
                },
                descriptors::TSlotDescriptor {
                    id: Some(3),
                    parent: Some(tuple_id),
                    slot_type: Some(bigint_type_desc()),
                    column_pos: None,
                    byte_offset: None,
                    null_indicator_byte: None,
                    null_indicator_bit: None,
                    col_name: Some("_row_id".to_string()),
                    slot_idx: None,
                    is_materialized: Some(true),
                    is_output_column: Some(true),
                    is_nullable: Some(false),
                    col_unique_id: None,
                    col_physical_name: None,
                    is_virtual_column: None,
                },
                descriptors::TSlotDescriptor {
                    id: Some(4),
                    parent: Some(tuple_id),
                    slot_type: Some(int_type_desc()),
                    column_pos: None,
                    byte_offset: None,
                    null_indicator_byte: None,
                    null_indicator_bit: None,
                    col_name: Some("v".to_string()),
                    slot_idx: None,
                    is_materialized: Some(true),
                    is_output_column: Some(true),
                    is_nullable: Some(false),
                    col_unique_id: None,
                    col_physical_name: None,
                    is_virtual_column: None,
                },
            ]),
            table_descriptors: None,
            tuple_descriptors: Vec::new(),
            is_cached: None,
        }
    }

    #[test]
    fn test_handle_transmit_chunk_delivers_payload_and_eos() {
        let finst_id = common_unique_id(31, 42);
        let key = exchange::ExchangeKey {
            finst_id_hi: finst_id.hi,
            finst_id_lo: finst_id.lo,
            node_id: 7,
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .expect("record batch");
        let chunk = {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        };
        exchange::register_expected_chunk_schema(key, 1, chunk.chunk_schema_ref())
            .expect("register expected chunk schema");
        let payload = exchange::encode_chunks(&[chunk], true).expect("encode chunks");

        let response = handle_transmit_chunk(proto::novarocks::ExchangeRequest {
            finst_id_hi: finst_id.hi,
            finst_id_lo: finst_id.lo,
            node_id: 7,
            sender_id: 3,
            be_number: 9,
            eos: true,
            sequence: 42,
            payload,
        });

        assert!(ok_common_status(response.status.as_ref()));
        assert_eq!(response.ack_sequence, 42);
        let snapshot =
            exchange::snapshot_receiver_state(key).expect("receiver snapshot after transmit_chunk");
        assert_eq!(snapshot.queued_chunks, 1);
        assert_eq!(snapshot.queued_rows, 3);
        assert_eq!(snapshot.finished_senders, 1);
        exchange::cancel_exchange_key(key);
    }

    #[test]
    fn test_handle_transmit_chunk_empty_payload_marks_eos_sender_finished() {
        let key = exchange::ExchangeKey {
            finst_id_hi: 11,
            finst_id_lo: 22,
            node_id: 7,
        };

        let response = handle_transmit_chunk(proto::novarocks::ExchangeRequest {
            finst_id_hi: key.finst_id_hi,
            finst_id_lo: key.finst_id_lo,
            node_id: key.node_id,
            sender_id: 3,
            be_number: 9,
            eos: true,
            sequence: 42,
            payload: Vec::new(),
        });

        assert_eq!(response.ack_sequence, 42);
        assert!(ok_common_status(response.status.as_ref()));
        let snapshot =
            exchange::snapshot_receiver_state(key).expect("receiver snapshot after empty EOS");
        assert_eq!(snapshot.queued_chunks, 0);
        assert_eq!(snapshot.queued_rows, 0);
        assert_eq!(snapshot.finished_senders, 1);
        exchange::cancel_exchange_key(key);
    }

    #[cfg(feature = "compat")]
    #[test]
    fn test_handle_transmit_chunk_legacy_wrapper_rejects_missing_payload() {
        let response = handle_transmit_chunk_compat(CompatTransmitChunkRequest {
            finst_id: Some(unique_id(1, 2)),
            node_id: Some(7),
            sender_id: Some(3),
            be_number: Some(9),
            eos: Some(true),
            sequence: Some(42),
            chunks: vec![proto::starrocks::ChunkPb::default()],
            ..Default::default()
        });

        assert!(!ok_status(response.status.as_ref()));
        assert_eq!(
            response
                .status
                .as_ref()
                .and_then(|status| status.error_msgs.first())
                .map(String::as_str),
            Some("missing chunks[0].data for transmit_chunk")
        );
    }

    #[test]
    #[cfg(feature = "compat")]
    fn test_handle_transmit_runtime_filter_compat_rejects_missing_filter_id() {
        let response = handle_transmit_runtime_filter_compat(CompatTransmitRuntimeFilterRequest {
            is_partial: Some(false),
            query_id: Some(unique_id(10, 20)),
            filter_id: None,
            data: Some(vec![1]),
            ..Default::default()
        });

        assert!(!ok_status(response.status.as_ref()));
        assert_eq!(
            error_status_message(response.status.as_ref()),
            "missing filter_id for transmit_runtime_filter"
        );
        assert_eq!(response.filter_id, None);
    }

    #[test]
    #[cfg(feature = "compat")]
    fn test_handle_transmit_runtime_filter_partial_merge_broadcasts_on_completion() {
        let _hook_guard = internal_rpc_client::test_hook_lock();
        let _transport_guard =
            crate::service::internal_rpc_transport::use_brpc_compat_internal_rpc_transport_for_test(
            );
        internal_rpc_client::clear_test_hooks();

        let query_id = QueryId { hi: 100, lo: 200 };
        query_context_manager()
            .ensure_compat_context(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("ensure query context");
        query_context_manager()
            .set_runtime_filter_params(
                query_id,
                runtime_filter_params_from_thrift_fixture(
                    crate::thrift::runtime_filter::TRuntimeFilterParams {
                        id_to_prober_params: Some(BTreeMap::from([(
                            7,
                            vec![crate::thrift::runtime_filter::TRuntimeFilterProberParams {
                                fragment_instance_id: Some(types::TUniqueId::new(700, 701)),
                                fragment_instance_address: Some(types::TNetworkAddress::new(
                                    "probe-host".to_string(),
                                    9010,
                                )),
                            }],
                        )])),
                        runtime_filter_builder_number: Some(BTreeMap::from([(7, 2)])),
                        runtime_filter_max_size: None,
                        skew_join_runtime_filters: None,
                    },
                ),
            )
            .expect("set runtime filter params");

        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_capture = Arc::clone(&sent);
        internal_rpc_client::set_transmit_runtime_filter_hook(move |host, port, params| {
            sent_capture
                .lock()
                .expect("sent lock")
                .push((host.to_string(), port, params));
            Ok(())
        });

        let filter =
            RuntimeInFilter::empty(7, SlotId::new(11), &DataType::Int32).expect("empty in filter");
        let payload = encode_starrocks_in_filter(&filter).expect("encode runtime filter");

        let first = handle_transmit_runtime_filter_compat(CompatTransmitRuntimeFilterRequest {
            is_partial: Some(true),
            query_id: Some(unique_id(query_id.hi, query_id.lo)),
            filter_id: Some(7),
            build_be_number: Some(1),
            data: Some(payload.clone()),
            ..Default::default()
        });
        let second = handle_transmit_runtime_filter_compat(CompatTransmitRuntimeFilterRequest {
            is_partial: Some(true),
            query_id: Some(unique_id(query_id.hi, query_id.lo)),
            filter_id: Some(7),
            build_be_number: Some(2),
            data: Some(payload),
            ..Default::default()
        });

        assert!(ok_status(first.status.as_ref()));
        assert!(ok_status(second.status.as_ref()));
        let sent = sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "probe-host");
        assert_eq!(sent[0].1, 9010);
        assert!(!sent[0].2.is_partial);
        internal_rpc_client::clear_test_hooks();
    }

    #[test]
    #[cfg(feature = "compat")]
    fn test_handle_transmit_runtime_filter_decimal_partial_uses_build_column_type() {
        let _hook_guard = internal_rpc_client::test_hook_lock();
        let _transport_guard =
            crate::service::internal_rpc_transport::use_brpc_compat_internal_rpc_transport_for_test(
            );
        internal_rpc_client::clear_test_hooks();

        let query_id = QueryId { hi: 101, lo: 201 };
        query_context_manager()
            .ensure_compat_context(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("ensure query context");
        query_context_manager()
            .set_runtime_filter_params(
                query_id,
                runtime_filter_params_from_thrift_fixture(
                    crate::thrift::runtime_filter::TRuntimeFilterParams {
                        id_to_prober_params: Some(BTreeMap::from([(
                            9,
                            vec![crate::thrift::runtime_filter::TRuntimeFilterProberParams {
                                fragment_instance_id: Some(types::TUniqueId::new(900, 901)),
                                fragment_instance_address: Some(types::TNetworkAddress::new(
                                    "probe-host".to_string(),
                                    9010,
                                )),
                            }],
                        )])),
                        runtime_filter_builder_number: Some(BTreeMap::from([(9, 2)])),
                        runtime_filter_max_size: None,
                        skew_join_runtime_filters: None,
                    },
                ),
            )
            .expect("set runtime filter params");

        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_capture = Arc::clone(&sent);
        internal_rpc_client::set_transmit_runtime_filter_hook(move |host, port, params| {
            sent_capture
                .lock()
                .expect("sent lock")
                .push((host.to_string(), port, params));
            Ok(())
        });

        let dt = DataType::Decimal128(18, 2);
        let column_type =
            arrow_type_to_proto_type_desc(&dt).expect("decimal runtime filter column type");
        let filter = RuntimeInFilter::empty(9, SlotId::new(11), &dt).expect("empty in filter");
        let payload = encode_starrocks_in_filter(&filter).expect("encode runtime filter");

        let first = handle_transmit_runtime_filter_compat(CompatTransmitRuntimeFilterRequest {
            is_partial: Some(true),
            query_id: Some(unique_id(query_id.hi, query_id.lo)),
            filter_id: Some(9),
            build_be_number: Some(1),
            data: Some(payload.clone()),
            column_type: Some(column_type.clone()),
            ..Default::default()
        });
        let second = handle_transmit_runtime_filter_compat(CompatTransmitRuntimeFilterRequest {
            is_partial: Some(true),
            query_id: Some(unique_id(query_id.hi, query_id.lo)),
            filter_id: Some(9),
            build_be_number: Some(2),
            data: Some(payload),
            column_type: Some(column_type),
            ..Default::default()
        });

        assert!(
            ok_status(first.status.as_ref()),
            "first status: {:?}",
            first.status
        );
        assert!(
            ok_status(second.status.as_ref()),
            "second status: {:?}",
            second.status
        );
        let sent = sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "probe-host");
        assert_eq!(sent[0].1, 9010);
        assert!(!sent[0].2.is_partial);
        let final_payload = sent[0].2.data.as_slice();
        let decoded = decode_starrocks_in_filter(9, SlotId::new(11), Some(&dt), final_payload)
            .expect("decode final decimal runtime filter");
        assert!(decoded.is_empty());
        internal_rpc_client::clear_test_hooks();
    }

    #[test]
    #[cfg(feature = "compat")]
    fn test_handle_transmit_runtime_filter_pending_decimal_partial_preserves_build_column_type() {
        let _hook_guard = internal_rpc_client::test_hook_lock();
        let _transport_guard =
            crate::service::internal_rpc_transport::use_brpc_compat_internal_rpc_transport_for_test(
            );
        internal_rpc_client::clear_test_hooks();

        let query_id = QueryId { hi: 102, lo: 202 };
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_capture = Arc::clone(&sent);
        internal_rpc_client::set_transmit_runtime_filter_hook(move |host, port, params| {
            sent_capture
                .lock()
                .expect("sent lock")
                .push((host.to_string(), port, params));
            Ok(())
        });

        let dt = DataType::Decimal128(18, 2);
        let column_type =
            arrow_type_to_proto_type_desc(&dt).expect("decimal runtime filter column type");
        let filter = RuntimeInFilter::empty(10, SlotId::new(11), &dt).expect("empty in filter");
        let payload = encode_starrocks_in_filter(&filter).expect("encode runtime filter");

        let first = handle_transmit_runtime_filter_compat(CompatTransmitRuntimeFilterRequest {
            is_partial: Some(true),
            query_id: Some(unique_id(query_id.hi, query_id.lo)),
            filter_id: Some(10),
            build_be_number: Some(1),
            data: Some(payload.clone()),
            column_type: Some(column_type.clone()),
            ..Default::default()
        });
        assert!(
            ok_status(first.status.as_ref()),
            "first status: {:?}",
            first.status
        );
        assert!(
            sent.lock().expect("sent lock").is_empty(),
            "pending first partial must not broadcast before params are installed"
        );

        submit_compat_fragment_with_runtime_filter_params(
            query_id,
            crate::thrift::runtime_filter::TRuntimeFilterParams {
                id_to_prober_params: Some(BTreeMap::from([(
                    10,
                    vec![crate::thrift::runtime_filter::TRuntimeFilterProberParams {
                        fragment_instance_id: Some(types::TUniqueId::new(1000, 1001)),
                        fragment_instance_address: Some(types::TNetworkAddress::new(
                            "probe-host".to_string(),
                            9010,
                        )),
                    }],
                )])),
                runtime_filter_builder_number: Some(BTreeMap::from([(10, 2)])),
                runtime_filter_max_size: None,
                skew_join_runtime_filters: None,
            },
        )
        .expect("submit compat fragment with runtime-filter params");
        assert!(
            sent.lock().expect("sent lock").is_empty(),
            "single replayed partial must not broadcast before all builders arrive"
        );

        let second = handle_transmit_runtime_filter_compat(CompatTransmitRuntimeFilterRequest {
            is_partial: Some(true),
            query_id: Some(unique_id(query_id.hi, query_id.lo)),
            filter_id: Some(10),
            build_be_number: Some(2),
            data: Some(payload),
            column_type: Some(column_type),
            ..Default::default()
        });
        assert!(
            ok_status(second.status.as_ref()),
            "second status: {:?}",
            second.status
        );

        let sent = sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "probe-host");
        assert_eq!(sent[0].1, 9010);
        assert!(!sent[0].2.is_partial);
        let final_payload = sent[0].2.data.as_slice();
        let decoded = decode_starrocks_in_filter(10, SlotId::new(11), Some(&dt), final_payload)
            .expect("decode final decimal runtime filter");
        assert!(decoded.is_empty());
        internal_rpc_client::clear_test_hooks();
    }

    #[test]
    #[cfg(feature = "compat")]
    fn compat_fragment_before_rf_rpc_delivers_without_pending_queue() {
        let _hook_guard = internal_rpc_client::test_hook_lock();
        let _transport_guard =
            crate::service::internal_rpc_transport::use_brpc_compat_internal_rpc_transport_for_test(
            );
        internal_rpc_client::clear_test_hooks();

        let query_id = QueryId {
            hi: 72_001,
            lo: 72_002,
        };
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_capture = Arc::clone(&sent);
        internal_rpc_client::set_transmit_runtime_filter_hook(move |host, port, params| {
            sent_capture
                .lock()
                .expect("sent lock")
                .push((host.to_string(), port, params));
            Ok(())
        });

        submit_compat_fragment_with_runtime_filter_params(
            query_id,
            runtime_filter::TRuntimeFilterParams {
                id_to_prober_params: Some(BTreeMap::from([(
                    11,
                    vec![runtime_filter::TRuntimeFilterProberParams {
                        fragment_instance_id: Some(types::TUniqueId::new(1100, 1101)),
                        fragment_instance_address: Some(types::TNetworkAddress::new(
                            "probe-host".to_string(),
                            9011,
                        )),
                    }],
                )])),
                runtime_filter_builder_number: Some(BTreeMap::from([(11, 1)])),
                runtime_filter_max_size: None,
                skew_join_runtime_filters: None,
            },
        )
        .expect("submit compat fragment");
        assert!(
            query_context_manager()
                .get_runtime_filter_worker(query_id)
                .expect("compat worker lookup")
                .is_some()
        );

        let filter = RuntimeInFilter::empty(11, SlotId::new(11), &DataType::Int32)
            .expect("empty runtime filter");
        let response = handle_transmit_runtime_filter_compat(CompatTransmitRuntimeFilterRequest {
            is_partial: Some(true),
            query_id: Some(unique_id(query_id.hi, query_id.lo)),
            filter_id: Some(11),
            build_be_number: Some(1),
            data: Some(encode_starrocks_in_filter(&filter).expect("encode runtime filter")),
            ..Default::default()
        });
        assert!(
            ok_status(response.status.as_ref()),
            "response status: {:?}",
            response.status
        );
        let sent = sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "probe-host");
        assert_eq!(sent[0].1, 9011);
        assert!(!sent[0].2.is_partial);
        internal_rpc_client::clear_test_hooks();
    }

    #[test]
    #[cfg(feature = "compat")]
    fn native_rf_rpc_then_compat_fragment_rejects_cross_mode_without_partial_state() {
        let query_id = QueryId {
            hi: 72_003,
            lo: 72_004,
        };
        let native_response =
            handle_transmit_runtime_filter(proto::filter::TransmitRuntimeFilterRequest {
                is_partial: true,
                query_id: Some(common_unique_id(query_id.hi, query_id.lo)),
                filter_id: 12,
                data: vec![1, 2, 3],
                build_be_number: 1,
                column_type: None,
            });
        assert!(!ok_common_status(native_response.status.as_ref()));

        let error = submit_compat_fragment_with_runtime_filter_params(
            query_id,
            runtime_filter::TRuntimeFilterParams::default(),
        )
        .expect_err("compat fragment must reject native-claimed query");
        assert!(error.contains("NativeDisabled"), "{error}");
        let manager = query_context_manager();
        assert!(manager.get_runtime_filter_hub(query_id).is_err());
        assert!(manager.get_runtime_filter_worker(query_id).is_err());
        assert!(manager.get_runtime_filter_params(query_id).is_err());
    }

    #[test]
    #[cfg(feature = "compat")]
    fn compat_rf_rpc_then_native_fragment_rejects_cross_mode_without_native_partial_state() {
        let query_id = QueryId {
            hi: 72_005,
            lo: 72_006,
        };
        let filter = RuntimeInFilter::empty(13, SlotId::new(11), &DataType::Int32)
            .expect("empty runtime filter");
        let compat_response =
            handle_transmit_runtime_filter_compat(CompatTransmitRuntimeFilterRequest {
                is_partial: Some(true),
                query_id: Some(unique_id(query_id.hi, query_id.lo)),
                filter_id: Some(13),
                build_be_number: Some(1),
                data: Some(encode_starrocks_in_filter(&filter).expect("encode runtime filter")),
                ..Default::default()
            });
        assert!(ok_status(compat_response.status.as_ref()));

        let error = submit_native_fragment(query_id)
            .expect_err("native fragment must reject compat-claimed query");
        assert!(error.contains("Compat"), "{error}");
        let manager = query_context_manager();
        assert!(
            manager
                .get_runtime_filter_params(query_id)
                .expect("compat params lookup")
                .is_none()
        );
        assert!(
            manager
                .get_runtime_filter_worker(query_id)
                .expect("compat worker lookup")
                .is_none()
        );
        assert!(
            manager
                .get_runtime_filter_hub(query_id)
                .expect("compat hub lookup")
                .is_none()
        );
    }

    #[test]
    fn test_handle_transmit_runtime_filter_final_delivery_updates_probe() {
        let query_id = QueryId { hi: 300, lo: 400 };
        let query_key = QueryKey::from_hi_lo(query_id.hi, query_id.lo);
        let registry = RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        query_context_manager()
            .ensure_compat_context(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("ensure query context");
        let hub = Arc::new(
            crate::runtime::runtime_filter_hub::RuntimeFilterHub::new_for_query(
                crate::exec::pipeline::dependency::DependencyManager::new(),
                query_id,
            ),
        );
        hub.register_probe_specs(
            88,
            &[RuntimeFilterProbeSpec {
                filter_id: 0,
                expr_id: ExprId(0),
                slot_id: SlotId::new(11),
                data_type: arrow::datatypes::DataType::Int32,
            }],
        );
        let probe = hub.register_probe(88);
        query_context_manager()
            .with_context_mut(query_id, |ctx| {
                ctx.set_runtime_filter_hub(Arc::clone(&hub))?;
                Ok(())
            })
            .expect("install runtime filter hub");

        let filter =
            RuntimeInFilter::empty(0, SlotId::new(11), &DataType::Int32).expect("empty in filter");
        let payload = encode_starrocks_in_filter(&filter).expect("encode runtime filter");
        let response = handle_transmit_runtime_filter_compat(CompatTransmitRuntimeFilterRequest {
            is_partial: Some(false),
            query_id: Some(unique_id(query_id.hi, query_id.lo)),
            filter_id: Some(0),
            data: Some(payload),
            build_be_number: Some(0),
            ..Default::default()
        });

        assert!(ok_status(response.status.as_ref()));
        let snapshot = probe.snapshot();
        assert_eq!(snapshot.in_filters().len(), 1);
        assert_eq!(snapshot.in_filters()[0].filter_id(), 0);
        let lifecycle = registry
            .snapshot(query_key)
            .expect("query lifecycle snapshot");
        let filter = lifecycle.filters.get(&0).expect("filter lifecycle");
        assert!(filter.delivered);
        registry.remove_query(query_key);
    }

    #[test]
    fn prober_install_path_rejects_partial_filter() {
        let query_id = QueryId { hi: 301, lo: 401 };
        query_context_manager()
            .ensure_compat_context(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("ensure query context");
        let hub = Arc::new(crate::runtime::runtime_filter_hub::RuntimeFilterHub::new(
            crate::exec::pipeline::dependency::DependencyManager::new(),
        ));
        hub.register_probe_specs(
            88,
            &[RuntimeFilterProbeSpec {
                filter_id: 7,
                expr_id: ExprId(0),
                slot_id: SlotId::new(11),
                data_type: arrow::datatypes::DataType::Int32,
            }],
        );
        let probe = hub.register_probe(88);
        query_context_manager()
            .with_context_mut(query_id, |ctx| {
                ctx.set_runtime_filter_hub(Arc::clone(&hub))?;
                Ok(())
            })
            .expect("install runtime filter hub");

        let filter =
            RuntimeInFilter::empty(7, SlotId::new(11), &DataType::Int32).expect("empty in filter");
        let payload = encode_starrocks_in_filter(&filter).expect("encode runtime filter");
        let err = receive_total_runtime_filter(query_id, 7, true, &payload)
            .expect_err("partial must not be installed on the prober path");

        assert!(
            err.contains("partial filter reached prober install path"),
            "unexpected error: {err}"
        );
        assert!(
            probe.snapshot().is_empty(),
            "partial reaching the prober path must not update the probe handle"
        );
    }

    #[test]
    fn test_handle_lookup_returns_encoded_columns() {
        let query_id = QueryId { hi: 500, lo: 600 };
        let tuple_id = 1;
        query_context_manager()
            .ensure_compat_context(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("ensure query context");
        query_context_manager()
            .set_cache_options(
                query_id,
                CacheOptions::from_query_options(None).expect("default cache options"),
            )
            .expect("set cache options");
        query_context_manager()
            .with_context_mut(query_id, |ctx| {
                let desc_tbl = lookup_desc_tbl(tuple_id);
                let snapshot =
                    descriptor_snapshot_from_thrift(&desc_tbl).expect("descriptor snapshot");
                ctx.desc_tbl = Some(desc_tbl);
                ctx.desc_snapshot = Some(Arc::new(snapshot));
                Ok(())
            })
            .expect("set descriptor table");
        query_context_manager()
            .register_row_pos_descs(
                query_id,
                HashMap::from([(
                    tuple_id,
                    RowPositionDescriptor {
                        row_position_type: RowPositionType::Iceberg,
                        row_source_slot: SlotId::new(1),
                        fetch_ref_slots: vec![SlotId::new(2), SlotId::new(3)],
                        lookup_ref_slots: Vec::new(),
                    },
                )]),
            )
            .expect("register row position descriptors");

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("lookup.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef],
        )
        .expect("build parquet batch");
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
        writer.write(&batch).expect("write parquet batch");
        writer.close().expect("close parquet writer");

        query_context_manager()
            .register_glm_scan_ranges(
                query_id,
                SlotId::new(1),
                RowPositionScanConfig {
                    file_format: HdfsScanFileFormat::Parquet,
                    case_sensitive: true,
                    batch_size: Some(1024),
                    enable_file_metacache: false,
                    enable_file_pagecache: false,
                    oss_config: None,
                },
                vec![FileScanRange {
                    path: path.to_string_lossy().to_string(),
                    file_len: std::fs::metadata(&path).expect("metadata").len(),
                    offset: 0,
                    length: std::fs::metadata(&path).expect("metadata").len(),
                    scan_range_id: 9,
                    first_row_id: Some(0),
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                }],
            )
            .expect("register glm scan ranges");

        let scan_range = encode_column_ipc(&(Arc::new(Int32Array::from(vec![9])) as ArrayRef))
            .expect("encode scan_range_id column");
        let row_id = encode_column_ipc(&(Arc::new(Int64Array::from(vec![1])) as ArrayRef))
            .expect("encode row_id column");
        let response = handle_lookup(proto::filter::LookupRequest {
            query_id: Some(common_unique_id(query_id.hi, query_id.lo)),
            lookup_node_id: 77,
            request_tuple_id: tuple_id,
            request_columns: vec![
                proto::filter::Column {
                    slot_id: 2,
                    data_size: scan_range.len() as i64,
                    data: scan_range,
                },
                proto::filter::Column {
                    slot_id: 3,
                    data_size: row_id.len() as i64,
                    data: row_id,
                },
            ],
        });

        assert!(ok_common_status(response.status.as_ref()));
        assert_eq!(response.columns.len(), 1);
        assert_eq!(response.columns[0].slot_id, 4);
        let data = &response.columns[0].data;
        let values = decode_column_ipc(data).expect("decode lookup response column");
        let values = values
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 lookup response");
        assert_eq!(values.values(), &[20]);
    }

    #[cfg(feature = "compat")]
    #[test]
    fn test_handle_lookup_compat_returns_encoded_columns() {
        let query_id = QueryId { hi: 510, lo: 610 };
        let tuple_id = 1;
        query_context_manager()
            .ensure_compat_context(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("ensure query context");
        query_context_manager()
            .set_cache_options(
                query_id,
                CacheOptions::from_query_options(None).expect("default cache options"),
            )
            .expect("set cache options");
        query_context_manager()
            .with_context_mut(query_id, |ctx| {
                let desc_tbl = lookup_desc_tbl(tuple_id);
                let snapshot =
                    descriptor_snapshot_from_thrift(&desc_tbl).expect("descriptor snapshot");
                ctx.desc_tbl = Some(desc_tbl);
                ctx.desc_snapshot = Some(Arc::new(snapshot));
                Ok(())
            })
            .expect("set descriptor table");
        query_context_manager()
            .register_row_pos_descs(
                query_id,
                HashMap::from([(
                    tuple_id,
                    RowPositionDescriptor {
                        row_position_type: RowPositionType::Iceberg,
                        row_source_slot: SlotId::new(1),
                        fetch_ref_slots: vec![SlotId::new(2), SlotId::new(3)],
                        lookup_ref_slots: Vec::new(),
                    },
                )]),
            )
            .expect("register row position descriptors");

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("lookup-compat.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef],
        )
        .expect("build parquet batch");
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
        writer.write(&batch).expect("write parquet batch");
        writer.close().expect("close parquet writer");

        query_context_manager()
            .register_glm_scan_ranges(
                query_id,
                SlotId::new(1),
                RowPositionScanConfig {
                    file_format: HdfsScanFileFormat::Parquet,
                    case_sensitive: true,
                    batch_size: Some(1024),
                    enable_file_metacache: false,
                    enable_file_pagecache: false,
                    oss_config: None,
                },
                vec![FileScanRange {
                    path: path.to_string_lossy().to_string(),
                    file_len: std::fs::metadata(&path).expect("metadata").len(),
                    offset: 0,
                    length: std::fs::metadata(&path).expect("metadata").len(),
                    scan_range_id: 9,
                    first_row_id: Some(0),
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    external_datacache: None,
                    delete_files: Vec::new(),
                    iceberg_file_pruning: None,
                }],
            )
            .expect("register glm scan ranges");

        let scan_range = encode_column_ipc(&(Arc::new(Int32Array::from(vec![9])) as ArrayRef))
            .expect("encode scan_range_id column");
        let row_id = encode_column_ipc(&(Arc::new(Int64Array::from(vec![1])) as ArrayRef))
            .expect("encode row_id column");
        let response = handle_lookup_compat(CompatLookupRequest {
            query_id: Some(unique_id(query_id.hi, query_id.lo)),
            lookup_node_id: Some(77),
            request_tuple_id: Some(tuple_id),
            request_columns: vec![
                CompatColumn {
                    slot_id: Some(2),
                    data_size: Some(scan_range.len() as i64),
                    data: Some(scan_range),
                },
                CompatColumn {
                    slot_id: Some(3),
                    data_size: Some(row_id.len() as i64),
                    data: Some(row_id),
                },
            ],
            lookup_slots: Vec::new(),
        });

        assert!(ok_status(response.status.as_ref()));
        assert_eq!(response.columns.len(), 1);
        assert_eq!(response.columns[0].slot_id, Some(4));
        let data = response.columns[0]
            .data
            .as_ref()
            .expect("lookup response column data");
        let values = decode_column_ipc(data).expect("decode compat lookup response column");
        let values = values
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 compat lookup response");
        assert_eq!(values.values(), &[20]);
    }

    #[cfg(feature = "compat")]
    #[test]
    fn test_handle_lookup_compat_rejects_missing_request_tuple_id() {
        let response = handle_lookup_compat(CompatLookupRequest {
            query_id: Some(unique_id(1, 2)),
            lookup_node_id: Some(77),
            request_tuple_id: None,
            request_columns: Vec::new(),
            lookup_slots: Vec::new(),
        });

        assert!(!ok_status(response.status.as_ref()));
        assert_eq!(
            error_status_message(response.status.as_ref()),
            "missing request_tuple_id for lookup"
        );
    }

    #[cfg(feature = "compat")]
    #[test]
    fn test_handle_lookup_compat_rejects_missing_slot_id() {
        let response = handle_lookup_compat(CompatLookupRequest {
            query_id: Some(unique_id(1, 2)),
            lookup_node_id: Some(77),
            request_tuple_id: Some(1),
            request_columns: vec![CompatColumn {
                slot_id: None,
                data_size: Some(1),
                data: Some(vec![1]),
            }],
            lookup_slots: Vec::new(),
        });

        assert!(!ok_status(response.status.as_ref()));
        assert_eq!(
            error_status_message(response.status.as_ref()),
            "lookup request column missing slot_id"
        );
    }

    #[cfg(feature = "compat")]
    #[test]
    fn test_handle_lookup_compat_rejects_empty_data() {
        let response = handle_lookup_compat(CompatLookupRequest {
            query_id: Some(unique_id(1, 2)),
            lookup_node_id: Some(77),
            request_tuple_id: Some(1),
            request_columns: vec![CompatColumn {
                slot_id: Some(2),
                data_size: Some(0),
                data: Some(Vec::new()),
            }],
            lookup_slots: Vec::new(),
        });

        assert!(!ok_status(response.status.as_ref()));
        assert_eq!(
            error_status_message(response.status.as_ref()),
            "lookup request column 2 missing data"
        );
    }
}
