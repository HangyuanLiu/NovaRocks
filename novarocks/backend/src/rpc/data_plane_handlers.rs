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
//! Synchronous data-plane handlers delegated by the Backend RPC service.

use std::collections::HashMap;

use crate::query_lifecycle::QueryLifecycleIngress;
use crate::runtime::lookup::{
    decode_column_ipc, encode_column_ipc, execute_position_lookup_request,
};
use crate::runtime::query_context::QueryId;
use novarocks_execution::runtime::fragment::io::{
    ExchangeReceiverFrame, ExchangeReceiverKey, ExchangeReceiverPort,
};
use novarocks_proto_models as proto;
use novarocks_types::{SlotId, UniqueId};
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

pub fn handle_transmit_chunk(
    receiver_port: &dyn ExchangeReceiverPort,
    query_lifecycle_ingress: Option<&dyn QueryLifecycleIngress>,
    params: proto::novarocks::ExchangeRequest,
) -> proto::novarocks::ExchangeResponse {
    let mut response = proto::novarocks::ExchangeResponse {
        ack_sequence: params.sequence,
        status: Some(ok_common_status()),
    };

    let destination_fragment_instance_id = UniqueId::new(params.finst_id_hi, params.finst_id_lo);
    let source_fragment_instance_id =
        UniqueId::new(params.source_finst_id_hi, params.source_finst_id_lo);
    if destination_fragment_instance_id == UniqueId::new(0, 0)
        || source_fragment_instance_id == UniqueId::new(0, 0)
    {
        response.status = Some(error_common_status(
            "exchange ingress requires non-zero source and destination fragment instance IDs",
        ));
        return response;
    }
    if params.node_id < 0 {
        response.status = Some(error_common_status(
            "exchange ingress requires a non-negative destination node ID",
        ));
        return response;
    }
    let Some(query_lifecycle_ingress) = query_lifecycle_ingress else {
        response.status = Some(error_common_status(
            "exchange ingress has no lifecycle route authorizer",
        ));
        return response;
    };
    if let Err(error) = query_lifecycle_ingress.authorize_exchange(
        destination_fragment_instance_id,
        params.node_id,
        source_fragment_instance_id,
        params.sender_ordinal,
        params.sender_count,
    ) {
        response.status = Some(error_common_status(format!(
            "exchange ingress route rejected: {error}"
        )));
        return response;
    }

    let key = ExchangeReceiverKey {
        fragment_instance_id: destination_fragment_instance_id,
        node_id: params.node_id,
    };
    let frame = ExchangeReceiverFrame {
        source_fragment_instance_id,
        sender_ordinal: params.sender_ordinal,
        sender_count: params.sender_count,
        sender_id: params.sender_id,
        backend_number: params.be_number,
        sequence: params.sequence,
        eos: params.eos,
        payload: params.payload,
    };
    if let Err(err) = receiver_port.push(key, frame) {
        response.status = Some(error_common_status(format!(
            "exchange ingress failed: {err}"
        )));
    }
    response
}

pub fn handle_lookup(req: proto::filter::LookupRequest) -> proto::filter::LookupResponse {
    let mut response = proto::filter::LookupResponse {
        status: Some(ok_common_status()),
        columns: Vec::new(),
    };

    let Some(query_id) = req.query_id.as_ref() else {
        response.status = Some(error_common_status("missing query_id for lookup"));
        return response;
    };
    let query_id = QueryId::new(query_id.hi, query_id.lo);
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

#[allow(
    dead_code,
    reason = "Retained for backend service integration and protocol compatibility."
)]
pub fn handle_lookup_close(query_id: QueryId, lookup_node_id: i32) -> Result<(), String> {
    crate::runtime::query_context::query_context_manager()
        .complete_lookup_fetcher(query_id, lookup_node_id)
}

#[cfg(test)]
mod tests {
    use super::handle_transmit_chunk;
    use crate::query_lifecycle::{
        QueryControlAttachment, QueryLifecycleError, QueryLifecycleIngress,
    };
    use novarocks_execution::runtime::fragment::io::UnavailableExchangeReceiverPort;
    use novarocks_proto_codec::lifecycle::{
        QueryAbortRequest, QueryControlAttach, QueryInitAck, QueryInitRequest, QueryTerminationAck,
    };
    use novarocks_proto_models as proto;
    use novarocks_types::{BackendProcessId, UniqueId};

    struct RejectingLifecycleIngress;

    impl QueryLifecycleIngress for RejectingLifecycleIngress {
        fn backend_process_id(&self) -> BackendProcessId {
            BackendProcessId::new_v7()
        }

        fn init_query(&self, _request: QueryInitRequest) -> QueryInitAck {
            unreachable!("exchange ingress test does not initialize a query")
        }

        fn authorize_exchange(
            &self,
            _destination_fragment_instance_id: UniqueId,
            _destination_node_id: i32,
            _source_fragment_instance_id: UniqueId,
            _sender_ordinal: u32,
            _sender_count: u32,
        ) -> Result<(), String> {
            Err("route is not present in the admitted manifest".to_string())
        }

        fn abort_query(
            &self,
            _request: QueryAbortRequest,
        ) -> Result<QueryTerminationAck, QueryLifecycleError> {
            unreachable!("exchange ingress test does not abort a query")
        }

        fn attach_control(
            &self,
            _attach: QueryControlAttach,
        ) -> Result<QueryControlAttachment, QueryLifecycleError> {
            unreachable!("exchange ingress test does not attach query control")
        }
    }

    #[test]
    fn exchange_route_rejection_happens_before_receiver_delivery() {
        let ingress = RejectingLifecycleIngress;
        let response = handle_transmit_chunk(
            &UnavailableExchangeReceiverPort,
            Some(&ingress),
            proto::novarocks::ExchangeRequest {
                finst_id_hi: 1,
                finst_id_lo: 2,
                node_id: 7,
                source_finst_id_hi: 3,
                source_finst_id_lo: 4,
                sender_ordinal: 0,
                sender_count: 1,
                sender_id: 11,
                be_number: 0,
                eos: false,
                sequence: 42,
                payload: vec![0xff],
            },
        );

        assert_eq!(response.ack_sequence, 42);
        let status = response.status.expect("status");
        assert_eq!(status.code, 1);
        assert!(status.message.contains("route rejected"));
    }

    #[test]
    fn pre_lnp9_exchange_shape_is_rejected_before_route_authorization() {
        let ingress = RejectingLifecycleIngress;
        let response = handle_transmit_chunk(
            &UnavailableExchangeReceiverPort,
            Some(&ingress),
            proto::novarocks::ExchangeRequest {
                finst_id_hi: 1,
                finst_id_lo: 2,
                node_id: 7,
                sender_id: 11,
                be_number: 0,
                eos: false,
                sequence: 42,
                payload: vec![0xff],
                ..Default::default()
            },
        );

        let status = response.status.expect("status");
        assert_eq!(status.code, 1);
        assert!(status.message.contains("requires non-zero source"));
    }
}
