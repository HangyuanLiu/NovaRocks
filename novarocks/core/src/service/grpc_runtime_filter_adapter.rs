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

use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};

use crate::common::types::UniqueId;
use crate::proto;
use crate::proto::common::{PrimitiveType, ScalarType, TypeDesc, type_desc::Kind};
use crate::runtime::runtime_filter_transmission::RuntimeFilterTransmission;
use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, PartitionId, ProducerSequence, RouteEdgeId,
};
use crate::runtime_filter::port::transport::{
    ContributionRouteIdentity, DeliveryRouteIdentity, ProducerOpenMetadata,
    RuntimeFilterAcceptStatus, RuntimeFilterEnvelope, RuntimeFilterEnvelopeIngress,
    RuntimeFilterEnvelopeKind, RuntimeFilterRouteIdentity, RuntimeFilterTransportError,
};

pub(crate) type NativeRuntimeFilterRequest = proto::filter::TransmitRuntimeFilterRequest;
pub(crate) type NativeRuntimeFilterResponse = proto::filter::TransmitRuntimeFilterResponse;

pub(crate) fn decode_runtime_filter_transmission(
    request: proto::filter::TransmitRuntimeFilterRequest,
) -> Result<RuntimeFilterTransmission, String> {
    let query_id = request
        .query_id
        .ok_or_else(|| "missing query_id for transmit_runtime_filter".to_string())?;
    RuntimeFilterTransmission::try_new(
        request.is_partial,
        UniqueId {
            hi: query_id.hi,
            lo: query_id.lo,
        },
        request.filter_id,
        request.data,
        request.build_be_number,
        request
            .column_type
            .as_ref()
            .and_then(arrow_type_from_common_type_desc),
    )
}

pub(crate) fn encode_runtime_filter_transmission(
    transmission: RuntimeFilterTransmission,
) -> proto::filter::TransmitRuntimeFilterRequest {
    proto::filter::TransmitRuntimeFilterRequest {
        is_partial: transmission.is_partial,
        query_id: Some(proto::common::UniqueId {
            hi: transmission.query_id.hi,
            lo: transmission.query_id.lo,
        }),
        filter_id: transmission.filter_id,
        data: transmission.data,
        build_be_number: transmission.build_be_number,
        column_type: transmission
            .column_type
            .as_ref()
            .and_then(arrow_type_to_common_type_desc),
    }
}

pub(crate) fn encode_runtime_filter_result(
    filter_id: i32,
    result: Result<(), String>,
) -> proto::filter::TransmitRuntimeFilterResponse {
    let status = match result {
        Ok(()) => proto::common::Status {
            code: 0,
            message: String::new(),
        },
        Err(message) => proto::common::Status { code: 1, message },
    };
    proto::filter::TransmitRuntimeFilterResponse {
        status: Some(status),
        filter_id,
    }
}

fn arrow_type_to_common_type_desc(data_type: &DataType) -> Option<TypeDesc> {
    let (primitive, precision, scale) = match data_type {
        DataType::Boolean => (PrimitiveType::Boolean, None, None),
        DataType::Int8 => (PrimitiveType::Tinyint, None, None),
        DataType::Int16 => (PrimitiveType::Smallint, None, None),
        DataType::Int32 => (PrimitiveType::Int, None, None),
        DataType::Int64 => (PrimitiveType::Bigint, None, None),
        DataType::Float32 => (PrimitiveType::Float, None, None),
        DataType::Float64 => (PrimitiveType::Double, None, None),
        DataType::Date32 => (PrimitiveType::Date, None, None),
        DataType::Timestamp(_, _) => (PrimitiveType::Datetime, None, None),
        DataType::Utf8 => (PrimitiveType::Varchar, None, None),
        DataType::Decimal128(precision, scale) if is_valid_decimal128(*precision, *scale) => (
            PrimitiveType::Decimal128,
            Some(i32::from(*precision)),
            Some(i32::from(*scale)),
        ),
        _ => return None,
    };
    Some(TypeDesc {
        kind: Some(Kind::Scalar(ScalarType {
            r#type: primitive as i32,
            len: None,
            precision,
            scale,
            time_unit: None,
        })),
    })
}

fn arrow_type_from_common_type_desc(desc: &TypeDesc) -> Option<DataType> {
    let Kind::Scalar(scalar) = desc.kind.as_ref()? else {
        return None;
    };
    match PrimitiveType::try_from(scalar.r#type).ok()? {
        PrimitiveType::Boolean => Some(DataType::Boolean),
        PrimitiveType::Tinyint => Some(DataType::Int8),
        PrimitiveType::Smallint => Some(DataType::Int16),
        PrimitiveType::Int => Some(DataType::Int32),
        PrimitiveType::Bigint => Some(DataType::Int64),
        PrimitiveType::Float => Some(DataType::Float32),
        PrimitiveType::Double => Some(DataType::Float64),
        PrimitiveType::Date => Some(DataType::Date32),
        PrimitiveType::Datetime => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        PrimitiveType::Varchar | PrimitiveType::Char => Some(DataType::Utf8),
        PrimitiveType::Decimal128 => {
            let precision = scalar.precision?;
            let scale = scalar.scale?;
            if !(1..=38).contains(&precision) || scale < 0 || scale > precision {
                return None;
            }
            Some(DataType::Decimal128(
                u8::try_from(precision).ok()?,
                i8::try_from(scale).ok()?,
            ))
        }
        _ => None,
    }
}

fn is_valid_decimal128(precision: u8, scale: i8) -> bool {
    (1..=38).contains(&precision) && scale >= 0 && i32::from(scale) <= i32::from(precision)
}

pub(crate) fn handle_runtime_filter_envelope(
    ingress: Arc<dyn RuntimeFilterEnvelopeIngress>,
    request: proto::filter::RuntimeFilterEnvelope,
) -> Result<proto::filter::RuntimeFilterEnvelopeResponse, tonic::Status> {
    let proto::filter::RuntimeFilterEnvelope {
        kind,
        query_id,
        channel_id,
        deployment_epoch,
        route_identity,
        schema_digest,
        payload,
        producer_open,
    } = request;

    let kind = decode_kind(kind)?;
    let query_id =
        query_id.ok_or_else(|| invalid_argument("runtime filter query id is missing"))?;
    let query_id = UniqueId {
        hi: query_id.hi,
        lo: query_id.lo,
    };
    let route_identity = route_identity
        .ok_or_else(|| invalid_argument("runtime filter route identity is missing"))?;
    let domain_route_identity = decode_route_identity(&route_identity)?;
    let producer_open = ProducerOpenMetadata::try_from_raw_for_kind(
        kind,
        producer_open.map(|metadata| metadata.local_partition_count),
    )
    .map_err(transport_error)?;
    let envelope = RuntimeFilterEnvelope::try_new(
        kind,
        query_id,
        ChannelId::new(channel_id),
        DeploymentEpoch::new(deployment_epoch),
        domain_route_identity,
        producer_open,
        &schema_digest,
        payload,
    )
    .map_err(transport_error)?;

    let acked_route_identity = Some(route_identity.clone());
    let result = ingress.accept(envelope);
    let (accept_status, rejection_reason) = match result.accept_status() {
        RuntimeFilterAcceptStatus::Accepted => (
            proto::filter::RuntimeFilterAcceptStatus::Accepted,
            String::new(),
        ),
        RuntimeFilterAcceptStatus::Duplicate => (
            proto::filter::RuntimeFilterAcceptStatus::Duplicate,
            String::new(),
        ),
        RuntimeFilterAcceptStatus::Rejected => (
            proto::filter::RuntimeFilterAcceptStatus::Rejected,
            result
                .rejection_reason()
                .expect("rejected ingress result has a non-empty reason")
                .to_string(),
        ),
    };

    Ok(proto::filter::RuntimeFilterEnvelopeResponse {
        acked_route_identity,
        accept_status: accept_status as i32,
        rejection_reason,
    })
}

fn decode_kind(kind: i32) -> Result<RuntimeFilterEnvelopeKind, tonic::Status> {
    let kind = proto::filter::RuntimeFilterEnvelopeKind::try_from(kind)
        .map_err(|_| invalid_argument("runtime filter envelope kind is unknown"))?;
    match kind {
        proto::filter::RuntimeFilterEnvelopeKind::Unspecified => Err(invalid_argument(
            "runtime filter envelope kind must be specified",
        )),
        proto::filter::RuntimeFilterEnvelopeKind::Contribution => {
            Ok(RuntimeFilterEnvelopeKind::Contribution)
        }
        proto::filter::RuntimeFilterEnvelopeKind::Artifact => {
            Ok(RuntimeFilterEnvelopeKind::Artifact)
        }
        proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed => {
            Ok(RuntimeFilterEnvelopeKind::ProducerClosed)
        }
        proto::filter::RuntimeFilterEnvelopeKind::Unavailable => {
            Ok(RuntimeFilterEnvelopeKind::Unavailable)
        }
        proto::filter::RuntimeFilterEnvelopeKind::Ack => Ok(RuntimeFilterEnvelopeKind::Ack),
    }
}

fn decode_route_identity(
    route_identity: &proto::filter::RuntimeFilterRouteIdentity,
) -> Result<RuntimeFilterRouteIdentity, tonic::Status> {
    use proto::filter::runtime_filter_route_identity::Value;

    match route_identity.value.as_ref() {
        Some(Value::Contribution(identity)) => {
            let fragment_instance_id = identity.fragment_instance_id.ok_or_else(|| {
                invalid_argument("runtime filter fragment instance id is missing")
            })?;
            let identity = ContributionRouteIdentity::try_new(
                BindingId::new(identity.producer_binding_id),
                UniqueId {
                    hi: fragment_instance_id.hi,
                    lo: fragment_instance_id.lo,
                },
                PartitionId::new(identity.partition_id),
                ProducerSequence::new(identity.sequence),
            )
            .map_err(transport_error)?;
            Ok(RuntimeFilterRouteIdentity::contribution(identity))
        }
        Some(Value::Delivery(identity)) => {
            let identity = DeliveryRouteIdentity::try_new(
                RouteEdgeId::new(identity.route_edge_id),
                ProducerSequence::new(identity.sequence),
            )
            .map_err(transport_error)?;
            Ok(RuntimeFilterRouteIdentity::delivery(identity))
        }
        None => Err(invalid_argument(
            "runtime filter route identity value is missing",
        )),
    }
}

fn transport_error(error: RuntimeFilterTransportError) -> tonic::Status {
    invalid_argument(error.to_string())
}

fn invalid_argument(message: impl Into<String>) -> tonic::Status {
    tonic::Status::invalid_argument(message.into())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tonic::Code;

    use arrow::datatypes::DataType;

    use super::{decode_runtime_filter_transmission, encode_runtime_filter_transmission};
    use crate::runtime::runtime_filter_transmission::RuntimeFilterTransmission;

    use crate::common::types::UniqueId;
    use crate::proto;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, PartitionId, ProducerSequence, RouteEdgeId,
    };
    use crate::runtime_filter::port::transport::{
        RuntimeFilterEnvelope, RuntimeFilterEnvelopeIngress, RuntimeFilterEnvelopeKind,
        RuntimeFilterIngressResult,
    };

    #[test]
    fn legacy_native_runtime_filter_transmission_round_trips_domain_values() {
        let transmission = RuntimeFilterTransmission {
            is_partial: true,
            query_id: UniqueId { hi: 31, lo: 47 },
            filter_id: 59,
            data: vec![1, 2, 3],
            build_be_number: 7,
            column_type: Some(DataType::Decimal128(18, 2)),
        };

        let wire = encode_runtime_filter_transmission(transmission.clone());
        let decoded = decode_runtime_filter_transmission(wire).expect("decode native transmission");

        assert_eq!(decoded, transmission);
    }

    #[test]
    fn legacy_native_runtime_filter_transmission_rejects_missing_query_id() {
        let error = decode_runtime_filter_transmission(
            crate::proto::filter::TransmitRuntimeFilterRequest {
                is_partial: false,
                query_id: None,
                filter_id: 61,
                data: vec![9],
                build_be_number: 0,
                column_type: None,
            },
        )
        .expect_err("missing query id must fail at native wire boundary");

        assert!(error.contains("query_id"), "{error}");
    }

    #[test]
    fn legacy_native_runtime_filter_transmission_rejects_zero_query_id() {
        let error = decode_runtime_filter_transmission(
            crate::proto::filter::TransmitRuntimeFilterRequest {
                is_partial: false,
                query_id: Some(crate::proto::common::UniqueId { hi: 0, lo: 0 }),
                filter_id: 63,
                data: vec![9],
                build_be_number: 0,
                column_type: None,
            },
        )
        .expect_err("zero query id must fail at native wire boundary");

        assert!(error.contains("query_id"), "{error}");
    }

    #[test]
    fn legacy_native_runtime_filter_types_preserve_supported_subset() {
        use arrow::datatypes::TimeUnit;

        for column_type in [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Utf8,
            DataType::Decimal128(38, 9),
        ] {
            let transmission = RuntimeFilterTransmission {
                is_partial: true,
                query_id: UniqueId { hi: 67, lo: 71 },
                filter_id: 73,
                data: vec![8],
                build_be_number: 2,
                column_type: Some(column_type.clone()),
            };
            let decoded = decode_runtime_filter_transmission(encode_runtime_filter_transmission(
                transmission,
            ))
            .expect("decode supported native type");
            assert_eq!(decoded.column_type, Some(column_type));
        }

        let unsupported = RuntimeFilterTransmission {
            is_partial: true,
            query_id: UniqueId { hi: 79, lo: 83 },
            filter_id: 89,
            data: vec![9],
            build_be_number: 1,
            column_type: Some(DataType::Binary),
        };
        let decoded =
            decode_runtime_filter_transmission(encode_runtime_filter_transmission(unsupported))
                .expect("decode unsupported native type as absent");
        assert_eq!(decoded.column_type, None);
    }

    use super::handle_runtime_filter_envelope;

    #[derive(Debug)]
    struct RecordingIngress {
        envelopes: Mutex<Vec<RuntimeFilterEnvelope>>,
        result: RuntimeFilterIngressResult,
    }

    impl RecordingIngress {
        fn new(result: RuntimeFilterIngressResult) -> Self {
            Self {
                envelopes: Mutex::new(Vec::new()),
                result,
            }
        }

        fn take(&self) -> Vec<RuntimeFilterEnvelope> {
            std::mem::take(&mut *self.envelopes.lock().unwrap())
        }

        fn is_empty(&self) -> bool {
            self.envelopes.lock().unwrap().is_empty()
        }
    }

    impl RuntimeFilterEnvelopeIngress for RecordingIngress {
        fn accept(&self, envelope: RuntimeFilterEnvelope) -> RuntimeFilterIngressResult {
            self.envelopes.lock().unwrap().push(envelope);
            self.result.clone()
        }
    }

    fn contribution_route() -> proto::filter::RuntimeFilterRouteIdentity {
        proto::filter::RuntimeFilterRouteIdentity {
            value: Some(
                proto::filter::runtime_filter_route_identity::Value::Contribution(
                    proto::filter::RuntimeFilterContributionRouteIdentity {
                        producer_binding_id: 17,
                        fragment_instance_id: Some(proto::common::UniqueId { hi: 18, lo: 19 }),
                        partition_id: 20,
                        sequence: 21,
                    },
                ),
            ),
        }
    }

    fn delivery_route() -> proto::filter::RuntimeFilterRouteIdentity {
        proto::filter::RuntimeFilterRouteIdentity {
            value: Some(
                proto::filter::runtime_filter_route_identity::Value::Delivery(
                    proto::filter::RuntimeFilterDeliveryRouteIdentity {
                        route_edge_id: 22,
                        sequence: 23,
                    },
                ),
            ),
        }
    }

    fn valid_wire_envelope(
        kind: proto::filter::RuntimeFilterEnvelopeKind,
    ) -> proto::filter::RuntimeFilterEnvelope {
        let (route_identity, payload, producer_open) = match kind {
            proto::filter::RuntimeFilterEnvelopeKind::Contribution => (
                contribution_route(),
                b"contribution".to_vec(),
                Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                    local_partition_count: 24,
                }),
            ),
            proto::filter::RuntimeFilterEnvelopeKind::Artifact => {
                (delivery_route(), b"artifact".to_vec(), None)
            }
            proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed => (
                contribution_route(),
                Vec::new(),
                Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                    local_partition_count: 24,
                }),
            ),
            proto::filter::RuntimeFilterEnvelopeKind::Unavailable => {
                (delivery_route(), b"unavailable".to_vec(), None)
            }
            proto::filter::RuntimeFilterEnvelopeKind::Ack => {
                (contribution_route(), Vec::new(), None)
            }
            proto::filter::RuntimeFilterEnvelopeKind::Unspecified => {
                panic!("unspecified kind is not a valid fixture")
            }
        };
        proto::filter::RuntimeFilterEnvelope {
            kind: kind as i32,
            query_id: Some(proto::common::UniqueId { hi: 11, lo: 12 }),
            channel_id: 13,
            deployment_epoch: 14,
            route_identity: Some(route_identity),
            schema_digest: vec![15; 32],
            payload,
            producer_open,
        }
    }

    #[test]
    fn all_valid_kinds_reach_ingress_with_exact_domain_values() {
        let cases = [
            (
                proto::filter::RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::Contribution,
                b"contribution".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::Artifact,
                b"artifact".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                b"".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
                RuntimeFilterEnvelopeKind::Unavailable,
                b"unavailable".as_slice(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Ack,
                RuntimeFilterEnvelopeKind::Ack,
                b"".as_slice(),
            ),
        ];

        for (wire_kind, domain_kind, expected_payload) in cases {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            handle_runtime_filter_envelope(ingress.clone(), valid_wire_envelope(wire_kind))
                .unwrap();

            let envelopes = ingress.take();
            assert_eq!(envelopes.len(), 1);
            let envelope = &envelopes[0];
            assert_eq!(envelope.kind(), domain_kind);
            assert_eq!(envelope.query_id(), UniqueId { hi: 11, lo: 12 });
            assert_eq!(envelope.channel_id(), ChannelId::new(13));
            assert_eq!(envelope.deployment_epoch(), DeploymentEpoch::new(14));
            assert_eq!(envelope.schema_digest(), &[15; 32]);
            assert_eq!(envelope.payload(), expected_payload);

            match domain_kind {
                RuntimeFilterEnvelopeKind::Contribution
                | RuntimeFilterEnvelopeKind::ProducerClosed
                | RuntimeFilterEnvelopeKind::Ack => {
                    let identity = envelope
                        .route_identity()
                        .as_contribution()
                        .expect("contribution identity");
                    assert_eq!(identity.producer_binding_id(), BindingId::new(17));
                    assert_eq!(identity.fragment_instance_id(), UniqueId { hi: 18, lo: 19 });
                    assert_eq!(identity.partition_id(), PartitionId::new(20));
                    assert_eq!(identity.sequence(), ProducerSequence::new(21));
                }
                RuntimeFilterEnvelopeKind::Artifact | RuntimeFilterEnvelopeKind::Unavailable => {
                    let identity = envelope
                        .route_identity()
                        .as_delivery()
                        .expect("delivery identity");
                    assert_eq!(identity.route_edge_id(), RouteEdgeId::new(22));
                    assert_eq!(identity.sequence(), ProducerSequence::new(23));
                }
            }
        }
    }

    #[test]
    fn partial_unique_ids_reach_ingress_as_exact_domain_values() {
        let cases = [
            (UniqueId { hi: 0, lo: 29 }, UniqueId { hi: 18, lo: 19 }),
            (UniqueId { hi: 31, lo: 0 }, UniqueId { hi: 18, lo: 19 }),
            (UniqueId { hi: 11, lo: 12 }, UniqueId { hi: 0, lo: 37 }),
            (UniqueId { hi: 11, lo: 12 }, UniqueId { hi: 41, lo: 0 }),
        ];

        for (query_id, fragment_instance_id) in cases {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let mut request =
                valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
            request.query_id = Some(proto::common::UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            });
            let Some(proto::filter::runtime_filter_route_identity::Value::Contribution(identity)) =
                request.route_identity.as_mut().unwrap().value.as_mut()
            else {
                unreachable!()
            };
            identity.fragment_instance_id = Some(proto::common::UniqueId {
                hi: fragment_instance_id.hi,
                lo: fragment_instance_id.lo,
            });

            let response = handle_runtime_filter_envelope(ingress.clone(), request).unwrap();
            assert_eq!(
                response.accept_status,
                proto::filter::RuntimeFilterAcceptStatus::Accepted as i32
            );

            let envelopes = ingress.take();
            assert_eq!(envelopes.len(), 1);
            let envelope = &envelopes[0];
            assert_eq!(envelope.query_id(), query_id);
            let identity = envelope
                .route_identity()
                .as_contribution()
                .expect("contribution identity");
            assert_eq!(identity.fragment_instance_id(), fragment_instance_id);
        }
    }

    #[test]
    fn zero_based_contribution_coordinates_reach_ingress_unchanged() {
        for kind in [
            proto::filter::RuntimeFilterEnvelopeKind::Contribution,
            proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
        ] {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let mut request = valid_wire_envelope(kind);
            let Some(proto::filter::runtime_filter_route_identity::Value::Contribution(identity)) =
                request.route_identity.as_mut().unwrap().value.as_mut()
            else {
                unreachable!()
            };
            identity.partition_id = 0;
            identity.sequence = 0;

            handle_runtime_filter_envelope(ingress.clone(), request).unwrap();

            let envelopes = ingress.take();
            assert_eq!(envelopes.len(), 1);
            let identity = envelopes[0]
                .route_identity()
                .as_contribution()
                .expect("contribution identity");
            assert_eq!(identity.partition_id(), PartitionId::new(0));
            assert_eq!(identity.sequence(), ProducerSequence::new(0));
        }
    }

    #[test]
    fn adapter_rejects_missing_zero_and_forbidden_open_metadata_before_ingress() {
        let mut malformed = Vec::new();
        for kind in [
            proto::filter::RuntimeFilterEnvelopeKind::Contribution,
            proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
        ] {
            let mut missing = valid_wire_envelope(kind);
            missing.producer_open = None;
            malformed.push(missing);

            let mut zero = valid_wire_envelope(kind);
            zero.producer_open = Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                local_partition_count: 0,
            });
            malformed.push(zero);
        }
        for kind in [
            proto::filter::RuntimeFilterEnvelopeKind::Artifact,
            proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
            proto::filter::RuntimeFilterEnvelopeKind::Ack,
        ] {
            let mut forbidden = valid_wire_envelope(kind);
            forbidden.producer_open = Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                local_partition_count: 24,
            });
            malformed.push(forbidden);
        }

        for request in malformed {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let result = handle_runtime_filter_envelope(ingress.clone(), request);
            assert!(
                result.is_err(),
                "invalid producer-open metadata must be rejected"
            );
            assert_eq!(result.unwrap_err().code(), Code::InvalidArgument);
            assert!(ingress.is_empty());
        }
    }

    #[test]
    fn forbidden_producer_open_presence_precedes_zero_count_validation() {
        for kind in [
            proto::filter::RuntimeFilterEnvelopeKind::Artifact,
            proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
            proto::filter::RuntimeFilterEnvelopeKind::Ack,
        ] {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let mut request = valid_wire_envelope(kind);
            request.producer_open = Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                local_partition_count: 0,
            });

            let error = handle_runtime_filter_envelope(ingress.clone(), request).unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument);
            assert_eq!(
                error.message(),
                format!(
                    "runtime filter envelope kind {:?} forbids producer-open metadata",
                    match kind {
                        proto::filter::RuntimeFilterEnvelopeKind::Artifact =>
                            RuntimeFilterEnvelopeKind::Artifact,
                        proto::filter::RuntimeFilterEnvelopeKind::Unavailable =>
                            RuntimeFilterEnvelopeKind::Unavailable,
                        proto::filter::RuntimeFilterEnvelopeKind::Ack =>
                            RuntimeFilterEnvelopeKind::Ack,
                        _ => unreachable!(),
                    }
                )
            );
            assert!(ingress.is_empty());
        }
    }

    #[test]
    fn adapter_preserves_exact_count_for_contribution_and_closed() {
        for (kind, local_partition_count) in [
            (proto::filter::RuntimeFilterEnvelopeKind::Contribution, 37),
            (
                proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
                u32::MAX,
            ),
        ] {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let mut request = valid_wire_envelope(kind);
            request.producer_open = Some(proto::filter::RuntimeFilterProducerOpenMetadata {
                local_partition_count,
            });

            handle_runtime_filter_envelope(ingress.clone(), request).unwrap();
            let envelopes = ingress.take();
            assert_eq!(envelopes.len(), 1);
            assert_eq!(
                envelopes[0]
                    .producer_open()
                    .map(|metadata| metadata.local_partition_count().get()),
                Some(local_partition_count)
            );
        }
    }

    #[test]
    fn ingress_results_map_exactly_and_echo_validated_route() {
        let cases = [
            (
                RuntimeFilterIngressResult::accepted(),
                proto::filter::RuntimeFilterAcceptStatus::Accepted,
                "",
            ),
            (
                RuntimeFilterIngressResult::duplicate(),
                proto::filter::RuntimeFilterAcceptStatus::Duplicate,
                "",
            ),
            (
                RuntimeFilterIngressResult::rejected("not authorized").unwrap(),
                proto::filter::RuntimeFilterAcceptStatus::Rejected,
                "not authorized",
            ),
        ];

        for (result, expected_status, expected_reason) in cases {
            let ingress = Arc::new(RecordingIngress::new(result));
            let request =
                valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
            let expected_route = request.route_identity.clone();
            let response = handle_runtime_filter_envelope(ingress.clone(), request).unwrap();

            assert_eq!(response.accept_status, expected_status as i32);
            assert_eq!(response.rejection_reason, expected_reason);
            assert_eq!(response.acked_route_identity, expected_route);
            assert_eq!(ingress.take().len(), 1);
        }
    }

    #[test]
    fn malformed_wire_is_invalid_argument_and_never_reaches_ingress() {
        let mut malformed = Vec::new();

        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.kind = proto::filter::RuntimeFilterEnvelopeKind::Unspecified as i32;
        malformed.push(request);
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.kind = 99;
        malformed.push(request);

        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.query_id = None;
        malformed.push(request);
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.query_id = Some(proto::common::UniqueId { hi: 0, lo: 0 });
        malformed.push(request);
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.channel_id = 0;
        malformed.push(request);
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.deployment_epoch = 0;
        malformed.push(request);

        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.route_identity = None;
        malformed.push(request);
        let mut request =
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
        request.route_identity = Some(proto::filter::RuntimeFilterRouteIdentity { value: None });
        malformed.push(request);

        for mutate in [
            |identity: &mut proto::filter::RuntimeFilterContributionRouteIdentity| {
                identity.producer_binding_id = 0
            },
            |identity: &mut proto::filter::RuntimeFilterContributionRouteIdentity| {
                identity.fragment_instance_id = None
            },
            |identity: &mut proto::filter::RuntimeFilterContributionRouteIdentity| {
                identity.fragment_instance_id = Some(proto::common::UniqueId { hi: 0, lo: 0 })
            },
        ] {
            let mut request =
                valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
            let Some(proto::filter::runtime_filter_route_identity::Value::Contribution(identity)) =
                request.route_identity.as_mut().unwrap().value.as_mut()
            else {
                unreachable!()
            };
            mutate(identity);
            malformed.push(request);
        }

        for mutate in [
            |identity: &mut proto::filter::RuntimeFilterDeliveryRouteIdentity| {
                identity.route_edge_id = 0
            },
            |identity: &mut proto::filter::RuntimeFilterDeliveryRouteIdentity| {
                identity.sequence = 0
            },
        ] {
            let mut request =
                valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Artifact);
            let Some(proto::filter::runtime_filter_route_identity::Value::Delivery(identity)) =
                request.route_identity.as_mut().unwrap().value.as_mut()
            else {
                unreachable!()
            };
            mutate(identity);
            malformed.push(request);
        }

        for (kind, wrong_route) in [
            (
                proto::filter::RuntimeFilterEnvelopeKind::Contribution,
                delivery_route(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Artifact,
                contribution_route(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
                delivery_route(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
                contribution_route(),
            ),
        ] {
            let mut request = valid_wire_envelope(kind);
            request.route_identity = Some(wrong_route);
            malformed.push(request);
        }

        for digest_len in [0, 31, 33] {
            let mut request =
                valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Contribution);
            request.schema_digest = vec![15; digest_len];
            malformed.push(request);
        }

        for (kind, payload) in [
            (
                proto::filter::RuntimeFilterEnvelopeKind::Contribution,
                Vec::new(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Artifact,
                Vec::new(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::ProducerClosed,
                b"unexpected".to_vec(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Unavailable,
                Vec::new(),
            ),
            (
                proto::filter::RuntimeFilterEnvelopeKind::Ack,
                b"unexpected".to_vec(),
            ),
        ] {
            let mut request = valid_wire_envelope(kind);
            request.payload = payload;
            malformed.push(request);
        }

        assert_eq!(malformed.len(), 25);
        for request in malformed {
            let ingress = Arc::new(RecordingIngress::new(RuntimeFilterIngressResult::accepted()));
            let error = handle_runtime_filter_envelope(ingress.clone(), request).unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument, "{error}");
            assert!(ingress.is_empty());
        }
    }

    #[test]
    fn domain_rejection_is_not_a_tonic_error() {
        let ingress = Arc::new(RecordingIngress::new(
            RuntimeFilterIngressResult::rejected("semantic rejection").unwrap(),
        ));
        let response = handle_runtime_filter_envelope(
            ingress,
            valid_wire_envelope(proto::filter::RuntimeFilterEnvelopeKind::Ack),
        )
        .expect("domain rejection must produce a response");

        assert_eq!(
            response.accept_status,
            proto::filter::RuntimeFilterAcceptStatus::Rejected as i32
        );
        assert_eq!(response.rejection_reason, "semantic rejection");
    }
}
