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

use arrow::datatypes::{DataType, TimeUnit};

use crate::common::types::UniqueId;
use crate::exec::runtime_filter::starrocks_primitive as primitive;
use crate::proto::starrocks::{
    PScalarType, PTransmitRuntimeFilterParams, PTransmitRuntimeFilterResult, PTypeDesc, PTypeNode,
    PUniqueId, StatusPb,
};
use crate::runtime::runtime_filter_transmission::RuntimeFilterTransmission;

const TYPE_NODE_SCALAR: i32 = 0;

pub(crate) type StarRocksRuntimeFilterRequest = PTransmitRuntimeFilterParams;
pub(crate) type StarRocksRuntimeFilterResponse = PTransmitRuntimeFilterResult;

pub(crate) fn decode_runtime_filter_transmission(
    request: StarRocksRuntimeFilterRequest,
) -> Result<RuntimeFilterTransmission, String> {
    let query_id = request
        .query_id
        .ok_or_else(|| "missing query_id for transmit_runtime_filter".to_string())?;
    let filter_id = request
        .filter_id
        .ok_or_else(|| "missing filter_id for transmit_runtime_filter".to_string())?;
    RuntimeFilterTransmission::try_new(
        request.is_partial.unwrap_or(false),
        UniqueId {
            hi: query_id.hi,
            lo: query_id.lo,
        },
        filter_id,
        request.data.unwrap_or_default(),
        request.build_be_number.unwrap_or_default(),
        request
            .column_type
            .as_ref()
            .and_then(arrow_type_from_starrocks_type_desc),
    )
}

pub(crate) fn encode_runtime_filter_transmission(
    transmission: RuntimeFilterTransmission,
) -> StarRocksRuntimeFilterRequest {
    StarRocksRuntimeFilterRequest {
        is_partial: Some(transmission.is_partial),
        query_id: Some(PUniqueId {
            hi: transmission.query_id.hi,
            lo: transmission.query_id.lo,
        }),
        filter_id: Some(transmission.filter_id),
        data: Some(transmission.data),
        build_be_number: transmission
            .is_partial
            .then_some(transmission.build_be_number),
        column_type: transmission
            .column_type
            .as_ref()
            .and_then(arrow_type_to_starrocks_type_desc),
        ..Default::default()
    }
}

pub(crate) fn encode_runtime_filter_result(
    filter_id: Option<i32>,
    result: Result<(), String>,
) -> StarRocksRuntimeFilterResponse {
    let status = match result {
        Ok(()) => StatusPb {
            status_code: 0,
            error_msgs: Vec::new(),
        },
        Err(message) => StatusPb {
            status_code: 1,
            error_msgs: vec![message],
        },
    };
    StarRocksRuntimeFilterResponse {
        status: Some(status),
        filter_id,
    }
}

fn arrow_type_to_starrocks_type_desc(data_type: &DataType) -> Option<PTypeDesc> {
    let (primitive_type, precision, scale) = match data_type {
        DataType::Boolean => (primitive::BOOLEAN, None, None),
        DataType::Int8 => (primitive::TINYINT, None, None),
        DataType::Int16 => (primitive::SMALLINT, None, None),
        DataType::Int32 => (primitive::INT, None, None),
        DataType::Int64 => (primitive::BIGINT, None, None),
        DataType::Float32 => (primitive::FLOAT, None, None),
        DataType::Float64 => (primitive::DOUBLE, None, None),
        DataType::Date32 => (primitive::DATE, None, None),
        DataType::Timestamp(_, _) => (primitive::DATETIME, None, None),
        DataType::Utf8 => (primitive::VARCHAR, None, None),
        DataType::Decimal128(precision, scale) if is_valid_decimal128(*precision, *scale) => (
            primitive::DECIMAL128,
            Some(i32::from(*precision)),
            Some(i32::from(*scale)),
        ),
        _ => return None,
    };
    Some(PTypeDesc {
        types: vec![PTypeNode {
            r#type: TYPE_NODE_SCALAR,
            scalar_type: Some(PScalarType {
                r#type: primitive_type,
                len: None,
                precision,
                scale,
            }),
            struct_fields: Vec::new(),
        }],
    })
}

fn arrow_type_from_starrocks_type_desc(desc: &PTypeDesc) -> Option<DataType> {
    if desc.types.len() != 1 {
        return None;
    }
    let node = desc.types.first()?;
    if node.r#type != TYPE_NODE_SCALAR {
        return None;
    }
    let scalar = node.scalar_type.as_ref()?;
    match scalar.r#type {
        primitive::BOOLEAN => Some(DataType::Boolean),
        primitive::TINYINT => Some(DataType::Int8),
        primitive::SMALLINT => Some(DataType::Int16),
        primitive::INT => Some(DataType::Int32),
        primitive::BIGINT => Some(DataType::Int64),
        primitive::FLOAT => Some(DataType::Float32),
        primitive::DOUBLE => Some(DataType::Float64),
        primitive::DATE => Some(DataType::Date32),
        primitive::DATETIME => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        primitive::VARCHAR | primitive::CHAR => Some(DataType::Utf8),
        primitive::DECIMAL128 => {
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

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::{decode_runtime_filter_transmission, encode_runtime_filter_transmission};
    use crate::common::types::UniqueId;
    use crate::proto::starrocks::{
        PScalarType, PTransmitRuntimeFilterParams, PTypeDesc, PTypeNode,
    };
    use crate::runtime::runtime_filter_transmission::RuntimeFilterTransmission;

    #[test]
    fn starrocks_runtime_filter_transmission_round_trips_domain_values() {
        let transmission = RuntimeFilterTransmission {
            is_partial: true,
            query_id: UniqueId { hi: 71, lo: 73 },
            filter_id: 79,
            data: vec![4, 5, 6],
            build_be_number: 3,
            column_type: Some(DataType::Decimal128(38, 9)),
        };

        let wire = encode_runtime_filter_transmission(transmission.clone());
        let decoded =
            decode_runtime_filter_transmission(wire).expect("decode StarRocks transmission");

        assert_eq!(decoded, transmission);
    }

    #[test]
    fn starrocks_runtime_filter_types_preserve_supported_subset() {
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
            DataType::Decimal128(18, 2),
        ] {
            let transmission = RuntimeFilterTransmission {
                is_partial: true,
                query_id: UniqueId { hi: 109, lo: 113 },
                filter_id: 127,
                data: vec![2],
                build_be_number: 5,
                column_type: Some(column_type.clone()),
            };
            let decoded = decode_runtime_filter_transmission(encode_runtime_filter_transmission(
                transmission,
            ))
            .expect("decode supported StarRocks type");
            assert_eq!(decoded.column_type, Some(column_type));
        }

        let unsupported = RuntimeFilterTransmission {
            is_partial: false,
            query_id: UniqueId { hi: 131, lo: 137 },
            filter_id: 139,
            data: vec![3],
            build_be_number: 0,
            column_type: Some(DataType::Binary),
        };
        let decoded =
            decode_runtime_filter_transmission(encode_runtime_filter_transmission(unsupported))
                .expect("decode unsupported StarRocks type as absent");
        assert_eq!(decoded.column_type, None);
    }

    #[test]
    fn starrocks_runtime_filter_wire_normalizes_char_and_rejects_invalid_decimal() {
        let mut params = PTransmitRuntimeFilterParams {
            is_partial: Some(false),
            query_id: Some(crate::proto::starrocks::PUniqueId { hi: 83, lo: 89 }),
            filter_id: Some(97),
            data: Some(vec![1]),
            build_be_number: None,
            column_type: Some(PTypeDesc {
                types: vec![PTypeNode {
                    r#type: super::TYPE_NODE_SCALAR,
                    scalar_type: Some(PScalarType {
                        r#type: crate::exec::runtime_filter::starrocks_primitive::CHAR,
                        len: Some(12),
                        precision: None,
                        scale: None,
                    }),
                    struct_fields: Vec::new(),
                }],
            }),
            ..Default::default()
        };

        let decoded = decode_runtime_filter_transmission(params.clone()).expect("decode CHAR");
        assert_eq!(decoded.column_type, Some(DataType::Utf8));

        params.column_type = Some(PTypeDesc {
            types: vec![PTypeNode {
                r#type: super::TYPE_NODE_SCALAR,
                scalar_type: Some(PScalarType {
                    r#type: crate::exec::runtime_filter::starrocks_primitive::DECIMAL128,
                    len: None,
                    precision: Some(39),
                    scale: Some(2),
                }),
                struct_fields: Vec::new(),
            }],
        });
        let decoded = decode_runtime_filter_transmission(params).expect("decode request");
        assert_eq!(decoded.column_type, None);
    }

    #[test]
    fn starrocks_runtime_filter_transmission_rejects_missing_required_ids() {
        let error = decode_runtime_filter_transmission(PTransmitRuntimeFilterParams {
            is_partial: Some(false),
            query_id: None,
            filter_id: Some(101),
            data: Some(vec![1]),
            ..Default::default()
        })
        .expect_err("missing query id must fail at StarRocks wire boundary");
        assert!(error.contains("query_id"), "{error}");

        let error = decode_runtime_filter_transmission(PTransmitRuntimeFilterParams {
            is_partial: Some(false),
            query_id: Some(crate::proto::starrocks::PUniqueId { hi: 103, lo: 107 }),
            filter_id: None,
            data: Some(vec![1]),
            ..Default::default()
        })
        .expect_err("missing filter id must fail at StarRocks wire boundary");
        assert!(error.contains("filter_id"), "{error}");
    }

    #[test]
    fn starrocks_runtime_filter_transmission_rejects_zero_query_id() {
        let error = decode_runtime_filter_transmission(PTransmitRuntimeFilterParams {
            is_partial: Some(false),
            query_id: Some(crate::proto::starrocks::PUniqueId { hi: 0, lo: 0 }),
            filter_id: Some(149),
            data: Some(vec![1]),
            ..Default::default()
        })
        .expect_err("zero query id must fail at StarRocks wire boundary");
        assert!(error.contains("query_id"), "{error}");
    }
}
