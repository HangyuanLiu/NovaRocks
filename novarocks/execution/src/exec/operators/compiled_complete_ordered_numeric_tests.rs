// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Literal source and artifact authors for the checked Complete pipeline.
//! These are not Worker ordering oracles. Actual Arrow values are decoded only
//! for comparison with independent literal results, never to derive expected.
use super::*;
use arrow::array::ArrayRef;

const KEYS: [Option<i64>; 8] = [
    Some(3),
    Some(1),
    Some(3),
    None,
    Some(2),
    None,
    Some(4),
    Some(1),
];

pub(super) fn key_array(ty: &FunctionValueType) -> ArrayRef {
    let keys = KEYS.to_vec();
    match ty.data_type() {
        DataType::Utf8 => Arc::new(StringArray::from(
            KEYS.iter()
                .map(|v| {
                    v.map(|v| match v {
                        1 => "a",
                        2 => "b",
                        3 => "c",
                        4 => "d",
                        _ => unreachable!(),
                    })
                })
                .collect::<Vec<_>>(),
        )),
        DataType::Int8 => Arc::new(Int8Array::from(
            keys.into_iter()
                .map(|v| v.map(|v| i8::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int16 => Arc::new(Int16Array::from(
            keys.into_iter()
                .map(|v| v.map(|v| i16::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            keys.into_iter()
                .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(keys)),
        DataType::Date32 => Arc::new(Date32Array::from(
            keys.into_iter()
                .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            Arc::new(TimestampMillisecondArray::from(keys))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Arc::new(TimestampMicrosecondArray::from(keys))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            Arc::new(TimestampNanosecondArray::from(keys))
        }
        DataType::Decimal128(p, s) => Arc::new(
            Decimal128Array::from(
                keys.into_iter()
                    .map(|v| v.map(i128::from))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(*p, *s)
            .unwrap(),
        ),
        DataType::FixedSizeBinary(16) if ty.logical_type() == ValueLogicalType::LargeInt => {
            let mut builder = arrow::array::FixedSizeBinaryBuilder::new(16);
            for value in keys {
                match value {
                    Some(v) => builder.append_value(i128::from(v).to_be_bytes()).unwrap(),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        other => panic!("undeclared literal key: {other:?}"),
    }
}

pub(super) fn key_at(array: &ArrayRef, row: usize) -> Option<i128> {
    if array.is_null(row) {
        return None;
    }
    macro_rules! value {
        ($a:ty) => {
            i128::from(array.as_any().downcast_ref::<$a>().unwrap().value(row))
        };
    }
    Some(match array.data_type() {
        DataType::Utf8 => {
            let key = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row);
            assert_eq!(key, "a");
            1
        }
        DataType::Int8 => value!(Int8Array),
        DataType::Int16 => value!(Int16Array),
        DataType::Int32 => value!(Int32Array),
        DataType::Int64 => value!(Int64Array),
        DataType::Date32 => value!(Date32Array),
        DataType::Timestamp(TimeUnit::Millisecond, None) => value!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, None) => value!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => value!(TimestampNanosecondArray),
        DataType::Decimal128(_, _) => array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(row),
        DataType::FixedSizeBinary(16) => i128::from_be_bytes(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(row)
                .try_into()
                .unwrap(),
        ),
        other => panic!("undeclared output key: {other:?}"),
    })
}

pub(super) struct LiteralOrderQuery {
    data_type: DataType,
    pub(super) calls: AtomicUsize,
}
impl LiteralOrderQuery {
    pub(super) fn new(data_type: DataType) -> Arc<Self> {
        Arc::new(Self {
            data_type,
            calls: AtomicUsize::new(0),
        })
    }
    fn observe(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), RuntimeFilterControlledQueryError> {
        control.checkpoint(1)?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl RuntimeFilterArtifactQuery for LiteralOrderQuery {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }
    fn controlled_scalar_query(&self) -> Option<&dyn RuntimeFilterControlledScalarQuery> {
        Some(self)
    }
    fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled NULL query")
    }
    fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled existence query")
    }
    fn non_null_value_may_match(
        &self,
        _: RuntimeFilterScalarRef<'_>,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled scalar query")
    }
    fn non_null_range_may_match(
        &self,
        _: &ConnectorScalarValue,
        _: &ConnectorScalarValue,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("uncontrolled range query")
    }
}
impl RuntimeFilterControlledScalarQuery for LiteralOrderQuery {
    fn matches_null_controlled(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        self.observe(control)?;
        Ok(true)
    }
    fn non_null_value_may_match_controlled(
        &self,
        value: RuntimeFilterScalarRef<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        self.observe(control)?;
        use RuntimeFilterScalarRef as V;
        Ok(match (&self.data_type, value) {
            (DataType::Utf8, V::Utf8(v)) => v == "a",
            (DataType::Int8, V::Int8(v)) => v == 1,
            (DataType::Int16, V::Int16(v)) => v == 1,
            (DataType::Int32, V::Int32(v)) => v == 1,
            (DataType::Int64, V::Int64(v)) => v == 1,
            (DataType::Date32, V::Date32(v)) => v == 1,
            (DataType::Timestamp(TimeUnit::Millisecond, None), V::TimestampMillisecond(v)) => {
                v == 1
            }
            (DataType::Timestamp(TimeUnit::Microsecond, None), V::TimestampMicrosecond(v)) => {
                v == 1
            }
            (DataType::Timestamp(TimeUnit::Nanosecond, None), V::TimestampNanosecond(v)) => v == 1,
            (DataType::Decimal128(20, 3), V::Decimal128(v)) => v == 1,
            (DataType::FixedSizeBinary(16), V::LargeInt(v)) => v == 1,
            pair => panic!("literal artifact type differs from the declared key: {pair:?}"),
        })
    }
}
