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

use arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
use arrow::datatypes::DataType;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregateViewFailure {
    InputContract(novarocks_failure_contract::BoundedDetail),
    PhysicalWrapper {
        expected: &'static str,
    },
    Unsupported {
        view: &'static str,
        actual: novarocks_failure_contract::BoundedDetail,
    },
}
impl std::fmt::Display for AggregateViewFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InputContract(detail) => detail.fmt(f),
            Self::PhysicalWrapper { expected } => write!(f, "failed to downcast to {expected}"),
            Self::Unsupported { view, actual } => {
                write!(f, "unsupported {view} input type: {actual}")
            }
        }
    }
}
impl std::error::Error for AggregateViewFailure {}
impl novarocks_failure_contract::FailureProjection for AggregateViewFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        let reason = match self {
            Self::InputContract(_) => {
                crate::runtime::fragment::ExecutionReason::AggregateInputContract
            }
            Self::PhysicalWrapper { .. } => {
                crate::runtime::fragment::ExecutionReason::AggregateViewWrapper
            }
            Self::Unsupported { .. } => {
                crate::runtime::fragment::ExecutionReason::AggregateViewType
            }
        };
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            reason.reason(),
        )
    }
}
impl From<AggregateViewFailure> for crate::runtime::fragment::ExecutionFailure {
    fn from(source: AggregateViewFailure) -> Self {
        Self::from_domain(source)
    }
}

pub(crate) fn aggregate_input_contract(
    detail: impl std::fmt::Display,
) -> crate::runtime::fragment::ExecutionFailure {
    crate::runtime::fragment::ExecutionFailure::from_domain(AggregateViewFailure::InputContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}

#[derive(Clone, Debug)]
pub enum IntArrayView<'a> {
    Int64(&'a Int64Array),
    Int32(&'a Int32Array),
    Int16(&'a Int16Array),
    Int8(&'a Int8Array),
}

impl<'a> IntArrayView<'a> {
    pub fn new(array: &'a ArrayRef) -> Result<Self, AggregateViewFailure> {
        match array.data_type() {
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(Self::Int64)
                .ok_or(AggregateViewFailure::PhysicalWrapper {
                    expected: "Int64Array",
                }),
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<Int32Array>()
                .map(Self::Int32)
                .ok_or(AggregateViewFailure::PhysicalWrapper {
                    expected: "Int32Array",
                }),
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<Int16Array>()
                .map(Self::Int16)
                .ok_or(AggregateViewFailure::PhysicalWrapper {
                    expected: "Int16Array",
                }),
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<Int8Array>()
                .map(Self::Int8)
                .ok_or(AggregateViewFailure::PhysicalWrapper {
                    expected: "Int8Array",
                }),
            other => Err(AggregateViewFailure::Unsupported {
                view: "int",
                actual: novarocks_failure_contract::BoundedDetail::truncating_display(
                    format_args!("{other:?}"),
                ),
            }),
        }
    }

    pub fn value_at(&self, row: usize) -> Option<i64> {
        match self {
            IntArrayView::Int64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            IntArrayView::Int32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            IntArrayView::Int16(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
            IntArrayView::Int8(arr) => (!arr.is_null(row)).then(|| arr.value(row) as i64),
        }
    }
}

#[derive(Clone, Debug)]
pub enum FloatArrayView<'a> {
    Float64(&'a Float64Array),
    Float32(&'a Float32Array),
}

impl<'a> FloatArrayView<'a> {
    pub fn new(array: &'a ArrayRef) -> Result<Self, AggregateViewFailure> {
        match array.data_type() {
            DataType::Float64 => array
                .as_any()
                .downcast_ref::<Float64Array>()
                .map(Self::Float64)
                .ok_or(AggregateViewFailure::PhysicalWrapper {
                    expected: "Float64Array",
                }),
            DataType::Float32 => array
                .as_any()
                .downcast_ref::<Float32Array>()
                .map(Self::Float32)
                .ok_or(AggregateViewFailure::PhysicalWrapper {
                    expected: "Float32Array",
                }),
            other => Err(AggregateViewFailure::Unsupported {
                view: "float",
                actual: novarocks_failure_contract::BoundedDetail::truncating_display(
                    format_args!("{other:?}"),
                ),
            }),
        }
    }

    pub fn value_at(&self, row: usize) -> Option<f64> {
        match self {
            FloatArrayView::Float64(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
            FloatArrayView::Float32(arr) => (!arr.is_null(row)).then(|| arr.value(row) as f64),
        }
    }
}
