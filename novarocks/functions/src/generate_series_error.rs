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
//! Causes authored by the integer-series computation, before any host or SQL policy.
use super::IntegerDiagnosticContext;
use arrow_schema::DataType;
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};

#[derive(Debug)]
pub enum SeriesError {
    ZeroStep,
    ArgumentType {
        function: IntegerDiagnosticContext,
        argument: usize,
        data_type: DataType,
    },
    ArgumentBounds {
        argument: usize,
        row: usize,
        rows: usize,
    },
    ArrayType {
        function: IntegerDiagnosticContext,
        argument: usize,
        data_type: DataType,
    },
    LargeIntWidth {
        function: IntegerDiagnosticContext,
        argument: usize,
        bytes: usize,
    },
    ReturnCount {
        actual: usize,
    },
    ReturnType {
        data_type: DataType,
    },
    ValueOutOfRange {
        value: i128,
        data_type: DataType,
    },
    OutputRows {
        required: Option<u128>,
    },
    ValueProgress {
        current: i128,
        step: i128,
    },
    OutputLength {
        values: usize,
        rows: usize,
    },
}
impl fmt::Display for SeriesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroStep => {
                f.write_str("table function generate_series step size cannot equal zero")
            }
            Self::ArgumentType {
                function,
                argument,
                data_type,
            } => write!(
                f,
                "table function {} arg {argument} expects TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got {data_type:?}",
                function.name()
            ),
            Self::ArgumentBounds {
                argument,
                row,
                rows,
            } => write!(
                f,
                "table function integer arg {argument} row {row} is outside {rows} rows"
            ),
            Self::ArrayType {
                function,
                argument,
                data_type,
            } => write!(
                f,
                "table function {} arg {argument} array implementation differs from {data_type:?}",
                function.name()
            ),
            Self::LargeIntWidth {
                function,
                argument,
                bytes,
            } => write!(
                f,
                "table function {} arg {argument} LARGEINT requires 16 bytes, got {bytes}",
                function.name()
            ),
            Self::ReturnCount { actual } => write!(
                f,
                "table function generate_series expects 1 return type, got {actual}"
            ),
            Self::ReturnType { data_type } => write!(
                f,
                "table function generate_series return type expects TINYINT/SMALLINT/INT/BIGINT/LARGEINT, got {data_type:?}"
            ),
            Self::ValueOutOfRange { value, data_type } => {
                let label = match data_type {
                    DataType::Int8 => "TINYINT",
                    DataType::Int16 => "SMALLINT",
                    DataType::Int32 => "INT",
                    DataType::Int64 => "BIGINT",
                    _ => "declared integer",
                };
                write!(
                    f,
                    "table function generate_series value out of {label} range: {value}"
                )
            }
            Self::OutputRows { .. } => f.write_str("table function output too large"),
            Self::ValueProgress { current, step } => write!(
                f,
                "table function generate_series value overflow: current={current} step={step}"
            ),
            Self::OutputLength { values, rows } => write!(
                f,
                "table function generate_series internal output size mismatch: values={values} rows={rows}"
            ),
        }
    }
}
impl Error for SeriesError {}

impl FailureProjection for SeriesError {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::ZeroStep => (FailureClass::Data, 70),
            Self::ValueOutOfRange { .. } => {
                return crate::DataFailureReason::NumericOutOfRange.failure_fact();
            }
            Self::OutputRows { .. } => (FailureClass::Representation, 72),
            Self::ArgumentType { .. } => (FailureClass::Invariant, 73),
            Self::ArgumentBounds { .. } => (FailureClass::Invariant, 74),
            Self::ArrayType { .. } => (FailureClass::Invariant, 75),
            Self::LargeIntWidth { .. } => (FailureClass::Invariant, 76),
            Self::ReturnCount { .. } => (FailureClass::Invariant, 77),
            Self::ReturnType { .. } => (FailureClass::Invariant, 78),
            Self::ValueProgress { .. } => (FailureClass::Invariant, 79),
            Self::OutputLength { .. } => (FailureClass::Invariant, 80),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
        );
        match self {
            Self::OutputRows { required } => fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: NonZeroU16::new(1).unwrap(),
                },
                required: required.and_then(|value| u64::try_from(value).ok()),
                extent_limit: Some(super::MAX_TABLE_FUNCTION_OUTPUT_ROWS as u64),
                representable_prefix: None,
            }),
            _ => fact,
        }
    }
}
