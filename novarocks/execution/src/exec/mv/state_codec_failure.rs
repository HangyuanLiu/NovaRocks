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

//! The state codec owns encoded-byte validity and its own carrier contracts.
use crate::runtime::fragment::ExecutionReason;
use novarocks_failure_contract::{BoundedDetail, FailureClass, FailureFact, FailureProjection};
use std::fmt;
#[derive(Debug)]
pub enum StateCodecFailure {
    InvalidEncoding(BoundedDetail),
    CarrierContract(BoundedDetail),
    Utf8(std::str::Utf8Error),
    Length {
        required: u64,
        source: std::num::TryFromIntError,
    },
    CountOverflow {
        current: i64,
        incoming: i64,
    },
}
impl StateCodecFailure {
    pub(super) fn malformed(detail: impl fmt::Display) -> Self {
        Self::InvalidEncoding(BoundedDetail::truncating_display(detail))
    }
    pub(super) fn contract(detail: impl fmt::Display) -> Self {
        Self::CarrierContract(BoundedDetail::truncating_display(detail))
    }
}
impl fmt::Display for StateCodecFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEncoding(detail) | Self::CarrierContract(detail) => detail.fmt(f),
            Self::Utf8(source) => write!(f, "read_key: invalid Utf8: {source}"),
            Self::Length { required, .. } => write!(
                f,
                "read_key: declared Utf8 length {required} does not fit usize"
            ),
            Self::CountOverflow { current, incoming } => write!(
                f,
                "state_codec: multiset count overflow while unioning {current} and {incoming}"
            ),
        }
    }
}
impl std::error::Error for StateCodecFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Utf8(source) => Some(source),
            Self::Length { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for StateCodecFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::InvalidEncoding(_) | Self::Utf8(_) | Self::Length { .. } => {
                novarocks_functions::DataFailureReason::InvalidEncoding.failure_fact()
            }
            Self::CountOverflow { .. } => {
                novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
            }
            Self::CarrierContract(_) => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::MvStateCodecContract.reason(),
            ),
        }
    }
}
impl From<StateCodecFailure> for crate::runtime::fragment::ExecutionFailure {
    fn from(source: StateCodecFailure) -> Self {
        Self::from_domain(source)
    }
}
