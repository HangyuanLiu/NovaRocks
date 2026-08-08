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

use arrow::array::{ArrayRef, BooleanArray};
use novarocks_execution::runtime_filter as execution;
use novarocks_spi::connector::ConnectorScalarValue;

use super::membership_predicate::NativeRuntimeFilterPredicate;
use super::ordered_range_predicate::NativeOrderedRangePredicate;

/// Core-local executable predicates carried by immutable execution snapshots.
/// This is deliberately an execution adapter rather than a Service type so
/// consumers can retain evaluator-specific capabilities without observing
/// artifact delivery or installed deployment state.
pub enum NativeExecutionPredicate {
    Membership(NativeRuntimeFilterPredicate),
    Ordered(Arc<NativeOrderedRangePredicate>),
}

impl execution::RuntimeFilterPredicate for NativeExecutionPredicate {
    fn evaluate(
        &self,
        input: &ArrayRef,
    ) -> Result<BooleanArray, execution::RuntimeFilterContractViolation> {
        match self {
            Self::Membership(predicate) => predicate.evaluate(input).map_err(|error| {
                execution::RuntimeFilterContractViolation::new(
                    execution::RuntimeFilterContractViolationKind::ContractMismatch,
                    error.to_string(),
                )
            }),
            Self::Ordered(predicate) => predicate.evaluate(input).map_err(|error| {
                execution::RuntimeFilterContractViolation::new(
                    execution::RuntimeFilterContractViolationKind::ContractMismatch,
                    error.to_string(),
                )
            }),
        }
    }
}

impl execution::scan_domain::RuntimeFilterScanDomainPredicate for NativeExecutionPredicate {
    fn data_type(&self) -> &arrow::datatypes::DataType {
        match self {
            Self::Membership(predicate) => predicate.data_type(),
            Self::Ordered(predicate) => predicate.data_type(),
        }
    }

    fn matches_null(
        &self,
    ) -> Result<bool, execution::scan_domain::RuntimeFilterScanDomainCapabilityError> {
        match self {
            Self::Membership(predicate) => predicate.matches_null(),
            Self::Ordered(predicate) => predicate.matches_null(),
        }
    }

    fn has_non_null_matches(
        &self,
    ) -> Result<bool, execution::scan_domain::RuntimeFilterScanDomainCapabilityError> {
        match self {
            Self::Membership(predicate) => predicate.has_non_null_matches(),
            Self::Ordered(predicate) => predicate.has_non_null_matches(),
        }
    }

    fn non_null_range_may_match(
        &self,
        inclusive_min: &ConnectorScalarValue,
        inclusive_max: &ConnectorScalarValue,
    ) -> Result<bool, execution::scan_domain::RuntimeFilterScanDomainCapabilityError> {
        match self {
            Self::Membership(predicate) => {
                predicate.non_null_range_may_match(inclusive_min, inclusive_max)
            }
            Self::Ordered(predicate) => {
                predicate.non_null_range_may_match(inclusive_min, inclusive_max)
            }
        }
    }
}
