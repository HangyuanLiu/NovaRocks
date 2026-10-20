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
//! Actual non-value Arrow operations shared by function families.
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason};
#[derive(Debug)]
struct FunctionArrowFailure {
    operation: &'static str,
    source: arrow::error::ArrowError,
}
impl std::fmt::Display for FunctionArrowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.operation, self.source)
    }
}
impl std::error::Error for FunctionArrowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for FunctionArrowFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact};
        match &self.source {
            arrow::error::ArrowError::MemoryError(_) => FailureFact::new(
                FailureClass::Resource,
                ExecutionReason::FunctionArrowAllocation.reason(),
            ),
            _ => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::FunctionArrowContract.reason(),
            ),
        }
    }
}
/// Only non-value Arrow operations (take/concat/construction) use this port.
/// Value casts retain the CAST domain's own typed refusal projection.
pub(in crate::exec::expr::function) fn arrow_contract_failure(
    operation: &'static str,
    source: arrow::error::ArrowError,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(FunctionArrowFailure { operation, source })
}
