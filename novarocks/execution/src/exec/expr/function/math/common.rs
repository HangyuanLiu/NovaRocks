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
//! Original math reader and cast helpers are owned by Functions.
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
pub(super) fn cast_output(out: ArrayRef, target: Option<&DataType>) -> ExecutionResult<ArrayRef> {
    novarocks_functions::math_numeric::cast_output(out, target).map_err(ExecutionFailure::from)
}
impl From<novarocks_functions::math_numeric::NumericArrayFailure> for ExecutionFailure {
    fn from(source: novarocks_functions::math_numeric::NumericArrayFailure) -> Self {
        Self::from_domain(source)
    }
}
impl From<novarocks_functions::math_numeric::MathNumericError> for ExecutionFailure {
    fn from(source: novarocks_functions::math_numeric::MathNumericError) -> Self {
        source.into_kernel().into()
    }
}
impl From<novarocks_functions::builtin::numeric_elementary::NumericElementaryError>
    for ExecutionFailure
{
    fn from(
        source: novarocks_functions::builtin::numeric_elementary::NumericElementaryError,
    ) -> Self {
        source.into_kernel().into()
    }
}
impl From<novarocks_functions::builtin::numeric_mod_core::NumericModError> for ExecutionFailure {
    fn from(source: novarocks_functions::builtin::numeric_mod_core::NumericModError) -> Self {
        source.into_kernel().into()
    }
}
impl From<novarocks_functions::builtin::abs_core::AbsError> for ExecutionFailure {
    fn from(source: novarocks_functions::builtin::abs_core::AbsError) -> Self {
        source.into_kernel().into()
    }
}
#[derive(Debug)]
enum MathInputFailure {
    Invalid(novarocks_failure_contract::BoundedDetail),
    NumericRange,
}
impl std::fmt::Display for MathInputFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(detail) => f.write_str(detail.as_str()),
            Self::NumericRange => f.write_str("equiwidth_bucket result exceeds BIGINT"),
        }
    }
}
impl std::error::Error for MathInputFailure {}
impl novarocks_failure_contract::FailureProjection for MathInputFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::NumericRange => {
                novarocks_functions::DataFailureReason::NumericOutOfRange.failure_fact()
            }
            Self::Invalid(_) => novarocks_failure_contract::FailureFact::new(
                novarocks_failure_contract::FailureClass::Data,
                crate::runtime::fragment::ExecutionReason::MathInputValue.reason(),
            ),
        }
    }
}
pub(super) fn invalid_value(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(MathInputFailure::Invalid(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn numeric_range() -> ExecutionFailure {
    ExecutionFailure::from_domain(MathInputFailure::NumericRange)
}
#[derive(Debug)]
struct MathCastFailure {
    context: novarocks_failure_contract::BoundedDetail,
    source: ExecutionFailure,
}
impl std::fmt::Display for MathCastFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.context.as_str(), self.source)
    }
}
impl std::error::Error for MathCastFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for MathCastFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.source.failure_fact()
    }
}
pub(super) fn input_cast_failure(
    context: impl std::fmt::Display,
    source: arrow::error::ArrowError,
) -> ExecutionFailure {
    ExecutionFailure::from_domain(MathCastFailure {
        context: novarocks_failure_contract::BoundedDetail::truncating_display(context),
        source: crate::exec::expr::cast::cast_arrow_failure(source),
    })
}
pub use novarocks_functions::math_numeric::{NumericArrayView, value_at_f64, value_at_i64};
