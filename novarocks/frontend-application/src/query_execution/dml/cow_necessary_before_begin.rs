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

use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
    ConnectorRowMutationPreparation, ConnectorRowMutationSelection,
};

#[derive(Debug)]
pub(super) enum Error {
    Control(ConnectorError),
    Scope(novarocks_workload_control::WorkError),
    ExistingCoverage(String),
    MissingAdmission,
    ResourceExhausted(ConnectorError),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Scope(error) => error.fmt(f),
            Self::ExistingCoverage(s) => f.write_str(s),
            Self::MissingAdmission => {
                f.write_str("COW construction requires the original Internal result binding")
            }
            Self::ResourceExhausted(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(e) | Self::ResourceExhausted(e) => Some(e),
            Self::Scope(e) => Some(e),
            _ => None,
        }
    }
}
fn exhausted() -> Error {
    Error::ResourceExhausted(ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "COW generated query minimum exceeds the original Internal construction profile",
    ))
}

// Valid only for this FE's two signed-intent stock Iceberg COW callers: the
// stock signer maps each distinct after-field into every routed row's VALUES.
// Success here grants no capacity and establishes no full coexistence upper.
fn preflight(
    preparation: &ConnectorRowMutationPreparation,
    selection: &ConnectorRowMutationSelection,
    binding: Option<&QueryResultCapacityBinding>,
    context: &ConnectorRequestContext,
) -> Result<(), Error> {
    // Preserve original control's error and priority over shape arithmetic.
    context.check_active().map_err(Error::Control)?;
    let binding = binding.ok_or(Error::MissingAdmission)?;
    binding.scope().check().map_err(Error::Scope)?;
    crate::query_execution::internal_result_cpu::require_internal_result_capacity(
        binding.scope(),
        &binding.window_alias(),
    )
    .map_err(Error::ExistingCoverage)?;
    let width = u64::try_from(preparation.match_contract().after_fields().len())
        .map_err(|_| exhausted())?;
    let minimum = selection
        .row_count()
        .checked_mul(width)
        .and_then(|cells| cells.checked_mul(2))
        .and_then(|nodes| {
            nodes.checked_mul(std::mem::size_of::<novarocks_parser::ast::Expr>() as u64)
        })
        .ok_or_else(exhausted)?;
    if minimum > crate::query_execution::internal_result_cpu::INTERNAL_PEAK_BYTES {
        return Err(exhausted());
    }
    // Neither check renews the same absolute clock; no sleep, task, or permit.
    context.check_active().map_err(Error::Control)?;
    binding.scope().check().map_err(Error::Scope)?;
    Ok(())
}

// Move the original selection only after checks have returned: no borrow/move
// conflict and no selection clone added solely for the preflight call.
pub(super) fn before_request_owned<R>(
    preparation: &ConnectorRowMutationPreparation,
    selection: ConnectorRowMutationSelection,
    binding: Option<&QueryResultCapacityBinding>,
    context: &ConnectorRequestContext,
    construct_request_and_begin: impl FnOnce(ConnectorRowMutationSelection) -> R,
) -> Result<R, Error> {
    preflight(preparation, &selection, binding, context)?;
    Ok(construct_request_and_begin(selection))
}
#[cfg(test)]
pub(super) fn before_request<R>(
    preparation: &ConnectorRowMutationPreparation,
    selection: &ConnectorRowMutationSelection,
    binding: Option<&QueryResultCapacityBinding>,
    context: &ConnectorRequestContext,
    next: impl FnOnce() -> R,
) -> Result<R, Error> {
    preflight(preparation, selection, binding, context)?;
    Ok(next())
}
