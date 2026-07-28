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

use novarocks::mv::repository::{MvRepositoryError, MvRepositoryErrorKind};
use novarocks_spi::state_store::{
    CommitResolution, StateStore, StateStoreError, StateStoreErrorKind, TransactionId,
};
use novarocks_state_store::metrics::StateStoreMetrics;
use novarocks_state_store::{OperationId, RunFailure, run_side_effect_free};

pub(crate) fn state_store_error(error: StateStoreError) -> MvRepositoryError {
    let kind = match error.kind() {
        StateStoreErrorKind::InvalidRequest | StateStoreErrorKind::LimitExceeded => {
            MvRepositoryErrorKind::InvalidRequest
        }
        StateStoreErrorKind::PreconditionFailed | StateStoreErrorKind::Conflict => {
            MvRepositoryErrorKind::Conflict
        }
        StateStoreErrorKind::Corruption => MvRepositoryErrorKind::Corruption,
        StateStoreErrorKind::DeadlineExceeded => MvRepositoryErrorKind::CommitUnknown,
        StateStoreErrorKind::InvalidConfiguration
        | StateStoreErrorKind::UnsupportedDeployment
        | StateStoreErrorKind::Transient
        | StateStoreErrorKind::ProviderUnavailable
        | StateStoreErrorKind::Cancelled
        | StateStoreErrorKind::Internal => MvRepositoryErrorKind::Unavailable,
    };
    MvRepositoryError::new(kind, format!("MV StateStore operation failed: {error}"))
}

pub(crate) fn run_failure(error: RunFailure) -> MvRepositoryError {
    match error {
        RunFailure::Operation(error) => state_store_error(error),
        RunFailure::RetryExhausted(error) => MvRepositoryError::new(
            MvRepositoryErrorKind::Conflict,
            format!("MV StateStore transaction conflict: {error}"),
        ),
        RunFailure::CommitUnknown {
            transaction_id,
            error,
        } => MvRepositoryError::new(
            MvRepositoryErrorKind::CommitUnknown,
            format!("MV StateStore commit outcome is unknown for {transaction_id:?}: {error}"),
        ),
        RunFailure::Begin(error) | RunFailure::DefiniteFailure(error) => state_store_error(error),
        RunFailure::DeadlineExceeded => MvRepositoryError::new(
            MvRepositoryErrorKind::CommitUnknown,
            "MV StateStore transaction deadline exceeded",
        ),
    }
}

pub(crate) async fn resolve_commit(
    store: &dyn StateStore,
    transaction_id: &TransactionId,
) -> Result<CommitResolution, MvRepositoryError> {
    store
        .resolve_commit(transaction_id)
        .await
        .map_err(state_store_error)
}

pub(crate) async fn run<T, F>(
    store: &dyn StateStore,
    metrics: &StateStoreMetrics,
    operation_id: uuid::Uuid,
    purpose: &str,
    operation: F,
) -> Result<T, MvRepositoryError>
where
    F: for<'a> FnMut(
        &'a mut dyn novarocks_spi::state_store::WriteTransaction,
    ) -> futures::future::BoxFuture<'a, Result<T, StateStoreError>>,
{
    run_raw(store, metrics, operation_id, purpose, operation)
        .await
        .map_err(run_failure)
}

pub(crate) async fn run_raw<T, F>(
    store: &dyn StateStore,
    metrics: &StateStoreMetrics,
    operation_id: uuid::Uuid,
    purpose: &str,
    operation: F,
) -> Result<T, RunFailure>
where
    F: for<'a> FnMut(
        &'a mut dyn novarocks_spi::state_store::WriteTransaction,
    ) -> futures::future::BoxFuture<'a, Result<T, StateStoreError>>,
{
    run_side_effect_free(
        store,
        metrics,
        OperationId::from(operation_id),
        purpose,
        operation,
    )
    .await
    .map(|success| success.value)
}
