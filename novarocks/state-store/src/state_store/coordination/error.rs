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

use std::fmt;

use crate::{StateStoreError, StateStoreErrorKind, TransactionId};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CoordinationErrorKind {
    InvalidRequest,
    LimitExceeded,
    NotBootstrapped,
    WriteClosed,
    ClockUnsafe,
    FenceLost,
    IncarnationChanged,
    EpochExhausted,
    IncarnationExhausted,
    OperationNotCommitted,
    CommitUncertain,
    Corruption,
    StoreUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoordinationError {
    kind: CoordinationErrorKind,
    message: &'static str,
    transaction_id: Option<TransactionId>,
}

impl CoordinationError {
    const fn new(kind: CoordinationErrorKind, message: &'static str) -> Self {
        Self {
            kind,
            message,
            transaction_id: None,
        }
    }

    pub(crate) const fn invalid_request(message: &'static str) -> Self {
        Self::new(CoordinationErrorKind::InvalidRequest, message)
    }

    pub(crate) const fn limit_exceeded(message: &'static str) -> Self {
        Self::new(CoordinationErrorKind::LimitExceeded, message)
    }

    pub(crate) const fn corruption() -> Self {
        Self::new(
            CoordinationErrorKind::Corruption,
            "coordination record is corrupt",
        )
    }

    pub(crate) const fn epoch_exhausted() -> Self {
        Self::new(
            CoordinationErrorKind::EpochExhausted,
            "resource epoch is exhausted",
        )
    }

    pub(crate) const fn incarnation_exhausted() -> Self {
        Self::new(
            CoordinationErrorKind::IncarnationExhausted,
            "control plane incarnation is exhausted",
        )
    }

    pub(crate) const fn not_bootstrapped() -> Self {
        Self::new(
            CoordinationErrorKind::NotBootstrapped,
            "control plane is not bootstrapped",
        )
    }

    pub(crate) const fn write_closed() -> Self {
        Self::new(
            CoordinationErrorKind::WriteClosed,
            "control plane writes are closed",
        )
    }

    pub const fn clock_unsafe() -> Self {
        Self::new(CoordinationErrorKind::ClockUnsafe, "lease clock is unsafe")
    }

    pub(crate) const fn fence_lost() -> Self {
        Self::new(
            CoordinationErrorKind::FenceLost,
            "coordination fence is no longer current",
        )
    }

    pub(crate) const fn incarnation_changed() -> Self {
        Self::new(
            CoordinationErrorKind::IncarnationChanged,
            "control plane incarnation changed",
        )
    }

    pub fn operation_not_committed(transaction_id: TransactionId) -> Self {
        Self {
            kind: CoordinationErrorKind::OperationNotCommitted,
            message: "coordination operation was not committed",
            transaction_id: Some(transaction_id),
        }
    }

    pub fn commit_uncertain(transaction_id: TransactionId) -> Self {
        Self {
            kind: CoordinationErrorKind::CommitUncertain,
            message: "coordination commit outcome is uncertain",
            transaction_id: Some(transaction_id),
        }
    }

    pub const fn kind(&self) -> CoordinationErrorKind {
        self.kind
    }

    pub const fn transaction_id(&self) -> Option<TransactionId> {
        self.transaction_id
    }

    pub(crate) const fn from_state_store(error: StateStoreError) -> Self {
        let kind = match error.kind() {
            StateStoreErrorKind::InvalidRequest | StateStoreErrorKind::InvalidConfiguration => {
                CoordinationErrorKind::InvalidRequest
            }
            StateStoreErrorKind::LimitExceeded => CoordinationErrorKind::LimitExceeded,
            StateStoreErrorKind::Corruption => CoordinationErrorKind::Corruption,
            StateStoreErrorKind::ProviderUnavailable
            | StateStoreErrorKind::DeadlineExceeded
            | StateStoreErrorKind::Transient
            | StateStoreErrorKind::Cancelled
            | StateStoreErrorKind::Internal
            | StateStoreErrorKind::Conflict
            | StateStoreErrorKind::PreconditionFailed
            | StateStoreErrorKind::UnsupportedDeployment => CoordinationErrorKind::StoreUnavailable,
        };
        let message = match kind {
            CoordinationErrorKind::InvalidRequest => "invalid coordination request",
            CoordinationErrorKind::LimitExceeded => "coordination request exceeds store limits",
            CoordinationErrorKind::Corruption => "coordination record is corrupt",
            CoordinationErrorKind::StoreUnavailable => "coordination store is unavailable",
            _ => "coordination operation failed",
        };
        Self::new(kind, message)
    }
}

impl fmt::Display for CoordinationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for CoordinationError {}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::CoordinationError;
    use crate::{StateStoreError, StateStoreErrorKind, TransactionId};

    fn transaction_id() -> TransactionId {
        TransactionId::from(Uuid::now_v7())
    }

    #[test]
    fn transaction_errors_always_expose_their_transaction_id() {
        let transaction_id = transaction_id();
        for error in [
            CoordinationError::operation_not_committed(transaction_id),
            CoordinationError::commit_uncertain(transaction_id),
        ] {
            assert_eq!(error.transaction_id(), Some(transaction_id));
        }
    }

    #[test]
    fn no_payload_error_constructors_expose_no_transaction_id() {
        let errors = [
            CoordinationError::invalid_request("invalid request"),
            CoordinationError::limit_exceeded("limit exceeded"),
            CoordinationError::corruption(),
            CoordinationError::epoch_exhausted(),
            CoordinationError::incarnation_exhausted(),
            CoordinationError::clock_unsafe(),
            CoordinationError::from_state_store(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "provider detail must not escape",
            )),
        ];

        for error in errors {
            assert_eq!(error.transaction_id(), None);
        }
    }
}
