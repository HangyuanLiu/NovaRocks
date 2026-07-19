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

use crate::{
    CommitOutcome, CommitResolution, OperationId, StateStore, TransactionId, derive_transaction_id,
};

use super::{ControlPlaneIncarnation, CoordinationError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadBackCertainty {
    Confirmed,
    Conflict,
    Unresolved,
}

pub(crate) fn transaction_id(operation_id: OperationId) -> TransactionId {
    derive_transaction_id(operation_id, 1)
}

pub(crate) async fn classify_commit(
    store: &dyn StateStore,
    transaction_id: TransactionId,
    outcome: CommitOutcome,
) -> Result<ReadBackCertainty, CoordinationError> {
    match outcome {
        CommitOutcome::Committed(_) => Ok(ReadBackCertainty::Confirmed),
        CommitOutcome::Conflict(_) => Ok(ReadBackCertainty::Conflict),
        CommitOutcome::TransientBeforeCommit(error) | CommitOutcome::DefiniteFailure(error) => {
            Err(CoordinationError::from_state_store(error))
        }
        CommitOutcome::CommitUnknown(_) => recover_commit(store, transaction_id).await,
    }
}

pub(crate) async fn recover_commit(
    store: &dyn StateStore,
    transaction_id: TransactionId,
) -> Result<ReadBackCertainty, CoordinationError> {
    match store
        .resolve_commit(&transaction_id)
        .await
        .map_err(CoordinationError::from_state_store)?
    {
        CommitResolution::Committed(_) => Ok(ReadBackCertainty::Confirmed),
        CommitResolution::NotCommitted => {
            Err(CoordinationError::operation_not_committed(transaction_id))
        }
        CommitResolution::Unresolved => Ok(ReadBackCertainty::Unresolved),
    }
}

pub(crate) fn candidate_mismatch(
    certainty: ReadBackCertainty,
    transaction_id: TransactionId,
    current: ControlPlaneIncarnation,
    candidate: ControlPlaneIncarnation,
) -> CoordinationError {
    if certainty == ReadBackCertainty::Unresolved {
        return CoordinationError::commit_uncertain(transaction_id);
    }
    if current != candidate {
        return CoordinationError::incarnation_changed();
    }
    CoordinationError::fence_lost()
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{ReadBackCertainty, candidate_mismatch};
    use crate::TransactionId;
    use crate::coordination::{ControlPlaneIncarnation, CoordinationErrorKind};

    fn incarnation(value: u64) -> ControlPlaneIncarnation {
        ControlPlaneIncarnation::new(value).expect("nonzero incarnation")
    }

    fn transaction_id() -> TransactionId {
        TransactionId::from(Uuid::now_v7())
    }

    #[test]
    fn unresolved_candidate_mismatch_is_the_only_uncertain_classification() {
        let transaction_id = transaction_id();
        let error = candidate_mismatch(
            ReadBackCertainty::Unresolved,
            transaction_id,
            incarnation(3),
            incarnation(2),
        );

        assert_eq!(error.kind(), CoordinationErrorKind::CommitUncertain);
        assert_eq!(error.transaction_id(), Some(transaction_id));
    }

    #[test]
    fn confirmed_supersession_is_not_commit_uncertainty() {
        assert_eq!(
            candidate_mismatch(
                ReadBackCertainty::Confirmed,
                transaction_id(),
                incarnation(3),
                incarnation(2),
            )
            .kind(),
            CoordinationErrorKind::IncarnationChanged
        );
        assert_eq!(
            candidate_mismatch(
                ReadBackCertainty::Confirmed,
                transaction_id(),
                incarnation(2),
                incarnation(2),
            )
            .kind(),
            CoordinationErrorKind::FenceLost
        );
    }
}
