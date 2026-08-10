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

//! Projection of the generic Connector terminal contract into the durable DML
//! journal. The frontend records only SPI-owned wire envelopes and never
//! inspects provider payloads.

use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorWriteAbortOutcome, ConnectorWriteReceipt,
    ExternalMutationEffect, ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::dml::model::{
    ConnectorWriteFinalizationRecord, ConnectorWriteLifecycleRecord, ConnectorWriteReceiptWire,
    ExternalMutationEvidenceWire, OperationFact, OperationState,
};

pub fn operation_fact_from_outcome(
    outcome: &ExternalMutationOutcome<ConnectorWriteReceipt>,
) -> Result<OperationFact, String> {
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            ..
        } => Ok(OperationFact {
            state: OperationState::Committed,
            lifecycle: ConnectorWriteLifecycleRecord::KnownEmpty,
        }),
        ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            finalization,
        } => known_committed_fact(receipt, finalization),
        ExternalMutationOutcome::KnownUncommitted { failure } => Ok(OperationFact {
            state: OperationState::FailedKnownUncommitted,
            lifecycle: ConnectorWriteLifecycleRecord::KnownUncommitted {
                failure: failure.into(),
            },
        }),
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => Ok(OperationFact {
            state: OperationState::CommitUnknown,
            lifecycle: ConnectorWriteLifecycleRecord::CommitUnknown {
                evidence_wire: ExternalMutationEvidenceWire::try_from_evidence(evidence)?,
                failure: failure.into(),
            },
        }),
    }
}

pub fn operation_fact_from_finalize_failure(
    receipt: &ConnectorWriteReceipt,
    failure: &ConnectorMutationFailure,
) -> Result<OperationFact, String> {
    known_committed_fact(
        receipt,
        &ExternalMutationFinalization::Failed(failure.clone()),
    )
}

pub fn operation_fact_from_abort_outcome(
    outcome: &ConnectorWriteAbortOutcome,
) -> Result<OperationFact, String> {
    match outcome {
        ConnectorWriteAbortOutcome::KnownUncommitted { .. } => Ok(OperationFact {
            state: OperationState::FailedKnownUncommitted,
            lifecycle: ConnectorWriteLifecycleRecord::KnownUncommitted {
                failure: failure_record("writer abort completed"),
            },
        }),
        ConnectorWriteAbortOutcome::KnownCommitted {
            receipt,
            finalization,
        } => known_committed_fact(receipt, finalization),
        ConnectorWriteAbortOutcome::CommitUnknown { failure, evidence } => Ok(OperationFact {
            state: OperationState::CommitUnknown,
            lifecycle: ConnectorWriteLifecycleRecord::CommitUnknown {
                evidence_wire: ExternalMutationEvidenceWire::try_from_evidence(evidence)?,
                failure: failure.into(),
            },
        }),
    }
}

fn known_committed_fact(
    receipt: &ConnectorWriteReceipt,
    finalization: &ExternalMutationFinalization,
) -> Result<OperationFact, String> {
    let finalization = match finalization {
        ExternalMutationFinalization::Complete => ConnectorWriteFinalizationRecord::Complete,
        ExternalMutationFinalization::Failed(failure) => {
            ConnectorWriteFinalizationRecord::Failed(failure.into())
        }
    };
    let state = match finalization {
        ConnectorWriteFinalizationRecord::Complete => OperationState::Committed,
        ConnectorWriteFinalizationRecord::Failed(_) => OperationState::FinalizeFailedKnownCommitted,
    };
    Ok(OperationFact {
        state,
        lifecycle: ConnectorWriteLifecycleRecord::KnownCommitted {
            receipt_wire: ConnectorWriteReceiptWire::try_from_receipt(receipt)?,
            finalization,
        },
    })
}

fn failure_record(message: &str) -> crate::dml::model::ConnectorWriteFailureRecord {
    use novarocks_spi::connector::ConnectorMutationFailureKind;

    (&ConnectorMutationFailure::new(ConnectorMutationFailureKind::Cancelled, message)).into()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorMutationFailure, ConnectorMutationFailureKind, ExternalMutationFinalization,
    };

    use super::*;

    fn receipt() -> ConnectorWriteReceipt {
        ConnectorWriteReceipt::try_new(Bytes::from_static(b"opaque-provider-receipt"))
            .expect("receipt")
    }

    #[test]
    fn applied_commit_persists_only_a_receipt_wire() {
        let outcome = ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt: receipt(),
            finalization: ExternalMutationFinalization::Complete,
        };
        let fact = operation_fact_from_outcome(&outcome).expect("fact");
        assert_eq!(fact.state, OperationState::Committed);
        let ConnectorWriteLifecycleRecord::KnownCommitted { receipt_wire, .. } = fact.lifecycle
        else {
            panic!("expected known committed lifecycle");
        };
        assert_eq!(receipt_wire.try_decode().expect("wire"), receipt());
    }

    #[test]
    fn no_op_commit_is_known_empty_without_a_provider_projection() {
        let outcome = ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            receipt: receipt(),
            finalization: ExternalMutationFinalization::Complete,
        };
        let fact = operation_fact_from_outcome(&outcome).expect("fact");
        assert_eq!(fact.state, OperationState::Committed);
        assert_eq!(fact.lifecycle, ConnectorWriteLifecycleRecord::KnownEmpty);
    }

    #[test]
    fn finalization_failure_keeps_the_known_committed_receipt() {
        let failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Internal,
            "cache invalidation failed",
        );
        let fact = operation_fact_from_finalize_failure(&receipt(), &failure).expect("fact");
        assert_eq!(fact.state, OperationState::FinalizeFailedKnownCommitted);
        assert!(matches!(
            fact.lifecycle,
            ConnectorWriteLifecycleRecord::KnownCommitted {
                finalization: ConnectorWriteFinalizationRecord::Failed(_),
                ..
            }
        ));
    }
}
