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

//! The shared write-evidence ledger owns its exact count/byte budget facts.
use super::{
    ConnectorError, ConnectorErrorKind, WriteCommitEvidenceLimits, WriteCommitEvidenceUsage,
};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, ResourceDimension, ResourceFacts,
    ResourceScope, domain,
};
use std::{fmt, num::NonZeroU16};
#[derive(Debug)]
pub(super) enum WriteEvidenceFailure {
    Budget {
        requested_bytes: usize,
        requested_entries: usize,
        committed: WriteCommitEvidenceUsage,
        limits: WriteCommitEvidenceLimits,
    },
    Arithmetic {
        dimension: ResourceDimension,
        requested: usize,
        committed: usize,
        limit: usize,
    },
    // PoisonError owns a borrowed MutexGuard. Retain the actual poison fact
    // without extending that guard's lifetime or inventing an SDK source.
    Poisoned,
}
impl fmt::Display for WriteEvidenceFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Budget {
                requested_bytes,
                requested_entries,
                committed,
                limits,
            } => write!(
                f,
                "write commit evidence exceeds fragment budget: bytes {}/{} entries {}/{}",
                committed.bytes as u128 + *requested_bytes as u128,
                limits.max_bytes,
                committed.entries as u128 + *requested_entries as u128,
                limits.max_entries,
            ),
            Self::Arithmetic {
                dimension,
                requested,
                committed,
                ..
            } => write!(
                f,
                "write commit evidence {dimension:?} accounting overflowed: {committed} + {requested}"
            ),
            Self::Poisoned => f.write_str("write commit evidence ledger lock is poisoned"),
        }
    }
}
impl std::error::Error for WriteEvidenceFailure {}
impl WriteEvidenceFailure {
    pub(super) fn into_connector(self) -> ConnectorError {
        let kind = match self {
            Self::Poisoned => ConnectorErrorKind::Internal,
            _ => ConnectorErrorKind::ResourceExhausted,
        };
        ConnectorError::from_domain(kind, self)
    }
}
impl FailureProjection for WriteEvidenceFailure {
    fn failure_fact(&self) -> FailureFact {
        let (code, dimension, requested, committed, limit) = match self {
            Self::Budget {
                requested_bytes,
                requested_entries,
                committed,
                limits,
            } => {
                // Both obligations are checked before mutation. When both are
                // refused, byte capacity is the first constraint observed.
                if committed.bytes as u128 + *requested_bytes as u128 > limits.max_bytes as u128 {
                    (
                        47,
                        ResourceDimension::Bytes,
                        *requested_bytes,
                        committed.bytes,
                        limits.max_bytes,
                    )
                } else {
                    (
                        47,
                        ResourceDimension::Items,
                        *requested_entries,
                        committed.entries,
                        limits.max_entries,
                    )
                }
            }
            Self::Arithmetic {
                dimension,
                requested,
                committed,
                limit,
            } => (48, *dimension, *requested, *committed, *limit),
            Self::Poisoned => {
                return FailureFact::new(
                    FailureClass::Invariant,
                    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(49).unwrap()),
                );
            }
        };
        FailureFact::new(
            FailureClass::Resource,
            FailureReason::new(domain::CONNECTOR, NonZeroU16::new(code).unwrap()),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Request),
            constraint_scope: Some(ResourceScope::Task),
            dimension,
            requested: u64::try_from(requested).ok(),
            required_growth: u64::try_from(requested).ok(),
            limit: u64::try_from(limit).ok(),
            live: None,
            committed: u64::try_from(committed).ok(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::super::WriteCommitEvidenceLedger;
    use super::*;
    #[test]
    fn real_ledger_refusals_keep_exact_constraint_and_leave_previous_evidence_unchanged() {
        for (limits, request, dimension, committed, limit) in [
            ((32, 4), (17, 1), ResourceDimension::Bytes, 16, 32),
            ((1024, 1), (16, 1), ResourceDimension::Items, 1, 1),
        ] {
            let ledger = WriteCommitEvidenceLedger::new(
                WriteCommitEvidenceLimits::try_new(limits.0, limits.1).unwrap(),
            );
            ledger.reserve(16, 1).unwrap();
            let before = ledger.usage().unwrap();
            let error = ledger.reserve(request.0, request.1).unwrap_err();
            assert_eq!(ledger.usage().unwrap(), before);
            let fact = error.failure_fact();
            assert_eq!(fact.class(), FailureClass::Resource);
            assert_eq!(
                fact.reason(),
                FailureReason::new(domain::CONNECTOR, NonZeroU16::new(47).unwrap())
            );
            let facts = fact.resource().unwrap();
            assert_eq!(facts.dimension, dimension);
            assert_eq!(
                facts.requested,
                Some(if dimension == ResourceDimension::Bytes {
                    request.0 as u64
                } else {
                    request.1 as u64
                })
            );
            assert_eq!(facts.committed, Some(committed));
            assert_eq!(facts.limit, Some(limit));
            assert_eq!(facts.constraint_scope, Some(ResourceScope::Task));
            assert!(fact.representation().is_none());
            assert!(std::error::Error::source(&error).is_some());
        }
        let ledger = WriteCommitEvidenceLedger::new(
            WriteCommitEvidenceLimits::try_new(usize::MAX, usize::MAX).unwrap(),
        );
        ledger.reserve(usize::MAX, 0).unwrap();
        let error = ledger.reserve(1, 0).unwrap_err();
        assert_eq!(ledger.usage().unwrap().bytes(), usize::MAX);
        assert_eq!(error.failure_fact().reason().code(), 48);
        assert!(
            error.failure_fact().representation().is_none(),
            "accounting is not a physical backing format"
        );
    }
}
