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

//! Target-scoped discovery of MV refresh attempts that exist in the lake.
//!
//! Recovery can already inspect an attempt whose descriptor it was handed, but
//! enumeration still comes from the StateStore ledger. After the ledger is lost
//! — disaster recovery, or a rebuild that reassigns the numeric `mv_id` — a
//! frontend cannot prove it has found every attempt for a target, so it cannot
//! safely decide that a staging artifact is abandoned.
//!
//! This contract adds the missing step *before* inspection: given a stable
//! target resource, enumerate the attempts the lake actually holds. Three
//! properties are load-bearing:
//!
//! * **Bounded.** A page carries at most [`ConnectorMvAttemptDiscoveryRequest::page_size`]
//!   summaries, so a target with a pathological number of stale refs cannot turn
//!   startup into an unbounded catalog scan.
//! * **A continuation is not a snapshot.** Metadata may change mid-scan.
//!   Consumers deduplicate by attempt identity and must inspect exactly before
//!   classifying, so duplicates, reordering, and attempts moving between pages
//!   cannot cause a wrong deletion.
//! * **An incomplete scan is never "no attempts".** [`ConnectorMvAttemptPage::complete`]
//!   is the only thing that may be read as exhaustive. A provider that hit a
//!   limit, lost its catalog, or could not decode an entry says so, and the
//!   caller leaves that target unreconciled.
//!
//! No Iceberg ref name, snapshot ancestry, or provenance JSON crosses this
//! boundary: the provider owns enumeration and decoding, the consumer owns
//! policy.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;

use super::context::ConnectorRequestContext;
use super::handle::ConnectorTableHandle;
use super::identity::{ConnectorInstanceDescriptor, ConnectorInstanceId};
use super::mutation::ConnectorCommittedVersion;
use super::mv_publication_fencing::{
    ConnectorMvPublicationFenceGeneration, ConnectorMvRefreshAttemptId,
    ConnectorMvRefreshResourceIdentity,
};
use super::{ConnectorError, ConnectorErrorKind, ConnectorInstanceIncarnation};

pub const CONNECTOR_MV_ATTEMPT_DISCOVERY_CONTRACT_VERSION: u16 = 1;

/// Hard ceiling on one page, independent of what a caller asks for.
pub const MAX_CONNECTOR_MV_ATTEMPT_PAGE_ITEMS: usize = 512;

/// Hard ceiling on an opaque continuation token.
pub const MAX_CONNECTOR_MV_ATTEMPT_CONTINUATION_BYTES: usize = 4 * 1024;

fn invalid(message: &str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

/// Opaque provider-owned position in a discovery scan.
///
/// It is deliberately not a snapshot handle: resuming from it may observe lake
/// changes that happened since the previous page. That is why classification
/// requires an exact inspection rather than trusting a page's contents.
#[derive(Clone, Eq, PartialEq)]
pub struct ConnectorMvAttemptContinuation(Bytes);

impl ConnectorMvAttemptContinuation {
    pub fn try_new(payload: Bytes) -> Result<Self, ConnectorError> {
        if payload.is_empty() {
            return Err(invalid(
                "MV attempt discovery continuation must not be empty",
            ));
        }
        if payload.len() > MAX_CONNECTOR_MV_ATTEMPT_CONTINUATION_BYTES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "MV attempt discovery continuation exceeds 4 KiB",
            ));
        }
        Ok(Self(payload))
    }

    pub fn payload(&self) -> &Bytes {
        &self.0
    }
}

impl fmt::Debug for ConnectorMvAttemptContinuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorMvAttemptContinuation")
            .field("payload_len", &self.0.len())
            .finish()
    }
}

/// Why a discovery scan could not be proven exhaustive.
///
/// Each variant is a distinct operational situation, and none of them may be
/// treated as "this target has no attempts".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorMvAttemptScanLimit {
    /// The provider stopped at its own bound; resume from the continuation.
    PageBudgetExhausted,
    /// The catalog or object store could not be read far enough.
    StorageUnavailable,
    /// An entry carried evidence this provider version cannot interpret.
    UnknownEvidenceVersion,
    /// The supplied continuation no longer resolves; restart the scan.
    ContinuationExpired,
}

/// One attempt the provider found for a target.
///
/// Identity is the stable attempt ID plus the fence generation that produced it,
/// never a numeric refresh ID: after a ledger loss the numeric IDs are exactly
/// what is missing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMvAttemptSummary {
    attempt: ConnectorMvRefreshAttemptId,
    generation: ConnectorMvPublicationFenceGeneration,
    /// Provider-produced pointer to the staged result, when one exists.
    staged_version: Option<ConnectorCommittedVersion>,
    /// Schema version of the evidence this summary was decoded from, so a
    /// consumer can tell a V2 attempt from a legacy one without parsing it.
    evidence_version: u16,
}

impl ConnectorMvAttemptSummary {
    pub fn try_new(
        attempt: ConnectorMvRefreshAttemptId,
        generation: ConnectorMvPublicationFenceGeneration,
        staged_version: Option<ConnectorCommittedVersion>,
        evidence_version: u16,
    ) -> Result<Self, ConnectorError> {
        generation.validate()?;
        if let Some(version) = &staged_version {
            version.validate()?;
        }
        if evidence_version == 0 {
            return Err(invalid("MV attempt evidence version must be nonzero"));
        }
        Ok(Self {
            attempt,
            generation,
            staged_version,
            evidence_version,
        })
    }

    pub const fn attempt(&self) -> ConnectorMvRefreshAttemptId {
        self.attempt
    }

    pub fn generation(&self) -> &ConnectorMvPublicationFenceGeneration {
        &self.generation
    }

    pub fn staged_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.staged_version.as_ref()
    }

    pub const fn evidence_version(&self) -> u16 {
        self.evidence_version
    }
}

/// A bounded request for one page of a target's attempts.
#[derive(Clone)]
pub struct ConnectorMvAttemptDiscoveryRequest {
    pub table: ConnectorTableHandle,
    pub resource: ConnectorMvRefreshResourceIdentity,
    pub page_size: usize,
    /// `None` starts a fresh scan.
    pub continuation: Option<ConnectorMvAttemptContinuation>,
    pub context: ConnectorRequestContext,
}

impl ConnectorMvAttemptDiscoveryRequest {
    pub fn validate(&self) -> Result<(), ConnectorError> {
        self.resource.validate()?;
        if self.page_size == 0 {
            return Err(invalid("MV attempt discovery page size must be positive"));
        }
        if self.page_size > MAX_CONNECTOR_MV_ATTEMPT_PAGE_ITEMS {
            return Err(invalid(
                "MV attempt discovery page size exceeds the contract bound",
            ));
        }
        Ok(())
    }
}

/// One page of discovered attempts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorMvAttemptPage {
    resource: ConnectorMvRefreshResourceIdentity,
    attempts: Vec<ConnectorMvAttemptSummary>,
    /// The target's currently visible version, so a consumer can tell a
    /// published attempt from a staged one without a second call.
    current_visible_version: Option<ConnectorCommittedVersion>,
    /// The generation that currently owns publication, when a fence exists.
    established_generation: Option<ConnectorMvPublicationFenceGeneration>,
    continuation: Option<ConnectorMvAttemptContinuation>,
    /// `true` only when the provider proved it enumerated everything.
    complete: bool,
    limit: Option<ConnectorMvAttemptScanLimit>,
}

impl ConnectorMvAttemptPage {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        resource: ConnectorMvRefreshResourceIdentity,
        attempts: Vec<ConnectorMvAttemptSummary>,
        current_visible_version: Option<ConnectorCommittedVersion>,
        established_generation: Option<ConnectorMvPublicationFenceGeneration>,
        continuation: Option<ConnectorMvAttemptContinuation>,
        complete: bool,
        limit: Option<ConnectorMvAttemptScanLimit>,
    ) -> Result<Self, ConnectorError> {
        resource.validate()?;
        if attempts.len() > MAX_CONNECTOR_MV_ATTEMPT_PAGE_ITEMS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "MV attempt discovery page exceeds the contract item bound",
            ));
        }
        if let Some(version) = &current_visible_version {
            version.validate()?;
        }
        if let Some(generation) = &established_generation {
            generation.validate()?;
        }
        // A complete scan is a claim of exhaustiveness. It cannot also be
        // truncated or carry a reason for having stopped early, or a caller
        // could read "complete" and act destructively on a partial view.
        if complete && (continuation.is_some() || limit.is_some()) {
            return Err(invalid(
                "a complete MV attempt scan must not carry a continuation or a scan limit",
            ));
        }
        // Conversely, an incomplete scan must say why, so the caller can tell
        // "resume from here" apart from "this target is unreadable".
        if !complete && limit.is_none() {
            return Err(invalid(
                "an incomplete MV attempt scan must report why it stopped",
            ));
        }
        // Only a budget stop is resumable; the other limits mean the caller must
        // restart or give up rather than continue from a stale position.
        if let Some(limit) = limit
            && continuation.is_some()
            && limit != ConnectorMvAttemptScanLimit::PageBudgetExhausted
        {
            return Err(invalid(
                "only a page-budget stop may carry a resumable continuation",
            ));
        }
        Ok(Self {
            resource,
            attempts,
            current_visible_version,
            established_generation,
            continuation,
            complete,
            limit,
        })
    }

    pub fn resource(&self) -> &ConnectorMvRefreshResourceIdentity {
        &self.resource
    }

    pub fn attempts(&self) -> &[ConnectorMvAttemptSummary] {
        &self.attempts
    }

    pub fn current_visible_version(&self) -> Option<&ConnectorCommittedVersion> {
        self.current_visible_version.as_ref()
    }

    pub fn established_generation(&self) -> Option<&ConnectorMvPublicationFenceGeneration> {
        self.established_generation.as_ref()
    }

    pub fn continuation(&self) -> Option<&ConnectorMvAttemptContinuation> {
        self.continuation.as_ref()
    }

    /// The only signal that may be read as "these are all of them".
    pub const fn is_complete(&self) -> bool {
        self.complete
    }

    pub const fn limit(&self) -> Option<ConnectorMvAttemptScanLimit> {
        self.limit
    }
}

/// Optional FE-only capability that enumerates a target's refresh attempts.
///
/// It is a strict predecessor to inspection, not a replacement: a summary is
/// enough to decide *what to look at*, never enough to classify or delete.
pub trait ConnectorMvAttemptDiscovery: Send + Sync {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor;
    fn incarnation(&self) -> ConnectorInstanceIncarnation;

    fn discover_attempts(
        &self,
        request: ConnectorMvAttemptDiscoveryRequest,
    ) -> Result<ConnectorMvAttemptPage, ConnectorError>;
}

pub(crate) fn validate_mv_attempt_discovery_owner(
    descriptor: &ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    capability: &dyn ConnectorMvAttemptDiscovery,
) -> Result<(), ConnectorError> {
    if capability.descriptor() != descriptor || capability.incarnation() != incarnation {
        return Err(invalid(
            "MV attempt discovery capability owner does not match its control binding generation",
        ));
    }
    Ok(())
}

/// Narrow consumer port for the discovery capability.
pub trait ConnectorMvAttemptDiscoveryResolver: Send + Sync {
    fn acquire_current_mv_attempt_discovery(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Arc<dyn ConnectorMvAttemptDiscovery>, ConnectorError>;
}

#[cfg(test)]
mod tests {
    use super::super::identity::ConnectorProviderId;
    use super::*;
    use std::time::{Duration, Instant};
    use uuid::Uuid;

    struct NeverCancelled;

    impl super::super::context::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            1024,
            1024,
        )
        .expect("valid connector request context")
    }

    fn resource() -> ConnectorMvRefreshResourceIdentity {
        ConnectorMvRefreshResourceIdentity::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            Uuid::from_u128(0x1234),
        )
        .unwrap()
    }

    fn generation() -> ConnectorMvPublicationFenceGeneration {
        ConnectorMvPublicationFenceGeneration::try_new("cluster-a", 1, 1, [7u8; 32]).unwrap()
    }

    fn version(snapshot_id: i64) -> ConnectorCommittedVersion {
        ConnectorCommittedVersion::try_new(Bytes::from_static(b"v"), Some(snapshot_id)).unwrap()
    }

    fn summary() -> ConnectorMvAttemptSummary {
        ConnectorMvAttemptSummary::try_new(
            ConnectorMvRefreshAttemptId::new(),
            generation(),
            Some(version(300)),
            2,
        )
        .unwrap()
    }

    fn request(page_size: usize) -> ConnectorMvAttemptDiscoveryRequest {
        ConnectorMvAttemptDiscoveryRequest {
            table: ConnectorTableHandle::try_new(
                ConnectorInstanceId::parse("ice").unwrap(),
                Bytes::from_static(b"table"),
            )
            .unwrap(),
            resource: resource(),
            page_size,
            continuation: None,
            context: context(),
        }
    }

    #[test]
    fn discovery_requests_are_bounded() {
        request(64).validate().unwrap();
        assert!(
            request(0).validate().is_err(),
            "an unbounded page is what turns startup into a full catalog scan"
        );
        assert!(
            request(MAX_CONNECTOR_MV_ATTEMPT_PAGE_ITEMS + 1)
                .validate()
                .is_err(),
            "a caller must not be able to raise the contract bound"
        );
    }

    #[test]
    fn a_complete_page_cannot_also_be_truncated() {
        // The whole point of `complete` is that it is safe to act on. A page that
        // claims completeness while carrying a continuation or a stop reason
        // would let a caller delete artifacts it never enumerated.
        ConnectorMvAttemptPage::try_new(
            resource(),
            vec![summary()],
            Some(version(100)),
            Some(generation()),
            None,
            true,
            None,
        )
        .unwrap();

        assert!(
            ConnectorMvAttemptPage::try_new(
                resource(),
                vec![],
                None,
                None,
                Some(ConnectorMvAttemptContinuation::try_new(Bytes::from_static(b"c")).unwrap()),
                true,
                None,
            )
            .is_err(),
            "complete + continuation is contradictory"
        );
        assert!(
            ConnectorMvAttemptPage::try_new(
                resource(),
                vec![],
                None,
                None,
                None,
                true,
                Some(ConnectorMvAttemptScanLimit::StorageUnavailable),
            )
            .is_err(),
            "complete + a stop reason is contradictory"
        );
    }

    #[test]
    fn an_incomplete_page_must_say_why_it_stopped() {
        // An empty incomplete page is the dangerous case: without a reason it is
        // indistinguishable from "this target has no attempts".
        assert!(
            ConnectorMvAttemptPage::try_new(resource(), vec![], None, None, None, false, None)
                .is_err(),
            "an incomplete scan with no reason could be read as 'no attempts'"
        );

        ConnectorMvAttemptPage::try_new(
            resource(),
            vec![],
            None,
            None,
            None,
            false,
            Some(ConnectorMvAttemptScanLimit::StorageUnavailable),
        )
        .unwrap();
    }

    #[test]
    fn only_a_budget_stop_is_resumable() {
        let continuation =
            ConnectorMvAttemptContinuation::try_new(Bytes::from_static(b"cursor")).unwrap();

        ConnectorMvAttemptPage::try_new(
            resource(),
            vec![summary()],
            None,
            None,
            Some(continuation.clone()),
            false,
            Some(ConnectorMvAttemptScanLimit::PageBudgetExhausted),
        )
        .unwrap();

        for limit in [
            ConnectorMvAttemptScanLimit::StorageUnavailable,
            ConnectorMvAttemptScanLimit::UnknownEvidenceVersion,
            ConnectorMvAttemptScanLimit::ContinuationExpired,
        ] {
            assert!(
                ConnectorMvAttemptPage::try_new(
                    resource(),
                    vec![],
                    None,
                    None,
                    Some(continuation.clone()),
                    false,
                    Some(limit),
                )
                .is_err(),
                "{limit:?} must not look resumable"
            );
        }
    }

    #[test]
    fn continuations_are_bounded_and_redacted() {
        assert!(ConnectorMvAttemptContinuation::try_new(Bytes::new()).is_err());
        assert!(
            ConnectorMvAttemptContinuation::try_new(Bytes::from(vec![
                0u8;
                MAX_CONNECTOR_MV_ATTEMPT_CONTINUATION_BYTES
                    + 1
            ]))
            .is_err()
        );

        let continuation =
            ConnectorMvAttemptContinuation::try_new(Bytes::from_static(b"secret-cursor")).unwrap();
        let rendered = format!("{continuation:?}");
        assert!(
            !rendered.contains("secret-cursor") && rendered.contains("payload_len"),
            "a continuation must not print its provider-private contents: {rendered}"
        );
    }

    #[test]
    fn attempt_summaries_carry_stable_identity_not_a_refresh_id() {
        let summary = summary();
        assert_eq!(summary.generation(), &generation());
        assert_eq!(summary.evidence_version(), 2);
        assert_eq!(summary.staged_version(), Some(&version(300)));

        assert!(
            ConnectorMvAttemptSummary::try_new(
                ConnectorMvRefreshAttemptId::new(),
                generation(),
                None,
                0,
            )
            .is_err(),
            "an unversioned summary cannot be told apart from a legacy one"
        );
    }
}
