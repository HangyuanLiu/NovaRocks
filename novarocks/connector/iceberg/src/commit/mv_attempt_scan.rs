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

//! Iceberg-side enumeration of the MV refresh attempts a target table holds.
//!
//! The catalog interaction is a thin wrapper; the decision this module exists to
//! make is pure, so it is expressed as a function over already-observed refs.
//! That matters because the interesting cases are all about *what a page is
//! allowed to claim* — and those must be testable without a live catalog.
//!
//! Two rules drive everything here:
//!
//! * **A ref we cannot decode is not an absence.** A staging ref whose snapshot
//!   is missing, carries no V2 provenance, or belongs to another target does not
//!   silently drop out of the result. It either becomes a legacy observation or
//!   forces the page to report itself incomplete. Dropping it would let a caller
//!   conclude "no attempts" and clean up data it never looked at.
//! * **Ordering is by attempt ID, not by ref name or discovery order.** Ref names
//!   are provider-private and reused; the attempt ID is UUIDv7, so ordering by it
//!   gives a stable, resumable cursor that survives refs being added or removed
//!   mid-scan.

use novarocks_spi::connector::{
    ConnectorMvAttemptScanLimit, ConnectorMvRefreshAttemptId, ConnectorMvRefreshResourceIdentity,
};

use super::mv_provenance::{MV_PROVENANCE_V2_VERSION, MvProvenanceV2};

/// One staging ref as observed on the target table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedAttemptRef {
    pub ref_name: String,
    pub snapshot_id: i64,
    /// `None` when the ref names a snapshot the metadata does not contain.
    pub snapshot_present: bool,
    /// Decoded V2 provenance, when the snapshot carried a well-formed record.
    pub provenance: Option<MvProvenanceV2>,
    /// `true` when the snapshot carried MV marker keys this provider could not
    /// decode as V2 — a legacy attempt, or a newer schema.
    pub undecodable_marker: bool,
}

/// What one attempt contributes to a discovery page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScannedAttempt {
    pub attempt: ConnectorMvRefreshAttemptId,
    pub ref_name: String,
    pub staged_snapshot_id: i64,
    pub provenance: MvProvenanceV2,
}

/// An attempt that exists but cannot be claimed.
///
/// These are surfaced rather than dropped so a caller can report an unresolved
/// artifact instead of treating the target as clean. Nothing here may be deleted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnclaimableAttemptRef {
    pub ref_name: String,
    pub snapshot_id: i64,
    pub reason: UnclaimableReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnclaimableReason {
    /// Pre-V2 identity, or a marker this provider version cannot interpret. It
    /// cannot be bound to a stable resource, so it is never auto-claimed.
    LegacyOrUnknownSchema,
    /// Well-formed V2, but for a different target's fence domain.
    ForeignResource,
    /// The ref points at a snapshot the metadata does not contain.
    MissingSnapshot,
    /// V2 provenance whose attempt ID is not a valid stable identity.
    MalformedAttemptIdentity,
}

/// The result of scanning one page worth of refs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptScanPage {
    pub attempts: Vec<ScannedAttempt>,
    pub unclaimable: Vec<UnclaimableAttemptRef>,
    /// Exclusive cursor for the next page, as an attempt ID.
    pub next_after: Option<ConnectorMvRefreshAttemptId>,
    pub complete: bool,
    pub limit: Option<ConnectorMvAttemptScanLimit>,
}

/// Selects one bounded page of attempts from the observed refs.
///
/// `after` is an exclusive cursor over attempt IDs. Because the cursor is an
/// attempt ID rather than a positional offset, refs appearing or disappearing
/// between pages shift nothing: a resumed scan returns exactly the attempts
/// ordered after the cursor, whatever else changed.
pub fn scan_attempt_page(
    observed: &[ObservedAttemptRef],
    resource: &ConnectorMvRefreshResourceIdentity,
    page_size: usize,
    after: Option<ConnectorMvRefreshAttemptId>,
) -> Result<AttemptScanPage, String> {
    if page_size == 0 {
        return Err("iceberg mv attempt scan: page size must be positive".to_string());
    }

    let mut claimable: Vec<ScannedAttempt> = Vec::new();
    let mut unclaimable: Vec<UnclaimableAttemptRef> = Vec::new();

    for entry in observed {
        if !entry.snapshot_present {
            unclaimable.push(UnclaimableAttemptRef {
                ref_name: entry.ref_name.clone(),
                snapshot_id: entry.snapshot_id,
                reason: UnclaimableReason::MissingSnapshot,
            });
            continue;
        }
        let Some(provenance) = &entry.provenance else {
            // No V2 record. Whether it carried an older marker or none at all,
            // it cannot be bound to a stable resource, so it is reported and
            // left alone.
            if entry.undecodable_marker {
                unclaimable.push(UnclaimableAttemptRef {
                    ref_name: entry.ref_name.clone(),
                    snapshot_id: entry.snapshot_id,
                    reason: UnclaimableReason::LegacyOrUnknownSchema,
                });
            }
            continue;
        };
        if provenance.provenance_version != MV_PROVENANCE_V2_VERSION {
            unclaimable.push(UnclaimableAttemptRef {
                ref_name: entry.ref_name.clone(),
                snapshot_id: entry.snapshot_id,
                reason: UnclaimableReason::LegacyOrUnknownSchema,
            });
            continue;
        }
        if provenance.target_table_uuid != resource.target_table_uuid().to_string() {
            unclaimable.push(UnclaimableAttemptRef {
                ref_name: entry.ref_name.clone(),
                snapshot_id: entry.snapshot_id,
                reason: UnclaimableReason::ForeignResource,
            });
            continue;
        }
        match decode_attempt_id(&provenance.attempt_id) {
            Some(attempt) => claimable.push(ScannedAttempt {
                attempt,
                ref_name: entry.ref_name.clone(),
                staged_snapshot_id: entry.snapshot_id,
                provenance: provenance.clone(),
            }),
            None => unclaimable.push(UnclaimableAttemptRef {
                ref_name: entry.ref_name.clone(),
                snapshot_id: entry.snapshot_id,
                reason: UnclaimableReason::MalformedAttemptIdentity,
            }),
        }
    }

    // Order by stable attempt ID so the cursor is meaningful across pages.
    claimable.sort_by_key(|scanned| scanned.attempt);
    claimable.dedup_by_key(|scanned| scanned.attempt);
    if let Some(cursor) = after {
        claimable.retain(|scanned| scanned.attempt > cursor);
    }

    let truncated = claimable.len() > page_size;
    if truncated {
        claimable.truncate(page_size);
    }
    let next_after = if truncated {
        claimable.last().map(|scanned| scanned.attempt)
    } else {
        None
    };

    // A page is only complete when nothing was truncated. Unclaimable refs do
    // not make it incomplete: they are reported facts, not gaps — the caller has
    // seen them and can decide, which is exactly what "complete" should mean.
    Ok(AttemptScanPage {
        attempts: claimable,
        unclaimable,
        next_after,
        complete: !truncated,
        limit: truncated.then_some(ConnectorMvAttemptScanLimit::PageBudgetExhausted),
    })
}

/// Decodes the hex attempt identity a V2 record carries.
fn decode_attempt_id(encoded: &str) -> Option<ConnectorMvRefreshAttemptId> {
    if encoded.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (index, slot) in bytes.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16).ok()?;
    }
    ConnectorMvRefreshAttemptId::try_from_bytes(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::mv_provenance::{MvPublicationV2Identity, RefreshTechnique};
    use novarocks_spi::connector::{
        ConnectorCommittedVersion, ConnectorMvPublicationFenceGeneration,
        ConnectorMvPublicationFenceReceipt, ConnectorMvPublicationPermit, ConnectorProviderId,
    };
    use uuid::Uuid;

    fn resource(uuid: u128) -> ConnectorMvRefreshResourceIdentity {
        ConnectorMvRefreshResourceIdentity::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            Uuid::from_u128(uuid),
        )
        .unwrap()
    }

    fn permit_for(
        uuid: u128,
        attempt: ConnectorMvRefreshAttemptId,
    ) -> ConnectorMvPublicationPermit {
        let generation =
            ConnectorMvPublicationFenceGeneration::try_new("cluster-a", 1, 1, [7u8; 32]).unwrap();
        let fence_version =
            ConnectorCommittedVersion::try_new(bytes::Bytes::from_static(b"fence"), Some(500))
                .unwrap();
        let receipt =
            ConnectorMvPublicationFenceReceipt::try_new(resource(uuid), generation, fence_version)
                .unwrap();
        ConnectorMvPublicationPermit::try_new(attempt, receipt).unwrap()
    }

    fn provenance_for(uuid: u128, attempt: ConnectorMvRefreshAttemptId) -> MvProvenanceV2 {
        MvProvenanceV2::new(
            &MvPublicationV2Identity::from_permit(&permit_for(uuid, attempt)),
            RefreshTechnique::Incremental,
            vec![],
            "fp".to_string(),
            1,
        )
    }

    fn staged(
        name: &str,
        snapshot_id: i64,
        attempt: ConnectorMvRefreshAttemptId,
    ) -> ObservedAttemptRef {
        ObservedAttemptRef {
            ref_name: name.to_string(),
            snapshot_id,
            snapshot_present: true,
            provenance: Some(provenance_for(0x1234, attempt)),
            undecodable_marker: false,
        }
    }

    #[test]
    fn a_full_scan_reports_every_attempt_and_claims_completeness() {
        let first = ConnectorMvRefreshAttemptId::new();
        let second = ConnectorMvRefreshAttemptId::new();
        let observed = vec![staged("a", 300, first), staged("b", 301, second)];

        let page = scan_attempt_page(&observed, &resource(0x1234), 8, None).unwrap();

        assert_eq!(page.attempts.len(), 2);
        assert!(page.complete && page.limit.is_none() && page.next_after.is_none());
        // Ordered by stable attempt ID, not by ref name or observation order.
        assert!(page.attempts[0].attempt < page.attempts[1].attempt);
    }

    #[test]
    fn truncation_is_resumable_and_never_claims_completeness() {
        let attempts: Vec<_> = (0..5).map(|_| ConnectorMvRefreshAttemptId::new()).collect();
        let observed: Vec<_> = attempts
            .iter()
            .enumerate()
            .map(|(index, attempt)| staged(&format!("r{index}"), 300 + index as i64, *attempt))
            .collect();

        let first = scan_attempt_page(&observed, &resource(0x1234), 2, None).unwrap();
        assert_eq!(first.attempts.len(), 2);
        assert!(!first.complete);
        assert_eq!(
            first.limit,
            Some(ConnectorMvAttemptScanLimit::PageBudgetExhausted)
        );
        let cursor = first
            .next_after
            .expect("a truncated page must be resumable");

        let second = scan_attempt_page(&observed, &resource(0x1234), 2, Some(cursor)).unwrap();
        assert_eq!(second.attempts.len(), 2);
        assert!(
            second
                .attempts
                .iter()
                .all(|scanned| scanned.attempt > cursor),
            "a resumed page must not repeat attempts already returned"
        );

        let third = scan_attempt_page(&observed, &resource(0x1234), 2, second.next_after).unwrap();
        assert_eq!(third.attempts.len(), 1);
        assert!(third.complete, "the final page proves exhaustiveness");
    }

    #[test]
    fn refs_appearing_or_vanishing_mid_scan_do_not_shift_the_cursor() {
        let attempts: Vec<_> = (0..4).map(|_| ConnectorMvRefreshAttemptId::new()).collect();
        let mut sorted = attempts.clone();
        sorted.sort();

        let observed: Vec<_> = attempts
            .iter()
            .enumerate()
            .map(|(index, attempt)| staged(&format!("r{index}"), 300 + index as i64, *attempt))
            .collect();
        let first = scan_attempt_page(&observed, &resource(0x1234), 2, None).unwrap();
        let cursor = first.next_after.unwrap();

        // Between pages, an unrelated attempt is added and an already-returned
        // one is removed. A positional cursor would skip or repeat; an
        // attempt-ID cursor cannot.
        let mut changed = observed.clone();
        changed.remove(0);
        changed.push(staged("late", 999, ConnectorMvRefreshAttemptId::new()));

        let second = scan_attempt_page(&changed, &resource(0x1234), 8, Some(cursor)).unwrap();
        assert!(
            second
                .attempts
                .iter()
                .all(|scanned| scanned.attempt > cursor),
            "every returned attempt must still be strictly after the cursor"
        );
        assert!(
            second.attempts.iter().any(|s| s.attempt == sorted[2]),
            "an attempt that was already pending must not be skipped"
        );
    }

    #[test]
    fn undecodable_and_foreign_refs_are_reported_never_dropped() {
        let attempt = ConnectorMvRefreshAttemptId::new();
        let observed = vec![
            staged("ours", 300, attempt),
            // A different target's fence domain.
            ObservedAttemptRef {
                ref_name: "foreign".to_string(),
                snapshot_id: 301,
                snapshot_present: true,
                provenance: Some(provenance_for(0x9999, ConnectorMvRefreshAttemptId::new())),
                undecodable_marker: false,
            },
            // Pre-V2 or newer-than-known marker.
            ObservedAttemptRef {
                ref_name: "legacy".to_string(),
                snapshot_id: 302,
                snapshot_present: true,
                provenance: None,
                undecodable_marker: true,
            },
            // Ref pointing at a snapshot the metadata does not contain.
            ObservedAttemptRef {
                ref_name: "dangling".to_string(),
                snapshot_id: 303,
                snapshot_present: false,
                provenance: None,
                undecodable_marker: false,
            },
        ];

        let page = scan_attempt_page(&observed, &resource(0x1234), 8, None).unwrap();

        assert_eq!(
            page.attempts.len(),
            1,
            "only our own V2 attempt is claimable"
        );
        let reasons: Vec<_> = page.unclaimable.iter().map(|u| u.reason).collect();
        assert!(reasons.contains(&UnclaimableReason::ForeignResource));
        assert!(reasons.contains(&UnclaimableReason::LegacyOrUnknownSchema));
        assert!(reasons.contains(&UnclaimableReason::MissingSnapshot));
        assert_eq!(
            page.unclaimable.len(),
            3,
            "nothing may be silently dropped: a dropped ref reads as 'no attempt'"
        );
        assert!(
            page.complete,
            "reported unclaimable refs are facts the caller has seen, not gaps"
        );
    }

    #[test]
    fn malformed_attempt_identity_is_unclaimable_rather_than_guessed() {
        let mut provenance = provenance_for(0x1234, ConnectorMvRefreshAttemptId::new());
        provenance.attempt_id = "not-hex".to_string();
        let observed = vec![ObservedAttemptRef {
            ref_name: "broken".to_string(),
            snapshot_id: 300,
            snapshot_present: true,
            provenance: Some(provenance),
            undecodable_marker: false,
        }];

        let page = scan_attempt_page(&observed, &resource(0x1234), 8, None).unwrap();

        assert!(page.attempts.is_empty());
        assert_eq!(
            page.unclaimable[0].reason,
            UnclaimableReason::MalformedAttemptIdentity
        );
    }

    #[test]
    fn an_empty_target_is_distinguishable_from_an_unreadable_one() {
        // A genuinely empty target: complete, with nothing reported. This is the
        // only shape a caller may treat as "there is nothing here".
        let page = scan_attempt_page(&[], &resource(0x1234), 8, None).unwrap();
        assert!(page.attempts.is_empty() && page.unclaimable.is_empty());
        assert!(page.complete && page.limit.is_none());

        assert!(
            scan_attempt_page(&[], &resource(0x1234), 0, None).is_err(),
            "an unbounded page request is rejected outright"
        );
    }
}
