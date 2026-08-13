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

//! Lake-first enumeration of a target's refresh attempts.
//!
//! Existing recovery enumerates from the StateStore ledger. This is the entry
//! point that does not: it asks the provider what the lake holds for a stable
//! target resource, pages until the provider proves exhaustiveness, and hands
//! the result to classification.
//!
//! It is additive on purpose. Replacing ledger-driven enumeration is a separate
//! change to startup ordering, and doing both at once would make a regression in
//! either indistinguishable from a regression in the other.
//!
//! The paging loop is where a subtle bug would hide, so two things are explicit:
//! it is bounded by a page budget so an unreadable target cannot spin, and it
//! treats *any* non-exhaustive outcome as "this target is unreconciled" rather
//! than working with a partial list.

use novarocks_spi::connector::{
    ConnectorControlBinding, ConnectorMvAttemptDiscoveryRequest, ConnectorMvAttemptPage,
    ConnectorMvAttemptScanLimit, ConnectorMvAttemptSummary, ConnectorMvRefreshResourceIdentity,
    ConnectorRequestContext, ConnectorTableHandle,
};

/// Upper bound on pages fetched for one target in a single sweep.
///
/// A target that keeps reporting more work is suspicious, not merely large: it
/// bounds the damage from a provider whose continuation never converges.
pub(super) const MAX_DISCOVERY_PAGES: usize = 64;

/// What a full sweep of one target concluded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum TargetSweep {
    /// The provider proved it enumerated everything.
    Complete {
        attempts: Vec<ConnectorMvAttemptSummary>,
    },
    /// Enumeration could not be proven exhaustive. The attempts gathered so far
    /// are deliberately dropped: acting on a partial list is the failure this
    /// whole path exists to prevent.
    Unreconciled { reason: UnreconciledReason },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UnreconciledReason {
    /// The provider reported a scan limit it could not get past.
    ScanLimit(ConnectorMvAttemptScanLimit),
    /// The page budget ran out before the provider proved exhaustiveness.
    PageBudgetExhausted,
    /// A page claimed to be truncated but offered no way to continue.
    TruncatedWithoutContinuation,
    /// The provider's own call failed.
    ProviderUnavailable,
}

/// Accumulates pages for one target until exhaustiveness is proven.
///
/// `fetch` is a closure so the loop's decisions are testable without a provider:
/// the interesting behaviour is which outcomes are accepted as complete, and that
/// must be verifiable directly.
pub(super) fn sweep_target<F>(mut fetch: F) -> TargetSweep
where
    F: FnMut(Option<ConnectorMvAttemptPage>) -> Option<ConnectorMvAttemptPage>,
{
    let mut attempts: Vec<ConnectorMvAttemptSummary> = Vec::new();
    let mut previous: Option<ConnectorMvAttemptPage> = None;

    for _ in 0..MAX_DISCOVERY_PAGES {
        let Some(page) = fetch(previous.clone()) else {
            return TargetSweep::Unreconciled {
                reason: UnreconciledReason::ProviderUnavailable,
            };
        };
        attempts.extend(page.attempts().iter().cloned());

        if page.is_complete() {
            // Deduplicate across pages: a mid-scan change can surface the same
            // attempt twice, and a duplicate must never look like two attempts.
            attempts.sort_by_key(|summary| summary.attempt());
            attempts.dedup_by_key(|summary| summary.attempt());
            return TargetSweep::Complete { attempts };
        }
        if let Some(limit) = page.limit()
            && limit != ConnectorMvAttemptScanLimit::PageBudgetExhausted
        {
            // A storage failure, an undecodable entry, or an expired
            // continuation cannot be paged past.
            return TargetSweep::Unreconciled {
                reason: UnreconciledReason::ScanLimit(limit),
            };
        }
        if page.continuation().is_none() {
            return TargetSweep::Unreconciled {
                reason: UnreconciledReason::TruncatedWithoutContinuation,
            };
        }
        previous = Some(page);
    }

    TargetSweep::Unreconciled {
        reason: UnreconciledReason::PageBudgetExhausted,
    }
}

/// Sweeps one target through the provider's discovery capability.
///
/// This is the production path: it adapts the capability's paged calls to
/// [`sweep_target`], so the loop's rules -- bounded pages, non-exhaustive means
/// unreconciled, dedup by identity -- apply to real provider output rather than
/// only to fixtures.
///
/// A provider without the capability yields `ProviderUnavailable` rather than an
/// empty sweep. "This provider cannot enumerate attempts" and "this target has no
/// attempts" must never produce the same answer, since only the latter is safe to
/// act on.
pub(super) fn sweep_target_through_provider(
    binding: &ConnectorControlBinding,
    table: &ConnectorTableHandle,
    resource: &ConnectorMvRefreshResourceIdentity,
    context: &ConnectorRequestContext,
    page_size: usize,
) -> TargetSweep {
    let Some(discovery) = binding.mv_attempt_discovery() else {
        return TargetSweep::Unreconciled {
            reason: UnreconciledReason::ProviderUnavailable,
        };
    };
    sweep_target(|previous| {
        let continuation = previous.and_then(|page| page.continuation().cloned());
        discovery
            .discover_attempts(ConnectorMvAttemptDiscoveryRequest {
                table: table.clone(),
                resource: resource.clone(),
                page_size,
                continuation,
                context: context.clone(),
            })
            .ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorMvAttemptContinuation, ConnectorMvPublicationFenceGeneration,
        ConnectorMvRefreshAttemptId, ConnectorMvRefreshResourceIdentity, ConnectorProviderId,
    };
    use uuid::Uuid;

    fn resource() -> ConnectorMvRefreshResourceIdentity {
        ConnectorMvRefreshResourceIdentity::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            Uuid::from_u128(0x1234),
        )
        .unwrap()
    }

    fn summary(attempt: ConnectorMvRefreshAttemptId) -> ConnectorMvAttemptSummary {
        ConnectorMvAttemptSummary::try_new(
            attempt,
            ConnectorMvPublicationFenceGeneration::try_new("cluster-a", 1, 1, [7u8; 32]).unwrap(),
            None,
            2,
        )
        .unwrap()
    }

    fn page(
        attempts: Vec<ConnectorMvAttemptSummary>,
        complete: bool,
        limit: Option<ConnectorMvAttemptScanLimit>,
        continuation: bool,
    ) -> ConnectorMvAttemptPage {
        ConnectorMvAttemptPage::try_new(
            resource(),
            attempts,
            None,
            None,
            continuation.then(|| {
                ConnectorMvAttemptContinuation::try_new(Bytes::from_static(b"cursor")).unwrap()
            }),
            complete,
            limit,
        )
        .unwrap()
    }

    #[test]
    fn a_single_complete_page_finishes_the_sweep() {
        let attempt = ConnectorMvRefreshAttemptId::new();
        let sweep = sweep_target(|_| Some(page(vec![summary(attempt)], true, None, false)));

        match sweep {
            TargetSweep::Complete { attempts } => {
                assert_eq!(attempts.len(), 1);
                assert_eq!(attempts[0].attempt(), attempt);
            }
            other => panic!("expected a complete sweep, got {other:?}"),
        }
    }

    #[test]
    fn pages_accumulate_until_the_provider_proves_exhaustiveness() {
        let first = ConnectorMvRefreshAttemptId::new();
        let second = ConnectorMvRefreshAttemptId::new();
        let mut calls = 0;

        let sweep = sweep_target(|_| {
            calls += 1;
            Some(match calls {
                1 => page(
                    vec![summary(first)],
                    false,
                    Some(ConnectorMvAttemptScanLimit::PageBudgetExhausted),
                    true,
                ),
                _ => page(vec![summary(second)], true, None, false),
            })
        });

        match sweep {
            TargetSweep::Complete { attempts } => assert_eq!(attempts.len(), 2),
            other => panic!("expected a complete sweep, got {other:?}"),
        }
        assert_eq!(calls, 2);
    }

    #[test]
    fn an_attempt_seen_twice_across_pages_counts_once() {
        // A mid-scan change can surface the same attempt on two pages. Counting
        // it twice would inflate the backlog and could classify one attempt
        // under two dispositions.
        let repeated = ConnectorMvRefreshAttemptId::new();
        let mut calls = 0;

        let sweep = sweep_target(|_| {
            calls += 1;
            Some(match calls {
                1 => page(
                    vec![summary(repeated)],
                    false,
                    Some(ConnectorMvAttemptScanLimit::PageBudgetExhausted),
                    true,
                ),
                _ => page(vec![summary(repeated)], true, None, false),
            })
        });

        match sweep {
            TargetSweep::Complete { attempts } => assert_eq!(attempts.len(), 1),
            other => panic!("expected a complete sweep, got {other:?}"),
        }
    }

    #[test]
    fn an_unpageable_limit_leaves_the_target_unreconciled() {
        for limit in [
            ConnectorMvAttemptScanLimit::StorageUnavailable,
            ConnectorMvAttemptScanLimit::UnknownEvidenceVersion,
            ConnectorMvAttemptScanLimit::ContinuationExpired,
        ] {
            let attempt = ConnectorMvRefreshAttemptId::new();
            // The page carries attempts, and they are deliberately discarded. It
            // carries no continuation because the contract forbids one for a
            // limit that cannot be paged past.
            let sweep =
                sweep_target(|_| Some(page(vec![summary(attempt)], false, Some(limit), false)));

            assert_eq!(
                sweep,
                TargetSweep::Unreconciled {
                    reason: UnreconciledReason::ScanLimit(limit)
                },
                "{limit:?} must not yield a usable attempt list"
            );
        }
    }

    #[test]
    fn a_truncated_page_without_a_continuation_is_unreconciled() {
        let sweep = sweep_target(|_| {
            Some(page(
                vec![],
                false,
                Some(ConnectorMvAttemptScanLimit::PageBudgetExhausted),
                false,
            ))
        });

        assert_eq!(
            sweep,
            TargetSweep::Unreconciled {
                reason: UnreconciledReason::TruncatedWithoutContinuation
            }
        );
    }

    #[test]
    fn a_never_converging_provider_cannot_spin_forever() {
        let mut calls = 0;
        let sweep = sweep_target(|_| {
            calls += 1;
            Some(page(
                vec![summary(ConnectorMvRefreshAttemptId::new())],
                false,
                Some(ConnectorMvAttemptScanLimit::PageBudgetExhausted),
                true,
            ))
        });

        assert_eq!(
            sweep,
            TargetSweep::Unreconciled {
                reason: UnreconciledReason::PageBudgetExhausted
            }
        );
        assert_eq!(calls, MAX_DISCOVERY_PAGES, "the budget must bound the loop");
    }

    #[test]
    fn a_provider_failure_is_unreconciled_not_empty() {
        // The dangerous misreading: a failed call producing an empty attempt list
        // that looks like a clean target.
        let sweep = sweep_target(|_| None);

        assert_eq!(
            sweep,
            TargetSweep::Unreconciled {
                reason: UnreconciledReason::ProviderUnavailable
            }
        );
    }
}
