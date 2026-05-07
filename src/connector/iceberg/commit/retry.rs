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

//! OCC (Optimistic Concurrency Control) retry helper for Iceberg commit operations.
//!
//! Provides [`commit_with_retry`] and [`is_retryable_commit_conflict`] for use by
//! both schema-evolution DDL and snapshot-lifecycle maintenance commands (EXPIRE /
//! REWRITE MANIFESTS).

/// Whether an iceberg-rust commit error represents a transient table-requirement
/// conflict that warrants a retry (after re-loading the table). Network / IO /
/// data-invalid / programmer errors are non-retryable.
pub fn is_retryable_commit_conflict(err: &iceberg::Error) -> bool {
    use iceberg::ErrorKind;
    match err.kind() {
        ErrorKind::CatalogCommitConflicts => true,
        ErrorKind::PreconditionFailed => {
            let msg = format!("{err}").to_ascii_lowercase();
            msg.contains("assertcurrentschemaidmatch")
                || msg.contains("assertlastassignedfieldidmatch")
                || msg.contains("assertrefsnapshotidmatch")
        }
        _ => false,
    }
}

pub const COMMIT_RETRY_MAX_ATTEMPTS: usize = 3;
pub const COMMIT_RETRY_BACKOFF_MS: [u64; 3] = [10, 100, 500];

/// Run an iceberg commit closure with up to `COMMIT_RETRY_MAX_ATTEMPTS` attempts,
/// retrying only on `is_retryable_commit_conflict` errors with a fixed exponential
/// backoff. Each attempt receives its zero-based index so the caller can re-load
/// the table and rebuild the action against the latest metadata.
///
/// On non-retryable error: returns immediately on the first attempt.
/// On exhausted retries: returns an error including "after N attempts".
///
/// TODO(cancellation): this helper has no cancellation hook today because
/// neither the schema-evolution DDL path nor the snapshot-lifecycle
/// maintenance path carries a QueryContext through to here. Add a check
/// before the sleep when cancellation is plumbed in.
pub async fn commit_with_retry<F, Fut>(mut do_attempt: F) -> Result<(), String>
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = Result<(), iceberg::Error>>,
{
    let mut last_err: Option<iceberg::Error> = None;
    for attempt in 0..COMMIT_RETRY_MAX_ATTEMPTS {
        match do_attempt(attempt).await {
            Ok(()) => return Ok(()),
            Err(e) if is_retryable_commit_conflict(&e) => {
                last_err = Some(e);
                if attempt + 1 < COMMIT_RETRY_MAX_ATTEMPTS {
                    tokio::time::sleep(std::time::Duration::from_millis(
                        COMMIT_RETRY_BACKOFF_MS[attempt],
                    ))
                    .await;
                }
            }
            Err(e) => {
                return Err(format!("iceberg commit error: {e}"));
            }
        }
    }
    let detail = last_err
        .map(|e| format!("{e}"))
        .unwrap_or_else(|| "no error captured".to_string());
    Err(format!(
        "iceberg commit conflict after {} attempts due to concurrent table commits: {detail}",
        COMMIT_RETRY_MAX_ATTEMPTS
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classifies_current_schema_id_mismatch() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::PreconditionFailed,
            "Requirement failed: AssertCurrentSchemaIdMatch{current_schema_id=2}",
        );
        assert!(is_retryable_commit_conflict(&e));
    }

    #[test]
    fn retryable_classifies_last_assigned_field_id_mismatch() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::PreconditionFailed,
            "Requirement failed: AssertLastAssignedFieldIdMatch{last_assigned_field_id=12}",
        );
        assert!(is_retryable_commit_conflict(&e));
    }

    #[test]
    fn retryable_classifies_ref_snapshot_id_mismatch() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::PreconditionFailed,
            "Requirement failed: AssertRefSnapshotIdMatch{ref=main, snapshot_id=null}",
        );
        assert!(is_retryable_commit_conflict(&e));
    }

    #[test]
    fn retryable_classifies_catalog_commit_conflicts_kind() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::CatalogCommitConflicts,
            "concurrent commit",
        );
        assert!(is_retryable_commit_conflict(&e));
    }

    #[test]
    fn retryable_rejects_unrelated_io_error() {
        let e = iceberg::Error::new(iceberg::ErrorKind::Unexpected, "connection refused");
        assert!(!is_retryable_commit_conflict(&e));
    }

    #[test]
    fn retryable_rejects_data_invalid_error() {
        let e = iceberg::Error::new(
            iceberg::ErrorKind::DataInvalid,
            "schema rebuild failed: column already exists",
        );
        assert!(!is_retryable_commit_conflict(&e));
    }

    #[test]
    fn retryable_rejects_precondition_with_unrelated_message() {
        let e = iceberg::Error::new(iceberg::ErrorKind::PreconditionFailed, "table is read-only");
        assert!(!is_retryable_commit_conflict(&e));
    }

    #[tokio::test]
    async fn commit_with_retry_succeeds_on_first_attempt() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = AtomicUsize::new(0);
        let res = commit_with_retry(|_attempt| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Ok::<(), iceberg::Error>(()) }
        })
        .await;
        assert!(res.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn commit_with_retry_succeeds_after_one_conflict() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = AtomicUsize::new(0);
        let res = commit_with_retry(|_attempt| {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    Err(iceberg::Error::new(
                        iceberg::ErrorKind::PreconditionFailed,
                        "Requirement failed: AssertCurrentSchemaIdMatch{...}",
                    ))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert!(res.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn commit_with_retry_stops_at_max_attempts() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = AtomicUsize::new(0);
        let res = commit_with_retry(|_attempt| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::CatalogCommitConflicts,
                    "concurrent commit detected",
                ))
            }
        })
        .await;
        let err = res.unwrap_err();
        assert!(err.contains("after 3 attempts"));
        assert!(err.to_lowercase().contains("concurrent"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn commit_with_retry_does_not_retry_non_conflict_error() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = AtomicUsize::new(0);
        let res = commit_with_retry(|_attempt| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "connection refused",
                ))
            }
        })
        .await;
        assert!(res.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn commit_with_retry_passes_attempt_index_to_closure() {
        use std::sync::{Arc, Mutex};
        let seen: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_inner = seen.clone();
        let res = commit_with_retry(move |attempt| {
            seen_inner.lock().unwrap().push(attempt);
            async move {
                if attempt < 2 {
                    Err(iceberg::Error::new(
                        iceberg::ErrorKind::PreconditionFailed,
                        "Requirement failed: AssertCurrentSchemaIdMatch{...}",
                    ))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert!(res.is_ok());
        assert_eq!(*seen.lock().unwrap(), vec![0, 1, 2]);
    }

    // ---------- Phase D: invariant tests for spec §5.5 ----------
    //
    // These tests assert behavior the spec §5.5 calls out as invariants of
    // the schema-update retry path:
    //   1. Persistent state unchanged on every failure path (by construction
    //      with closure injection: if the closure never reaches commit, the
    //      catalog never sees a write attempt).
    //   2. Retry eventually succeeds after a transient concurrent commit.
    //   3. Retry stops at MAX_ATTEMPTS with a clear "after N attempts" error.
    //   4. Non-retryable errors short-circuit the loop.
    //   5. Backoff durations sum to a known bound.

    #[tokio::test]
    async fn commit_failure_returns_after_three_attempts() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = AtomicUsize::new(0);
        let res = commit_with_retry(|_attempt| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::PreconditionFailed,
                    "Requirement failed: AssertCurrentSchemaIdMatch{...}",
                ))
            }
        })
        .await;
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        let err = res.unwrap_err();
        assert!(err.contains("after 3 attempts"), "actual: {err}");
    }

    #[tokio::test]
    async fn commit_retry_eventually_succeeds_after_concurrent_commit() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = AtomicUsize::new(0);
        let res = commit_with_retry(|_attempt| {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    Err(iceberg::Error::new(
                        iceberg::ErrorKind::PreconditionFailed,
                        "Requirement failed: AssertCurrentSchemaIdMatch{current_schema_id=2}",
                    ))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert!(res.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn commit_retry_uses_correct_backoff_durations() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Instant;
        let attempts = AtomicUsize::new(0);
        let start = Instant::now();
        let _res = commit_with_retry(|_attempt| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::PreconditionFailed,
                    "Requirement failed: AssertCurrentSchemaIdMatch{...}",
                ))
            }
        })
        .await;
        let elapsed = start.elapsed();
        // Sum of the first two backoffs is 10ms + 100ms = 110ms (no sleep after
        // the final attempt). Allow generous slop for CI scheduling.
        assert!(
            elapsed >= std::time::Duration::from_millis(105),
            "elapsed {elapsed:?} should be >= 105ms (sum of 10ms + 100ms backoffs)"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "elapsed {elapsed:?} should be < 2s (no extra trailing sleep)"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn non_retryable_error_short_circuits_the_loop() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let attempts = AtomicUsize::new(0);
        let res = commit_with_retry(|_attempt| {
            attempts.fetch_add(1, Ordering::SeqCst);
            async {
                Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "connection refused",
                ))
            }
        })
        .await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let err = res.unwrap_err();
        assert!(
            !err.contains("after 3 attempts"),
            "non-retryable error must not be reported as exhausted retries: {err}"
        );
        assert!(err.contains("connection refused"));
    }

    #[tokio::test]
    async fn commit_retry_attempt_index_starts_at_zero() {
        use std::sync::Mutex;
        let seen: Mutex<Vec<usize>> = Mutex::new(Vec::new());
        let _res: Result<(), String> = commit_with_retry(|attempt| {
            seen.lock().unwrap().push(attempt);
            async move {
                if attempt < 1 {
                    Err(iceberg::Error::new(
                        iceberg::ErrorKind::PreconditionFailed,
                        "Requirement failed: AssertCurrentSchemaIdMatch{...}",
                    ))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(*seen.lock().unwrap(), vec![0, 1]);
    }
}
