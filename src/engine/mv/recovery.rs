//! Lake-native IMV crash-recovery primitives.
//!
//! Recovery converges each `__nova_mv_refresh_*` staging branch by the lineage
//! position of its snapshot relative to `main`, using W3a's snapshot-summary
//! marker — never the SQLite ledger. This module holds the pure classifier;
//! the driver that enumerates branches and applies drop/rollback lives in
//! `iceberg_refresh.rs`.

/// Disposition of a staging branch relative to the MV table's `main`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StagingDisposition {
    /// Staging snapshot IS main's current snapshot (published, uncleaned).
    PublishedCurrent,
    /// Staging snapshot is a main ancestor (published, then superseded).
    Superseded,
    /// Staging snapshot is off main's lineage (never published; roll back).
    Diverged,
}

/// Classify a staging branch snapshot against main's ancestor chain.
///
/// `main_ancestors` is main's snapshot ids reachable via parent links, with
/// `main_ancestors[0]` == main's current snapshot (empty when main has no
/// snapshot). `marker_matches` is whether the staging snapshot's summary
/// carries this MV-refresh's marker.
///
/// Fail-loud when the staging snapshot equals main's current but the marker
/// does NOT match — that means an external writer moved `main` onto a
/// non-MV-refresh snapshot, violating the sole-writer invariant.
pub(crate) fn classify_staging_branch(
    main_ancestors: &[i64],
    staging_snapshot_id: i64,
    marker_matches: bool,
) -> Result<StagingDisposition, String> {
    match main_ancestors.first() {
        Some(&current) if current == staging_snapshot_id => {
            if marker_matches {
                Ok(StagingDisposition::PublishedCurrent)
            } else {
                Err(format!(
                    "MV recovery: staging snapshot {staging_snapshot_id} is main's current \
                     snapshot but its refresh marker does not match; main was moved by an \
                     external writer (NovaRocks must be the sole writer)"
                ))
            }
        }
        _ if main_ancestors.contains(&staging_snapshot_id) => Ok(StagingDisposition::Superseded),
        _ => Ok(StagingDisposition::Diverged),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_published_current() {
        assert_eq!(
            classify_staging_branch(&[300, 200, 100], 300, true),
            Ok(StagingDisposition::PublishedCurrent)
        );
    }

    #[test]
    fn classifies_superseded_ancestor() {
        assert_eq!(
            classify_staging_branch(&[300, 200, 100], 200, true),
            Ok(StagingDisposition::Superseded)
        );
    }

    #[test]
    fn classifies_diverged_off_lineage() {
        assert_eq!(
            classify_staging_branch(&[300, 200, 100], 999, true),
            Ok(StagingDisposition::Diverged)
        );
    }

    #[test]
    fn current_with_marker_mismatch_is_fail_loud() {
        let err = classify_staging_branch(&[300, 200, 100], 300, false)
            .expect_err("marker mismatch on main's current snapshot must fail loud");
        assert!(
            err.contains("external writer"),
            "error message should call out an external writer, got: {err}"
        );
    }

    #[test]
    fn empty_main_lineage_is_diverged() {
        assert_eq!(
            classify_staging_branch(&[], 300, true),
            Ok(StagingDisposition::Diverged)
        );
    }

    #[test]
    fn superseded_marker_mismatch_still_superseded() {
        assert_eq!(
            classify_staging_branch(&[300, 200, 100], 200, false),
            Ok(StagingDisposition::Superseded)
        );
    }
}
