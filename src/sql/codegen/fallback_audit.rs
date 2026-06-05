//! Non-fatal counters for temporary name-based codegen fallback usage.
//!
//! G1 P2 uses these counters to prove that planner/codegen gaps no longer
//! depend on semantic name binding before P3 removes the fallback paths.

#[cfg(test)]
use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering};

static COLUMN_REF_NAME_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static DISPLAY_EXPR_NAME_FALLBACKS: AtomicU64 = AtomicU64::new(0);
static AGGREGATE_DISPLAY_NAME_FALLBACKS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static ISOLATED_AUDIT: RefCell<Option<FallbackAuditSnapshot>> = const { RefCell::new(None) };
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FallbackAuditSnapshot {
    pub(crate) column_ref_name_fallbacks: u64,
    pub(crate) display_expr_name_fallbacks: u64,
    pub(crate) aggregate_display_name_fallbacks: u64,
}

pub(crate) fn record_column_ref_name_fallback() {
    #[cfg(test)]
    if record_in_isolated_audit(|audit| audit.column_ref_name_fallbacks += 1) {
        return;
    }
    COLUMN_REF_NAME_FALLBACKS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_display_expr_name_fallback() {
    #[cfg(test)]
    if record_in_isolated_audit(|audit| audit.display_expr_name_fallbacks += 1) {
        return;
    }
    DISPLAY_EXPR_NAME_FALLBACKS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_aggregate_display_name_fallback() {
    #[cfg(test)]
    if record_in_isolated_audit(|audit| audit.aggregate_display_name_fallbacks += 1) {
        return;
    }
    AGGREGATE_DISPLAY_NAME_FALLBACKS.fetch_add(1, Ordering::Relaxed);
}

#[allow(dead_code)]
pub(crate) fn snapshot() -> FallbackAuditSnapshot {
    FallbackAuditSnapshot {
        column_ref_name_fallbacks: COLUMN_REF_NAME_FALLBACKS.load(Ordering::Relaxed),
        display_expr_name_fallbacks: DISPLAY_EXPR_NAME_FALLBACKS.load(Ordering::Relaxed),
        aggregate_display_name_fallbacks: AGGREGATE_DISPLAY_NAME_FALLBACKS.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn reset_global_counters() {
    COLUMN_REF_NAME_FALLBACKS.store(0, Ordering::Relaxed);
    DISPLAY_EXPR_NAME_FALLBACKS.store(0, Ordering::Relaxed);
    AGGREGATE_DISPLAY_NAME_FALLBACKS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn run_with_isolated_audit<F, R>(f: F) -> (R, FallbackAuditSnapshot)
where
    F: FnOnce() -> R,
{
    struct IsolatedAuditGuard;

    impl Drop for IsolatedAuditGuard {
        fn drop(&mut self) {
            ISOLATED_AUDIT.with(|cell| {
                *cell.borrow_mut() = None;
            });
        }
    }

    ISOLATED_AUDIT.with(|cell| {
        let mut current = cell.borrow_mut();
        assert!(
            current.is_none(),
            "nested isolated fallback audit sessions are unsupported"
        );
        *current = Some(FallbackAuditSnapshot::default());
    });
    let guard = IsolatedAuditGuard;
    let result = f();
    let audit = ISOLATED_AUDIT.with(|cell| {
        cell.borrow()
            .as_ref()
            .copied()
            .expect("isolated fallback audit session")
    });
    drop(guard);
    (result, audit)
}

#[cfg(test)]
fn record_in_isolated_audit(record: impl FnOnce(&mut FallbackAuditSnapshot)) -> bool {
    ISOLATED_AUDIT.with(|cell| {
        let mut current = cell.borrow_mut();
        let Some(audit) = current.as_mut() else {
            return false;
        };
        record(audit);
        true
    })
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn assert_no_codegen_name_fallbacks_after<F>(f: F)
where
    F: FnOnce(),
{
    let ((), audit) = run_with_isolated_audit(f);
    assert_eq!(
        audit,
        FallbackAuditSnapshot::default(),
        "codegen name fallback audit must stay empty"
    );
}
