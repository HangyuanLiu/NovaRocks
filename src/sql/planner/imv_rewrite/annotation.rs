//! Single-tenant extension payload for the IMV rewrite pipeline.
//!
//! `RewriteContext::set_extension::<T>()` stores one `Arc<dyn Any + Send + Sync>`.
//! IMV needs both the MV rewrite context handle and a per-pipeline annotation;
//! both ride inside `ImvExtension` so the single slot is sufficient.

use std::sync::Arc;

use crate::engine::mv::partition::PartitionDerivationSpec;
use crate::engine::mv::refresh_context::IcebergMvRewriteContext;

/// IMV-pipeline-level plan annotations, populated by rewrite rules and
/// returned to the refresh driver via `ImvRewriteOutcome.annotation`.
#[derive(Clone, Debug, Default)]
pub(crate) struct ImvPlanAnnotation {
    /// Partition derivation outcome. `None` means the derivation stage did
    /// not run or did not match (non-aggregate shapes in P1, or the rule was
    /// disabled via `disable_optimizer_rules`).
    pub partition: Option<ImvPartitionAnnotation>,
}

/// Plan-time partition derivation outcome (umbrella spec §4.2).
///
/// This is the *plan-time* sibling of the runtime result
/// [`crate::engine::mv::partition::AffectedTargetPartitions`]: `Derivable`
/// records that a spec can be resolved (the rule attaches it), whereas the
/// runtime type later evaluates that spec over delta chunks into concrete
/// partition keys. The naming mirrors that split — `NotDerivable` is a
/// plan-time "this plan shape cannot yield a spec" verdict, distinct from the
/// runtime `AffectedTargetPartitions::NotDerived` "no keys were produced".
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ImvPartitionAnnotation {
    /// The target contract has no partition spec — pruning is a no-op.
    Unpartitioned,
    /// One spec for non-union shapes; one per branch for union families (P2).
    Derivable { specs: Vec<PartitionDerivationSpec> },
    /// The plan shape cannot yield a partition spec (e.g. non-pure lineage or
    /// an unsupported transform). Recorded, never fatal in v1 (policy is
    /// `BestEffort`, spec D5).
    NotDerivable { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_annotation_has_no_partition_outcome() {
        let annotation = ImvPlanAnnotation::default();
        assert!(annotation.partition.is_none());
    }
}

/// Single value stored in `RewriteContext::set_extension`. Bundles the IMV
/// rewrite context handle with the per-pipeline annotation.
#[derive(Clone, Debug)]
pub(crate) struct ImvExtension {
    pub mv_ctx: Arc<IcebergMvRewriteContext>,
    pub annotation: ImvPlanAnnotation,
}
