pub(crate) mod derivation;
pub(crate) mod key;
pub(crate) mod mapping;
pub(crate) mod planner;

pub(crate) use derivation::AffectedTargetPartitions;
// P2/P3 partition-pruning asset; the sole P1 caller (pre-cutover apply path)
// is removed in this commit. Live consumer lands in PR-3 / P2 (umbrella spec §5.1).
#[allow(unused_imports)]
pub(crate) use derivation::{
    AffectedPartitionError, BoundPartitionField, PartitionDerivationField, PartitionDerivationSpec,
    bind_spec_to_aggregate_layout, evaluate_partition_spec, resolve_partition_derivation_spec,
};
pub(crate) use key::{
    MvPartitionKey, MvPartitionKeyField, MvPartitionValue, TargetPartitionFilter,
};
