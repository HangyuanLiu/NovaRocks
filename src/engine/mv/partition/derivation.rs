//! Unified partition-derivation library for Iceberg MV refresh.
//!
//! `AffectedTargetPartitions` is the single result type for every affected-
//! partition source (plan-time manifest planning and delta-chunk evaluation).
//! `NotDerived` carries an explicit reason; consumers decide via
//! `PartitionPruningPolicy` (BestEffort in v1, spec D5) whether that means
//! "no pruning" or "fail the refresh".

use std::collections::BTreeSet;

use crate::engine::mv::partition::MvPartitionKey;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedTargetPartitions {
    Unpartitioned,
    Known { partitions: BTreeSet<MvPartitionKey> },
    NotDerived { reason: String },
}

impl AffectedTargetPartitions {
    pub(crate) fn known<I: IntoIterator<Item = MvPartitionKey>>(partitions: I) -> Self {
        Self::Known {
            partitions: partitions.into_iter().collect(),
        }
    }

    pub(crate) fn not_derived(reason: impl Into<String>) -> Self {
        Self::NotDerived {
            reason: reason.into(),
        }
    }

    pub(crate) fn not_derived_reason(&self) -> Option<&str> {
        match self {
            Self::NotDerived { reason } => Some(reason.as_str()),
            Self::Unpartitioned | Self::Known { .. } => None,
        }
    }

    pub(crate) fn is_not_derived(&self) -> bool {
        matches!(self, Self::NotDerived { .. })
    }

    pub(crate) fn partition_count(&self) -> usize {
        match self {
            Self::Unpartitioned | Self::NotDerived { .. } => 0,
            Self::Known { partitions } => partitions.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::mv::partition::{MvPartitionKey, MvPartitionKeyField, MvPartitionValue};

    fn key(value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "region".to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn affected_target_partitions_known_dedupes_and_sorts() {
        let result = AffectedTargetPartitions::known([key("b"), key("a"), key("a")]);
        let AffectedTargetPartitions::Known { partitions } = result else {
            panic!("expected Known");
        };
        assert_eq!(
            partitions.into_iter().collect::<Vec<_>>(),
            vec![key("a"), key("b")]
        );
    }

    #[test]
    fn affected_target_partitions_not_derived_preserves_reason() {
        let result = AffectedTargetPartitions::not_derived("join MV planning not implemented");
        assert_eq!(
            result.not_derived_reason(),
            Some("join MV planning not implemented")
        );
        assert!(result.is_not_derived());
        assert_eq!(result.partition_count(), 0);
    }

    #[test]
    fn affected_target_partitions_unpartitioned_is_not_not_derived() {
        assert!(!AffectedTargetPartitions::Unpartitioned.is_not_derived());
        assert_eq!(AffectedTargetPartitions::Unpartitioned.partition_count(), 0);
    }
}
