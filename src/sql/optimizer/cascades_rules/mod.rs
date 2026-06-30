//! Memo/Cascades rule registration.

pub(crate) mod equivalence_predicate;
pub(crate) mod implement;
pub(crate) mod join_associativity;
pub(crate) mod join_commutativity;
pub(crate) mod multi_join_reorder;
pub(crate) mod mv_rewrite;
pub(crate) mod push_topn_through_join;
pub(crate) mod push_topn_to_preagg;
pub(crate) mod sort_limit_to_top_n;
pub(crate) mod split_aggregate;
pub(crate) mod split_distinct_agg;
pub(crate) mod split_top_n;
pub(crate) mod topn_compactness;

use super::rule::Rule;

/// Returns all implementation rules (logical -> physical).
pub(crate) fn all_implementation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(implement::ScanToPhysical),
        Box::new(implement::FilterToPhysical),
        Box::new(implement::ProjectToPhysical),
        Box::new(implement::JoinToHashJoin),
        Box::new(implement::JoinToNestLoop),
        Box::new(implement::AggToHashAgg),
        Box::new(implement::SortToPhysical),
        Box::new(implement::LimitToPhysical),
        Box::new(implement::AssertOneRowToPhysical),
        Box::new(implement::TopNToPhysical), // NEW
        Box::new(implement::WindowToPhysical),
        Box::new(implement::CTEAnchorToPhysical),
        Box::new(implement::CTEProduceToPhysical),
        Box::new(implement::CTEConsumeToPhysical),
        Box::new(implement::RepeatToPhysical),
        Box::new(implement::ChangeEventExpandToPhysical),
        Box::new(implement::UnionToPhysical),
        Box::new(implement::IntersectToPhysical),
        Box::new(implement::ExceptToPhysical),
        Box::new(implement::ValuesToPhysical),
        Box::new(implement::GenerateSeriesToPhysical),
        Box::new(implement::TableFunctionToPhysical),
        Box::new(implement::DecodeToPhysical),
        Box::new(implement::AggregateStateMergeToPhysical),
        Box::new(split_distinct_agg::SplitDistinctAgg),
    ]
}

/// Returns all transformation rules (logical -> logical).
pub(crate) fn all_transformation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
        Box::new(equivalence_predicate::InnerJoinEquivalencePredicateRule),
        Box::new(sort_limit_to_top_n::SortLimitToTopN),
        Box::new(split_aggregate::SplitAggregateRule),
        Box::new(split_top_n::SplitTopN),
        Box::new(push_topn_to_preagg::PushDownTopNToPreAgg),
        Box::new(topn_compactness::MergeConsecutiveTopN),
        Box::new(topn_compactness::RemoveRedundantSortUnderTopN),
        Box::new(topn_compactness::PushTopNIntoScan),
        Box::new(topn_compactness::PushTopNThroughProject),
        Box::new(push_topn_through_join::PushTopNThroughJoin),
        Box::new(topn_compactness::PushTopNThroughSetOp),
    ]
}
