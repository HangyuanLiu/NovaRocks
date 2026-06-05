//! Capability property algebra for Iceberg IMV refresh.
//!
//! This module synthesizes a `RefreshFragmentProperty` (a `TargetIdentity` +
//! `StateContract` + base refs + branch count + join key count) recursively
//! over an analyzed MV query, then lowers it into the executable
//! [`ImvRefreshContract`] via [`RefreshFragmentProperty::into_refresh_contract`].
//! This is now the single source of contract derivation: the old flat
//! classifier in [`crate::engine::mv::refresh_contract`] has been removed and
//! `derive_imv_refresh_contract` delegates here.
//!
//! The synthesis MIRRORS the structural acceptance/rejection of the former flat
//! classifier (non-inner / non-equi joins, non-UNION-ALL set ops, metadata /
//! delta / generate-series / unnest / CTE relations, DISTINCT, HAVING,
//! ROLLUP/CUBE/GROUPING SETS, ORDER BY / LIMIT / OFFSET, WITH, unsupported /
//! non-deterministic expressions, etc.) but emits a compositional property
//! instead of a closed enum of named strategies.
//!
//! The property algebra accepts a strictly larger set of UNION ALL shapes than
//! the classifier did: it admits any UNION ALL whose branches synthesize the
//! same `(TargetIdentity kind, StateContract kind)` (with matching aggregate
//! arities), including previously-rejected composed branches such as
//! `Aggregate(Join(..))`. `into_refresh_contract` then narrows the property to
//! the set whose apply key is representable. For every shape the legacy
//! classifier supported, that narrowing emits a byte-for-byte equivalent
//! contract; on top of that it additionally admits a `BranchScoped(GroupRowId)`
//! UNION ALL of *composed* aggregate branches (aggregate-over-join / fan-in),
//! whose apply key is still `BranchUtf8` (A3). Truly-unrepresentable shapes
//! (e.g. a UNION ALL of joins) are still rejected. See
//! [`RefreshFragmentProperty::into_refresh_contract`] for the precise narrowing.

use crate::connector::starrocks::table::model::IcebergTableRef;
use crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType;
use crate::engine::mv::refresh_contract::{
    AggregateRefreshContract, ApplyKeyContract, BranchRefreshContract, ImvRefreshContract,
    JoinRefreshContract, RefreshStrategy,
};
use crate::engine::mv::refresh_driver::BaseSnapshotPolicy;
use crate::meta::repository::mv_contract::{ApplyKeySource, MvSchemaContract};
use crate::sql::analysis::{
    BinOp, ExprKind, JoinKind, QueryBody, Relation, ResolvedQuery, ResolvedSelect, ResolvedSetOp,
    SetOpKind, SortItem, TypedExpr,
};
use crate::sql::catalog::ScanSource;

/// The row-identity contract synthesized for a refresh fragment. This describes
/// *what a single output row is identified by* so the apply path can compute a
/// stable apply key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TargetIdentity {
    /// A single base-table row (a direct scan).
    BaseRowId,
    /// A joined row, identified by the composition of its two input
    /// identities.
    JoinRowKey(Box<TargetIdentity>, Box<TargetIdentity>),
    /// An aggregated group row, identified by the listed group-key output
    /// names.
    GroupRowId(Vec<String>),
    /// A branch-scoped identity (UNION ALL): the underlying per-branch identity
    /// tagged with a branch discriminant. Construction flattens nested
    /// `BranchScoped` so that `BranchScoped(BranchScoped(x)) == BranchScoped(x)`.
    BranchScoped(Box<TargetIdentity>),
}

impl TargetIdentity {
    /// Wrap an identity in `BranchScoped`, flattening an already branch-scoped
    /// inner identity so wrapping is idempotent.
    fn branch_scoped(inner: TargetIdentity) -> TargetIdentity {
        match inner {
            TargetIdentity::BranchScoped(_) => inner,
            other => TargetIdentity::BranchScoped(Box::new(other)),
        }
    }

    /// A stable kind label used for UNION ALL homogeneity comparison. Two
    /// identities are "same kind" iff their labels match. For `BranchScoped`
    /// and `JoinRowKey` only the top-level constructor participates; nested
    /// shape is intentionally ignored to match the property-kind contract.
    fn kind_label(&self) -> &'static str {
        match self {
            TargetIdentity::BaseRowId => "BaseRowId",
            TargetIdentity::JoinRowKey(_, _) => "JoinRowKey",
            TargetIdentity::GroupRowId(_) => "GroupRowId",
            TargetIdentity::BranchScoped(_) => "BranchScoped",
        }
    }
}

// ---------------------------------------------------------------------------
// RefreshCapabilities — derived from the persisted MvSchemaContract
// ---------------------------------------------------------------------------

/// A compact row-identity discriminant used at refresh time. Unlike
/// `TargetIdentity`, which carries full query-analysis payload (group-key
/// names, nested join children), this enum only retains the top-level
/// constructor the driver needs to choose the apply-key path. It is
/// reconstructed from the persisted `MvSchemaContract` by
/// `from_schema_contract`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefreshIdentity {
    /// A single base-table row (projection / filter over a single scan, or a
    /// single-scan aggregate).
    BaseRowId,
    /// A joined row (two-table inner equi-join, with or without aggregation).
    JoinRowKey,
    /// An aggregated group row (aggregate over a single scan or a fan-in
    /// union). Group-key names are not needed at refresh; the apply key is
    /// derived from the persisted column name.
    GroupRowId,
    /// A branch-scoped composite identity (UNION ALL). The inner identity is
    /// the per-branch discriminant, reconstructed from
    /// `BranchUnionContract::inner_apply_key_source`.
    BranchScoped(Box<RefreshIdentity>),
}

/// Refresh-time capabilities derived directly from a persisted
/// `MvSchemaContract`. This replaces the legacy `RefreshStrategy` enum for
/// the purposes of driver dispatch (Phase 3 / B2+): all driver code that
/// currently matches on `RefreshStrategy` can be rewritten to match on
/// `RefreshCapabilities` fields instead.
///
/// Constructed by `from_schema_contract`; the parity test in this module
/// asserts that every field is consistent with the legacy `RefreshStrategy`
/// the driver currently derives from the same contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshCapabilities {
    /// How the driver treats the set of base-table snapshots.
    pub(crate) snapshot_policy: BaseSnapshotPolicy,
    /// Whether the MV carries incremental aggregate state columns.
    pub(crate) has_agg_state: bool,
    /// The row-identity discriminant reconstructed from the persisted contract.
    pub(crate) identity: RefreshIdentity,
    /// The name of the hidden apply-key column in the target Iceberg table.
    pub(crate) apply_key_column: String,
    /// The value type of the apply-key column, used to drive the merge-sink
    /// apply-key reader.
    pub(crate) apply_key_value_type: ApplyKeyValueType,
}

impl RefreshCapabilities {
    /// Derive `RefreshCapabilities` from a persisted `MvSchemaContract`.
    ///
    /// Thin associated-function alias for the module-level
    /// [`from_schema_contract`]; this is the entry point the refresh driver
    /// dispatches on (Phase 3 / B2+).
    pub(crate) fn from_schema_contract(
        c: &MvSchemaContract,
    ) -> Result<RefreshCapabilities, String> {
        from_schema_contract(c)
    }
}

/// Derive `RefreshCapabilities` from a persisted `MvSchemaContract`.
///
/// Returns `Err(String)` for contract shapes that do not correspond to any
/// supported strategy (e.g. joint+branch, which is rejected at CREATE time
/// and should never be persisted).
pub(crate) fn from_schema_contract(c: &MvSchemaContract) -> Result<RefreshCapabilities, String> {
    let has_join = c.join.is_some();
    let has_agg = c.aggregate.is_some();
    let has_branch = c.branch.is_some();
    let has_extra_bases = !c.bases.is_empty();

    // Snapshot policy: mirrors stored_refresh_strategy_for_plan + the
    // per-strategy base-snapshot-policy chosen by the driver wrappers.
    //   join                     -> JoinPairPartialInitialSkip
    //   multiple bases or branch -> AllBasesRequired
    //   single base, no branch   -> SingleBase
    let snapshot_policy = if has_join {
        BaseSnapshotPolicy::JoinPairPartialInitialSkip
    } else if has_extra_bases || has_branch {
        BaseSnapshotPolicy::AllBasesRequired
    } else {
        BaseSnapshotPolicy::SingleBase
    };

    // Row identity at refresh time.
    let identity = if has_branch {
        let branch = c.branch.as_ref().unwrap();
        let inner = apply_key_source_to_refresh_identity(branch.inner_apply_key_source);
        RefreshIdentity::BranchScoped(Box::new(inner))
    } else {
        apply_key_source_to_refresh_identity(c.target.hidden_apply_key.source)
    };

    // Apply key column name from the persisted target contract.
    let apply_key_column = c.target.hidden_apply_key.column_name.clone();

    // Apply key value type: derived from (source, branch).
    //   BaseRowId  + no branch -> Int64
    //   BaseRowId  + branch    -> BranchInt64
    //   JoinRowKey + no branch -> Utf8   (composite row-key string)
    //   GroupRowId + no branch -> Utf8   (group-key hash string)
    //   GroupRowId + branch    -> BranchUtf8
    //
    // Cross-checked against ApplyKeyContract::{projection_filter,
    // union_projection_filter, join_projection_filter, aggregate_group_row,
    // join_aggregate_group_row, branch_union_aggregate_group_row}.
    let apply_key_value_type = match (c.target.hidden_apply_key.source, has_branch) {
        (ApplyKeySource::BaseRowId, false) => ApplyKeyValueType::Int64,
        (ApplyKeySource::BaseRowId, true) => ApplyKeyValueType::BranchInt64,
        (ApplyKeySource::JoinRowKey, _) => ApplyKeyValueType::Utf8,
        (ApplyKeySource::GroupRowId, false) => ApplyKeyValueType::Utf8,
        (ApplyKeySource::GroupRowId, true) => ApplyKeyValueType::BranchUtf8,
    };

    // Validate the contract shape is one the driver supports (mirrors the
    // catch-all error in stored_refresh_strategy_for_plan). The tuple
    // mirrors stored_refresh_strategy_for_plan's (join, agg, branch) match:
    // join contracts legitimately have non-empty `bases` (the two join sides),
    // so `extra_bases` is not part of the gating predicate here — it is only
    // needed to disambiguate FanIn (bases non-empty) vs Single (bases empty)
    // within the (false, true, false) aggregate-without-join arm.
    match (has_join, has_agg, has_branch) {
        (false, false, false) // ProjectionFilter
        | (true,  false, false) // JoinProjectionFilter
        | (false, false, true)  // UnionProjectionFilter
        | (false, true,  false) // SingleAggregate or FanInAggregate
        | (true,  true,  false) // JoinAggregate
        | (false, true,  true)  // BranchUnionAggregate
        => {}
        _ => {
            return Err(format!(
                "unsupported schema contract shape \
                 (join={has_join}, agg={has_agg}, branch={has_branch})"
            ));
        }
    }

    Ok(RefreshCapabilities {
        snapshot_policy,
        has_agg_state: has_agg,
        identity,
        apply_key_column,
        apply_key_value_type,
    })
}

/// Convert an `ApplyKeySource` discriminant to a `RefreshIdentity` leaf.
fn apply_key_source_to_refresh_identity(source: ApplyKeySource) -> RefreshIdentity {
    match source {
        ApplyKeySource::BaseRowId => RefreshIdentity::BaseRowId,
        ApplyKeySource::JoinRowKey => RefreshIdentity::JoinRowKey,
        ApplyKeySource::GroupRowId => RefreshIdentity::GroupRowId,
    }
}

/// The aggregation-state contract synthesized for a refresh fragment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StateContract {
    /// No incremental aggregate state — projection / filter / join only.
    Stateless,
    /// Aggregate state with the given number of group keys and aggregate
    /// outputs.
    AggregateState {
        group_key_count: usize,
        aggregate_count: usize,
    },
}

impl StateContract {
    /// A stable kind label used for UNION ALL homogeneity comparison. The
    /// aggregate arities are intentionally NOT part of the kind label — branch
    /// arity compatibility is enforced separately in `derive_from_set_operation`
    /// (mirroring the legacy "compatible aggregate branch contracts" rejection).
    fn kind_label(&self) -> &'static str {
        match self {
            StateContract::Stateless => "Stateless",
            StateContract::AggregateState { .. } => "AggregateState",
        }
    }
}

/// The shared structural shape of the branches of a UNION ALL. Carried up so
/// the contract mapping can gate which branch-bearing strategy each union
/// admits without re-walking the branch queries.
///
/// Private to this module: it is an internal detail of the property synthesis
/// and the [`RefreshFragmentProperty::into_refresh_contract`] narrowing, and is
/// not read by any consumer of the (otherwise `pub(crate)`) property.
///
/// The legacy flat classifier only admitted two branch shapes per set
/// operation: a UNION ALL of plain `ProjectionFilter` branches (-> the legacy
/// `UnionProjection`) and a UNION ALL of *simple* `SingleAggregate` branches
/// (-> the legacy `BranchUnionAggregate`). Any composed branch — a join, a
/// fan-in aggregate, a nested/subquery union, an aggregate over a join — landed
/// in the classifier's catch-all rejection. `BranchShape` encodes which of
/// those cases the synthesized branches correspond to; A3 now admits a
/// `Composed` *aggregate* branch union (`into_refresh_contract`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BranchShape {
    /// Every branch is a plain projection/filter over a single scan
    /// (legacy `DerivedStructure::ProjectionFilter`). Eligible for
    /// `UnionProjectionFilter` and, under an aggregate, `FanInAggregate`.
    SimpleScan,
    /// Every branch is a *simple* aggregate over a single scan
    /// (legacy `DerivedStructure::SingleAggregate`). Eligible for
    /// `BranchUnionAggregate`.
    SimpleAggregate,
    /// At least one branch is composed (a join, a fan-in aggregate, an
    /// aggregate over a join, or a nested/subquery union). The legacy
    /// classifier rejected every such branch shape.
    Composed,
}

/// The synthesized capability property of a refresh fragment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshFragmentProperty {
    pub(crate) identity: TargetIdentity,
    pub(crate) state: StateContract,
    pub(crate) base_refs: Vec<IcebergTableRef>,
    /// `Some(n)` iff the identity top is `BranchScoped`, where `n` is the
    /// number of UNION ALL branches; `None` otherwise.
    pub(crate) branch_count: Option<usize>,
    /// `Some(k)` iff *this fragment's own top* is a two-table inner equi-join,
    /// or an aggregate sitting directly over one, where `k` is the number of
    /// equi-join predicates; `None` otherwise. It describes only the fragment's
    /// own top-level join — it is never set on a `BranchScoped` property (a
    /// UNION ALL top is not itself a join), and a join *inside* a UNION ALL
    /// branch is recorded on that branch's own property, not propagated up here.
    /// Carried alongside the identity — rather than only inside the
    /// `JoinRowKey` identity — because aggregation drops the join identity to
    /// `GroupRowId` yet the `JoinAggregate` contract still needs the join key
    /// count.
    pub(crate) join_key_count: Option<usize>,
    /// The shared per-branch shape of the UNION ALL this fragment's identity
    /// derives from, or `None` when no UNION ALL is involved. It is set on a
    /// `BranchScoped` property (the shape of its direct branches) and inherited
    /// by an aggregate synthesized directly over a UNION ALL (the shape of the
    /// union the aggregate fans in over). The contract mapping uses it to gate
    /// which branch shapes each branch-bearing strategy admits — accepting a
    /// composed aggregate branch union while still rejecting composed
    /// projection/filter and fan-in shapes (see
    /// [`RefreshFragmentProperty::into_refresh_contract`]). Private: it is an
    /// internal narrowing input and is not read by property consumers.
    branch_shape: Option<BranchShape>,
}

impl RefreshFragmentProperty {
    /// Lower this synthesized property into the executable
    /// [`ImvRefreshContract`]. This is the single source of contract derivation
    /// (the legacy flat classifier has been removed): it (1) validates base-ref
    /// arity by deduplicating the per-scan base refs and checking the distinct
    /// count against the structure, and (2) maps the `(identity, state,
    /// branch_count, join_key_count, branch_shape)` tuple onto the same
    /// `ApplyKeyContract` / `RefreshStrategy` the classifier chose.
    ///
    /// The property algebra accepts a strictly larger set of query shapes than
    /// the legacy classifier (composed UNION ALL branches). This mapping is the
    /// *single narrowing point*: it narrows back to the set whose apply key is
    /// representable, so the emitted contract and its rejections stay aligned
    /// with what the apply path can compute. The `branch_shape` carried up from
    /// the set operation gates the three branch-bearing strategies:
    ///   - `UnionProjectionFilter` requires `BranchShape::SimpleScan` branches;
    ///   - `FanInAggregate` requires the aggregated union to be
    ///     `BranchShape::SimpleScan`;
    ///   - `BranchUnionAggregate` admits either `BranchShape::SimpleAggregate`
    ///     *or* `BranchShape::Composed` branches.
    ///
    /// What this NOW allows (A3 loosening): a `BranchScoped(GroupRowId)` union
    /// whose branches are composed aggregates — an aggregate over a join
    /// (`Agg(a JOIN b)`) or an aggregate over a fan-in union. Every such branch
    /// still produces a per-branch group-row identity, so the composite apply
    /// key is `BranchUtf8` (branch discriminant + per-group utf8), which the
    /// apply path already represents. CREATE therefore succeeds and persists a
    /// branch+aggregate contract whose per-branch lineage is built recursively
    /// (see `build_iceberg_mv_schema_contract`).
    ///
    /// What this STILL rejects: shapes whose apply key has no representation —
    /// e.g. a top-level `JoinRowKey(GroupRowId, ..)` (a join over aggregated
    /// inputs) or a `BranchScoped(JoinRowKey)` (UNION ALL of joins), both of
    /// which fall into the catch-all `_` arm below. A composed
    /// projection/filter branch union is also still rejected: the
    /// `UnionProjectionFilter` and `FanInAggregate` arms keep requiring
    /// `BranchShape::SimpleScan`, since A3 only enables the representable
    /// branch-union-aggregate composition.
    pub(crate) fn into_refresh_contract(self) -> Result<ImvRefreshContract, String> {
        match self.expected_distinct_base_refs() {
            // Exact arity is known (single scan, join, or a branch union whose
            // branches are simple per-scan structures): enforce it, mirroring
            // the legacy `validate_base_ref_contract` rejection of self-joins
            // and duplicate fan-ins.
            Some(expected) => validate_distinct_base_ref_arity(&self.base_refs, expected)?,
            // Composed branch union (A3): each branch carries more than one
            // base, so "branch_count distinct bases" is the wrong invariant.
            // The exact per-branch base arity is validated structurally when the
            // schema contract is built per branch; here we only require that at
            // least one Iceberg base was resolved.
            None => {
                if self.base_refs.is_empty() {
                    return Err(
                        "Iceberg IMV refresh contract requires at least one Iceberg base table ref"
                            .to_string(),
                    );
                }
            }
        }

        let RefreshFragmentProperty {
            identity,
            state,
            base_refs,
            branch_count,
            join_key_count,
            branch_shape,
        } = self;

        match (&identity, &state) {
            // Projection / filter over a single scan.
            (TargetIdentity::BaseRowId, StateContract::Stateless) => Ok(ImvRefreshContract {
                strategy: RefreshStrategy::ProjectionFilter,
                base_refs,
                apply_key: ApplyKeyContract::projection_filter(),
                aggregate: None,
                join: None,
                branch: None,
            }),
            // Two-table inner equi-join projection / filter.
            (TargetIdentity::JoinRowKey(_, _), StateContract::Stateless) => {
                let join_key_count = join_key_count.ok_or_else(|| {
                    "Iceberg IMV refresh contract internal error: join identity without a join key count".to_string()
                })?;
                Ok(ImvRefreshContract {
                    strategy: RefreshStrategy::JoinProjectionFilter,
                    base_refs,
                    apply_key: ApplyKeyContract::join_projection_filter(),
                    aggregate: None,
                    join: Some(JoinRefreshContract { join_key_count }),
                    branch: None,
                })
            }
            // UNION ALL of projection / filter branches.
            (TargetIdentity::BranchScoped(inner), StateContract::Stateless)
                if matches!(inner.as_ref(), TargetIdentity::BaseRowId) =>
            {
                // The legacy classifier's `UnionProjection` accepted only a
                // UNION ALL of plain `ProjectionFilter` branches. Reaching this
                // arm already pins every branch to `(BaseRowId, Stateless)`, and
                // such a branch maps to `BranchShape::SimpleScan`, so under the
                // current synthesis `branch_shape` is expected to be
                // `Some(SimpleScan)` here. This guard is therefore mostly
                // defense-in-depth: it is the backstop for any future synthesis
                // that lets a branch present a `BaseRowId` identity while still
                // being `Composed` (e.g. a flattened nested/subquery union), for
                // which we keep the legacy projection/filter-only rejection.
                if branch_shape != Some(BranchShape::SimpleScan) {
                    return Err(
                        "Iceberg IMV refresh contract only supports UNION ALL of projection/filter branches or aggregate branches"
                            .to_string(),
                    );
                }
                let branch_count = branch_count.ok_or_else(|| {
                    "Iceberg IMV refresh contract internal error: branch-scoped identity without a branch count".to_string()
                })?;
                Ok(ImvRefreshContract {
                    strategy: RefreshStrategy::UnionProjectionFilter,
                    base_refs,
                    apply_key: ApplyKeyContract::union_projection_filter(),
                    aggregate: None,
                    join: None,
                    branch: Some(BranchRefreshContract { branch_count }),
                })
            }
            // Aggregate group row, dispatched by what it sits over.
            (
                TargetIdentity::GroupRowId(_),
                StateContract::AggregateState {
                    group_key_count,
                    aggregate_count,
                },
            ) => {
                let aggregate = AggregateRefreshContract {
                    group_key_count: *group_key_count,
                    aggregate_count: *aggregate_count,
                };
                match (branch_count, join_key_count) {
                    // Aggregate directly over a UNION ALL (fan-in). The legacy
                    // classifier only built `FanInAggregate` over a
                    // `UnionProjection` (a union of plain scans/projections); an
                    // aggregate over a union of joins or nested unions hit its
                    // catch-all rejection. The inherited branch shape encodes
                    // the union's per-branch shape, so reject anything but a
                    // union of simple scans.
                    (Some(branch_count), None) => {
                        if branch_shape != Some(BranchShape::SimpleScan) {
                            return Err(
                                "Iceberg IMV refresh contract only supports UNION ALL of projection/filter branches or aggregate branches"
                                    .to_string(),
                            );
                        }
                        Ok(ImvRefreshContract {
                            strategy: RefreshStrategy::FanInAggregate,
                            base_refs,
                            apply_key: ApplyKeyContract::aggregate_group_row(),
                            aggregate: Some(aggregate),
                            join: None,
                            branch: Some(BranchRefreshContract { branch_count }),
                        })
                    }
                    // Aggregate directly over a two-table inner equi-join.
                    (None, Some(join_key_count)) => Ok(ImvRefreshContract {
                        strategy: RefreshStrategy::JoinAggregate,
                        base_refs,
                        apply_key: ApplyKeyContract::join_aggregate_group_row(),
                        aggregate: Some(aggregate),
                        join: Some(JoinRefreshContract { join_key_count }),
                        branch: None,
                    }),
                    // Aggregate directly over a single scan.
                    (None, None) => Ok(ImvRefreshContract {
                        strategy: RefreshStrategy::SingleAggregate,
                        base_refs,
                        apply_key: ApplyKeyContract::aggregate_group_row(),
                        aggregate: Some(aggregate),
                        join: None,
                        branch: None,
                    }),
                    (Some(_), Some(_)) => Err(
                        "Iceberg IMV refresh contract does not support aggregate over a joined union"
                            .to_string(),
                    ),
                }
            }
            // UNION ALL of aggregate branches.
            (TargetIdentity::BranchScoped(inner), StateContract::AggregateState { .. })
                if matches!(inner.as_ref(), TargetIdentity::GroupRowId(_)) =>
            {
                // A3 loosening point. Every branch produces a per-branch
                // group-row identity, so the composite apply key is `BranchUtf8`
                // regardless of how each branch is computed underneath; that key
                // is representable, so we admit both:
                //   - `BranchShape::SimpleAggregate` — aggregate directly over a
                //     single scan (the only shape the legacy classifier accepted
                //     here), and
                //   - `BranchShape::Composed` — an aggregate over a join
                //     (`Agg(a JOIN b)`) or over a fan-in union.
                // Anything that is NOT one of those (e.g. an inherited
                // `SimpleScan`/`None` shape, which cannot occur under an
                // aggregate branch scope) is rejected. The branch top is never
                // itself a join, so `join_key_count` is always `None` here — the
                // discriminator is the per-branch shape, not the branch scope's
                // own join key count.
                if !matches!(
                    branch_shape,
                    Some(BranchShape::SimpleAggregate) | Some(BranchShape::Composed)
                ) {
                    return Err(
                        "Iceberg IMV refresh contract only supports UNION ALL of projection/filter branches or aggregate branches"
                            .to_string(),
                    );
                }
                let branch_count = branch_count.ok_or_else(|| {
                    "Iceberg IMV refresh contract internal error: branch-scoped identity without a branch count".to_string()
                })?;
                let StateContract::AggregateState {
                    group_key_count,
                    aggregate_count,
                } = state
                else {
                    unreachable!("aggregate state matched above");
                };
                Ok(ImvRefreshContract {
                    strategy: RefreshStrategy::BranchUnionAggregate,
                    base_refs,
                    apply_key: ApplyKeyContract::branch_union_aggregate_group_row(),
                    aggregate: Some(AggregateRefreshContract {
                        group_key_count,
                        aggregate_count,
                    }),
                    join: None,
                    branch: Some(BranchRefreshContract { branch_count }),
                })
            }
            // Every other property shape (e.g. UNION ALL of joins) is outside
            // the legacy-supported set.
            _ => Err(format!(
                "Iceberg IMV refresh contract does not support the synthesized property shape \
                 (identity={identity:?}, state={state:?})"
            )),
        }
    }

    /// The number of *distinct* Iceberg base table refs this structure
    /// requires, or `None` when no exact count can be imposed. `Some(1)` for a
    /// single scan or single aggregate, `Some(2)` for a two-table join, and
    /// `Some(branch_count)` for a UNION ALL whose branches are simple per-scan
    /// structures. Mirrors the legacy `validate_base_ref_contract` expectations.
    ///
    /// Returns `None` for a *composed* branch union (A3): there each branch
    /// carries more than one base, so the per-branch "one base per branch"
    /// assumption behind `branch_count` no longer holds and the exact distinct
    /// count is validated structurally during contract building instead.
    fn expected_distinct_base_refs(&self) -> Option<usize> {
        if let Some(branch_count) = self.branch_count {
            if self.branch_shape == Some(BranchShape::Composed) {
                return None;
            }
            return Some(branch_count);
        }
        if self.join_key_count.is_some() {
            return Some(2);
        }
        Some(1)
    }

    /// Classify this property as a single UNION ALL branch, mapping it onto the
    /// [`BranchShape`] the legacy flat classifier would have assigned. A branch
    /// is legacy-simple only when it is a bare projection/filter over a single
    /// scan (`SimpleScan`) or a bare aggregate over a single scan
    /// (`SimpleAggregate`); anything carrying a join key count or its own branch
    /// count (an aggregate over a join, a fan-in aggregate, or a nested/subquery
    /// union) is `Composed`, exactly the set of branch shapes the classifier's
    /// `derive_from_set_operation` catch-all rejected.
    fn branch_shape_as_union_branch(&self) -> BranchShape {
        if self.join_key_count.is_some() || self.branch_count.is_some() {
            return BranchShape::Composed;
        }
        match (&self.identity, &self.state) {
            (TargetIdentity::BaseRowId, StateContract::Stateless) => BranchShape::SimpleScan,
            (TargetIdentity::GroupRowId(_), StateContract::AggregateState { .. }) => {
                BranchShape::SimpleAggregate
            }
            // A join branch (`JoinRowKey`) or any other shape is composed; the
            // legacy classifier rejected such UNION ALL branches.
            _ => BranchShape::Composed,
        }
    }
}

/// Deduplicate `base_refs` (order-preserving) and require the distinct count to
/// equal `expected`. Ports the legacy `validate_base_ref_contract` rejection so
/// self-joins (`T JOIN T` → 1 distinct base for a 2-side structure) and
/// duplicate-base fan-ins are rejected.
fn validate_distinct_base_ref_arity(
    base_refs: &[IcebergTableRef],
    expected: usize,
) -> Result<(), String> {
    let mut distinct: Vec<&IcebergTableRef> = Vec::new();
    for base_ref in base_refs {
        if !distinct.contains(&base_ref) {
            distinct.push(base_ref);
        }
    }
    if distinct.len() != expected {
        return Err(format!(
            "Iceberg IMV refresh contract requires {expected} distinct Iceberg base table refs, got {}",
            distinct.len()
        ));
    }
    Ok(())
}

/// Synthesize the refresh-fragment property for an analyzed MV query.
///
/// Recursively walks the query mirroring the structural validation of the flat
/// classifier (`derive_from_query` and friends) while emitting a compositional
/// property instead of a named strategy enum. Returns a precise `Err(String)`
/// for every shape the classifier rejects.
pub(crate) fn derive_fragment_property(
    query: &ResolvedQuery,
) -> Result<RefreshFragmentProperty, String> {
    validate_query_wrapper(query)?;
    derive_from_query_body(&query.body)
}

fn validate_query_wrapper(query: &ResolvedQuery) -> Result<(), String> {
    if !query.local_cte_ids.is_empty() {
        return Err("Iceberg IMV refresh contract does not support WITH queries".to_string());
    }
    if !query.order_by.is_empty() || query.limit.is_some() || query.offset.is_some() {
        return Err(
            "Iceberg IMV refresh contract does not support ORDER BY, LIMIT, or OFFSET".to_string(),
        );
    }
    Ok(())
}

fn derive_from_query_body(body: &QueryBody) -> Result<RefreshFragmentProperty, String> {
    match body {
        QueryBody::Select(select) => derive_from_select(select),
        QueryBody::SetOperation(set_op) => derive_from_set_operation(set_op),
        QueryBody::Values(_) => {
            Err("Iceberg IMV refresh contract does not support VALUES queries".to_string())
        }
    }
}

fn derive_from_select(select: &ResolvedSelect) -> Result<RefreshFragmentProperty, String> {
    if select.distinct {
        return Err("Iceberg IMV refresh contract does not support SELECT DISTINCT".to_string());
    }
    if select.having.is_some() || select.repeat.is_some() {
        return Err(
            "Iceberg IMV refresh contract does not support HAVING, ROLLUP, CUBE, or GROUPING SETS"
                .to_string(),
        );
    }

    let has_aggregate = select.has_aggregation || !select.group_by.is_empty();
    if has_aggregate {
        let group_key_count = select.group_by.len();
        if group_key_count == 0 {
            return Err(
                "Iceberg IMV refresh contract requires aggregate queries to use a non-empty GROUP BY"
                    .to_string(),
            );
        }
        if let Some(filter) = &select.filter {
            validate_projection_filter_expr(filter)?;
        }
        for group_key in &select.group_by {
            validate_projection_filter_expr(group_key)?;
        }
        let aggregate_count = count_aggregate_projection_outputs(select)?;
        if aggregate_count == 0 {
            return Err(
                "Iceberg IMV refresh contract requires at least one aggregate output".to_string(),
            );
        }
        let child = derive_from_optional_relation(select.from.as_ref())?;
        let group_key_output_names = group_key_output_names(select);
        Ok(RefreshFragmentProperty {
            identity: TargetIdentity::GroupRowId(group_key_output_names),
            state: StateContract::AggregateState {
                group_key_count,
                aggregate_count,
            },
            base_refs: child.base_refs,
            branch_count: child.branch_count,
            // Aggregation drops the child identity, but the join key count (if
            // the child was a join) is inherited so a `JoinAggregate` contract
            // can still recover it.
            join_key_count: child.join_key_count,
            // Inherit the child's branch shape so an aggregate directly over a
            // UNION ALL (fan-in) carries the union's per-branch shape. The
            // contract mapping's fan-in arm uses it to admit only a fan-in over
            // a union of plain scans (legacy `FanInAggregate`).
            branch_shape: child.branch_shape,
        })
    } else {
        validate_projection_filter_exprs(select)?;
        let child = derive_from_optional_relation(select.from.as_ref())?;
        // Mirror refresh_contract.rs:382-392: projection/filter over an
        // aggregate subquery is rejected. In the property world every aggregate
        // subquery synthesizes AggregateState, so key on that.
        if matches!(child.state, StateContract::AggregateState { .. }) {
            return Err(
                "Iceberg IMV refresh contract does not support projection/filter over aggregate subqueries"
                    .to_string(),
            );
        }
        // Projection / filter passthrough: identity, state, refs, and branch
        // count are inherited unchanged from the child relation.
        Ok(child)
    }
}

fn derive_from_optional_relation(
    relation: Option<&Relation>,
) -> Result<RefreshFragmentProperty, String> {
    let Some(relation) = relation else {
        return Err(
            "Iceberg IMV refresh contract requires a SELECT with at least one base relation"
                .to_string(),
        );
    };
    derive_from_relation(relation)
}

fn derive_from_relation(relation: &Relation) -> Result<RefreshFragmentProperty, String> {
    match relation {
        Relation::Scan(scan) => {
            let base_ref = iceberg_ref_from_scan(scan)?;
            Ok(RefreshFragmentProperty {
                identity: TargetIdentity::BaseRowId,
                state: StateContract::Stateless,
                base_refs: vec![base_ref],
                branch_count: None,
                join_key_count: None,
                branch_shape: None,
            })
        }
        Relation::Subquery { query, .. } => derive_fragment_property(query),
        Relation::Join(join) => {
            if join.join_type != JoinKind::Inner {
                return Err(
                    "Iceberg IMV refresh contract supports only two-table inner equi-join shapes"
                        .to_string(),
                );
            }
            let condition = join.condition.as_ref().ok_or_else(|| {
                "Iceberg IMV refresh contract requires JOIN ... ON equi-join predicates".to_string()
            })?;
            let left_qualifiers = relation_qualifiers(&join.left)?;
            let right_qualifiers = relation_qualifiers(&join.right)?;
            let join_key_count =
                count_equality_join_keys(condition, &left_qualifiers, &right_qualifiers)?;
            if join_key_count == 0 {
                return Err(
                    "Iceberg IMV refresh contract requires at least one equi-join predicate"
                        .to_string(),
                );
            }
            let left = derive_from_relation(&join.left)?;
            let right = derive_from_relation(&join.right)?;
            let mut base_refs = left.base_refs;
            base_refs.extend(right.base_refs);
            Ok(RefreshFragmentProperty {
                identity: TargetIdentity::JoinRowKey(
                    Box::new(left.identity),
                    Box::new(right.identity),
                ),
                // Compose: both join inputs are stateless today, so the join is
                // stateless.
                state: StateContract::Stateless,
                base_refs,
                branch_count: None,
                join_key_count: Some(join_key_count),
                branch_shape: None,
            })
        }
        Relation::IcebergMetadataScan(_)
        | Relation::IcebergDeltaScan(_)
        | Relation::GenerateSeries(_)
        | Relation::Unnest(_)
        | Relation::CTEConsume { .. } => Err(format!(
            "Iceberg IMV refresh contract does not support relation {relation:?}"
        )),
    }
}

fn derive_from_set_operation(set_op: &ResolvedSetOp) -> Result<RefreshFragmentProperty, String> {
    let mut branches = Vec::new();
    collect_union_all_branches(set_op, &mut branches)?;
    if branches.len() < 2 {
        return Err(
            "Iceberg IMV refresh contract requires UNION ALL with at least two branches"
                .to_string(),
        );
    }
    let derived = branches
        .iter()
        .map(|query| derive_fragment_property(query))
        .collect::<Result<Vec<_>, _>>()?;
    let branch_count = derived.len();

    // Homogeneity is checked on the synthesized property: every branch must
    // produce the same (identity kind, state kind). Unlike the old shape
    // classifier this admits composed branches (e.g. Aggregate(Join(..))) as
    // long as every branch agrees on the synthesized property kind.
    let first = derived
        .first()
        .expect("UNION ALL branch list was checked as non-empty");
    let first_identity_kind = first.identity.kind_label();
    let first_state_kind = first.state.kind_label();
    for (index, branch) in derived.iter().enumerate().skip(1) {
        let branch_identity_kind = branch.identity.kind_label();
        let branch_state_kind = branch.state.kind_label();
        if branch_identity_kind != first_identity_kind || branch_state_kind != first_state_kind {
            return Err(format!(
                "Iceberg IMV refresh contract requires homogeneous UNION ALL branches: branch {index} \
                 synthesizes ({branch_identity_kind}, {branch_state_kind}) but branch 0 synthesizes \
                 ({first_identity_kind}, {first_state_kind})"
            ));
        }
    }

    // Aggregate branch arity compatibility. The kind label intentionally omits
    // the aggregate arities, so it is enforced here: every aggregate branch
    // must agree on group-key and aggregate counts. This mirrors the legacy
    // flat classifier (`derive_from_set_operation`), which rejects mismatched
    // branch arities with "compatible aggregate branch contracts".
    if let StateContract::AggregateState {
        group_key_count,
        aggregate_count,
    } = first.state
    {
        for branch in &derived[1..] {
            let StateContract::AggregateState {
                group_key_count: other_group_key_count,
                aggregate_count: other_aggregate_count,
            } = branch.state
            else {
                unreachable!("branch state kind checked above");
            };
            if other_group_key_count != group_key_count || other_aggregate_count != aggregate_count
            {
                return Err(
                    "Iceberg IMV refresh contract requires compatible aggregate branch contracts"
                        .to_string(),
                );
            }
        }
    }

    let mut base_refs = Vec::new();
    for branch in &derived {
        base_refs.extend(branch.base_refs.iter().cloned());
    }

    // Classify the branches' shared shape so the contract mapping can re-narrow
    // to the *exact* legacy-supported branch set. Homogeneity above only pins
    // the (identity kind, state kind); a simple aggregate branch and an
    // aggregate-over-join branch share that kind, yet the legacy classifier
    // accepted only the former in a branch union. The union shape is the common
    // branch shape, collapsing to `Composed` the moment any branch is composed
    // (mirroring the classifier's all-or-nothing branch acceptance).
    let branch_shape = derived
        .iter()
        .map(RefreshFragmentProperty::branch_shape_as_union_branch)
        .reduce(|acc, shape| {
            if acc == shape {
                acc
            } else {
                BranchShape::Composed
            }
        })
        .expect("UNION ALL branch list was checked as non-empty");

    let identity = TargetIdentity::branch_scoped(first.identity.clone());
    let state = first.state.clone();
    Ok(RefreshFragmentProperty {
        identity,
        state,
        base_refs,
        branch_count: Some(branch_count),
        // A UNION ALL top is never itself a join; legacy never carries a join
        // key count under a branch scope.
        join_key_count: None,
        branch_shape: Some(branch_shape),
    })
}

fn collect_union_all_branches<'a>(
    set_op: &'a ResolvedSetOp,
    out: &mut Vec<&'a ResolvedQuery>,
) -> Result<(), String> {
    if set_op.kind != SetOpKind::Union || !set_op.all {
        return Err(
            "Iceberg IMV refresh contract only supports UNION ALL set operations".to_string(),
        );
    }
    collect_union_all_query(&set_op.left, out)?;
    collect_union_all_query(&set_op.right, out)
}

fn collect_union_all_query<'a>(
    query: &'a ResolvedQuery,
    out: &mut Vec<&'a ResolvedQuery>,
) -> Result<(), String> {
    validate_query_wrapper(query)?;
    match &query.body {
        QueryBody::SetOperation(set_op) => collect_union_all_branches(set_op, out),
        _ => {
            out.push(query);
            Ok(())
        }
    }
}

/// Derive the Iceberg base-table ref for a direct scan. Mirrors
/// `iceberg_ref_from_resolved` in the flat classifier, but reads the identity
/// off the scan's `ScanSource` (the relation tree, not the MV-declared refs).
fn iceberg_ref_from_scan(
    scan: &crate::sql::analysis::ScanRelation,
) -> Result<IcebergTableRef, String> {
    match &scan.table.source {
        ScanSource::IcebergDataFiles { table, .. } => Ok(IcebergTableRef {
            catalog: table.catalog.clone(),
            namespace: table.namespace.clone(),
            table: table.table.clone(),
        }),
        _ => Err(format!(
            "Iceberg IMV refresh contract requires Iceberg base tables, got non-Iceberg scan of `{}`",
            scan.table.name
        )),
    }
}

/// Group-key output names for an aggregate select: the SELECT-list output names
/// of the projection items that are themselves GROUP BY keys, in projection
/// order. `count_aggregate_projection_outputs` separately guarantees every
/// GROUP BY key is projected, so this captures the full group-key output set.
fn group_key_output_names(select: &ResolvedSelect) -> Vec<String> {
    select
        .projection
        .iter()
        .filter(|item| {
            select
                .group_by
                .iter()
                .any(|group_key| typed_expr_eq(group_key, &item.expr))
        })
        .map(|item| item.output_name.clone())
        .collect()
}

// ---------------------------------------------------------------------------
// Expression / shape validators.
//
// These are now the CANONICAL implementations of the IMV refresh-contract
// expression/shape acceptance rules. They were originally duplicated from the
// flat classifier in `refresh_contract.rs`; A2 deleted that classifier, so
// these are the single remaining copies and the source of truth for which
// projection/filter, aggregate, and join-key shapes a refresh fragment admits.
// ---------------------------------------------------------------------------

fn count_aggregate_projection_outputs(select: &ResolvedSelect) -> Result<usize, String> {
    let mut aggregate_count = 0;
    let mut projected_group_keys = vec![false; select.group_by.len()];
    for item in &select.projection {
        if let Some(index) = select
            .group_by
            .iter()
            .position(|group_key| typed_expr_eq(group_key, &item.expr))
        {
            projected_group_keys[index] = true;
            continue;
        }

        match &item.expr.kind {
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
                ..
            } => {
                validate_supported_aggregate_call(name, args.len(), *distinct, order_by)?;
                validate_aggregate_argument_exprs(args)?;
                aggregate_count += 1;
                continue;
            }
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
            } if is_legacy_unresolved_aggregate_function_name(name) => {
                validate_supported_aggregate_call(name, args.len(), *distinct, &[])?;
                validate_aggregate_argument_exprs(args)?;
                aggregate_count += 1;
                continue;
            }
            _ => {}
        }

        validate_non_contract_aggregate_projection_expr(&item.expr)?;
        return Err(
            "Iceberg IMV refresh contract aggregate projections must be GROUP BY keys or direct aggregate calls"
                .to_string(),
        );
    }
    if projected_group_keys.iter().any(|projected| !projected) {
        return Err(
            "Iceberg IMV refresh contract aggregate projection must include every GROUP BY key"
                .to_string(),
        );
    }
    Ok(aggregate_count)
}

fn validate_non_contract_aggregate_projection_expr(expr: &TypedExpr) -> Result<(), String> {
    match &expr.kind {
        ExprKind::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            ..
        } => {
            validate_supported_aggregate_call(name, args.len(), *distinct, order_by)?;
            validate_aggregate_argument_exprs(args)
        }
        ExprKind::WindowCall { .. } => Err(
            "Iceberg IMV refresh contract does not support aggregate or window expressions outside direct aggregate outputs"
                .to_string(),
        ),
        ExprKind::BinaryOp { left, right, .. } => {
            validate_non_contract_aggregate_projection_expr(left)?;
            validate_non_contract_aggregate_projection_expr(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)
        }
        ExprKind::Nested(expr) => validate_non_contract_aggregate_projection_expr(expr),
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => {
            if is_legacy_unresolved_aggregate_function_name(name) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support aggregate function `{name}` outside direct aggregate outputs"
                ));
            }
            if *distinct {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support DISTINCT scalar function `{name}`"
                ));
            }
            if is_unsupported_contract_scalar_function(name, args.len()) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support non-deterministic or unsafe scalar function `{name}`"
                ));
            }
            args.iter()
                .try_for_each(validate_non_contract_aggregate_projection_expr)
        }
        ExprKind::LambdaFunction { body, .. } => {
            validate_non_contract_aggregate_projection_expr(body)
        }
        ExprKind::InList { expr, list, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            list.iter()
                .try_for_each(validate_non_contract_aggregate_projection_expr)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            validate_non_contract_aggregate_projection_expr(low)?;
            validate_non_contract_aggregate_projection_expr(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_non_contract_aggregate_projection_expr(expr)?;
            validate_non_contract_aggregate_projection_expr(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_non_contract_aggregate_projection_expr(operand)?;
            }
            for (when, then) in when_then {
                validate_non_contract_aggregate_projection_expr(when)?;
                validate_non_contract_aggregate_projection_expr(then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_non_contract_aggregate_projection_expr(else_expr)?;
            }
            Ok(())
        }
        ExprKind::Lambda { body, .. } => validate_non_contract_aggregate_projection_expr(body),
        ExprKind::SubqueryPlaceholder { .. } => Err(
            "Iceberg IMV refresh contract does not support subquery expressions in aggregate projections"
                .to_string(),
        ),
        ExprKind::ColumnRef { .. } | ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {
            Ok(())
        }
    }
}

fn validate_supported_aggregate_call(
    name: &str,
    arg_count: usize,
    distinct: bool,
    order_by: &[SortItem],
) -> Result<(), String> {
    if !order_by.is_empty() {
        return Err("Iceberg IMV refresh contract does not support aggregate ORDER BY".to_string());
    }
    let normalized = name.to_ascii_lowercase();
    let supported = matches!(
        normalized.as_str(),
        "count"
            | "count_distinct"
            | "multi_distinct_count"
            | "approx_count_distinct"
            | "ndv"
            | "hll_ndv"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "bool_or"
            | "boolor_agg"
            | "bool_and"
            | "booland_agg"
    );
    if !supported {
        return Err(format!(
            "Iceberg IMV refresh contract does not support aggregate function `{name}`"
        ));
    }
    if distinct && normalized != "count" {
        return Err(format!(
            "Iceberg IMV refresh contract does not support DISTINCT aggregate `{name}`"
        ));
    }
    if normalized == "count" {
        if (distinct && arg_count != 1) || (!distinct && arg_count > 1) {
            return Err(format!(
                "Iceberg IMV refresh contract supports only zero or one argument for aggregate function `{name}`"
            ));
        }
    } else if arg_count != 1 {
        return Err(format!(
            "Iceberg IMV refresh contract requires exactly one argument for aggregate function `{name}`"
        ));
    }
    Ok(())
}

fn validate_aggregate_argument_exprs(args: &[TypedExpr]) -> Result<(), String> {
    args.iter().try_for_each(validate_projection_filter_expr)
}

fn is_legacy_unresolved_aggregate_function_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count_distinct" | "hll_ndv"
    )
}

fn typed_expr_eq(left: &TypedExpr, right: &TypedExpr) -> bool {
    left.data_type == right.data_type
        && left.nullable == right.nullable
        && expr_kind_eq(&left.kind, &right.kind)
}

fn typed_exprs_eq(left: &[TypedExpr], right: &[TypedExpr]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| typed_expr_eq(left, right))
}

fn expr_kind_eq(left: &ExprKind, right: &ExprKind) -> bool {
    match (left, right) {
        (
            ExprKind::ColumnRef {
                column_id: left_id,
                qualifier: left_qualifier,
                column: left_column,
            },
            ExprKind::ColumnRef {
                column_id: right_id,
                qualifier: right_qualifier,
                column: right_column,
            },
        ) => {
            left_id == right_id
                && left_qualifier == right_qualifier
                && left_column.eq_ignore_ascii_case(right_column)
        }
        (
            ExprKind::LambdaParamRef {
                name: left_name,
                slot_id: left_slot,
            },
            ExprKind::LambdaParamRef {
                name: right_name,
                slot_id: right_slot,
            },
        ) => left_name == right_name && left_slot == right_slot,
        (ExprKind::Literal(left), ExprKind::Literal(right)) => left == right,
        (
            ExprKind::BinaryOp {
                left: left_left,
                op: left_op,
                right: left_right,
            },
            ExprKind::BinaryOp {
                left: right_left,
                op: right_op,
                right: right_right,
            },
        ) => {
            left_op == right_op
                && typed_expr_eq(left_left, right_left)
                && typed_expr_eq(left_right, right_right)
        }
        (
            ExprKind::UnaryOp {
                op: left_op,
                expr: left_expr,
            },
            ExprKind::UnaryOp {
                op: right_op,
                expr: right_expr,
            },
        ) => left_op == right_op && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::FunctionCall {
                name: left_name,
                args: left_args,
                distinct: left_distinct,
            },
            ExprKind::FunctionCall {
                name: right_name,
                args: right_args,
                distinct: right_distinct,
            },
        ) => {
            left_name.eq_ignore_ascii_case(right_name)
                && left_distinct == right_distinct
                && typed_exprs_eq(left_args, right_args)
        }
        (
            ExprKind::Cast {
                expr: left_expr,
                target: left_target,
            },
            ExprKind::Cast {
                expr: right_expr,
                target: right_target,
            },
        ) => left_target == right_target && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::IsNull {
                expr: left_expr,
                negated: left_negated,
            },
            ExprKind::IsNull {
                expr: right_expr,
                negated: right_negated,
            },
        ) => left_negated == right_negated && typed_expr_eq(left_expr, right_expr),
        (
            ExprKind::InList {
                expr: left_expr,
                list: left_list,
                negated: left_negated,
            },
            ExprKind::InList {
                expr: right_expr,
                list: right_list,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_exprs_eq(left_list, right_list)
        }
        (
            ExprKind::Between {
                expr: left_expr,
                low: left_low,
                high: left_high,
                negated: left_negated,
            },
            ExprKind::Between {
                expr: right_expr,
                low: right_low,
                high: right_high,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_expr_eq(left_low, right_low)
                && typed_expr_eq(left_high, right_high)
        }
        (
            ExprKind::Like {
                expr: left_expr,
                pattern: left_pattern,
                negated: left_negated,
            },
            ExprKind::Like {
                expr: right_expr,
                pattern: right_pattern,
                negated: right_negated,
            },
        ) => {
            left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
                && typed_expr_eq(left_pattern, right_pattern)
        }
        (
            ExprKind::Case {
                operand: left_operand,
                when_then: left_when_then,
                else_expr: left_else,
            },
            ExprKind::Case {
                operand: right_operand,
                when_then: right_when_then,
                else_expr: right_else,
            },
        ) => {
            option_typed_expr_eq(left_operand.as_deref(), right_operand.as_deref())
                && left_when_then.len() == right_when_then.len()
                && left_when_then.iter().zip(right_when_then.iter()).all(
                    |((left_when, left_then), (right_when, right_then))| {
                        typed_expr_eq(left_when, right_when) && typed_expr_eq(left_then, right_then)
                    },
                )
                && option_typed_expr_eq(left_else.as_deref(), right_else.as_deref())
        }
        (
            ExprKind::IsTruthValue {
                expr: left_expr,
                value: left_value,
                negated: left_negated,
            },
            ExprKind::IsTruthValue {
                expr: right_expr,
                value: right_value,
                negated: right_negated,
            },
        ) => {
            left_value == right_value
                && left_negated == right_negated
                && typed_expr_eq(left_expr, right_expr)
        }
        (ExprKind::Nested(left), ExprKind::Nested(right)) => typed_expr_eq(left, right),
        _ => false,
    }
}

fn option_typed_expr_eq(left: Option<&TypedExpr>, right: Option<&TypedExpr>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => typed_expr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn validate_projection_filter_exprs(select: &ResolvedSelect) -> Result<(), String> {
    for item in &select.projection {
        validate_projection_filter_expr(&item.expr)?;
    }
    if let Some(filter) = &select.filter {
        validate_projection_filter_expr(filter)?;
    }
    Ok(())
}

fn validate_projection_filter_expr(expr: &TypedExpr) -> Result<(), String> {
    match &expr.kind {
        ExprKind::AggregateCall { .. } | ExprKind::WindowCall { .. } => {
            Err("Iceberg IMV refresh contract does not support aggregate or window expressions in projection/filter shapes".to_string())
        }
        ExprKind::SubqueryPlaceholder { .. } => Err(
            "Iceberg IMV refresh contract does not support subquery expressions in projection/filter shapes"
                .to_string(),
        ),
        ExprKind::BinaryOp { left, right, .. } => {
            validate_projection_filter_expr(left)?;
            validate_projection_filter_expr(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::LambdaFunction { body: expr, .. }
        | ExprKind::Lambda { body: expr, .. } => validate_projection_filter_expr(expr),
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => {
            if is_legacy_unresolved_aggregate_function_name(name) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support aggregate function `{name}` in projection/filter shapes"
                ));
            }
            if *distinct {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support DISTINCT scalar function `{name}`"
                ));
            }
            if is_unsupported_contract_scalar_function(name, args.len()) {
                return Err(format!(
                    "Iceberg IMV refresh contract does not support non-deterministic or unsafe scalar function `{name}`"
                ));
            }
            for arg in args {
                validate_projection_filter_expr(arg)?;
            }
            Ok(())
        }
        ExprKind::InList { expr, list, .. } => {
            validate_projection_filter_expr(expr)?;
            for item in list {
                validate_projection_filter_expr(item)?;
            }
            Ok(())
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            validate_projection_filter_expr(expr)?;
            validate_projection_filter_expr(low)?;
            validate_projection_filter_expr(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            validate_projection_filter_expr(expr)?;
            validate_projection_filter_expr(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(operand) = operand {
                validate_projection_filter_expr(operand)?;
            }
            for (when, then) in when_then {
                validate_projection_filter_expr(when)?;
                validate_projection_filter_expr(then)?;
            }
            if let Some(else_expr) = else_expr {
                validate_projection_filter_expr(else_expr)?;
            }
            Ok(())
        }
        ExprKind::ColumnRef { .. } | ExprKind::LambdaParamRef { .. } | ExprKind::Literal(_) => {
            Ok(())
        }
    }
}

fn is_unsupported_contract_scalar_function(name: &str, arg_count: usize) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "now"
            | "current_timestamp"
            | "localtime"
            | "localtimestamp"
            | "utc_timestamp"
            | "current_date"
            | "curdate"
            | "current_time"
            | "curtime"
            | "utc_time"
            | "random"
            | "rand"
            | "uuid"
            | "sleep"
            | "version"
            | "database"
            | "current_user"
            | "user"
            | "grouping"
            | "grouping_id"
    ) || (name.eq_ignore_ascii_case("unix_timestamp") && arg_count == 0)
}

fn relation_qualifiers(relation: &Relation) -> Result<Vec<String>, String> {
    match relation {
        Relation::Scan(scan) => Ok(vec![
            scan.alias
                .clone()
                .unwrap_or_else(|| scan.table.name.clone())
                .to_ascii_lowercase(),
        ]),
        _ => Err(
            "Iceberg IMV refresh contract supports join keys only over direct scan inputs"
                .to_string(),
        ),
    }
}

fn count_equality_join_keys(
    expr: &TypedExpr,
    left_qualifiers: &[String],
    right_qualifiers: &[String],
) -> Result<usize, String> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => Ok(
            count_equality_join_keys(left, left_qualifiers, right_qualifiers)?
                + count_equality_join_keys(right, left_qualifiers, right_qualifiers)?,
        ),
        ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            let left_side = join_key_side(left, left_qualifiers, right_qualifiers)?;
            let right_side = join_key_side(right, left_qualifiers, right_qualifiers)?;
            if left_side == right_side {
                return Err(
                    "Iceberg IMV refresh contract equi-join predicates must compare left and right join inputs"
                        .to_string(),
                );
            }
            Ok(1)
        }
        ExprKind::Nested(expr) => count_equality_join_keys(expr, left_qualifiers, right_qualifiers),
        _ => Err(
            "Iceberg IMV refresh contract supports only AND-combined equi-join predicates"
                .to_string(),
        ),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinKeySide {
    Left,
    Right,
}

fn join_key_side(
    expr: &TypedExpr,
    left_qualifiers: &[String],
    right_qualifiers: &[String],
) -> Result<JoinKeySide, String> {
    match &expr.kind {
        ExprKind::ColumnRef {
            qualifier: Some(qualifier),
            ..
        } => {
            let qualifier = qualifier.to_ascii_lowercase();
            if left_qualifiers.iter().any(|left| left == &qualifier) {
                Ok(JoinKeySide::Left)
            } else if right_qualifiers.iter().any(|right| right == &qualifier) {
                Ok(JoinKeySide::Right)
            } else {
                Err(format!(
                    "Iceberg IMV refresh contract join key qualifier `{qualifier}` does not match either join input"
                ))
            }
        }
        ExprKind::Nested(expr) => join_key_side(expr, left_qualifiers, right_qualifiers),
        _ => Err(
            "Iceberg IMV refresh contract join keys must be qualified column references"
                .to_string(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::QueryBody;
    use crate::sql::catalog::{
        CatalogProvider, ColumnDef, IcebergDataFileBinding, IcebergSchemaDef, IcebergTableInfo,
        ScanSource, TableDef,
    };
    use arrow::datatypes::DataType;

    struct TestIcebergCatalog;

    impl CatalogProvider for TestIcebergCatalog {
        fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
            Ok(TableDef {
                name: table.to_string(),
                columns: vec![
                    column("id", DataType::Int64, false),
                    column("region", DataType::Utf8, true),
                    column("amount", DataType::Int64, true),
                    column("flag", DataType::Boolean, true),
                ],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::IcebergDataFiles {
                    table: iceberg_table_info(database, table),
                    files: Vec::new(),
                    cloud_properties: Default::default(),
                    binding: IcebergDataFileBinding::CurrentSnapshot,
                },
            })
        }
    }

    fn column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_table_info(database: &str, table: &str) -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: database.to_string(),
            table: table.to_string(),
            table_uuid: Some(format!("uuid-{table}")),
            current_snapshot_id: Some(7),
            schema_id: 1,
            location: format!("file:///tmp/{database}/{table}"),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn analyze_query(sql: &str) -> ResolvedQuery {
        let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse query");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        let (resolved_query, _, _) =
            crate::sql::analyzer::analyze(&query, &TestIcebergCatalog, "sales")
                .expect("analyze query");
        resolved_query
    }

    fn property(sql: &str) -> RefreshFragmentProperty {
        derive_fragment_property(&analyze_query(sql)).expect("derive fragment property")
    }

    fn error(sql: &str) -> String {
        derive_fragment_property(&analyze_query(sql)).expect_err("expected rejection")
    }

    fn base_ref_fqns(property: &RefreshFragmentProperty) -> Vec<String> {
        property
            .base_refs
            .iter()
            .map(IcebergTableRef::fqn)
            .collect()
    }

    // --- Acceptance: per-operator synthesis -------------------------------

    #[test]
    fn scan_synthesizes_base_row_identity_stateless() {
        let prop = property("SELECT region, amount FROM fact_east WHERE amount > 0");

        assert_eq!(prop.identity, TargetIdentity::BaseRowId);
        assert_eq!(prop.state, StateContract::Stateless);
        assert_eq!(base_ref_fqns(&prop), vec!["ice.sales.fact_east"]);
        assert_eq!(prop.branch_count, None);
    }

    #[test]
    fn aggregate_over_scan_synthesizes_group_row_and_aggregate_state() {
        let prop = property(
            "SELECT region, count(*) AS c, sum(amount) AS s FROM fact_east GROUP BY region",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::GroupRowId(vec!["region".to_string()])
        );
        assert_eq!(
            prop.state,
            StateContract::AggregateState {
                group_key_count: 1,
                aggregate_count: 2,
            }
        );
        assert_eq!(base_ref_fqns(&prop), vec!["ice.sales.fact_east"]);
        assert_eq!(prop.branch_count, None);
    }

    #[test]
    fn inner_equi_join_synthesizes_join_row_key_stateless() {
        let prop = property(
            "SELECT l.region, r.amount
             FROM fact_east l JOIN fact_west r ON l.id = r.id",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::JoinRowKey(
                Box::new(TargetIdentity::BaseRowId),
                Box::new(TargetIdentity::BaseRowId),
            )
        );
        assert_eq!(prop.state, StateContract::Stateless);
        assert_eq!(
            base_ref_fqns(&prop),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
        assert_eq!(prop.branch_count, None);
    }

    #[test]
    fn union_all_of_aggregates_synthesizes_branch_scoped_group_row() {
        let prop = property(
            "SELECT region, count(*) AS c, sum(amount) AS s
             FROM fact_east
             GROUP BY region
             UNION ALL
             SELECT region, count(*) AS c, sum(amount) AS s
             FROM fact_west
             GROUP BY region",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::BranchScoped(Box::new(TargetIdentity::GroupRowId(vec![
                "region".to_string()
            ])))
        );
        assert_eq!(
            prop.state,
            StateContract::AggregateState {
                group_key_count: 1,
                aggregate_count: 2,
            }
        );
        assert_eq!(
            base_ref_fqns(&prop),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
        assert_eq!(prop.branch_count, Some(2));
    }

    #[test]
    fn union_all_of_aggregate_joins_synthesizes_branch_scoped_group_row() {
        // The composed case the flat classifier REJECTED (it required each
        // branch to be a literal simple aggregate over a non-join input). The
        // property algebra ACCEPTS it because every branch synthesizes the same
        // (GroupRowId, AggregateState) kind.
        let prop = property(
            "SELECT l.region, count(*) AS c, sum(r.amount) AS s
             FROM fact_a l JOIN fact_b r ON l.id = r.id
             GROUP BY l.region
             UNION ALL
             SELECT l.region, count(*) AS c, sum(r.amount) AS s
             FROM fact_c l JOIN fact_d r ON l.id = r.id
             GROUP BY l.region",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::BranchScoped(Box::new(TargetIdentity::GroupRowId(vec![
                "region".to_string()
            ])))
        );
        assert_eq!(
            prop.state,
            StateContract::AggregateState {
                group_key_count: 1,
                aggregate_count: 2,
            }
        );
        assert_eq!(prop.base_refs.len(), 4);
        assert_eq!(
            base_ref_fqns(&prop),
            vec![
                "ice.sales.fact_a",
                "ice.sales.fact_b",
                "ice.sales.fact_c",
                "ice.sales.fact_d",
            ]
        );
        assert_eq!(prop.branch_count, Some(2));
    }

    #[test]
    fn nested_union_all_flattens_branch_scoped_identity() {
        // Three-branch UNION ALL parses as a nested set op; branch scoping must
        // flatten so the identity top is a single BranchScoped, and the branch
        // count counts every leaf branch.
        let prop = property(
            "SELECT region, amount FROM fact_a
             UNION ALL
             SELECT region, amount FROM fact_b
             UNION ALL
             SELECT region, amount FROM fact_c",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::BranchScoped(Box::new(TargetIdentity::BaseRowId))
        );
        assert_eq!(prop.state, StateContract::Stateless);
        assert_eq!(prop.branch_count, Some(3));
        assert_eq!(
            base_ref_fqns(&prop),
            vec!["ice.sales.fact_a", "ice.sales.fact_b", "ice.sales.fact_c"]
        );
    }

    #[test]
    fn projection_filter_passthrough_preserves_child_property() {
        // Plain projection/filter over a scan must inherit the child's identity
        // and state verbatim.
        let prop =
            property("SELECT region, amount + 1 AS adjusted FROM fact_east WHERE amount > 0");

        assert_eq!(prop.identity, TargetIdentity::BaseRowId);
        assert_eq!(prop.state, StateContract::Stateless);
        assert_eq!(base_ref_fqns(&prop), vec!["ice.sales.fact_east"]);
        assert_eq!(prop.branch_count, None);
    }

    #[test]
    fn projection_filter_passthrough_over_join_preserves_join_identity() {
        let prop = property(
            "SELECT joined.region, joined.amount
             FROM (
                 SELECT l.region AS region, r.amount AS amount
                 FROM fact_east l JOIN fact_west r ON l.id = r.id
             ) joined
             WHERE joined.amount > 0",
        );

        assert_eq!(
            prop.identity,
            TargetIdentity::JoinRowKey(
                Box::new(TargetIdentity::BaseRowId),
                Box::new(TargetIdentity::BaseRowId),
            )
        );
        assert_eq!(prop.state, StateContract::Stateless);
        assert_eq!(
            base_ref_fqns(&prop),
            vec!["ice.sales.fact_east", "ice.sales.fact_west"]
        );
    }

    // --- Rejections mirroring the flat classifier ------------------------

    #[test]
    fn rejects_non_union_all_set_op() {
        let err = error(
            "SELECT region FROM fact_east
             UNION
             SELECT region FROM fact_west",
        );
        assert!(
            err.contains("only supports UNION ALL"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_intersect_set_op() {
        let err = error(
            "SELECT region FROM fact_east
             INTERSECT
             SELECT region FROM fact_west",
        );
        assert!(
            err.contains("only supports UNION ALL"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_outer_join() {
        let err = error(
            "SELECT l.region, r.amount
             FROM fact_east l LEFT JOIN fact_west r ON l.id = r.id",
        );
        assert!(err.contains("inner equi-join"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_cross_join() {
        let err = error(
            "SELECT l.region, r.amount
             FROM fact_east l CROSS JOIN fact_west r",
        );
        assert!(err.contains("inner equi-join"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_non_equi_join() {
        let err = error(
            "SELECT l.region, r.amount
             FROM fact_east l JOIN fact_west r ON l.id > r.id",
        );
        assert!(err.contains("equi-join"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_heterogeneous_union_all_branches() {
        // One aggregate branch + one projection branch: same arity is
        // impossible to compare, but the branches diverge on property kind
        // (GroupRowId/AggregateState vs BaseRowId/Stateless).
        let err = error(
            "SELECT region, count(*) AS c FROM fact_east GROUP BY region
             UNION ALL
             SELECT region, amount AS c FROM fact_west",
        );
        assert!(
            err.contains("homogeneous UNION ALL branches") && err.contains("branch 1"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_select_distinct() {
        let err = error("SELECT DISTINCT region FROM fact_east");
        assert!(err.contains("SELECT DISTINCT"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_aggregate_without_group_keys() {
        let err = error("SELECT count(*) AS c FROM fact_east");
        assert!(
            err.contains("non-empty GROUP BY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_with_query() {
        let err = error(
            "WITH unused AS (SELECT id FROM fact_extra)
             SELECT region, amount FROM fact_east",
        );
        assert!(err.contains("WITH"), "unexpected error: {err}");
    }

    #[test]
    fn rejects_order_by_limit_offset() {
        for sql in [
            "SELECT region FROM fact_east ORDER BY region",
            "SELECT region FROM fact_east LIMIT 10",
            "SELECT region FROM fact_east OFFSET 1",
        ] {
            let err = error(sql);
            assert!(
                err.contains("ORDER BY, LIMIT, or OFFSET"),
                "unexpected error for {sql}: {err}"
            );
        }
    }

    #[test]
    fn rejects_join_subquery_side() {
        // Join keys must be over direct scan inputs; a subquery side is
        // rejected by relation_qualifiers, same as the flat classifier.
        let err = error(
            "SELECT l.region, r.amount
             FROM (SELECT id, region FROM fact_east WHERE amount > 0) l
             JOIN fact_west r ON l.id = r.id",
        );
        assert!(
            err.contains("direct scan inputs"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_nondeterministic_projection() {
        let err = error("SELECT region, rand() AS r FROM fact_east");
        assert!(
            err.contains("non-deterministic or unsafe"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn rejects_projection_filter_over_aggregate_subquery() {
        // A plain SELECT over an aggregate subquery must be rejected by the
        // property algebra, mirroring refresh_contract.rs:382-392.
        let err = error(
            "SELECT region, adjusted
             FROM (
                 SELECT region, count(*) AS adjusted
                 FROM fact_east
                 GROUP BY region
             ) s
             WHERE adjusted > 0",
        );
        assert!(
            err.contains("projection/filter over aggregate subqueries"),
            "unexpected error: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Parity tests: from_schema_contract vs legacy RefreshStrategy
    //
    // For each persisted-contract shape, assert that from_schema_contract
    // yields the snapshot_policy, has_agg_state, and apply_key_value_type
    // that are consistent with the legacy strategy the driver would derive
    // from the same contract via stored_refresh_strategy_for_plan.
    //
    // Strategy -> expected_policy table (from the driver wrapper call-sites):
    //   ProjectionFilter       -> SingleBase
    //   SingleAggregate        -> SingleBase
    //   JoinProjectionFilter   -> JoinPairPartialInitialSkip
    //   JoinAggregate          -> JoinPairPartialInitialSkip
    //   UnionProjectionFilter  -> AllBasesRequired
    //   FanInAggregate         -> AllBasesRequired
    //   BranchUnionAggregate   -> AllBasesRequired
    // -----------------------------------------------------------------------

    // ------------------------------------------------------------------
    // Minimal contract builders used only in the parity tests below.
    // ------------------------------------------------------------------

    use crate::engine::mv::iceberg_merge_sink::ApplyKeyValueType;
    use crate::engine::mv::refresh_driver::BaseSnapshotPolicy;
    use crate::meta::repository::mv_contract::{
        AggregateStateContract, ApplyKeySource, BaseContract, BaseFieldRecord, BaseSchemaSnapshot,
        BranchIdColumnContract, BranchUnionContract, ExpressionKind, ExpressionLineage,
        GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, HIDDEN_APPLY_KEY_COLUMN_NAME, HiddenApplyKeyContract,
        JOIN_APPLY_KEY_COLUMN_NAME, JoinContract, JoinContractKind, JoinPredicateLineage,
        MvSchemaContract, OutputColumnLineage, OutputContract, QualifiedFieldLineage,
        TargetContract, TargetVisibleColumn,
    };

    /// Minimal base contract (single-column Iceberg table).
    fn parity_base(fqn: &str, uuid: &str) -> BaseContract {
        BaseContract {
            table_fqn: fqn.to_string(),
            table_uuid: uuid.to_string(),
            alias_at_create: None,
            schema_id_at_create: 1,
            schema_at_create: BaseSchemaSnapshot {
                fields: vec![BaseFieldRecord {
                    field_id: 1,
                    name_at_create: "id".to_string(),
                    type_signature: "int".to_string(),
                    required: true,
                }],
            },
        }
    }

    fn parity_output() -> OutputContract {
        OutputContract {
            columns: vec![OutputColumnLineage {
                expression: ExpressionLineage {
                    kind: ExpressionKind::Column,
                    referenced_base_field_ids: vec![1],
                    referenced_base_fields: vec![],
                },
            }],
            filter: None,
        }
    }

    fn parity_target(column_name: &str, source: ApplyKeySource) -> TargetContract {
        TargetContract {
            table_fqn: "ice.db.mv".to_string(),
            table_uuid: "mv-uuid".to_string(),
            schema_id_at_create: 10,
            visible_columns: vec![TargetVisibleColumn {
                output_name: "id".to_string(),
                target_field_id: 1,
                type_signature: "int".to_string(),
                nullable: false,
            }],
            hidden_apply_key: HiddenApplyKeyContract {
                column_name: column_name.to_string(),
                target_field_id: 99,
                source,
            },
            partition: None,
        }
    }

    fn parity_agg() -> AggregateStateContract {
        AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME.to_string(),
            state_columns: vec![],
        }
    }

    fn parity_join() -> JoinContract {
        JoinContract {
            kind: JoinContractKind::InnerEquiJoin,
            predicates: vec![JoinPredicateLineage {
                left: QualifiedFieldLineage {
                    table_fqn: "ice.db.left".to_string(),
                    qualifier_at_create: "l".to_string(),
                    field_id: 1,
                },
                right: QualifiedFieldLineage {
                    table_fqn: "ice.db.right".to_string(),
                    qualifier_at_create: "r".to_string(),
                    field_id: 1,
                },
            }],
        }
    }

    // ------------------------------------------------------------------
    // Parity assertions
    // ------------------------------------------------------------------

    /// ProjectionFilter: single base, no join/agg/branch.
    /// Legacy strategy: ProjectionFilter -> SingleBase, Int64.
    #[test]
    fn parity_projection_filter() {
        let c = MvSchemaContract {
            contract_version: 1,
            base: parity_base("ice.db.orders", "uuid-orders"),
            bases: vec![],
            output: parity_output(),
            join: None,
            aggregate: None,
            branch: None,
            target: parity_target(HIDDEN_APPLY_KEY_COLUMN_NAME, ApplyKeySource::BaseRowId),
        };
        let caps = from_schema_contract(&c).expect("projection_filter");
        assert_eq!(caps.snapshot_policy, BaseSnapshotPolicy::SingleBase);
        assert!(!caps.has_agg_state);
        assert_eq!(caps.identity, RefreshIdentity::BaseRowId);
        assert_eq!(caps.apply_key_column, HIDDEN_APPLY_KEY_COLUMN_NAME);
        assert_eq!(caps.apply_key_value_type, ApplyKeyValueType::Int64);
    }

    /// SingleAggregate: single base, aggregate, no join/branch.
    /// Legacy strategy: SingleAggregate -> SingleBase, Utf8.
    #[test]
    fn parity_single_aggregate() {
        let c = MvSchemaContract {
            contract_version: 1,
            base: parity_base("ice.db.orders", "uuid-orders"),
            bases: vec![],
            output: parity_output(),
            join: None,
            aggregate: Some(parity_agg()),
            branch: None,
            target: parity_target(
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                ApplyKeySource::GroupRowId,
            ),
        };
        let caps = from_schema_contract(&c).expect("single_aggregate");
        assert_eq!(caps.snapshot_policy, BaseSnapshotPolicy::SingleBase);
        assert!(caps.has_agg_state);
        assert_eq!(caps.identity, RefreshIdentity::GroupRowId);
        assert_eq!(caps.apply_key_column, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME);
        assert_eq!(caps.apply_key_value_type, ApplyKeyValueType::Utf8);
    }

    /// JoinProjectionFilter: base + extra-base (join sides), join contract.
    /// Legacy strategy: JoinProjectionFilter -> JoinPairPartialInitialSkip, Utf8.
    #[test]
    fn parity_join_projection_filter() {
        let c = MvSchemaContract {
            contract_version: 2,
            base: parity_base("ice.db.left", "uuid-left"),
            bases: vec![
                parity_base("ice.db.left", "uuid-left"),
                parity_base("ice.db.right", "uuid-right"),
            ],
            output: parity_output(),
            join: Some(parity_join()),
            aggregate: None,
            branch: None,
            target: parity_target(JOIN_APPLY_KEY_COLUMN_NAME, ApplyKeySource::JoinRowKey),
        };
        let caps = from_schema_contract(&c).expect("join_projection_filter");
        assert_eq!(
            caps.snapshot_policy,
            BaseSnapshotPolicy::JoinPairPartialInitialSkip
        );
        assert!(!caps.has_agg_state);
        assert_eq!(caps.identity, RefreshIdentity::JoinRowKey);
        assert_eq!(caps.apply_key_column, JOIN_APPLY_KEY_COLUMN_NAME);
        assert_eq!(caps.apply_key_value_type, ApplyKeyValueType::Utf8);
    }

    /// JoinAggregate: base + extra-base (join sides), join + aggregate.
    /// Legacy strategy: JoinAggregate -> JoinPairPartialInitialSkip, Utf8.
    #[test]
    fn parity_join_aggregate() {
        let c = MvSchemaContract {
            contract_version: 2,
            base: parity_base("ice.db.left", "uuid-left"),
            bases: vec![
                parity_base("ice.db.left", "uuid-left"),
                parity_base("ice.db.right", "uuid-right"),
            ],
            output: parity_output(),
            join: Some(parity_join()),
            aggregate: Some(parity_agg()),
            branch: None,
            target: parity_target(
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                ApplyKeySource::GroupRowId,
            ),
        };
        let caps = from_schema_contract(&c).expect("join_aggregate");
        assert_eq!(
            caps.snapshot_policy,
            BaseSnapshotPolicy::JoinPairPartialInitialSkip
        );
        assert!(caps.has_agg_state);
        assert_eq!(caps.identity, RefreshIdentity::GroupRowId);
        assert_eq!(caps.apply_key_column, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME);
        assert_eq!(caps.apply_key_value_type, ApplyKeyValueType::Utf8);
    }

    /// UnionProjectionFilter: multiple extra-bases, no join/agg, branch with
    /// BaseRowId inner key.
    /// Legacy strategy: UnionProjectionFilter -> AllBasesRequired, BranchInt64.
    #[test]
    fn parity_union_projection_filter() {
        let c = MvSchemaContract {
            contract_version: 2,
            base: parity_base("ice.db.east", "uuid-east"),
            bases: vec![],
            output: parity_output(),
            join: None,
            aggregate: None,
            branch: Some(BranchUnionContract {
                branch_id_column: BranchIdColumnContract {
                    column_name: "__nova_branch_id".to_string(),
                    target_field_id: 100,
                },
                branch_count: 2,
                inner_apply_key_source: ApplyKeySource::BaseRowId,
            }),
            target: parity_target(HIDDEN_APPLY_KEY_COLUMN_NAME, ApplyKeySource::BaseRowId),
        };
        let caps = from_schema_contract(&c).expect("union_projection_filter");
        assert_eq!(caps.snapshot_policy, BaseSnapshotPolicy::AllBasesRequired);
        assert!(!caps.has_agg_state);
        assert_eq!(
            caps.identity,
            RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::BaseRowId))
        );
        assert_eq!(caps.apply_key_column, HIDDEN_APPLY_KEY_COLUMN_NAME);
        assert_eq!(caps.apply_key_value_type, ApplyKeyValueType::BranchInt64);
    }

    /// FanInAggregate: multiple extra-bases (fan-in inputs), aggregate, no
    /// join/branch.
    /// Legacy strategy: FanInAggregate -> AllBasesRequired, Utf8.
    #[test]
    fn parity_fan_in_aggregate() {
        let c = MvSchemaContract {
            contract_version: 2,
            base: parity_base("ice.db.east", "uuid-east"),
            bases: vec![
                parity_base("ice.db.east", "uuid-east"),
                parity_base("ice.db.west", "uuid-west"),
            ],
            output: parity_output(),
            join: None,
            aggregate: Some(parity_agg()),
            branch: None,
            target: parity_target(
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                ApplyKeySource::GroupRowId,
            ),
        };
        let caps = from_schema_contract(&c).expect("fan_in_aggregate");
        assert_eq!(caps.snapshot_policy, BaseSnapshotPolicy::AllBasesRequired);
        assert!(caps.has_agg_state);
        assert_eq!(caps.identity, RefreshIdentity::GroupRowId);
        assert_eq!(caps.apply_key_column, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME);
        assert_eq!(caps.apply_key_value_type, ApplyKeyValueType::Utf8);
    }

    /// BranchUnionAggregate: aggregate + branch with GroupRowId inner key.
    /// Legacy strategy: BranchUnionAggregate -> AllBasesRequired, BranchUtf8.
    #[test]
    fn parity_branch_union_aggregate() {
        let c = MvSchemaContract {
            contract_version: 2,
            base: parity_base("ice.db.east", "uuid-east"),
            bases: vec![],
            output: parity_output(),
            join: None,
            aggregate: Some(parity_agg()),
            branch: Some(BranchUnionContract {
                branch_id_column: BranchIdColumnContract {
                    column_name: "__nova_branch_id".to_string(),
                    target_field_id: 100,
                },
                branch_count: 2,
                inner_apply_key_source: ApplyKeySource::GroupRowId,
            }),
            target: parity_target(
                GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME,
                ApplyKeySource::GroupRowId,
            ),
        };
        let caps = from_schema_contract(&c).expect("branch_union_aggregate");
        assert_eq!(caps.snapshot_policy, BaseSnapshotPolicy::AllBasesRequired);
        assert!(caps.has_agg_state);
        assert_eq!(
            caps.identity,
            RefreshIdentity::BranchScoped(Box::new(RefreshIdentity::GroupRowId))
        );
        assert_eq!(caps.apply_key_column, GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME);
        assert_eq!(caps.apply_key_value_type, ApplyKeyValueType::BranchUtf8);
    }

    /// Unsupported contract shape (join + branch) must return an error.
    #[test]
    fn parity_unsupported_join_branch_errors() {
        let c = MvSchemaContract {
            contract_version: 2,
            base: parity_base("ice.db.left", "uuid-left"),
            bases: vec![],
            output: parity_output(),
            join: Some(parity_join()),
            aggregate: None,
            branch: Some(BranchUnionContract {
                branch_id_column: BranchIdColumnContract {
                    column_name: "__nova_branch_id".to_string(),
                    target_field_id: 100,
                },
                branch_count: 2,
                inner_apply_key_source: ApplyKeySource::BaseRowId,
            }),
            target: parity_target(HIDDEN_APPLY_KEY_COLUMN_NAME, ApplyKeySource::BaseRowId),
        };
        let err = from_schema_contract(&c);
        assert!(err.is_err(), "expected error for join+branch shape");
    }
}
