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

//! The registry: every frontend state family and the contract it declares.

use super::classification::{
    AcceleratorContract, AcceleratorRebuildAuthority, AcceleratorResidence, BootstrapFailureScope,
    ClonePolicy, DurabilityAdmission, ExternalProjectionContract, ExternalProjectionSource,
    PersistentKeyPrefix, ProcessRuntimeAuthority, ProcessRuntimeContract, RebuildDeterminism,
    SnapshotIdentity, StateFamilyClassification,
};

// Frozen key prefixes.  These bytes are already in deployed stores, so they are
// literals rather than anything composed: the whole point of moving them here
// is that they now have exactly one definition point, not that they became
// derivable.  `prefix_literals_are_byte_stable` is the tripwire against an
// edit that silently orphans existing records.
//
// The prefixes are deliberately not uniform.  Catalog attachment and GC
// observation end in `/` because their owners append a record path directly;
// backend desired state has no separator because the prefix *is* the single
// key; MV has none because its owner joins with `/` itself.  Normalizing them
// would rewrite live keys, so the manifest preserves each as frozen.
const CATALOG_ATTACHMENT_PREFIX: &str = "novarocks/frontend/catalog/v1/attachment/by-instance/";
const CLUSTER_BACKENDS_PREFIX: &str = "novarocks/frontend/cluster-backends/v1/state";
const MV_ACCELERATOR_PREFIX: &str = "novarocks/frontend/mv/accelerator/v1";
const GC_OWNED_REF_OBSERVATION_PREFIX: &str =
    "novarocks/frontend/table-maintenance/v7/gc-owned-ref-observations/";

/// Every frontend state family, registered exactly once.
///
/// Retired families are absent by deletion, not by a tombstone entry: this
/// binary has no reader for them, so registering them would be the compatibility
/// surface the hard cut exists to remove.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StateFamily {
    /// External catalog attachments: the logical configuration of every
    /// attached catalog.
    CatalogDesiredState,
    /// Cluster backend membership as an operator declared it, through
    /// configuration seeds and SQL.
    BackendDesiredState,
    /// MV definitions, target and dependency indexes, and the aggregate
    /// published waterline.
    MvAccelerator,
    /// The time at which one exact provider-proven owned-ref tuple was first
    /// observed by GC.
    GcOwnedRefObservation,
    /// Resolved connector table metadata, validated against the connector's
    /// current schema version.
    SchemaCache,
    /// Immutable connector statistics artifacts, keyed by evidence revision.
    StatisticsArtifactCache,
    /// Views defined in the local (non-external) catalog.
    LocalViewRegistry,
    /// DML operations, side records and their coordination state.
    DmlRuntime,
    /// Maintenance jobs, attempts, transactions and their indexes.
    MaintenanceRuntime,
    /// Statistics jobs, worker leases and cursors.
    StatisticsJobRuntime,
    /// MV refresh attempts, leases, scheduler backoff and cursors.
    MvRefreshRuntime,
    /// Backend liveness, generation and fragment activity as this frontend
    /// observed it.
    BackendObservedRuntime,
}

impl StateFamily {
    /// The number of registered families.
    ///
    /// Hand-written, and checked against the chain below at compile time.
    pub const COUNT: usize = 12;

    /// Every registered family, in manifest order.
    ///
    /// Derived from [`StateFamily::next_in_manifest`] rather than hand-listed,
    /// so it cannot fall behind the enum: a new variant must be linked into
    /// that exhaustive chain to compile at all, and once linked it appears here
    /// automatically.
    pub const ALL: [Self; Self::COUNT] = Self::enumerate();

    const FIRST: Self = Self::CatalogDesiredState;

    /// The contract this family declares.
    ///
    /// One exhaustive `match`, which is what forces a new family to pick a
    /// classification, an authority, a record version and a retain/clone policy
    /// before it can exist.
    pub const fn classification(self) -> StateFamilyClassification {
        match self {
            Self::CatalogDesiredState => {
                StateFamilyClassification::ExternalProjection(ExternalProjectionContract::new(
                    ExternalProjectionSource::SelectedCatalogSourceMode,
                    // A partial enumeration of attachments is indistinguishable
                    // from a smaller desired state, so identity is the complete
                    // enumeration and nothing less.
                    SnapshotIdentity::CompleteEnumeration,
                    BootstrapFailureScope::GlobalEnumerationPerEntryMaterialization,
                    PersistentKeyPrefix::new(CATALOG_ATTACHMENT_PREFIX),
                    1,
                    ClonePolicy::SemanticRebind,
                ))
            }
            Self::BackendDesiredState => {
                StateFamilyClassification::ExternalProjection(ExternalProjectionContract::new(
                    ExternalProjectionSource::ConfiguredSeedsAndSqlMembership,
                    SnapshotIdentity::SingleVersionedRecord,
                    BootstrapFailureScope::GlobalOnly,
                    PersistentKeyPrefix::new(CLUSTER_BACKENDS_PREFIX),
                    1,
                    // Backend identity belongs to the source deployment's
                    // machines, so a clone declares its own membership rather
                    // than inheriting endpoints it cannot reach.
                    ClonePolicy::NotCloned,
                ))
            }
            Self::MvAccelerator => {
                StateFamilyClassification::Accelerator(AcceleratorContract::new(
                    AcceleratorResidence::Durable {
                        prefix: PersistentKeyPrefix::new(MV_ACCELERATOR_PREFIX),
                        record_version: 1,
                    },
                    AcceleratorRebuildAuthority::MvLakeDescriptorAndPublicationFacts,
                    RebuildDeterminism::UserVisibleIdentical,
                    true,
                    ClonePolicy::RevalidateSourceRevisionOrWipe,
                ))
            }
            Self::GcOwnedRefObservation => {
                StateFamilyClassification::Accelerator(AcceleratorContract::new(
                    AcceleratorResidence::Durable {
                        prefix: PersistentKeyPrefix::new(GC_OWNED_REF_OBSERVATION_PREFIX),
                        record_version: 7,
                    },
                    AcceleratorRebuildAuthority::ProvenOwnedRefWithSafetyAgeWindow,
                    // A rebuilt observation carries today's timestamp, not the
                    // discarded one, so the safety window restarts.  That
                    // defers deletion; it never authorizes an early one.
                    RebuildDeterminism::ConservativeRestart,
                    true,
                    // A clone must not inherit a matured safety window it did
                    // not observe.
                    ClonePolicy::WipeAndRebuild,
                ))
            }
            Self::SchemaCache => StateFamilyClassification::Accelerator(AcceleratorContract::new(
                AcceleratorResidence::InProcess,
                AcceleratorRebuildAuthority::ConnectorSchemaVersion,
                RebuildDeterminism::UserVisibleIdentical,
                false,
                ClonePolicy::NotCloned,
            )),
            Self::StatisticsArtifactCache => {
                StateFamilyClassification::Accelerator(AcceleratorContract::new(
                    AcceleratorResidence::InProcess,
                    AcceleratorRebuildAuthority::ConnectorStatisticsEvidenceRevision,
                    RebuildDeterminism::UserVisibleIdentical,
                    false,
                    ClonePolicy::NotCloned,
                ))
            }
            // A local view exists only while the frontend that defined it does.
            // Deployments that need durable views define them in an external
            // catalog, which owns them as provider truth instead.
            Self::LocalViewRegistry => StateFamilyClassification::ProcessRuntime(
                ProcessRuntimeContract::new(ProcessRuntimeAuthority::FrontendIncarnation),
            ),
            Self::DmlRuntime => StateFamilyClassification::ProcessRuntime(
                ProcessRuntimeContract::new(ProcessRuntimeAuthority::Statement),
            ),
            Self::MaintenanceRuntime => StateFamilyClassification::ProcessRuntime(
                ProcessRuntimeContract::new(ProcessRuntimeAuthority::Attempt),
            ),
            Self::StatisticsJobRuntime => StateFamilyClassification::ProcessRuntime(
                ProcessRuntimeContract::new(ProcessRuntimeAuthority::Attempt),
            ),
            // Attempt records die with their attempt, but scheduler backoff and
            // cursors outlive individual attempts, so the family as a whole is
            // bounded by the incarnation.
            Self::MvRefreshRuntime => StateFamilyClassification::ProcessRuntime(
                ProcessRuntimeContract::new(ProcessRuntimeAuthority::FrontendIncarnation),
            ),
            Self::BackendObservedRuntime => StateFamilyClassification::ProcessRuntime(
                ProcessRuntimeContract::new(ProcessRuntimeAuthority::FrontendIncarnation),
            ),
        }
    }

    /// Stable identifier for logs, metrics and operator-facing errors.
    ///
    /// These strings outlive refactors of the Rust identifiers, so they are
    /// spelled out rather than derived from the variant name.
    pub const fn family_id(self) -> &'static str {
        match self {
            Self::CatalogDesiredState => "frontend/catalog/desired-state",
            Self::BackendDesiredState => "frontend/cluster-backends/desired-state",
            Self::MvAccelerator => "frontend/mv/accelerator",
            Self::GcOwnedRefObservation => "frontend/table-maintenance/gc-owned-ref-observation",
            Self::SchemaCache => "frontend/catalog/schema-cache",
            Self::StatisticsArtifactCache => "frontend/statistics/immutable-artifact-cache",
            Self::LocalViewRegistry => "frontend/view/local-registry",
            Self::DmlRuntime => "frontend/dml/runtime",
            Self::MaintenanceRuntime => "frontend/table-maintenance/runtime",
            Self::StatisticsJobRuntime => "frontend/statistics/job-runtime",
            Self::MvRefreshRuntime => "frontend/mv/refresh-runtime",
            Self::BackendObservedRuntime => "frontend/cluster-backends/observed-runtime",
        }
    }

    /// This family's persistent key prefix, or `None` when it owns no StateStore
    /// records.
    ///
    /// Owner modules read their prefix from here; there is no second definition
    /// point to drift from.
    pub const fn persistent_prefix(self) -> Option<PersistentKeyPrefix> {
        self.classification().persistent_prefix()
    }

    /// Whether this family may own a StateStore record at all.
    pub const fn durability_admission(self) -> DurabilityAdmission {
        self.classification().durability_admission()
    }

    /// Whether records of this family survive a frontend restart.
    pub const fn retain_on_restart(self) -> bool {
        self.classification().retain_on_restart()
    }

    /// What happens to this family when a deployment is cloned.
    pub const fn clone_policy(self) -> ClonePolicy {
        self.classification().clone_policy()
    }

    /// The single record version this binary reads and writes, or `None` when
    /// the family encodes no record.
    pub const fn record_version(self) -> Option<u8> {
        self.classification().record_version()
    }

    /// The registered family that owns `key`, or `None` when no family does.
    ///
    /// Attribution is by persistent prefix, and only the two persistent
    /// classifications can carry one, so a `Some` answer already implies the
    /// owner is allowed to be durable.  The store-content gate still asks
    /// [`StateFamily::durability_admission`] separately, so "the key is
    /// attributable" and "its owner may persist" stay two independent
    /// assertions instead of one inferring the other.
    ///
    /// Attribution is unambiguous because no registered prefix is a prefix of
    /// another — see `persistent_prefixes_are_unique_and_non_nested`.
    pub fn for_key(key: &[u8]) -> Option<Self> {
        Self::ALL.into_iter().find(|family| {
            family
                .persistent_prefix()
                .is_some_and(|prefix| key.starts_with(prefix.as_bytes()))
        })
    }

    /// The family that follows `self` in manifest order, or `None` for the last.
    ///
    /// This chain, not a hand-written array, is what makes [`StateFamily::ALL`]
    /// complete.  The `match` is exhaustive, so a new variant cannot be added
    /// without being linked in, and `enumerate` walks the chain at compile time
    /// and rejects a length that disagrees with [`StateFamily::COUNT`].
    const fn next_in_manifest(self) -> Option<Self> {
        match self {
            Self::CatalogDesiredState => Some(Self::BackendDesiredState),
            Self::BackendDesiredState => Some(Self::MvAccelerator),
            Self::MvAccelerator => Some(Self::GcOwnedRefObservation),
            Self::GcOwnedRefObservation => Some(Self::SchemaCache),
            Self::SchemaCache => Some(Self::StatisticsArtifactCache),
            Self::StatisticsArtifactCache => Some(Self::LocalViewRegistry),
            Self::LocalViewRegistry => Some(Self::DmlRuntime),
            Self::DmlRuntime => Some(Self::MaintenanceRuntime),
            Self::MaintenanceRuntime => Some(Self::StatisticsJobRuntime),
            Self::StatisticsJobRuntime => Some(Self::MvRefreshRuntime),
            Self::MvRefreshRuntime => Some(Self::BackendObservedRuntime),
            Self::BackendObservedRuntime => None,
        }
    }

    /// Walks the manifest chain into an array.
    ///
    /// Evaluated as a `const`, so both failures below are compile errors rather
    /// than runtime ones: a family linked into the chain without bumping
    /// `COUNT` ends the walk early, and a `COUNT` raised without linking a
    /// family leaves the chain running past the end.
    const fn enumerate() -> [Self; Self::COUNT] {
        let mut families = [Self::FIRST; Self::COUNT];
        let mut index = 1;
        while index < Self::COUNT {
            families[index] = match families[index - 1].next_in_manifest() {
                Some(next) => next,
                None => panic!("state family chain ends before COUNT families are registered"),
            };
            index += 1;
        }
        assert!(
            families[Self::COUNT - 1].next_in_manifest().is_none(),
            "state family chain continues past COUNT registered families"
        );
        families
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::state_family::WipeEntry;

    /// Spec §5.3 registers twelve families: two `ExternalProjection`, four
    /// `Accelerator` (two of them in-process) and six `ProcessRuntime`.
    #[test]
    fn manifest_registers_exactly_the_spec_family_table() {
        assert_eq!(
            StateFamily::ALL.len(),
            12,
            "spec 5.3 registers twelve frontend state families"
        );

        let mut external_projection = 0;
        let mut process_runtime = 0;
        let mut accelerator = 0;
        for family in StateFamily::ALL {
            // Exhaustive over the closed classification: a fourth variant makes
            // this match, and every other consumer, fail to compile.
            match family.classification() {
                StateFamilyClassification::ExternalProjection(_) => external_projection += 1,
                StateFamilyClassification::ProcessRuntime(_) => process_runtime += 1,
                StateFamilyClassification::Accelerator(_) => accelerator += 1,
            }
        }

        assert_eq!(external_projection, 2, "catalog and backend desired state");
        assert_eq!(
            accelerator, 4,
            "MV, GC observation, schema cache, statistics artifact cache"
        );
        assert_eq!(
            process_runtime, 6,
            "local views, DML, maintenance, statistics jobs, MV refresh, backend observations"
        );
    }

    #[test]
    fn manifest_ids_are_unique_and_the_chain_visits_each_family_once() {
        let ids: BTreeSet<&str> = StateFamily::ALL
            .into_iter()
            .map(StateFamily::family_id)
            .collect();
        assert_eq!(
            ids.len(),
            StateFamily::ALL.len(),
            "every registered family needs a distinct stable id"
        );

        let families: BTreeSet<StateFamily> = StateFamily::ALL.into_iter().collect();
        assert_eq!(
            families.len(),
            StateFamily::ALL.len(),
            "the manifest chain must not visit a family twice"
        );
    }

    /// Key-to-family attribution is only well defined when no prefix contains
    /// another; otherwise a key under the longer prefix would also match the
    /// shorter one and the store-content gate could not name its owner.
    #[test]
    fn persistent_prefixes_are_unique_and_non_nested() {
        let prefixes: Vec<(StateFamily, &str)> = StateFamily::ALL
            .into_iter()
            .filter_map(|family| {
                family
                    .persistent_prefix()
                    .map(|prefix| (family, prefix.as_str()))
            })
            .collect();
        assert_eq!(prefixes.len(), 4, "four families are durable today");

        let distinct: BTreeSet<&str> = prefixes.iter().map(|(_, prefix)| *prefix).collect();
        assert_eq!(
            distinct.len(),
            prefixes.len(),
            "two families must never share a prefix"
        );

        for (left_family, left) in &prefixes {
            for (right_family, right) in &prefixes {
                if left_family == right_family {
                    continue;
                }
                assert!(
                    !left.starts_with(*right),
                    "{} prefix {left:?} is nested under {} prefix {right:?}",
                    left_family.family_id(),
                    right_family.family_id()
                );
            }
        }
    }

    /// These bytes are already in deployed stores.  The literals are repeated
    /// here on purpose: reading them from the manifest constants would make the
    /// assertion vacuous, and the whole value of this test is that an edit to a
    /// prefix has to be made twice, deliberately.
    #[test]
    fn prefix_literals_are_byte_stable() {
        let expected: [(StateFamily, &[u8]); 4] = [
            (
                StateFamily::CatalogDesiredState,
                b"novarocks/frontend/catalog/v1/attachment/by-instance/",
            ),
            (
                StateFamily::BackendDesiredState,
                b"novarocks/frontend/cluster-backends/v1/state",
            ),
            (
                StateFamily::MvAccelerator,
                b"novarocks/frontend/mv/accelerator/v1",
            ),
            (
                StateFamily::GcOwnedRefObservation,
                b"novarocks/frontend/table-maintenance/v7/gc-owned-ref-observations/",
            ),
        ];

        for (family, bytes) in expected {
            let prefix = family
                .persistent_prefix()
                .expect("registered durable family");
            assert_eq!(
                prefix.as_bytes(),
                bytes,
                "{} prefix must stay byte-identical",
                family.family_id()
            );
        }
    }

    /// A `ProcessRuntime` family cannot yield a persistent prefix, and this test
    /// shows it by *shape* rather than by assertion.
    ///
    /// `prefix_of` is a total function over the closed classification.  Its
    /// `ProcessRuntime` arm has nothing to return a prefix from:
    /// `ProcessRuntimeContract` holds no prefix field and exposes no prefix
    /// accessor, so the only expression that arm can produce is `None`.  Adding
    /// a prefix to a `ProcessRuntime` entry would mean adding it to that
    /// contract, which is a change to the type, not to this test.
    #[test]
    fn process_runtime_cannot_express_a_persistent_prefix() {
        fn prefix_of(classification: StateFamilyClassification) -> Option<&'static str> {
            match classification {
                StateFamilyClassification::ExternalProjection(contract) => {
                    Some(contract.persistent_prefix().as_str())
                }
                StateFamilyClassification::Accelerator(contract) => contract
                    .persistent_prefix()
                    .map(PersistentKeyPrefix::as_str),
                StateFamilyClassification::ProcessRuntime(contract) => {
                    // The only fact available here is the authority the family
                    // belongs to.  There is no prefix to name.
                    let _authority = contract.authority();
                    None
                }
            }
        }

        for family in StateFamily::ALL {
            let classification = family.classification();
            assert_eq!(
                prefix_of(classification),
                classification
                    .persistent_prefix()
                    .map(PersistentKeyPrefix::as_str),
                "{} must derive its prefix only through the persistent contracts",
                family.family_id()
            );

            if matches!(classification, StateFamilyClassification::ProcessRuntime(_)) {
                assert_eq!(
                    family.durability_admission(),
                    DurabilityAdmission::Forbidden,
                    "{} is runtime state and must never own a StateStore record",
                    family.family_id()
                );
                assert!(!family.retain_on_restart());
                assert_eq!(family.clone_policy(), ClonePolicy::NotCloned);
                assert_eq!(family.record_version(), None);
            }
        }
    }

    #[test]
    fn every_accelerator_answers_retain_clone_and_wipe() {
        let mut accelerators = 0;
        for family in StateFamily::ALL {
            let StateFamilyClassification::Accelerator(contract) = family.classification() else {
                continue;
            };
            accelerators += 1;

            let retain = contract.retain_on_restart();
            let clone_policy = contract.clone_policy();
            let wipe_entry = contract.wipe_entry();
            let determinism = contract.rebuild_determinism();

            match contract.residence() {
                AcceleratorResidence::Durable {
                    prefix,
                    record_version,
                } => {
                    assert!(
                        !prefix.as_str().is_empty(),
                        "{} declares a durable prefix",
                        family.family_id()
                    );
                    assert!(record_version > 0, "{}", family.family_id());
                    assert!(
                        retain,
                        "{} is durable so a restart must find it",
                        family.family_id()
                    );
                    assert_eq!(wipe_entry, WipeEntry::DeleteWholePrefix);
                    assert!(
                        matches!(
                            clone_policy,
                            ClonePolicy::RevalidateSourceRevisionOrWipe
                                | ClonePolicy::WipeAndRebuild
                        ),
                        "{} must not carry derived records into a clone unchecked",
                        family.family_id()
                    );
                    assert_eq!(
                        family.durability_admission(),
                        DurabilityAdmission::Permitted
                    );
                }
                AcceleratorResidence::InProcess => {
                    assert!(
                        !retain,
                        "{} lives in process memory only",
                        family.family_id()
                    );
                    assert_eq!(wipe_entry, WipeEntry::DropInProcessCache);
                    assert_eq!(clone_policy, ClonePolicy::NotCloned);
                    assert_eq!(contract.record_version(), None);
                    assert_eq!(
                        family.durability_admission(),
                        DurabilityAdmission::Forbidden
                    );
                }
            }

            // Every accelerator states what a rebuild reproduces, so a wipe's
            // cost is a recorded fact and not a guess made during an incident.
            // GC observation is the one family whose rebuild is deliberately
            // not identical: it restarts a safety window instead.
            let expected_determinism = match family {
                StateFamily::GcOwnedRefObservation => RebuildDeterminism::ConservativeRestart,
                _ => RebuildDeterminism::UserVisibleIdentical,
            };
            assert_eq!(determinism, expected_determinism, "{}", family.family_id());
        }
        assert_eq!(accelerators, 4);
    }

    #[test]
    fn external_projections_declare_source_snapshot_and_failure_scope() {
        let catalog = StateFamily::CatalogDesiredState.classification();
        let StateFamilyClassification::ExternalProjection(catalog) = catalog else {
            panic!("catalog desired state is an external projection");
        };
        assert_eq!(
            catalog.source(),
            ExternalProjectionSource::SelectedCatalogSourceMode
        );
        assert_eq!(
            catalog.snapshot_identity(),
            SnapshotIdentity::CompleteEnumeration
        );
        assert_eq!(
            catalog.bootstrap_failure_scope(),
            BootstrapFailureScope::GlobalEnumerationPerEntryMaterialization
        );
        assert_eq!(catalog.clone_policy(), ClonePolicy::SemanticRebind);

        let backends = StateFamily::BackendDesiredState.classification();
        let StateFamilyClassification::ExternalProjection(backends) = backends else {
            panic!("backend desired state is an external projection");
        };
        assert_eq!(
            backends.source(),
            ExternalProjectionSource::ConfiguredSeedsAndSqlMembership
        );
        assert_eq!(
            backends.snapshot_identity(),
            SnapshotIdentity::SingleVersionedRecord
        );
        assert_eq!(
            backends.bootstrap_failure_scope(),
            BootstrapFailureScope::GlobalOnly
        );
        assert_eq!(backends.clone_policy(), ClonePolicy::NotCloned);
    }

    #[test]
    fn every_process_runtime_family_declares_its_authority() {
        let mut authorities = Vec::new();
        for family in StateFamily::ALL {
            if let StateFamilyClassification::ProcessRuntime(contract) = family.classification() {
                authorities.push((family, contract.authority()));
            }
        }
        assert_eq!(authorities.len(), 6);

        assert!(
            authorities.contains(&(StateFamily::DmlRuntime, ProcessRuntimeAuthority::Statement))
        );
        assert!(authorities.contains(&(
            StateFamily::MaintenanceRuntime,
            ProcessRuntimeAuthority::Attempt
        )));
        assert!(authorities.contains(&(
            StateFamily::StatisticsJobRuntime,
            ProcessRuntimeAuthority::Attempt
        )));
        assert!(authorities.contains(&(
            StateFamily::LocalViewRegistry,
            ProcessRuntimeAuthority::FrontendIncarnation
        )));
        assert!(authorities.contains(&(
            StateFamily::MvRefreshRuntime,
            ProcessRuntimeAuthority::FrontendIncarnation
        )));
        assert!(authorities.contains(&(
            StateFamily::BackendObservedRuntime,
            ProcessRuntimeAuthority::FrontendIncarnation
        )));
    }

    /// The prefix API has to serve all four current owners without any of them
    /// re-declaring a prefix.  These are the exact keys those owners build
    /// today, reproduced through the manifest.
    #[test]
    fn prefix_api_reproduces_every_owner_key_scheme() {
        // catalog_attachment: prefix ends in `/`, suffix is the hex-encoded
        // normalized instance id.
        let prefix = StateFamily::CatalogDesiredState
            .persistent_prefix()
            .expect("durable family");
        assert_eq!(
            prefix.key().expect("prefix key").as_bytes(),
            b"novarocks/frontend/catalog/v1/attachment/by-instance/"
        );
        assert_eq!(
            prefix
                .key_with_suffix("77617265686f7573652e6d61696e")
                .expect("attachment key")
                .as_bytes(),
            b"novarocks/frontend/catalog/v1/attachment/by-instance/77617265686f7573652e6d61696e"
        );

        // topology: the prefix is the whole key; there is no suffix.
        assert_eq!(
            StateFamily::BackendDesiredState
                .persistent_prefix()
                .expect("durable family")
                .key()
                .expect("cluster backends key")
                .as_bytes(),
            b"novarocks/frontend/cluster-backends/v1/state"
        );

        // mv: the prefix carries no trailing separator, so the owner joins with
        // its own `/`.
        assert_eq!(
            StateFamily::MvAccelerator
                .persistent_prefix()
                .expect("durable family")
                .key_with_suffix("/sequence/mv-id")
                .expect("mv sequence key")
                .as_bytes(),
            b"novarocks/frontend/mv/accelerator/v1/sequence/mv-id"
        );

        // gc observation: prefix ends in `/`, suffix is `<table uuid>/<hex ref>`.
        assert_eq!(
            StateFamily::GcOwnedRefObservation
                .persistent_prefix()
                .expect("durable family")
                .key_with_suffix("019205ff-0000-7000-8000-000000000001/6d61696e")
                .expect("gc observation key")
                .as_bytes(),
            concat!(
                "novarocks/frontend/table-maintenance/v7/gc-owned-ref-observations/",
                "019205ff-0000-7000-8000-000000000001/6d61696e"
            )
            .as_bytes()
        );
    }

    #[test]
    fn key_attribution_names_the_owning_family_or_nothing() {
        assert_eq!(
            StateFamily::for_key(
                b"novarocks/frontend/mv/accelerator/v1/projection/by-id/0000000000000001"
            ),
            Some(StateFamily::MvAccelerator)
        );
        assert_eq!(
            StateFamily::for_key(b"novarocks/frontend/cluster-backends/v1/state"),
            Some(StateFamily::BackendDesiredState)
        );

        // Retired families are unattributable by construction: they are absent
        // from the manifest, so the store-content gate reports them instead of
        // finding an owner willing to claim them.
        assert_eq!(StateFamily::for_key(b"\0novarocks/cp/v1/control"), None);
        assert_eq!(StateFamily::for_key(b"novarocks/frontend/views/v2/x"), None);
        assert_eq!(StateFamily::for_key(b""), None);

        for family in StateFamily::ALL {
            let Some(prefix) = family.persistent_prefix() else {
                continue;
            };
            assert_eq!(
                StateFamily::for_key(prefix.as_bytes()),
                Some(family),
                "{} must claim its own prefix",
                family.family_id()
            );
            assert_eq!(
                family.durability_admission(),
                DurabilityAdmission::Permitted,
                "{} owns keys so its classification must permit durability",
                family.family_id()
            );
        }
    }
}
