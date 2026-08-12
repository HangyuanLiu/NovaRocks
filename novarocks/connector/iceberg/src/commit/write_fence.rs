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

//! Provider-owned external write fence for distributed Iceberg DML.
//!
//! # Why an external fence is needed
//!
//! A control-plane lease can stop a stale frontend from mutating durable
//! coordination state, but it cannot withdraw a Connector commit that was
//! already dispatched. A frontend that was paused mid-write can wake up after
//! another frontend took over and still submit its commit to the catalog. The
//! only place that race can be settled is the external linearization point:
//! the catalog's own atomic conditional table update.
//!
//! # Mechanism
//!
//! Every distributed write attempt first publishes a *marker snapshot* on a
//! provider-private fence ref, then submits its real write with a
//! `RefSnapshotIdMatch` requirement pinning that ref to the marker it
//! published. Raising the fence (publishing a newer marker) therefore
//! invalidates the pinned requirement of every older attempt: the older
//! commit fails atomically inside the catalog update instead of landing late.
//!
//! Marker snapshots carry no data files and an empty manifest list. Their
//! summary carries the fence provenance so that a later owner — which cannot
//! obtain the old runtime session — can still classify the old attempt from
//! immutable external truth alone.
//!
//! # Fence ref granularity
//!
//! The fence ref is derived from the **stable write operation id**, not from
//! the table alone. A single table-wide fence ref would make every concurrent
//! DML statement on that table contend on one marker, effectively turning the
//! fence into a table-global write lease and serializing sibling operations
//! that are not stale at all. Per-operation refs keep the fence a takeover
//! guard: only a *later attempt of the same operation* can fence an earlier
//! one, while unrelated concurrent operations continue to be arbitrated by
//! ordinary Iceberg base-state CAS on the data ref.
//!
//! This also makes the "different operations must not reuse a marker"
//! invariant structural rather than merely checked.
//!
//! # What "provider-private" does and does not cover
//!
//! The fence *ref* is private: it lives outside the user's branch namespace and
//! nothing routes reads through it. The marker *snapshots* it points at are
//! not — Iceberg snapshots belong to the table, not to a ref, so they appear in
//! the table's global snapshot list. NovaRocks filters them out of its own
//! `$snapshots` metadata table, but another engine reading the same table will
//! still see them. That is inherent to using a snapshot as the carrier.

// Design: ADR-0065 (docs/adr/ADR-0065-external-write-fence-as-catalog-linearization-point.md)

use std::collections::HashMap;

use crate::commit::helpers::{effective_next_row_id, metadata_dir, now_ms, write_manifest_list};
use crate::iceberg::io::FileIO;
use crate::iceberg::spec::{
    Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary, TableMetadata,
};
use crate::iceberg::table::Table;
use crate::iceberg::{Catalog, TableCommit, TableRequirement, TableUpdate};

/// Prefix of the provider-private fence refs. Ref names under this prefix are
/// owned by the write fence and must never be treated as user branches.
pub const WRITE_FENCE_REF_PREFIX: &str = "novarocks-write-fence-";

/// Stable sentinel embedded in every "this attempt was fenced" message.
///
/// The commit pipeline classifies failures from message text
/// (`commit::service::classify_commit_error`). A fenced attempt is *definitely*
/// uncommitted — we hold positive proof, because the catalog rejected the
/// conditional update. Without a signal the classifier recognises, that proof
/// would degrade into `CommitUnknown`, staged files would be retained for a
/// commit that provably never happened, and the caller would be invited to
/// retry under an authority it no longer holds. Keep this constant and the
/// classifier's signal list in sync.
pub const WRITE_FENCE_SUPERSEDED_SIGNAL: &str = "external write fence superseded";

const FENCE_PROP_VERSION: &str = "novarocks.write-fence.version";
const FENCE_PROP_OPERATION_ID: &str = "novarocks.write-fence.operation-id";
const FENCE_PROP_CLUSTER_DIGEST: &str = "novarocks.write-fence.cluster-identity-digest";
const FENCE_PROP_INCARNATION: &str = "novarocks.write-fence.control-plane-incarnation";
const FENCE_PROP_RESOURCE_EPOCH: &str = "novarocks.write-fence.resource-epoch";
const FENCE_PROP_ATTEMPT_NUMBER: &str = "novarocks.write-fence.coordination-attempt";
const FENCE_PROP_ATTEMPT_ID: &str = "novarocks.write-fence.coordination-attempt-id";
const FENCE_PROP_NAMESPACE: &str = "novarocks.write-fence.namespace";
const FENCE_PROP_TABLE: &str = "novarocks.write-fence.table";
const FENCE_PROP_TARGET_REF: &str = "novarocks.write-fence.target-ref";
const FENCE_PROP_DIGEST: &str = "novarocks.write-fence.digest";

/// Wire version of the marker summary layout. A marker written by a future
/// layout must be reported as ambiguous rather than reinterpreted.
const FENCE_MARKER_VERSION: &str = "1";

/// Total-order fence generation.
///
/// Derived `Ord` is lexicographic in declaration order, which is exactly the
/// control-plane precedence: a newer control-plane incarnation outranks any
/// resource epoch, and a newer resource epoch outranks any coordination
/// attempt. Field order must stay in sync with the SPI fence generation.
///
/// The coordination attempt counter is load-bearing, not decoration: a
/// recovering owner can legitimately hold the same incarnation and resource
/// epoch as the attempt it is taking over and differ only in attempt number.
/// Without that third component `raise_fence` would refuse a valid takeover.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FenceGeneration {
    pub control_plane_incarnation: u64,
    pub resource_epoch: u64,
    pub coordination_attempt: u64,
}

/// Neutral scalar fence facts, projected by the frontend from its
/// control-plane fencing token and the DML operation identity.
///
/// This is the provider-private shape. The SPI-owned fence value is mapped
/// onto it at the write-control boundary so that the provider never depends on
/// control-plane types and never sees a lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergWriteFenceFacts {
    pub cluster_identity_digest: String,
    pub control_plane_incarnation: u64,
    pub resource_epoch: u64,
    /// Monotonic coordination attempt counter, the lowest-order component of
    /// the fence generation.
    pub coordination_attempt: u64,
    /// Stable write operation id. Identical across retries and across a
    /// takeover of the same operation.
    pub write_operation_id: String,
    /// Namespace of the fenced resource.
    pub namespace: String,
    /// Table name of the fenced resource.
    pub table_name: String,
    /// Data ref this operation writes to.
    pub target_ref: String,
    /// Identifies this coordination attempt. Differs between the original
    /// owner and a later owner recovering the same operation.
    pub coordination_attempt_id: String,
    /// Digest of the whole SPI fence value, carried so a later owner can prove
    /// the marker belongs to the fence it is reasoning about.
    pub fence_digest: String,
}

impl IcebergWriteFenceFacts {
    pub fn generation(&self) -> FenceGeneration {
        FenceGeneration {
            control_plane_incarnation: self.control_plane_incarnation,
            resource_epoch: self.resource_epoch,
            coordination_attempt: self.coordination_attempt,
        }
    }

    /// Provider-private fence ref for this operation.
    pub fn fence_ref(&self) -> String {
        format!("{WRITE_FENCE_REF_PREFIX}{}", self.write_operation_id)
    }

    fn validate(&self) -> Result<(), String> {
        for (label, value) in [
            ("cluster identity digest", &self.cluster_identity_digest),
            ("write operation id", &self.write_operation_id),
            ("namespace", &self.namespace),
            ("table name", &self.table_name),
            ("target ref", &self.target_ref),
            ("coordination attempt id", &self.coordination_attempt_id),
            ("fence digest", &self.fence_digest),
        ] {
            if value.is_empty() {
                return Err(format!("iceberg write fence: {label} must not be empty"));
            }
            if value.len() > 256 {
                return Err(format!(
                    "iceberg write fence: {label} exceeds the 256-byte bound"
                ));
            }
        }
        if self
            .write_operation_id
            .contains(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != '_')
        {
            return Err(
                "iceberg write fence: write operation id must be ASCII alphanumeric, '-' or '_'"
                    .to_string(),
            );
        }
        if self.target_ref.starts_with(WRITE_FENCE_REF_PREFIX) {
            return Err(
                "iceberg write fence: a data target ref must not live under the fence ref prefix"
                    .to_string(),
            );
        }
        // Mirror the SPI fence generation, which treats zero as "unset" and
        // refuses it. A generation component that can be defaulted is a
        // generation that can be forged.
        for (label, value) in [
            ("control-plane incarnation", self.control_plane_incarnation),
            ("resource epoch", self.resource_epoch),
            ("coordination attempt", self.coordination_attempt),
        ] {
            if value == 0 {
                return Err(format!("iceberg write fence: {label} must be nonzero"));
            }
        }
        Ok(())
    }
}

/// Project the SPI-owned fence value onto the provider-private facts.
///
/// This is the single mapping point between the neutral contract and the
/// Iceberg carrier. Both the ordinary write path and the historical recovery
/// facet must go through it so that one fence value can only ever name one
/// marker.
///
/// The operation id is rendered through its `Display` form and the two digests
/// through lowercase hex, because both end up in an Iceberg ref name or a
/// snapshot summary value and must be stable, bounded and ASCII.
pub fn fence_facts_from_spi(
    fence: &novarocks_spi::connector::ConnectorExternalOperationFence,
) -> IcebergWriteFenceFacts {
    let generation = fence.generation();
    IcebergWriteFenceFacts {
        cluster_identity_digest: hex_lower(&fence.cluster().digest()),
        control_plane_incarnation: generation.control_plane_incarnation(),
        resource_epoch: generation.resource_epoch(),
        coordination_attempt: generation.coordination_attempt(),
        write_operation_id: fence.operation_id().to_string(),
        namespace: fence.table().namespace.to_string(),
        table_name: fence.table().table.to_string(),
        target_ref: fence.target_ref().as_str().to_string(),
        coordination_attempt_id: hex_lower(&fence.coordination_attempt_id()),
        fence_digest: hex_lower(&fence.digest()),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// A fence marker observed on the fence ref.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedFence {
    pub snapshot_id: i64,
    pub facts: IcebergWriteFenceFacts,
}

impl ObservedFence {
    pub fn generation(&self) -> FenceGeneration {
        self.facts.generation()
    }
}

/// What an established fence lets a subsequent write assert atomically.
///
/// Carrying this into the write's `TableCommit` is what makes the fence
/// comparison and the write itself a single atomic decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergFenceAssertion {
    fence_ref: String,
    fence_snapshot_id: i64,
}

impl IcebergFenceAssertion {
    /// Build an assertion from a marker that was actually observed on the fence
    /// ref.
    ///
    /// There is deliberately no way to construct an assertion without an
    /// observation: an assertion asserts "the fence still points at *my*
    /// marker", and a value invented without looking would assert nothing.
    pub fn from_observed(fence_ref: &str, observed: &ObservedFence) -> Self {
        Self {
            fence_ref: fence_ref.to_string(),
            fence_snapshot_id: observed.snapshot_id,
        }
    }

    pub fn fence_ref(&self) -> &str {
        &self.fence_ref
    }

    pub fn fence_snapshot_id(&self) -> i64 {
        self.fence_snapshot_id
    }

    /// The requirement that pins the fence ref to this attempt's marker.
    pub fn requirements(&self) -> Vec<TableRequirement> {
        vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: self.fence_ref.clone(),
            snapshot_id: Some(self.fence_snapshot_id),
        }]
    }
}

/// Why a fence establishment or a fenced commit was refused.
///
/// `Superseded` is a terminal classification, never a retryable conflict and
/// never an unknown outcome: a higher generation is proof that another owner
/// took this operation over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FenceError {
    /// A strictly higher fence generation already exists for this operation.
    Superseded {
        observed: FenceGeneration,
        requested: FenceGeneration,
    },
    /// No marker exists for this operation at all.
    ///
    /// For a commit this is fail-closed, not "fence not needed": either the
    /// fence was never established before dispatch (a coordination bug) or a
    /// later owner already finalized the operation and retired the ref. Both
    /// mean this attempt has no authority to write.
    NotEstablished { fence_ref: String },
    /// The marker at this generation belongs to a different operation or a
    /// different fence value.
    MarkerConflict { detail: String },
    /// The marker exists but cannot be interpreted by this layout version.
    Ambiguous { detail: String },
    /// Establishing the fence failed for an ordinary reason (IO, catalog).
    Failed { detail: String },
}

impl std::fmt::Display for FenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Superseded {
                observed,
                requested,
            } => write!(
                f,
                "iceberg write fence superseded: observed generation {observed:?} is higher than requested {requested:?}"
            ),
            Self::NotEstablished { fence_ref } => write!(
                f,
                "iceberg write fence was never established (or was already retired) on ref '{fence_ref}'"
            ),
            Self::MarkerConflict { detail } => {
                write!(f, "iceberg write fence marker conflict: {detail}")
            }
            Self::Ambiguous { detail } => write!(f, "iceberg write fence ambiguous: {detail}"),
            Self::Failed { detail } => write!(f, "iceberg write fence failed: {detail}"),
        }
    }
}

/// Outcome of establishing (or re-establishing) this attempt's fence.
#[derive(Clone, Debug)]
pub struct EstablishedFence {
    pub assertion: IcebergFenceAssertion,
    /// True when an existing marker for the same operation and generation was
    /// reused instead of publishing a new one. Same operation + same
    /// generation retries are idempotent.
    pub reused: bool,
}

/// Read the fence marker currently published for `facts`' operation.
pub fn observe_fence(
    metadata: &TableMetadata,
    fence_ref: &str,
) -> Result<Option<ObservedFence>, FenceError> {
    let Some(reference) = metadata.refs().get(fence_ref) else {
        return Ok(None);
    };
    let snapshot_id = reference.snapshot_id;
    let snapshot = metadata.snapshot_by_id(snapshot_id).ok_or_else(|| {
        FenceError::Ambiguous {
            detail: format!(
                "fence ref '{fence_ref}' points at snapshot {snapshot_id}, which is not in table metadata"
            ),
        }
    })?;
    let facts = parse_marker_summary(snapshot.summary(), fence_ref)?;
    Ok(Some(ObservedFence { snapshot_id, facts }))
}

fn parse_marker_summary(
    summary: &Summary,
    fence_ref: &str,
) -> Result<IcebergWriteFenceFacts, FenceError> {
    let get = |key: &str| -> Result<String, FenceError> {
        summary
            .additional_properties
            .get(key)
            .cloned()
            .ok_or_else(|| FenceError::Ambiguous {
                detail: format!("fence marker on '{fence_ref}' is missing {key}"),
            })
    };
    let version = get(FENCE_PROP_VERSION)?;
    if version != FENCE_MARKER_VERSION {
        return Err(FenceError::Ambiguous {
            detail: format!(
                "fence marker on '{fence_ref}' has layout version {version}; this build understands {FENCE_MARKER_VERSION}"
            ),
        });
    }
    let parse_u64 = |key: &str, raw: String| -> Result<u64, FenceError> {
        raw.parse::<u64>().map_err(|error| FenceError::Ambiguous {
            detail: format!("fence marker on '{fence_ref}' has invalid {key}: {error}"),
        })
    };
    let control_plane_incarnation =
        parse_u64(FENCE_PROP_INCARNATION, get(FENCE_PROP_INCARNATION)?)?;
    let resource_epoch = parse_u64(FENCE_PROP_RESOURCE_EPOCH, get(FENCE_PROP_RESOURCE_EPOCH)?)?;
    let coordination_attempt =
        parse_u64(FENCE_PROP_ATTEMPT_NUMBER, get(FENCE_PROP_ATTEMPT_NUMBER)?)?;
    Ok(IcebergWriteFenceFacts {
        cluster_identity_digest: get(FENCE_PROP_CLUSTER_DIGEST)?,
        control_plane_incarnation,
        resource_epoch,
        coordination_attempt,
        write_operation_id: get(FENCE_PROP_OPERATION_ID)?,
        namespace: get(FENCE_PROP_NAMESPACE)?,
        table_name: get(FENCE_PROP_TABLE)?,
        target_ref: get(FENCE_PROP_TARGET_REF)?,
        coordination_attempt_id: get(FENCE_PROP_ATTEMPT_ID)?,
        fence_digest: get(FENCE_PROP_DIGEST)?,
    })
}

fn marker_summary(facts: &IcebergWriteFenceFacts) -> Summary {
    let mut additional_properties = HashMap::new();
    additional_properties.insert(
        FENCE_PROP_VERSION.to_string(),
        FENCE_MARKER_VERSION.to_string(),
    );
    additional_properties.insert(
        FENCE_PROP_OPERATION_ID.to_string(),
        facts.write_operation_id.clone(),
    );
    additional_properties.insert(
        FENCE_PROP_CLUSTER_DIGEST.to_string(),
        facts.cluster_identity_digest.clone(),
    );
    additional_properties.insert(
        FENCE_PROP_INCARNATION.to_string(),
        facts.control_plane_incarnation.to_string(),
    );
    additional_properties.insert(
        FENCE_PROP_RESOURCE_EPOCH.to_string(),
        facts.resource_epoch.to_string(),
    );
    additional_properties.insert(
        FENCE_PROP_ATTEMPT_NUMBER.to_string(),
        facts.coordination_attempt.to_string(),
    );
    additional_properties.insert(
        FENCE_PROP_ATTEMPT_ID.to_string(),
        facts.coordination_attempt_id.clone(),
    );
    additional_properties.insert(FENCE_PROP_NAMESPACE.to_string(), facts.namespace.clone());
    additional_properties.insert(FENCE_PROP_TABLE.to_string(), facts.table_name.clone());
    additional_properties.insert(FENCE_PROP_TARGET_REF.to_string(), facts.target_ref.clone());
    additional_properties.insert(FENCE_PROP_DIGEST.to_string(), facts.fence_digest.clone());
    // Marker snapshots add no data. `Append` is the only operation kind that
    // truthfully describes "nothing was added or removed".
    Summary {
        operation: Operation::Append,
        additional_properties,
    }
}

/// Recover this attempt's assertion from external truth, without any
/// process-local memory of having established it.
///
/// The provider deliberately does not cache assertions. Re-deriving them by
/// observation keeps the commit path correct across lease clones and across a
/// process restart, and it fails closed *before* any staging work when another
/// owner has already raised the fence.
///
/// The marker must match this fence value on both generation and digest.
/// Matching only the operation id would let a *different attempt* of the same
/// operation borrow this attempt's assertion.
pub fn derive_established_assertion(
    metadata: &TableMetadata,
    facts: &IcebergWriteFenceFacts,
) -> Result<IcebergFenceAssertion, FenceError> {
    facts
        .validate()
        .map_err(|detail| FenceError::Failed { detail })?;
    let fence_ref = facts.fence_ref();
    let observed =
        observe_fence(metadata, &fence_ref)?.ok_or_else(|| FenceError::NotEstablished {
            fence_ref: fence_ref.clone(),
        })?;
    if observed.facts.write_operation_id != facts.write_operation_id {
        return Err(FenceError::MarkerConflict {
            detail: format!(
                "fence ref '{fence_ref}' carries operation {} but this commit is operation {}",
                observed.facts.write_operation_id, facts.write_operation_id
            ),
        });
    }
    if observed.generation() != facts.generation() {
        return Err(FenceError::Superseded {
            observed: observed.generation(),
            requested: facts.generation(),
        });
    }
    if observed.facts.fence_digest != facts.fence_digest {
        return Err(FenceError::MarkerConflict {
            detail: format!(
                "fence ref '{fence_ref}' carries a different fence value at generation {:?}",
                facts.generation()
            ),
        });
    }
    Ok(IcebergFenceAssertion::from_observed(&fence_ref, &observed))
}

/// Establish this attempt's fence, or reuse an identical existing marker.
///
/// Fails closed: a strictly higher generation, a marker belonging to another
/// operation, or an uninterpretable marker all refuse rather than degrade.
/// The caller must not dispatch any writer or commit work unless this
/// succeeds.
pub async fn establish_fence(
    catalog: &dyn Catalog,
    table: &Table,
    file_io: &FileIO,
    facts: &IcebergWriteFenceFacts,
) -> Result<EstablishedFence, FenceError> {
    facts
        .validate()
        .map_err(|detail| FenceError::Failed { detail })?;
    let fence_ref = facts.fence_ref();
    let requested = facts.generation();
    let observed = observe_fence(table.metadata(), &fence_ref)?;

    if let Some(existing) = &observed {
        if existing.facts.write_operation_id != facts.write_operation_id {
            return Err(FenceError::MarkerConflict {
                detail: format!(
                    "fence ref '{fence_ref}' carries operation {} but this attempt is operation {}",
                    existing.facts.write_operation_id, facts.write_operation_id
                ),
            });
        }
        let existing_generation = existing.generation();
        if existing_generation > requested {
            return Err(FenceError::Superseded {
                observed: existing_generation,
                requested,
            });
        }
        if existing_generation == requested {
            // Same operation at the same generation: an idempotent retry of
            // this very attempt. Reuse the marker rather than publishing a
            // second one at the same generation.
            if existing.facts.fence_digest != facts.fence_digest {
                return Err(FenceError::MarkerConflict {
                    detail: format!(
                        "fence ref '{fence_ref}' carries a different fence value at generation {requested:?}"
                    ),
                });
            }
            return Ok(EstablishedFence {
                assertion: IcebergFenceAssertion {
                    fence_ref,
                    fence_snapshot_id: existing.snapshot_id,
                },
                reused: true,
            });
        }
    }

    let observed_snapshot_id = observed.as_ref().map(|existing| existing.snapshot_id);
    let assertion = publish_marker(
        catalog,
        table,
        file_io,
        facts,
        &fence_ref,
        observed_snapshot_id,
    )
    .await?;
    Ok(EstablishedFence {
        assertion,
        reused: false,
    })
}

/// Raise the fence above every existing generation for this operation.
///
/// A recovering owner calls this before inspecting the old attempt: once it
/// succeeds, no older generation can commit any more, which is what makes a
/// subsequent `NotDispatched` classification safe to act on.
pub async fn raise_fence(
    catalog: &dyn Catalog,
    table: &Table,
    file_io: &FileIO,
    facts: &IcebergWriteFenceFacts,
) -> Result<EstablishedFence, FenceError> {
    facts
        .validate()
        .map_err(|detail| FenceError::Failed { detail })?;
    let fence_ref = facts.fence_ref();
    let requested = facts.generation();
    let observed = observe_fence(table.metadata(), &fence_ref)?;
    if let Some(existing) = &observed {
        if existing.facts.write_operation_id != facts.write_operation_id {
            return Err(FenceError::MarkerConflict {
                detail: format!(
                    "fence ref '{fence_ref}' carries operation {} but recovery is for operation {}",
                    existing.facts.write_operation_id, facts.write_operation_id
                ),
            });
        }
        // Raising must be strictly monotonic. Equal generations do not close
        // the old authority, so they are refused instead of accepted as a
        // no-op.
        if existing.generation() >= requested {
            return Err(FenceError::Superseded {
                observed: existing.generation(),
                requested,
            });
        }
    }
    let observed_snapshot_id = observed.as_ref().map(|existing| existing.snapshot_id);
    let assertion = publish_marker(
        catalog,
        table,
        file_io,
        facts,
        &fence_ref,
        observed_snapshot_id,
    )
    .await?;
    Ok(EstablishedFence {
        assertion,
        reused: false,
    })
}

/// Publish one marker snapshot and move the fence ref onto it in a single
/// atomic conditional catalog update.
async fn publish_marker(
    catalog: &dyn Catalog,
    table: &Table,
    file_io: &FileIO,
    facts: &IcebergWriteFenceFacts,
    fence_ref: &str,
    observed_snapshot_id: Option<i64>,
) -> Result<IcebergFenceAssertion, FenceError> {
    let metadata = table.metadata();
    let snapshot_id = crate::commit::helpers::generate_snapshot_id();
    let sequence_number = metadata.last_sequence_number() + 1;
    let manifest_list_path = format!(
        "{}/novarocks-write-fence-{}-{}-{}.avro",
        metadata_dir(table),
        facts.write_operation_id,
        facts.control_plane_incarnation,
        snapshot_id
    );
    let first_row_id = match metadata.format_version() {
        crate::iceberg::spec::FormatVersion::V3 => {
            Some(effective_next_row_id(metadata).map_err(|detail| FenceError::Failed { detail })?)
        }
        _ => None,
    };
    // A marker carries no manifests at all: it is metadata provenance, not
    // data.
    write_manifest_list(
        file_io,
        &manifest_list_path,
        Vec::new(),
        snapshot_id,
        observed_snapshot_id,
        sequence_number,
        metadata.format_version(),
        first_row_id,
    )
    .await
    .map_err(|detail| FenceError::Failed { detail })?;

    // `SnapshotBuilder` is a typestate builder, so the v3 row-range variant
    // has to be a separate full chain rather than a conditional call.
    let snapshot = match first_row_id {
        Some(first_row_id) => Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(observed_snapshot_id)
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(marker_summary(facts))
            .with_schema_id(metadata.current_schema_id())
            .with_row_range(first_row_id, 0)
            .build(),
        None => Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(observed_snapshot_id)
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(now_ms())
            .with_manifest_list(manifest_list_path)
            .with_summary(marker_summary(facts))
            .with_schema_id(metadata.current_schema_id())
            .build(),
    };

    let commit = TableCommit::builder()
        .ident(table.identifier().clone())
        .updates(vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: fence_ref.to_string(),
                reference: SnapshotReference {
                    snapshot_id,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            },
        ])
        .requirements(vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: fence_ref.to_string(),
                snapshot_id: observed_snapshot_id,
            },
        ])
        .build();

    catalog
        .update_table(commit)
        .await
        .map_err(|error| FenceError::Failed {
            detail: format!("publish fence marker on '{fence_ref}': {error}"),
        })?;

    Ok(IcebergFenceAssertion {
        fence_ref: fence_ref.to_string(),
        fence_snapshot_id: snapshot_id,
    })
}

/// Drop this operation's fence ref once the operation reached a terminal state
/// and its evidence is no longer needed.
///
/// Retention is bounded by construction: one ref per in-flight operation,
/// removed at terminal. Callers must only invoke this while holding the
/// current fence for that operation.
pub async fn retire_fence_ref(
    catalog: &dyn Catalog,
    table: &Table,
    fence_ref: &str,
    expected_snapshot_id: i64,
) -> Result<(), FenceError> {
    if !fence_ref.starts_with(WRITE_FENCE_REF_PREFIX) {
        return Err(FenceError::Failed {
            detail: format!("refusing to retire '{fence_ref}': not a provider-owned fence ref"),
        });
    }
    let commit = TableCommit::builder()
        .ident(table.identifier().clone())
        .updates(vec![TableUpdate::RemoveSnapshotRef {
            ref_name: fence_ref.to_string(),
        }])
        .requirements(vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: fence_ref.to_string(),
            snapshot_id: Some(expected_snapshot_id),
        }])
        .build();
    catalog
        .update_table(commit)
        .await
        .map(|_| ())
        .map_err(|error| FenceError::Failed {
            detail: format!("retire fence ref '{fence_ref}': {error}"),
        })
}

/// Whether a snapshot is a provider-owned fence marker rather than a data
/// snapshot.
///
/// Fence markers are real snapshots in table metadata, so anything that counts
/// or reports snapshots — tests, snapshot listings, expiry policy — must be
/// able to tell them apart from data the user wrote.
pub fn is_fence_marker_snapshot(summary: &Summary) -> bool {
    summary
        .additional_properties
        .contains_key(FENCE_PROP_VERSION)
}

/// Whether `ref_name` is a provider-owned fence ref rather than a data ref.
pub fn is_fence_ref(ref_name: &str) -> bool {
    ref_name.starts_with(WRITE_FENCE_REF_PREFIX)
}

/// Fence ref for a stable write operation id, for callers that hold only the
/// operation identity.
pub fn fence_ref_for_operation(write_operation_id: &str) -> String {
    format!("{WRITE_FENCE_REF_PREFIX}{write_operation_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts() -> IcebergWriteFenceFacts {
        IcebergWriteFenceFacts {
            cluster_identity_digest: "cluster-digest".to_string(),
            control_plane_incarnation: 7,
            resource_epoch: 3,
            coordination_attempt: 1,
            write_operation_id: "op-1".to_string(),
            namespace: "db".to_string(),
            table_name: "t".to_string(),
            target_ref: "main".to_string(),
            coordination_attempt_id: "attempt-1".to_string(),
            fence_digest: "fence-digest".to_string(),
        }
    }

    #[test]
    fn generation_orders_incarnation_then_epoch_then_attempt() {
        let low = FenceGeneration {
            control_plane_incarnation: 1,
            resource_epoch: 999,
            coordination_attempt: 999,
        };
        let high = FenceGeneration {
            control_plane_incarnation: 2,
            resource_epoch: 1,
            coordination_attempt: 1,
        };
        assert!(high > low, "a newer control plane must dominate the epoch");

        let epoch_low = FenceGeneration {
            control_plane_incarnation: 2,
            resource_epoch: 1,
            coordination_attempt: 999,
        };
        let epoch_high = FenceGeneration {
            control_plane_incarnation: 2,
            resource_epoch: 2,
            coordination_attempt: 1,
        };
        assert!(
            epoch_high > epoch_low,
            "a newer resource epoch must dominate the attempt counter"
        );
    }

    #[test]
    fn a_takeover_that_only_advances_the_attempt_counter_still_outranks() {
        // A recovering owner can hold the same incarnation and resource epoch
        // as the attempt it takes over. If the attempt counter were not part of
        // the generation, `raise_fence` would refuse this legitimate takeover.
        let original = facts().generation();
        let mut recovering_facts = facts();
        recovering_facts.coordination_attempt = original.coordination_attempt + 1;
        assert!(
            recovering_facts.generation() > original,
            "same-epoch takeover must outrank the attempt it recovers"
        );
    }

    #[test]
    fn fence_ref_is_derived_from_the_operation_id() {
        assert_eq!(facts().fence_ref(), "novarocks-write-fence-op-1");
        assert!(is_fence_ref(&facts().fence_ref()));
        assert!(!is_fence_ref("main"));
    }

    #[test]
    fn marker_summary_round_trips_through_the_parser() {
        let original = facts();
        let summary = marker_summary(&original);
        let parsed = parse_marker_summary(&summary, "novarocks-write-fence-op-1")
            .expect("marker summary must parse");
        assert_eq!(parsed, original);
    }

    #[test]
    fn marker_summary_with_unknown_layout_version_is_ambiguous() {
        let mut summary = marker_summary(&facts());
        summary
            .additional_properties
            .insert(FENCE_PROP_VERSION.to_string(), "999".to_string());
        let error = parse_marker_summary(&summary, "fence-ref").expect_err("must refuse");
        assert!(
            matches!(error, FenceError::Ambiguous { .. }),
            "unknown layout must be ambiguous, got {error:?}"
        );
    }

    #[test]
    fn marker_summary_missing_a_field_is_ambiguous_not_a_default() {
        let mut summary = marker_summary(&facts());
        summary.additional_properties.remove(FENCE_PROP_DIGEST);
        let error = parse_marker_summary(&summary, "fence-ref").expect_err("must refuse");
        assert!(
            matches!(error, FenceError::Ambiguous { .. }),
            "missing provenance must be ambiguous, got {error:?}"
        );
    }

    #[test]
    fn validate_rejects_a_target_ref_inside_the_fence_namespace() {
        let mut invalid = facts();
        invalid.target_ref = format!("{WRITE_FENCE_REF_PREFIX}sneaky");
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn validate_rejects_an_operation_id_that_would_break_the_ref_name() {
        let mut invalid = facts();
        invalid.write_operation_id = "op/../escape".to_string();
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn validate_rejects_a_zero_generation_component() {
        for mutate in [
            (|f: &mut IcebergWriteFenceFacts| f.control_plane_incarnation = 0)
                as fn(&mut IcebergWriteFenceFacts),
            |f: &mut IcebergWriteFenceFacts| f.resource_epoch = 0,
            |f: &mut IcebergWriteFenceFacts| f.coordination_attempt = 0,
        ] {
            let mut invalid = facts();
            mutate(&mut invalid);
            assert!(
                invalid.validate().is_err(),
                "a zero generation component must be refused"
            );
        }
    }

    #[test]
    fn validate_rejects_empty_and_oversized_components() {
        let mut empty = facts();
        empty.fence_digest = String::new();
        assert!(empty.validate().is_err());

        let mut oversized = facts();
        oversized.cluster_identity_digest = "x".repeat(257);
        assert!(oversized.validate().is_err());
    }

    #[test]
    fn assertion_pins_the_fence_ref_to_this_attempts_marker() {
        let assertion = IcebergFenceAssertion {
            fence_ref: "novarocks-write-fence-op-1".to_string(),
            fence_snapshot_id: 42,
        };
        assert_eq!(
            assertion.requirements(),
            vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: "novarocks-write-fence-op-1".to_string(),
                snapshot_id: Some(42),
            }]
        );
    }
}
