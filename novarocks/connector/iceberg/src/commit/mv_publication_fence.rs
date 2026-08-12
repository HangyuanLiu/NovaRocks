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

//! Iceberg-side lake fence for MV publication.
//!
//! Iceberg's own `main` / staging-branch comparison cannot express "this owner
//! has been superseded": as long as `main` has not moved, a frontend that lost
//! its control-plane lease still passes. This module adds a third, dedicated
//! CAS target — one internal branch per MV target table — whose snapshot names
//! the ownership generation that currently owns publication.
//!
//! A generation becomes publication-capable only by winning
//! [`establish_publication_fence`]. That commit is where takeover order is
//! decided: it is provider-authoritative, monotonic, and idempotent per
//! generation. [`crate::commit::mv_refresh_ref`] then requires the exact fence
//! snapshot in the same commit that advances `main`, so a superseded generation
//! cannot publish even when its frozen `main` expectation still holds.
//!
//! Two shape decisions matter:
//!
//! * The fence snapshot is **data-free** and is parented on the *observed main*
//!   snapshot, not on the previous fence snapshot. Fence snapshots therefore
//!   never chain into a growing fence ancestry — the ref always names exactly
//!   one live fence snapshot.
//! * The marker records only *digests* of the CP-1 fencing token and cluster
//!   identity. The raw token never reaches the lake, so fence evidence is
//!   readable by any incarnation without becoming a credential.

use std::collections::{BTreeMap, HashMap};

use novarocks_spi::connector::{
    ConnectorMvPublicationFenceGeneration, ConnectorMvPublicationFenceOrder,
    ConnectorMvRefreshResourceIdentity,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::iceberg::spec::{
    FormatVersion, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary,
};
use crate::iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};

use super::helpers::{generate_snapshot_id, metadata_dir, now_ms, write_manifest_list};
use super::mv_provenance::{hex_encode, sort_json_value};

/// The single internal branch that carries an MV target's publication fence.
pub const MV_PUBLICATION_FENCE_REF: &str = "__novarocks_mv_publication_fence_v1";

/// Snapshot-summary key holding the canonical fence marker.
pub const MV_PUBLICATION_FENCE_MARKER_PROP: &str = "novarocks.mv.fence.v1";

pub const MV_PUBLICATION_FENCE_VERSION: u16 = 1;

/// Canonical fence marker, carried in the fence snapshot's summary.
///
/// Every field is a digest or a counter: the raw CP-1 fencing token is
/// deliberately absent, so this record proves *which* generation owns the fence
/// without letting a reader forge that generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MvPublicationFenceMarker {
    pub fence_version: u16,
    /// Hex of the provider-neutral stable resource digest.
    pub resource_digest: String,
    /// The immutable Iceberg table UUID this fence domain belongs to.
    pub target_table_uuid: String,
    pub cluster_digest: String,
    pub control_plane_incarnation: u64,
    pub resource_epoch: u64,
    pub token_digest: String,
    /// The operation that first established this generation's fence. It makes a
    /// lost reply resolvable without re-issuing the operation.
    pub operation_id: String,
}

impl MvPublicationFenceMarker {
    pub fn new(
        resource: &ConnectorMvRefreshResourceIdentity,
        generation: &ConnectorMvPublicationFenceGeneration,
        operation_id: [u8; 16],
    ) -> Self {
        Self {
            fence_version: MV_PUBLICATION_FENCE_VERSION,
            resource_digest: hex_encode(&resource.digest()),
            target_table_uuid: resource.target_table_uuid().to_string(),
            cluster_digest: hex_encode(&generation.cluster_digest()),
            control_plane_incarnation: generation.control_plane_incarnation(),
            resource_epoch: generation.resource_epoch(),
            token_digest: hex_encode(&generation.token_digest()),
            operation_id: hex_encode(&operation_id),
        }
    }

    pub fn to_summary_properties(&self) -> Result<BTreeMap<String, String>, String> {
        Ok(BTreeMap::from([(
            MV_PUBLICATION_FENCE_MARKER_PROP.to_string(),
            self.to_canonical_json()?,
        )]))
    }

    pub fn to_canonical_json(&self) -> Result<String, String> {
        let value = serde_json::to_value(self)
            .map_err(|err| format!("failed to serialize MV publication fence marker: {err}"))?;
        serde_json::to_string(&sort_json_value(value)).map_err(|err| {
            format!("failed to render canonical MV publication fence marker JSON: {err}")
        })
    }

    pub fn from_json(raw: &str) -> Result<Self, String> {
        let marker: Self = serde_json::from_str(raw)
            .map_err(|err| format!("failed to parse MV publication fence marker JSON: {err}"))?;
        if marker.fence_version != MV_PUBLICATION_FENCE_VERSION {
            return Err(format!(
                "unsupported MV publication fence version: expected {}, got {}",
                MV_PUBLICATION_FENCE_VERSION, marker.fence_version
            ));
        }
        Ok(marker)
    }

    /// Reads the marker from a fence snapshot. An absent key is an error rather
    /// than `None`: a snapshot on the fence ref without a marker is corrupt,
    /// not "no fence".
    pub fn from_snapshot_summary(snapshot: &Snapshot) -> Result<Self, String> {
        let raw = snapshot
            .summary()
            .additional_properties
            .get(MV_PUBLICATION_FENCE_MARKER_PROP)
            .ok_or_else(|| {
                format!(
                    "iceberg mv fence: snapshot {} on {MV_PUBLICATION_FENCE_REF} carries no fence marker",
                    snapshot.snapshot_id()
                )
            })?;
        Self::from_json(raw)
    }

    /// Rebuilds the SPI generation this marker names, so ordering is decided by
    /// the one contract implementation rather than by ad-hoc field comparison.
    pub fn generation(&self) -> Result<ConnectorMvPublicationFenceGeneration, String> {
        let cluster_digest = decode_digest(&self.cluster_digest, "cluster digest")?;
        let token_digest = decode_digest(&self.token_digest, "token digest")?;
        ConnectorMvPublicationFenceGeneration::try_from_digests(
            cluster_digest,
            self.control_plane_incarnation,
            self.resource_epoch,
            token_digest,
        )
        .map_err(|err| format!("iceberg mv fence: invalid marker generation: {err}"))
    }

    pub fn matches_resource(&self, resource: &ConnectorMvRefreshResourceIdentity) -> bool {
        self.resource_digest == hex_encode(&resource.digest())
            && self.target_table_uuid == resource.target_table_uuid().to_string()
    }
}

fn decode_digest(value: &str, field: &str) -> Result<[u8; 32], String> {
    if value.len() != 64 {
        return Err(format!(
            "iceberg mv fence: {field} must be 64 hex characters, got {}",
            value.len()
        ));
    }
    let mut digest = [0u8; 32];
    for (index, slot) in digest.iter_mut().enumerate() {
        let byte = &value[index * 2..index * 2 + 2];
        *slot = u8::from_str_radix(byte, 16)
            .map_err(|_| format!("iceberg mv fence: {field} is not valid hex"))?;
    }
    Ok(digest)
}

/// The externally observed fence state of one target table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedFence {
    pub snapshot_id: i64,
    pub marker: MvPublicationFenceMarker,
}

/// What [`establish_publication_fence`] should do after comparing generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvPublicationFenceDecision {
    /// The requesting generation already owns the fence; nothing to commit.
    AlreadyEstablished { fence_snapshot_id: i64 },
    /// Write a new fence snapshot, CAS-ing on this exact prior fence ref state.
    Establish {
        expected_fence_snapshot_id: Option<i64>,
    },
}

/// Immutable inputs of one fence establishment.
#[derive(Clone, Debug)]
pub struct MvPublicationFencePlan {
    pub namespace: String,
    pub table: String,
    pub resource: ConnectorMvRefreshResourceIdentity,
    pub generation: ConnectorMvPublicationFenceGeneration,
    pub operation_id: [u8; 16],
    /// The `main` snapshot the caller froze. The fence snapshot is parented on
    /// it, and the commit requires it to still hold.
    pub observed_main_snapshot_id: Option<i64>,
    /// The fence ref state the caller last observed, used as the CAS operand.
    pub expected_fence_snapshot_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvPublicationFenceOutcome {
    pub fence_snapshot_id: i64,
    /// `false` when the request was an idempotent re-establish of a generation
    /// that already owned the fence.
    pub established: bool,
}

/// Decides whether a generation may take the fence, purely from observed state.
///
/// Every rejection here is a fail-closed path, and each one corresponds to a
/// real concurrent history:
///
/// * a stale generation retrying after losing its lease,
/// * a target that was dropped and recreated under a reused display name,
/// * one operation ID reused across two generations,
/// * two owners claiming one epoch with different tokens (via `try_order`).
pub fn decide_fence_establishment(
    plan: &MvPublicationFencePlan,
    observed: Option<&ObservedFence>,
) -> Result<MvPublicationFenceDecision, String> {
    let Some(observed) = observed else {
        if plan.expected_fence_snapshot_id.is_some() {
            return Err(format!(
                "iceberg mv fence: expected fence snapshot {:?} but {MV_PUBLICATION_FENCE_REF} is absent",
                plan.expected_fence_snapshot_id
            ));
        }
        return Ok(MvPublicationFenceDecision::Establish {
            expected_fence_snapshot_id: None,
        });
    };

    if !observed.marker.matches_resource(&plan.resource) {
        return Err(format!(
            "iceberg mv fence: existing fence on {MV_PUBLICATION_FENCE_REF} belongs to target {}, not {}",
            observed.marker.target_table_uuid,
            plan.resource.target_table_uuid()
        ));
    }
    if let Some(expected) = plan.expected_fence_snapshot_id
        && expected != observed.snapshot_id
    {
        return Err(format!(
            "iceberg mv fence: {MV_PUBLICATION_FENCE_REF} points to {}, expected {expected}",
            observed.snapshot_id
        ));
    }

    let observed_generation = observed.marker.generation()?;
    let order = plan
        .generation
        .try_order(&observed_generation)
        .map_err(|err| format!("iceberg mv fence: {err}"))?;
    let requested_operation = hex_encode(&plan.operation_id);
    match order {
        ConnectorMvPublicationFenceOrder::Same => {
            Ok(MvPublicationFenceDecision::AlreadyEstablished {
                fence_snapshot_id: observed.snapshot_id,
            })
        }
        ConnectorMvPublicationFenceOrder::Superseded => Err(format!(
            "iceberg mv fence: generation (incarnation={}, epoch={}) is superseded by \
             (incarnation={}, epoch={}) and cannot publish",
            plan.generation.control_plane_incarnation(),
            plan.generation.resource_epoch(),
            observed_generation.control_plane_incarnation(),
            observed_generation.resource_epoch(),
        )),
        ConnectorMvPublicationFenceOrder::Supersedes => {
            // One operation ID must name one generation. Reusing it across a
            // takeover would make a lost reply unresolvable: `inspect` could
            // not tell which generation actually committed.
            if observed.marker.operation_id == requested_operation {
                return Err(format!(
                    "iceberg mv fence: operation {requested_operation} already established a \
                     different generation and must not be reused"
                ));
            }
            Ok(MvPublicationFenceDecision::Establish {
                expected_fence_snapshot_id: Some(observed.snapshot_id),
            })
        }
    }
}

/// Reads the current fence state of a loaded table's metadata.
pub fn observe_fence(
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<Option<ObservedFence>, String> {
    let Some(reference) = metadata.refs().get(MV_PUBLICATION_FENCE_REF) else {
        return Ok(None);
    };
    if !reference.is_branch() {
        return Err(format!(
            "iceberg mv fence: {MV_PUBLICATION_FENCE_REF} is a tag, expected branch"
        ));
    }
    let snapshot = metadata
        .snapshot_by_id(reference.snapshot_id)
        .ok_or_else(|| {
            format!(
                "iceberg mv fence: {MV_PUBLICATION_FENCE_REF} names missing snapshot {}",
                reference.snapshot_id
            )
        })?;
    Ok(Some(ObservedFence {
        snapshot_id: reference.snapshot_id,
        marker: MvPublicationFenceMarker::from_snapshot_summary(snapshot)?,
    }))
}

/// Builds the single atomic commit that moves the fence ref.
///
/// The requirements are the whole point: table UUID pins the fence domain
/// against an external DROP/recreate, `main` pins the state the caller froze,
/// and the fence ref pins the exact generation being superseded.
pub fn build_fence_commit(
    ident: TableIdent,
    target_table_uuid: Uuid,
    snapshot: Snapshot,
    observed_main_snapshot_id: Option<i64>,
    expected_fence_snapshot_id: Option<i64>,
) -> TableCommit {
    let fence_snapshot_id = snapshot.snapshot_id();
    TableCommit::builder()
        .ident(ident)
        .updates(vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MV_PUBLICATION_FENCE_REF.to_string(),
                reference: SnapshotReference {
                    snapshot_id: fence_snapshot_id,
                    retention: SnapshotRetention::Branch {
                        min_snapshots_to_keep: Some(1),
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                },
            },
        ])
        .requirements(vec![
            TableRequirement::UuidMatch {
                uuid: target_table_uuid,
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: observed_main_snapshot_id,
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MV_PUBLICATION_FENCE_REF.to_string(),
                snapshot_id: expected_fence_snapshot_id,
            },
        ])
        .build()
}

/// Establishes (or idempotently re-establishes) this generation's lake fence.
pub async fn establish_publication_fence(
    catalog: &dyn Catalog,
    plan: &MvPublicationFencePlan,
) -> Result<MvPublicationFenceOutcome, String> {
    let ident = TableIdent::from_strs([plan.namespace.as_str(), plan.table.as_str()])
        .map_err(|e| format!("iceberg mv fence: invalid table identifier: {e}"))?;
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| format!("iceberg mv fence: load table failed: {e}"))?;
    let metadata = table.metadata();

    if metadata.uuid() != plan.resource.target_table_uuid() {
        return Err(format!(
            "iceberg mv fence: table {}.{} has UUID {}, expected {}",
            plan.namespace,
            plan.table,
            metadata.uuid(),
            plan.resource.target_table_uuid()
        ));
    }
    let current_main = metadata.current_snapshot().map(|s| s.snapshot_id());
    if current_main != plan.observed_main_snapshot_id {
        return Err(format!(
            "iceberg mv fence: main snapshot moved for {}.{}: observed {:?}, current {:?}",
            plan.namespace, plan.table, plan.observed_main_snapshot_id, current_main
        ));
    }

    let observed = observe_fence(metadata)?;
    let decision = decide_fence_establishment(plan, observed.as_ref())?;
    let expected_fence_snapshot_id = match decision {
        MvPublicationFenceDecision::AlreadyEstablished { fence_snapshot_id } => {
            return Ok(MvPublicationFenceOutcome {
                fence_snapshot_id,
                established: false,
            });
        }
        MvPublicationFenceDecision::Establish {
            expected_fence_snapshot_id,
        } => expected_fence_snapshot_id,
    };

    let marker = MvPublicationFenceMarker::new(&plan.resource, &plan.generation, plan.operation_id);
    let snapshot = build_fence_snapshot(
        &table,
        &marker,
        plan.observed_main_snapshot_id,
        metadata.format_version(),
    )
    .await?;

    let commit = build_fence_commit(
        ident,
        plan.resource.target_table_uuid(),
        snapshot.clone(),
        plan.observed_main_snapshot_id,
        expected_fence_snapshot_id,
    );
    catalog
        .update_table(commit)
        .await
        .map_err(|e| format!("iceberg mv fence: commit failed: {e}"))?;
    Ok(MvPublicationFenceOutcome {
        fence_snapshot_id: snapshot.snapshot_id(),
        established: true,
    })
}

/// Writes the data-free fence snapshot's manifest list and builds the snapshot.
///
/// The snapshot carries no manifests at all, so it adds no files, no rows, and
/// no row-lineage range. It is parented on the observed `main` so the fence ref
/// never accumulates its own ancestry.
async fn build_fence_snapshot(
    table: &crate::iceberg::table::Table,
    marker: &MvPublicationFenceMarker,
    parent_snapshot_id: Option<i64>,
    format_version: FormatVersion,
) -> Result<Snapshot, String> {
    let metadata = table.metadata();
    let snapshot_id = generate_snapshot_id();
    let sequence_number = metadata.last_sequence_number() + 1;
    let manifest_list_path = format!(
        "{}/snap-{}-{}-fence.avro",
        metadata_dir(table),
        snapshot_id,
        Uuid::now_v7()
    );
    write_manifest_list(
        table.file_io(),
        &manifest_list_path,
        Vec::new(),
        snapshot_id,
        parent_snapshot_id,
        sequence_number,
        format_version,
        // A data-free snapshot claims no row-id range; V3 tables keep the
        // table's existing next-row-id untouched.
        Some(metadata.next_row_id()),
    )
    .await?;

    let mut additional_properties: HashMap<String, String> =
        marker.to_summary_properties()?.into_iter().collect();
    additional_properties.insert("added-data-files".to_string(), "0".to_string());
    additional_properties.insert("added-records".to_string(), "0".to_string());

    let summary = Summary {
        operation: Operation::Append,
        additional_properties,
    };
    let builder = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(parent_snapshot_id)
        .with_sequence_number(sequence_number)
        .with_timestamp_ms(now_ms())
        .with_manifest_list(manifest_list_path)
        .with_summary(summary)
        .with_schema_id(metadata.current_schema_id());
    // A V3 row-lineage table still expects every snapshot to carry a row range;
    // the fence claims a zero-length one so it never consumes row IDs.
    Ok(match format_version {
        FormatVersion::V3 => builder.with_row_range(metadata.next_row_id(), 0).build(),
        FormatVersion::V1 | FormatVersion::V2 => builder.build(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorProviderId;

    fn resource(uuid: u128) -> ConnectorMvRefreshResourceIdentity {
        ConnectorMvRefreshResourceIdentity::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            Uuid::from_u128(uuid),
        )
        .unwrap()
    }

    fn generation(
        incarnation: u64,
        epoch: u64,
        token: u8,
    ) -> ConnectorMvPublicationFenceGeneration {
        ConnectorMvPublicationFenceGeneration::try_new("cluster-a", incarnation, epoch, [token; 32])
            .unwrap()
    }

    fn plan(
        generation: ConnectorMvPublicationFenceGeneration,
        operation_id: u8,
        expected_fence_snapshot_id: Option<i64>,
    ) -> MvPublicationFencePlan {
        MvPublicationFencePlan {
            namespace: "db".to_string(),
            table: "mv".to_string(),
            resource: resource(0x1234),
            generation,
            operation_id: [operation_id; 16],
            observed_main_snapshot_id: Some(100),
            expected_fence_snapshot_id,
        }
    }

    fn observed(
        generation: &ConnectorMvPublicationFenceGeneration,
        operation_id: u8,
        snapshot_id: i64,
    ) -> ObservedFence {
        ObservedFence {
            snapshot_id,
            marker: MvPublicationFenceMarker::new(
                &resource(0x1234),
                generation,
                [operation_id; 16],
            ),
        }
    }

    #[test]
    fn marker_round_trips_through_canonical_json_and_summary() {
        let marker =
            MvPublicationFenceMarker::new(&resource(0x1234), &generation(1, 1, 7), [3; 16]);

        let json = marker.to_canonical_json().unwrap();
        assert_eq!(MvPublicationFenceMarker::from_json(&json).unwrap(), marker);
        // Canonical means byte-stable across renders.
        assert_eq!(json, marker.to_canonical_json().unwrap());

        let props = marker.to_summary_properties().unwrap();
        assert!(props.contains_key(MV_PUBLICATION_FENCE_MARKER_PROP));

        // The raw CP-1 token must never appear in the lake; only its digest.
        assert!(marker.generation().unwrap() == generation(1, 1, 7));
    }

    #[test]
    fn marker_rejects_unknown_version_and_malformed_digests() {
        let err = MvPublicationFenceMarker::from_json(
            r#"{"fence_version":9,"resource_digest":"","target_table_uuid":"","cluster_digest":"","control_plane_incarnation":1,"resource_epoch":1,"token_digest":"","operation_id":""}"#,
        )
        .unwrap_err();
        assert!(
            err.contains("unsupported MV publication fence version"),
            "{err}"
        );

        let mut marker =
            MvPublicationFenceMarker::new(&resource(0x1234), &generation(1, 1, 7), [3; 16]);
        marker.token_digest = "zz".to_string();
        assert!(
            marker.generation().is_err(),
            "short digest must be rejected"
        );

        marker.token_digest = "z".repeat(64);
        assert!(
            marker.generation().is_err(),
            "non-hex digest must be rejected"
        );
    }

    #[test]
    fn first_establish_requires_absent_fence_ref() {
        let decision =
            decide_fence_establishment(&plan(generation(1, 1, 7), 3, None), None).unwrap();
        assert_eq!(
            decision,
            MvPublicationFenceDecision::Establish {
                expected_fence_snapshot_id: None
            }
        );

        // Claiming a prior fence that does not exist is a lost-state bug, not a
        // first establish.
        let err =
            decide_fence_establishment(&plan(generation(1, 1, 7), 3, Some(9)), None).unwrap_err();
        assert!(err.contains("is absent"), "{err}");
    }

    #[test]
    fn same_generation_is_idempotent() {
        let generation = generation(1, 1, 7);
        let decision = decide_fence_establishment(
            &plan(generation.clone(), 3, Some(500)),
            Some(&observed(&generation, 3, 500)),
        )
        .unwrap();
        assert_eq!(
            decision,
            MvPublicationFenceDecision::AlreadyEstablished {
                fence_snapshot_id: 500
            }
        );

        // A retry that lost its reply and minted a fresh operation ID still
        // finds its own generation already owning the fence.
        let decision = decide_fence_establishment(
            &plan(generation.clone(), 4, Some(500)),
            Some(&observed(&generation, 3, 500)),
        )
        .unwrap();
        assert_eq!(
            decision,
            MvPublicationFenceDecision::AlreadyEstablished {
                fence_snapshot_id: 500
            }
        );
    }

    #[test]
    fn higher_generation_takes_over_and_lower_is_rejected() {
        let old = generation(1, 1, 7);

        let decision = decide_fence_establishment(
            &plan(generation(1, 2, 8), 4, Some(500)),
            Some(&observed(&old, 3, 500)),
        )
        .unwrap();
        assert_eq!(
            decision,
            MvPublicationFenceDecision::Establish {
                expected_fence_snapshot_id: Some(500)
            },
            "a takeover CASes on the exact superseded fence snapshot"
        );

        let newer = generation(2, 1, 9);
        let err = decide_fence_establishment(
            &plan(old.clone(), 3, Some(500)),
            Some(&observed(&newer, 4, 500)),
        )
        .unwrap_err();
        assert!(err.contains("is superseded by"), "{err}");
    }

    #[test]
    fn stale_fence_expectation_and_foreign_target_fail_closed() {
        let generation = generation(1, 1, 7);

        let err = decide_fence_establishment(
            &plan(generation.clone(), 3, Some(499)),
            Some(&observed(&generation, 3, 500)),
        )
        .unwrap_err();
        assert!(err.contains("expected 499"), "{err}");

        // Same display name, different table UUID: an external DROP/recreate
        // must not inherit the old fence domain.
        let foreign = ObservedFence {
            snapshot_id: 500,
            marker: MvPublicationFenceMarker::new(&resource(0x9999), &generation, [3; 16]),
        };
        let err = decide_fence_establishment(&plan(generation, 3, Some(500)), Some(&foreign))
            .unwrap_err();
        assert!(err.contains("belongs to target"), "{err}");
    }

    #[test]
    fn operation_id_must_not_be_reused_across_generations() {
        let old = generation(1, 1, 7);
        let err = decide_fence_establishment(
            &plan(generation(1, 2, 8), 3, Some(500)),
            Some(&observed(&old, 3, 500)),
        )
        .unwrap_err();
        assert!(err.contains("must not be reused"), "{err}");
    }

    #[test]
    fn cross_cluster_and_conflicting_token_fail_closed() {
        let other_cluster =
            ConnectorMvPublicationFenceGeneration::try_new("cluster-b", 1, 1, [7; 32]).unwrap();
        let err = decide_fence_establishment(
            &plan(generation(1, 1, 7), 3, Some(500)),
            Some(&observed(&other_cluster, 4, 500)),
        )
        .unwrap_err();
        assert!(err.contains("different clusters"), "{err}");

        // One epoch, two tokens: two owners claim one generation.
        let err = decide_fence_establishment(
            &plan(generation(1, 1, 7), 3, Some(500)),
            Some(&observed(&generation(1, 1, 8), 4, 500)),
        )
        .unwrap_err();
        assert!(err.contains("different tokens"), "{err}");
    }

    #[test]
    fn fence_commit_requires_uuid_main_and_exact_prior_fence() {
        let marker =
            MvPublicationFenceMarker::new(&resource(0x1234), &generation(1, 2, 8), [4; 16]);
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: marker
                .to_summary_properties()
                .unwrap()
                .into_iter()
                .collect(),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(700)
            .with_parent_snapshot_id(Some(100))
            .with_sequence_number(5)
            .with_timestamp_ms(1)
            .with_manifest_list("file:/tmp/snap-700-fence.avro".to_string())
            .with_summary(summary)
            .with_schema_id(0)
            .build();

        let mut commit = build_fence_commit(
            TableIdent::from_strs(["db", "mv"]).unwrap(),
            Uuid::from_u128(0x1234),
            snapshot,
            Some(100),
            Some(500),
        );

        let requirements = commit.take_requirements();
        assert!(requirements.contains(&TableRequirement::UuidMatch {
            uuid: Uuid::from_u128(0x1234)
        }));
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: Some(100),
            })
        );
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: MV_PUBLICATION_FENCE_REF.to_string(),
                snapshot_id: Some(500),
            })
        );

        let updates = commit.take_updates();
        assert!(matches!(updates[0], TableUpdate::AddSnapshot { .. }));
        match &updates[1] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, MV_PUBLICATION_FENCE_REF);
                assert_eq!(reference.snapshot_id, 700);
            }
            other => panic!("expected SetSnapshotRef, got {other:?}"),
        }
    }

    #[test]
    fn first_fence_commit_requires_the_ref_to_be_absent() {
        let marker =
            MvPublicationFenceMarker::new(&resource(0x1234), &generation(1, 1, 7), [3; 16]);
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: marker
                .to_summary_properties()
                .unwrap()
                .into_iter()
                .collect(),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(700)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:/tmp/snap-700-fence.avro".to_string())
            .with_summary(summary)
            .with_schema_id(0)
            .build();

        let mut commit = build_fence_commit(
            TableIdent::from_strs(["db", "mv"]).unwrap(),
            Uuid::from_u128(0x1234),
            snapshot,
            None,
            None,
        );

        let requirements = commit.take_requirements();
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: MV_PUBLICATION_FENCE_REF.to_string(),
                snapshot_id: None,
            }),
            "a None snapshot id asserts the ref does not yet exist"
        );
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: None,
            }),
            "an empty target freezes main as absent"
        );
    }
}
