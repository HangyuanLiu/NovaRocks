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

//! Collects per-table maintenance facts through the neutral connector surface:
//! snapshot list, current-snapshot summary counters, typed maintenance policy
//! facts, references, the provider-signed compaction count, and the
//! downstream-consumer floor that protects incremental MV lineage.
//!
//! Nothing here interprets a storage format. The provider signs every fact; the
//! frontend owns every policy decision made from them.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorExactSemanticRevision, ConnectorInstanceId, ConnectorReadSelector,
    ConnectorTableIdentity, ConnectorTableResolution,
};

use novarocks_mv_application::persistence::exact_revision::{
    persist_exact_connector_revision, restore_exact_query_revision,
};
use novarocks_mv_application::persistence::projection::{MvPublicationState, StoredMvProjection};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

/// Provider facts only. Frontend owns every policy decision and retry state.
#[derive(Clone, Debug, Default)]
pub struct TableMaintenanceStats {
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Vec<SnapshotInfo>,
    pub total_data_files: Option<u64>,
    pub max_compactable_data_files: Option<u64>,
    pub total_files_size_bytes: Option<u64>,
    pub total_delete_files: Option<u64>,
    /// Typed maintenance policy facts declared by the table. `None` means the
    /// table declares no usable value for that key; defaults and clamping are
    /// policy and belong to the frontend, never to this fact layer.
    pub maintenance_enabled: Option<bool>,
    pub expire_max_snapshot_age_ms: Option<i64>,
    pub expire_min_snapshots_to_keep: Option<u32>,
    pub target_file_size_bytes: Option<i64>,
    pub non_default_reference_count: usize,
    pub downstream_floor_ts_ms: Option<i64>,
    pub downstream_floor_unknown: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DownstreamFloor {
    pub(crate) floor_ts_ms: Option<i64>,
    pub(crate) unknown: bool,
}

/// Minimum published consumed-version timestamp across every durable relation
/// occurrence that reads this exact table object. A name match never revives a
/// dependency on a replaced object, while an unresolved version of the current
/// object blocks expiration for safety.
pub(crate) fn downstream_floor(
    projections: &[StoredMvProjection],
    catalog: &str,
    namespace: &str,
    table: &str,
    current_revision: &ConnectorExactSemanticRevision,
    timestamp_by_revision: &BTreeMap<ConnectorExactSemanticRevision, i64>,
) -> DownstreamFloor {
    let mut floor_ts: Option<i64> = None;
    let mut unknown = false;
    for projection in projections {
        let MvPublicationState::Published(publication) = projection.facts.publication() else {
            continue;
        };
        let inputs = publication
            .document()
            .inputs
            .iter()
            .map(|input| (input.relation_occurrence_id, input))
            .collect::<BTreeMap<_, _>>();
        for occurrence in projection
            .facts
            .definition()
            .relation_occurrences
            .iter()
            .filter(|occurrence| {
                occurrence.catalog_at_binding == catalog
                    && occurrence.namespace_at_binding == namespace
                    && occurrence.relation_at_binding == table
            })
        {
            let Some(input) = inputs.get(&occurrence.occurrence_id) else {
                unknown = true;
                continue;
            };
            let Ok(revision) =
                restore_exact_query_revision(&input.object_id, &input.native_data_version)
            else {
                unknown = true;
                continue;
            };
            if revision.object_identity() == current_revision.object_identity() {
                match timestamp_by_revision.get(&revision) {
                    Some(ts) => floor_ts = Some(floor_ts.map_or(*ts, |floor| floor.min(*ts))),
                    None => unknown = true,
                }
            }
        }
    }
    DownstreamFloor {
        floor_ts_ms: floor_ts,
        unknown,
    }
}

/// Read one MV storage table's maintenance facts through the neutral surface.
/// `projections` is the full ready MV list from the same pass, used for the
/// downstream floor.
///
/// The compaction observation runs first on purpose. Answering it forces the
/// provider to discard its cached table and re-read the catalog, and the
/// provider repopulates that cache with what it read, so the metadata load
/// below observes the very table version the count was taken from. Reversing
/// the two would let the projected facts describe an older table than the
/// count, and would drop the forced refresh this pass has always performed.
/// Read one MV storage table's maintenance facts through explicit frontend
/// control and observation ports. Background policy must retain only these
/// leaves, never the aggregate standalone engine state.
pub fn collect_table_stats_with_ports(
    connector_control: &dyn novarocks_spi::connector::ConnectorControlRegistry,
    storage_observation: &dyn novarocks_spi::connector::MvStorageObservationPort,
    catalog: &str,
    namespace: &str,
    table: &str,
    projections: &[StoredMvProjection],
) -> Result<TableMaintenanceStats, String> {
    let context = crate::connector::connector_request_context(
        None,
        novarocks_spi::connector::ConnectorStopOwner::new().view(),
    )?;
    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let identity = ConnectorTableIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from(namespace),
        table: Arc::from(table),
    };

    let max_compactable_data_files =
        crate::connector::metadata_maintenance::read_max_compactable_data_files(
            connector_control,
            &instance_id,
            identity,
            context.clone(),
        )
        .map_err(|error| {
            format!(
                "observe {catalog}.{namespace}.{table} compaction groups for maintenance: {error}"
            )
        })?;

    let exact_lease =
        crate::connector::acquire_metadata_planning_lease(connector_control, catalog)?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        context.clone(),
        namespace,
        table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    let observed = crate::mv::domain::storage_observation::observe_maintenance_metadata(
        storage_observation,
        &exact_lease,
        &metadata,
        context,
    )
    .map_err(|error| {
        format!("observe {catalog}.{namespace}.{table} maintenance metadata: {error}")
    })?;

    let snapshots: Vec<SnapshotInfo> = observed
        .snapshots()
        .iter()
        .map(|snapshot| SnapshotInfo {
            snapshot_id: snapshot.snapshot_id,
            timestamp_ms: snapshot.timestamp_ms,
        })
        .collect();
    let current_revision = exact_lease
        .binding()
        .metadata()
        .exact_semantic_revision(&metadata.table, ConnectorReadSelector::Current)
        .map_err(|error| {
            format!("observe exact {catalog}.{namespace}.{table} maintenance identity: {error}")
        })?;
    let timestamp_by_revision = snapshots
        .iter()
        .map(|snapshot| {
            exact_lease
                .binding()
                .metadata()
                .exact_semantic_revision(
                    &metadata.table,
                    ConnectorReadSelector::SnapshotId(snapshot.snapshot_id),
                )
                .map(|revision| (revision, snapshot.timestamp_ms))
                .map_err(|error| {
                    format!(
                        "observe exact {catalog}.{namespace}.{table} snapshot {}: {error}",
                        snapshot.snapshot_id
                    )
                })
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    let floor = downstream_floor(
        projections,
        catalog,
        namespace,
        table,
        &current_revision,
        &timestamp_by_revision,
    );
    let policy = *observed.policy();

    Ok(TableMaintenanceStats {
        current_snapshot_id: observed.current_snapshot_id(),
        snapshots,
        total_data_files: observed.total_data_files(),
        max_compactable_data_files,
        total_files_size_bytes: observed.total_files_size_bytes(),
        total_delete_files: observed.total_delete_files(),
        maintenance_enabled: policy.maintenance_enabled,
        expire_max_snapshot_age_ms: policy.expire_max_snapshot_age_ms,
        expire_min_snapshots_to_keep: policy.expire_min_snapshots_to_keep,
        target_file_size_bytes: policy.target_file_size_bytes,
        non_default_reference_count: observed.non_default_reference_count(),
        downstream_floor_ts_ms: floor.floor_ts_ms,
        downstream_floor_unknown: floor.unknown,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_mv_application::persistence::{
        projection::StoredMvProjection, test_support::ProjectionFixture,
    };
    use novarocks_mv_application::product::MvTarget;
    use novarocks_spi::connector::{ConnectorProviderId, ConnectorTableObjectId};

    fn exact(object: &'static [u8], snapshot_id: i64) -> ConnectorExactSemanticRevision {
        ConnectorExactSemanticRevision::try_from_table_object_and_snapshot(
            ConnectorProviderId::parse("iceberg").unwrap(),
            &ConnectorTableObjectId::try_new(Bytes::from_static(object)).unwrap(),
            Some(snapshot_id),
        )
        .unwrap()
    }

    fn projection(
        mv_id: i64,
        relation: &str,
        object: &'static [u8],
        snapshots: [i64; 2],
    ) -> StoredMvProjection {
        let target_name = format!("mv_{mv_id}");
        let mut fixture = ProjectionFixture::new(
            MvTarget::from_parts(Some("ice"), "analytics", &target_name),
            Some(11),
        );
        for (index, occurrence) in fixture
            .definition
            .relation_occurrences
            .iter_mut()
            .enumerate()
        {
            occurrence.relation_at_binding = relation.to_string();
            let revision = exact(object, snapshots[index]);
            let (object_id, data_version) = persist_exact_connector_revision(&revision).unwrap();
            occurrence.object_id = object_id.clone();
            let input = fixture
                .publication
                .as_mut()
                .unwrap()
                .inputs
                .iter_mut()
                .find(|input| input.relation_occurrence_id == occurrence.occurrence_id)
                .unwrap();
            input.object_id = object_id;
            input.native_data_version = data_version;
        }
        StoredMvProjection {
            mv_id,
            facts: fixture.build().unwrap(),
        }
    }

    #[test]
    fn floor_is_min_consumed_version_timestamp_across_occurrences() {
        let current = exact(b"source", 30);
        let timestamp_by_revision =
            BTreeMap::from([(exact(b"source", 10), 1_000), (exact(b"source", 20), 2_000)]);
        let projections = vec![projection(1, "orders", b"source", [20, 10])];
        let floor = downstream_floor(
            &projections,
            "ice",
            "sales",
            "orders",
            &current,
            &timestamp_by_revision,
        );
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: Some(1_000),
                unknown: false
            }
        );
    }

    #[test]
    fn floor_is_none_without_consumers() {
        let current = exact(b"source", 30);
        let projections = vec![projection(1, "other", b"source", [10, 10])];
        let floor = downstream_floor(
            &projections,
            "ice",
            "sales",
            "orders",
            &current,
            &BTreeMap::new(),
        );
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: None,
                unknown: false
            }
        );
    }

    #[test]
    fn floor_unknown_when_current_object_consumed_version_is_not_retained() {
        let current = exact(b"source", 30);
        let projections = vec![projection(1, "orders", b"source", [99, 99])];
        let floor = downstream_floor(
            &projections,
            "ice",
            "sales",
            "orders",
            &current,
            &BTreeMap::new(),
        );
        assert!(floor.unknown);
    }

    #[test]
    fn replaced_object_does_not_hold_the_new_objects_snapshots() {
        let current = exact(b"new-source", 30);
        let projections = vec![projection(1, "orders", b"old-source", [10, 10])];
        let floor = downstream_floor(
            &projections,
            "ice",
            "sales",
            "orders",
            &current,
            &BTreeMap::new(),
        );
        assert_eq!(
            floor,
            DownstreamFloor {
                floor_ts_ms: None,
                unknown: false,
            }
        );
    }
}
