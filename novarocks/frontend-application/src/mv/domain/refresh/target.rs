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

//! Iceberg MV target identity and statement-name resolution.

use crate::mv::domain::analysis::resolve_mv_name;
use crate::mv::domain::refresh::target_binding::{
    MvTargetBinding, load_mv_target_binding_with_ports, load_mv_target_binding_with_ports_typed,
};
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_spi::connector::MvStorageObservationPort;
use novarocks_spi::connector::{ConnectorControlResolver, ConnectorError, ConnectorRequestContext};
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;
use novarocks_sql::semantic::ObjectName;
use novarocks_types::naming::{TableIdentity, normalize_identifier};

/// A normalized Iceberg MV target identity.  Refresh planning and the domain
/// persistence paths share this value without acquiring query assembly state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergMvTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl From<&TableIdentity> for IcebergMvTarget {
    fn from(target: &TableIdentity) -> Self {
        Self {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        }
    }
}

/// Resolves the SQL-level MV name to its normalized Iceberg target identity.
pub fn resolve_refresh_target(
    current_catalog: Option<&str>,
    current_database: &str,
    name_parts: &[String],
) -> Result<IcebergMvTarget, String> {
    let catalog = current_catalog.ok_or_else(|| {
        "REFRESH MATERIALIZED VIEW for an Iceberg MV requires current Iceberg catalog context"
            .to_string()
    })?;
    let (namespace, table) = resolve_mv_name(
        &ObjectName {
            parts: name_parts.to_vec(),
        },
        current_database,
    )?;
    Ok(IcebergMvTarget {
        catalog: normalize_identifier(catalog)?,
        namespace,
        table,
    })
}

/// Resolves a SQL-level MV name to the public repository target used by
/// frontend command admission, without exposing the internal Iceberg planning
/// identity.
pub fn resolve_refresh_mv_target(
    current_catalog: Option<&str>,
    current_database: &str,
    name_parts: &[String],
) -> Result<MvTarget, String> {
    let target = resolve_refresh_target(current_catalog, current_database, name_parts)?;
    Ok(MvTarget {
        catalog: Some(target.catalog),
        database: target.namespace,
        name: target.table,
    })
}

/// Loads the sealed target binding used by one Iceberg MV refresh attempt.
///
/// This capability deliberately receives only the two admitted ports, the
/// normalized domain target, and the request context.  It does not depend on
/// the aggregate refresh source or query assembly state.
pub fn load_iceberg_mv_target_binding(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    target: &IcebergMvTarget,
    connector_context: &ConnectorRequestContext,
) -> Result<MvTargetBinding, String> {
    load_mv_target_binding_with_ports(
        connector_control,
        storage_observation,
        &TableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        connector_context,
    )
}

/// Typed counterpart used by the background refresh path. A caller must map
/// the returned connector category to its retry policy instead of inferring
/// one from text after it has crossed the preparation boundary.
pub(crate) fn load_iceberg_mv_target_binding_typed(
    connector_control: &dyn ConnectorControlResolver,
    storage_observation: &dyn MvStorageObservationPort,
    target: &IcebergMvTarget,
    connector_context: &ConnectorRequestContext,
    #[cfg(novarocks_dev_controls)] controls: Option<&crate::mv::test_controls::MvTestControls>,
) -> Result<MvTargetBinding, ConnectorError> {
    #[cfg(novarocks_dev_controls)]
    if let Some(controls) = controls {
        if controls
            .transient_preparation(&target.catalog, &target.namespace, &target.table)
            .map_err(|error| {
                ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                    error,
                )
            })?
        {
            return Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unavailable,
                format!(
                    "test-injected temporary connector unavailability while preparing {}.{}.{}",
                    target.catalog, target.namespace, target.table
                ),
            ));
        }
    }
    load_mv_target_binding_with_ports_typed(
        connector_control,
        storage_observation,
        &TableIdentity {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
        },
        connector_context,
    )
}

/// Validates that the persisted MV definition still names the target snapshot
/// that was observed for refresh planning.
pub fn validate_target_snapshot(
    target: &IcebergMvTarget,
    mv_definition: &StoredKeyedMvProjection,
    binding: &MvTargetBinding,
    entrance: &novarocks_mv_application::management::ManagementEntrance,
    context: &ConnectorRequestContext,
) -> Result<(), String> {
    let documents = binding
        .lease()
        .derive_document_storage_lease()
        .map_err(|e| e.to_string())?;
    let request = crate::mv::domain::staged_create::keyed_request(
        &documents,
        binding.identity().clone(),
        mv_definition
            .facts
            .source_revision()
            .target_object_id
            .clone(),
        context.clone(),
    );
    let (observed,_)=novarocks_mv_application::persistence::keyed_documents::observe_current_keyed_documents_with_management(&documents,request,Default::default(),entrance).map_err(|e|e.to_string())?;
    if observed.view.current_snapshot != binding.current_snapshot_id() {
        return Err(format!(
            "MV target {}.{}.{} changed during refresh planning",
            target.catalog, target.namespace, target.table
        ));
    }
    mv_definition
        .facts
        .validate_refresh_fence(&observed.view)
        .map_err(|e| e.to_string())?;

    Ok(())
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn recorded_target_snapshot_id(
    target: &IcebergMvTarget,
    mv_definition: &StoredKeyedMvProjection,
) -> Result<i64, String> {
    published_target_snapshot_id(mv_definition)?.ok_or_else(|| {
        format!(
            "iceberg materialized view {}.{}.{} has no recorded target snapshot",
            target.catalog, target.namespace, target.table
        )
    })
}

fn published_target_snapshot_id(
    projection: &StoredKeyedMvProjection,
) -> Result<Option<i64>, String> {
    if projection.facts.publication().is_some() {
        projection
            .facts
            .publication_snapshot()
            .map(Some)
            .ok_or_else(|| "published Iceberg MV output has no provider snapshot ID".to_string())
    } else {
        Ok(projection.facts.metadata_version().snapshot_id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::KeyedProjectionFixture as ProjectionFixture;
    use novarocks_mv_application::product::MvTarget as ProductMvTarget;

    #[test]
    fn target_snapshot_comes_only_from_the_publication_output() {
        let mut fixture = ProjectionFixture::new(
            ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(11),
        );
        fixture.metadata_version = novarocks_spi::connector::ConnectorCommittedVersion::try_new(
            bytes::Bytes::from_static(b"later-configuration-metadata"),
            Some(12),
        )
        .expect("metadata version");
        let projection = StoredKeyedMvProjection {
            mv_id: 1,
            facts: fixture.build().expect("projection"),
        };

        assert_eq!(published_target_snapshot_id(&projection).unwrap(), Some(12));
    }

    #[test]
    fn never_published_is_not_a_published_zero_row_result() {
        let projection = StoredKeyedMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(
                ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
                None,
            )
            .build()
            .expect("never published projection"),
        };
        assert_eq!(published_target_snapshot_id(&projection).unwrap(), None);
        let mut empty = ProjectionFixture::new(
            ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(11),
        );
        let publication = empty.publication.as_mut().expect("published fixture");
        publication.logical_result_rows = Some(0);
        let empty = StoredKeyedMvProjection {
            mv_id: 1,
            facts: empty.build().unwrap(),
        };
        assert_eq!(published_target_snapshot_id(&empty).unwrap(), Some(11));
    }

    #[test]
    fn iceberg_mv_target_from_table_identity_preserves_exact_case() {
        let identity = TableIdentity {
            catalog: "TargetCase".to_string(),
            namespace: "NameSpace".to_string(),
            table: "MvTable".to_string(),
        };

        let target = IcebergMvTarget::from(&identity);

        assert_eq!(target.catalog, identity.catalog);
        assert_eq!(target.namespace, identity.namespace);
        assert_eq!(target.table, identity.table);
    }
    #[test]
    fn first_refresh_accepts_an_empty_slot_change_parent() {
        let mut fixture = ProjectionFixture::new(
            ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
            None,
        );
        fixture.metadata_version = novarocks_spi::connector::ConnectorCommittedVersion::try_new(
            bytes::Bytes::from_static(b"empty-slot-11"),
            Some(11),
        )
        .unwrap();
        let projection = StoredKeyedMvProjection {
            mv_id: 1,
            facts: fixture.build().unwrap(),
        };
        assert!(projection.facts.never_published());
        assert_eq!(published_target_snapshot_id(&projection).unwrap(), Some(11));
        let (_, current) =
            novarocks_mv_application::persistence::keyed_documents::keyed_view_fixture(
                Some(12),
                false,
            );
        projection
            .facts
            .validate_refresh_fence(&current)
            .expect("another known-empty slot parent remains a first refresh");
    }

    #[test]
    fn first_refresh_rechecks_both_file_totals_and_rejects_missing_totals() {
        let fixture = ProjectionFixture::new(
            ProductMvTarget::from_parts(Some("ice"), "sales", "mv"),
            None,
        );
        let projection = fixture.build().unwrap();
        let (_, mut current) =
            novarocks_mv_application::persistence::keyed_documents::keyed_view_fixture(
                Some(12),
                false,
            );
        current.current_snapshot_file_totals.data_files = None;
        assert!(projection.validate_refresh_fence(&current).is_err());
        current.current_snapshot_file_totals.data_files = Some(0);
        current.current_snapshot_file_totals.delete_files = Some(1);
        assert!(projection.validate_refresh_fence(&current).is_err());
        current.current_snapshot_file_totals.delete_files = Some(0);
        current.current_snapshot_file_totals.data_files = Some(1);
        assert!(projection.validate_refresh_fence(&current).is_err());
    }
}
