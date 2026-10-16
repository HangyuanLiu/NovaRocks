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

use std::collections::BTreeMap;
use std::str::FromStr;

use novarocks_spi::connector::LakePublicationId;
use uuid::Uuid;

use crate::iceberg::spec::Snapshot;

pub const MV_PUBLICATION_ID_PROP: &str = "novarocks.mv.publication_id";
pub const MV_PUBLICATION_STAGING_REF_PREFIX: &str = "__novarocks_mv_publication_";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MvPublicationSnapshotMarker {
    pub publication_id: LakePublicationId,
}

impl MvPublicationSnapshotMarker {
    pub fn to_summary_properties(&self) -> BTreeMap<String, String> {
        BTreeMap::from([(
            MV_PUBLICATION_ID_PROP.to_string(),
            self.publication_id.to_string(),
        )])
    }
}

pub fn snapshot_matches_publication_marker(
    snapshot: &Snapshot,
    marker: &MvPublicationSnapshotMarker,
) -> bool {
    snapshot
        .summary()
        .additional_properties
        .get(MV_PUBLICATION_ID_PROP)
        .and_then(|value| LakePublicationId::from_str(value).ok())
        == Some(marker.publication_id)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshPublishPlan {
    pub namespace: String,
    pub table: String,
    pub target_table_uuid: Uuid,
    pub staging_branch: String,
    pub expected_main_snapshot_id: Option<i64>,
    pub staging_snapshot_id: i64,
    pub marker: MvPublicationSnapshotMarker,
}

impl MvRefreshPublishPlan {
    /// Validate the publication identity and lower the exact ref CAS without I/O.
    pub(crate) fn prepare(
        &self,
        loaded: &crate::iceberg::table::Table,
    ) -> Result<
        crate::commit::staging::PreparedChange,
        novarocks_spi::connector::ConnectorMutationFailure,
    > {
        use novarocks_spi::connector::{ConnectorMutationFailure, ConnectorMutationFailureKind};
        let invalid = |message| {
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::InvalidRequest, message)
        };
        if self.staging_branch
            != format!(
                "{MV_PUBLICATION_STAGING_REF_PREFIX}{}",
                self.marker.publication_id
            )
        {
            return Err(invalid(
                "Staging ref does not match the exact publication identity",
            ));
        }
        if loaded.metadata().uuid() != self.target_table_uuid {
            return Err(invalid("MV publication target table incarnation changed"));
        }
        let snapshot = loaded
            .metadata()
            .snapshot_by_id(self.staging_snapshot_id)
            .ok_or_else(|| {
                ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::NotFound,
                    "MV publication snapshot does not exist",
                )
            })?;
        if !snapshot_matches_publication_marker(snapshot, &self.marker) {
            return Err(invalid("MV publication snapshot marker does not match"));
        }
        crate::commit::RefActionPlan {
            catalog: String::new(),
            namespace: self.namespace.clone(),
            table: self.table.clone(),
            action: crate::commit::RefAction::FastForwardBranch {
                source_branch: self.staging_branch.clone(),
                target_branch: "main".to_string(),
                source_snapshot_id: self.staging_snapshot_id,
                expected_target_snapshot_id: self.expected_main_snapshot_id,
            },
        }
        .prepare(loaded)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, SortOrder,
        Summary, TableMetadataBuilder, Type,
    };
    use crate::iceberg::spec::{SnapshotReference, SnapshotRetention, TableMetadata};
    use crate::iceberg::{TableIdent, TableRequirement};

    fn metadata_with_uuid(uuid: Uuid) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/db/mv".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .assign_uuid(uuid)
        .build()
        .expect("metadata")
        .metadata
    }

    fn plan() -> MvRefreshPublishPlan {
        MvRefreshPublishPlan {
            namespace: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: Uuid::from_u128(0x1234),
            staging_branch: "__novarocks_mv_publication_01890f3c-4e70-7cc0-8000-000000000077"
                .to_string(),
            expected_main_snapshot_id: Some(100),
            staging_snapshot_id: 300,
            marker: MvPublicationSnapshotMarker {
                publication_id: LakePublicationId::try_from_uuid(
                    Uuid::parse_str("01890f3c-4e70-7cc0-8000-000000000077").unwrap(),
                )
                .unwrap(),
            },
        }
    }

    fn loaded(plan: &MvRefreshPublishPlan, uuid: Uuid) -> crate::iceberg::table::Table {
        let metadata = metadata_with_uuid(uuid);
        let time = metadata.last_updated_ms();
        let snapshot = |id, properties| {
            Snapshot::builder()
                .with_snapshot_id(id)
                .with_sequence_number(id)
                .with_timestamp_ms(time)
                .with_manifest_list(format!("memory://snap-{id}.avro"))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: properties,
                })
                .build()
        };
        let metadata = metadata
            .into_builder(None)
            .add_snapshot(snapshot(100, HashMap::new()))
            .unwrap()
            .add_snapshot(snapshot(
                300,
                plan.marker.to_summary_properties().into_iter().collect(),
            ))
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(100, SnapshotRetention::branch(None, None, None)),
            )
            .unwrap()
            .set_ref(
                &plan.staging_branch,
                SnapshotReference::new(300, SnapshotRetention::branch(None, None, None)),
            )
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        crate::iceberg::table::Table::builder()
            .runtime(crate::iceberg::Runtime::try_current().unwrap())
            .identifier(TableIdent::from_strs(["db", "mv"]).unwrap())
            .metadata(metadata)
            .file_io(crate::iceberg::io::FileIO::new_with_memory())
            .build()
            .unwrap()
    }
    #[tokio::test]
    async fn publish_commit_uses_uuid_main_and_staging_cas_only() {
        let plan = plan();
        let change = plan
            .prepare(&loaded(&plan, plan.target_table_uuid))
            .unwrap();
        assert_eq!(
            change.requirements,
            vec![
                TableRequirement::UuidMatch {
                    uuid: plan.target_table_uuid
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: plan.staging_branch.clone(),
                    snapshot_id: Some(300)
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: "main".into(),
                    snapshot_id: Some(100)
                }
            ]
        );
        let [
            crate::iceberg::TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            },
        ] = change.updates.as_slice()
        else {
            panic!("one main update")
        };
        assert_eq!(ref_name, "main");
        assert_eq!(reference.snapshot_id, 300);
    }
    #[tokio::test]
    async fn publish_commit_refuses_a_plan_frozen_for_another_incarnation() {
        let plan = plan();
        let error = plan
            .prepare(&loaded(&plan, Uuid::from_u128(0x5678)))
            .err()
            .expect("preparation must refuse");
        assert_eq!(
            error.kind(),
            novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest
        );
        assert_eq!(
            error.message(),
            "MV publication target table incarnation changed"
        );
    }

    #[test]
    fn marker_match_requires_one_valid_v7_publication_id() {
        let plan = plan();
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: plan.marker.to_summary_properties().into_iter().collect(),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(300)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:/tmp/manifest-list.avro".to_string())
            .with_summary(summary)
            .with_schema_id(0)
            .build();
        assert!(snapshot_matches_publication_marker(&snapshot, &plan.marker));
    }
}
