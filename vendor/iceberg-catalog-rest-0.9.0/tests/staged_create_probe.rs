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

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{
    NestedField, Operation, PrimitiveType, Schema, Snapshot, SnapshotReference, SnapshotRetention,
    Summary, Type,
};
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCommit, TableCreation, TableIdent,
    TableRequirement, TableUpdate,
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};

#[tokio::test]
#[ignore = "requires the local Iceberg REST fixture"]
async fn test_stage_create_local_fixture_probe() {
    let rest_endpoint = std::env::var("NOVAROCKS_ICEBERG_REST_URI")
        .expect("NOVAROCKS_ICEBERG_REST_URI must point at the local fixture");
    let warehouse = std::env::var("NOVAROCKS_ICEBERG_REST_WAREHOUSE")
        .expect("NOVAROCKS_ICEBERG_REST_WAREHOUSE must select the worktree warehouse");
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "rest",
            HashMap::from([
                (REST_CATALOG_PROP_URI.to_string(), rest_endpoint),
                (REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse),
            ]),
        )
        .await
        .unwrap();

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let namespace = NamespaceIdent::new(format!("dml3_t3a_{suffix}"));
    let ident = TableIdent::new(namespace.clone(), "staged_create_probe".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();

    let staged = catalog
        .stage_create_table(
            &namespace,
            TableCreation::builder()
                .name(ident.name().to_string())
                .schema(
                    Schema::builder()
                        .with_fields(vec![
                            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long))
                                .into(),
                        ])
                        .build()
                        .unwrap(),
                )
                .properties(HashMap::from([(
                    "dml3.t3a.probe".to_string(),
                    "true".to_string(),
                )]))
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(None, staged.table().metadata_location());
    assert!(!catalog.table_exists(&ident).await.unwrap());
    let staged_uuid = staged.table().metadata().uuid();
    let table_location = staged.table().metadata().location().to_string();
    let schema_id = staged.table().metadata().current_schema_id();
    let (_, mut updates) = staged.into_parts();
    let snapshot_id = 3055729675574597000i64;
    updates.push(TableUpdate::AddSnapshot {
        snapshot: Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_timestamp_ms(1657810968051)
            .with_sequence_number(1)
            .with_manifest_list(format!(
                "{table_location}/metadata/snap-dml3-t3a-proof.avro"
            ))
            .with_schema_id(schema_id)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::from([(
                    "dml3-t3a-proof".to_string(),
                    "combined-commit".to_string(),
                )]),
            })
            .build(),
    });
    updates.push(TableUpdate::SetSnapshotRef {
        ref_name: "main".to_string(),
        reference: SnapshotReference::new(snapshot_id, SnapshotRetention::branch(None, None, None)),
    });

    let committed = catalog
        .update_table(
            TableCommit::builder()
                .ident(ident.clone())
                .requirements(vec![TableRequirement::NotExist])
                .updates(updates)
                .build(),
        )
        .await
        .unwrap();
    assert_eq!(staged_uuid, committed.metadata().uuid());
    assert!(committed.metadata_location().is_some());
    let committed_snapshot = committed.metadata().current_snapshot().unwrap();
    assert_eq!(snapshot_id, committed_snapshot.snapshot_id());
    assert_eq!(
        Some(&"combined-commit".to_string()),
        committed_snapshot
            .summary()
            .additional_properties
            .get("dml3-t3a-proof")
    );
    assert!(catalog.table_exists(&ident).await.unwrap());

    catalog.drop_table(&ident).await.unwrap();
    catalog.drop_namespace(&namespace).await.unwrap();
}
