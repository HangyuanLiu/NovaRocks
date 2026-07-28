// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorCancellation, ConnectorInstanceId,
    ConnectorListTablesRequest, ConnectorNamespaceIdentity, ConnectorReadSelector,
    ConnectorRequestContext, ConnectorSplitPlanningRequest, ConnectorTableIdentity,
    ConnectorTableRequest, ConnectorTableResolution,
};

use super::iceberg::catalog::registry::{create_table, insert_rows};
use super::iceberg::catalog::{IcebergCatalogRegistry, create_namespace};
use super::iceberg::provider::IcebergConnectorInstance;
use crate::sql::{Literal, TableColumnDef};
use novarocks_catalog::schema::SqlType;

struct NotCancelled;

impl ConnectorCancellation for NotCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        Arc::new(NotCancelled),
        1024 * 1024,
        4 * 1024 * 1024,
    )
    .expect("request context")
}

fn registry_with_table() -> (Arc<RwLock<IcebergCatalogRegistry>>, tempfile::TempDir) {
    let warehouse = tempfile::Builder::new()
        .prefix("novarocks_spi_iceberg_provider_")
        .tempdir()
        .expect("warehouse tempdir");
    let registry = Arc::new(RwLock::new(IcebergCatalogRegistry::default()));
    {
        let mut guard = registry.write().expect("iceberg catalog write lock");
        guard
            .create_catalog(
                "ice",
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    (
                        "iceberg.catalog.warehouse".to_string(),
                        format!("file://{}", warehouse.path().join("warehouse").display()),
                    ),
                ],
            )
            .expect("create catalog");
    }
    let entry = registry
        .read()
        .expect("iceberg catalog read lock")
        .get("ice")
        .expect("catalog entry");
    create_namespace(&entry, "db").expect("create namespace");
    create_table(
        &entry,
        "db",
        "orders",
        &[TableColumnDef {
            name: "id".to_string(),
            data_type: SqlType::Int,
            nullable: false,
            aggregation: None,
            default: None,
        }],
        None,
        &[],
        &[],
    )
    .expect("create table");
    insert_rows(&entry, "db", "orders", &[vec![Literal::Int(7)]]).expect("insert table row");
    (registry, warehouse)
}

#[test]
fn iceberg_instance_resolves_metadata_and_plans_a_snapshot_split() {
    let (registry, _warehouse) = registry_with_table();
    let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
    let instance = IcebergConnectorInstance::new(instance_id.clone(), registry)
        .expect("iceberg connector instance");
    let metadata = instance.metadata().expect("metadata capability");
    let namespace = ConnectorNamespaceIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from("db"),
    };
    let table = ConnectorTableIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from("db"),
        table: Arc::from("orders"),
    };

    assert_eq!(
        metadata
            .list_tables(ConnectorListTablesRequest {
                namespace: namespace.clone(),
                context: context(),
            })
            .expect("list tables"),
        vec![table.clone()]
    );
    let resolved = metadata
        .load_table(ConnectorTableRequest {
            table,
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context(),
        })
        .expect("load table");
    assert_eq!(resolved.table.owner(), &instance_id);
    assert_eq!(resolved.schema.fields().len(), 1);

    let scan = instance
        .read()
        .begin_scan(
            &resolved.table,
            ConnectorBeginScanRequest {
                projection: vec![0],
                selector: ConnectorReadSelector::Current,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
                    max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero bytes"),
                },
                context: context(),
            },
        )
        .expect("begin scan");
    let splits = instance
        .read()
        .plan_splits(
            &scan.handle,
            ConnectorSplitPlanningRequest {
                target_parallelism: NonZeroUsize::new(1).expect("parallelism"),
                max_split_bytes: None,
                context: context(),
            },
        )
        .expect("plan splits");
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].owner(), &instance_id);
    assert!(splits[0].estimated_bytes().is_some_and(|bytes| bytes > 0));
}
