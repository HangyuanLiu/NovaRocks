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

//! The only place a concrete Iceberg catalog kind is chosen.
//!
//! Design: ADR-0110 (docs/adr/ADR-0110-iceberg-provider-private-catalog-owner.md)
//!
//! `IcebergCatalogKind` is a validated configuration value, and this is the one
//! function allowed to match on it. Past this point the rest of the provider
//! holds an `Arc<dyn NovaRocksCatalog>` and asks operations, never kinds.

use std::sync::Arc;

use crate::catalog_config::{IcebergCatalogConfiguration, IcebergCatalogKind};

use super::NovaRocksCatalog;

/// Builds the single catalog retained by one control generation.
pub(crate) struct NovaRocksCatalogFactory;

impl NovaRocksCatalogFactory {
    /// Wrap a client this generation already built.
    ///
    /// The generation must end up with exactly one client. Building a second
    /// one here would give it two with separate in-memory state, and they would
    /// disagree about the same lake — a table dropped through one still
    /// resolving through the other.
    pub(crate) fn adopt(
        configuration: &IcebergCatalogConfiguration,
        client: &crate::catalog_runtime::IcebergCatalogClient,
    ) -> Result<Arc<dyn NovaRocksCatalog>, String> {
        let warehouse: Option<Arc<str>> = if configuration.warehouse_uri.is_empty() {
            None
        } else {
            Some(Arc::from(configuration.warehouse_uri.as_str()))
        };
        match configuration.kind {
            IcebergCatalogKind::Hadoop => {
                let hadoop = client.hadoop().cloned().ok_or_else(|| {
                    "Hadoop Iceberg configuration produced no Hadoop client".to_string()
                })?;
                Ok(Arc::new(super::hadoop::NovaRocksHadoopCatalog::new(hadoop)))
            }
            IcebergCatalogKind::Rest => {
                let rest = client.rest().cloned().ok_or_else(|| {
                    "REST Iceberg configuration produced no REST client".to_string()
                })?;
                Ok(Arc::new(super::rest::NovaRocksRestCatalog::new(
                    rest, warehouse,
                )))
            }
            IcebergCatalogKind::Hive => Ok(Arc::new(super::hive::NovaRocksHiveCatalog::adopt(
                Arc::clone(client.generic()),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::error::CatalogOutcome;
    use crate::catalog::transaction::{
        CreateTableTransactionRequest, TransactionIdentity, TransactionRequest,
    };
    use crate::catalog::{
        CatalogCreateIntent, CatalogNamespaceName, CatalogTableName, CatalogTransactionStart,
    };
    use crate::iceberg::TableCreation;
    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use novarocks_spi::connector::ConnectorErrorKind;

    /// Build a catalog for a test the way production does.
    ///
    /// Production never constructs a client here: the generation already has
    /// one and this adopts it. A second construction path used to exist, and it
    /// had already drifted -- it wrapped a different Hive client than adoption
    /// did, and it built a second client of its own, which is exactly the
    /// two-clients-one-lake failure `adopt` documents.
    async fn adopted(
        configuration: &IcebergCatalogConfiguration,
    ) -> Result<Arc<dyn NovaRocksCatalog>, String> {
        let client = crate::catalog_runtime::build_catalog_client(configuration).await?;
        NovaRocksCatalogFactory::adopt(configuration, &client)
    }

    fn hadoop_configuration(warehouse: &std::path::Path) -> IcebergCatalogConfiguration {
        crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.display().to_string(),
            )],
        )
        .expect("hadoop catalog configuration")
    }

    fn creation(name: &str) -> TableCreation {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        TableCreation::builder()
            .name(name.to_string())
            .schema(schema)
            .build()
    }

    fn create_request(
        namespace: &str,
        table: &str,
        intent: CatalogCreateIntent,
    ) -> CreateTableTransactionRequest {
        CreateTableTransactionRequest {
            identity: TransactionIdentity::new("test", [1u8; 16]),
            target: CatalogTableName::new(namespace, table),
            intent,
            creation: creation(table),
            warehouse: None,
        }
    }

    #[tokio::test]
    async fn factory_builds_one_catalog_and_hides_the_concrete_kind() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = hadoop_configuration(warehouse.path());
        let catalog = adopted(&configuration).await.expect("catalog");
        assert_eq!(catalog.implementation_name(), "hadoop");
    }

    /// The behavior change this owner exists for.
    ///
    /// A Hadoop catalog cannot store views, and it used to answer view
    /// enumeration with `false` / an empty list — turning "cannot answer" into
    /// "authoritatively none". Callers could not tell the difference, and
    /// `DROP DATABASE ... FORCE` silently relied on the fiction.
    #[tokio::test]
    async fn view_enumeration_reports_unsupported_instead_of_faking_absence() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .expect("catalog");

        let exists = catalog
            .view_exists(CatalogTableName::new("db", "v"))
            .await
            .expect_err("view_exists must not answer false");
        assert_eq!(exists.kind(), ConnectorErrorKind::Unsupported);

        let listed = catalog
            .list_views(CatalogNamespaceName::new("db"))
            .await
            .expect_err("list_views must not answer with an empty list");
        assert_eq!(listed.kind(), ConnectorErrorKind::Unsupported);

        let loaded = catalog
            .load_view(CatalogTableName::new("db", "v"))
            .await
            .expect_err("load_view must not answer not-found");
        assert_eq!(loaded.kind(), ConnectorErrorKind::Unsupported);
    }

    /// Admission is a per-request question, not a per-catalog flag: the same
    /// catalog accepts one create intent and refuses the other.
    #[tokio::test]
    async fn hadoop_admits_empty_create_and_refuses_ctas_before_any_side_effect() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .expect("catalog");
        assert!(matches!(
            catalog
                .create_namespace(CatalogNamespaceName::new("db"))
                .await,
            CatalogOutcome::KnownCommitted { .. }
        ));

        let empty = catalog
            .new_create_table_transaction(create_request(
                "db",
                "t_empty",
                CatalogCreateIntent::EmptyTable,
            ))
            .await;
        assert!(
            matches!(empty, CatalogTransactionStart::Ready(_)),
            "empty-table creation is atomic on this catalog"
        );

        let before = std::fs::read_dir(warehouse.path())
            .expect("warehouse readable")
            .count();
        let ctas = catalog
            .new_create_table_transaction(create_request(
                "db",
                "t_ctas",
                CatalogCreateIntent::CreateTableAsSelect,
            ))
            .await;
        let CatalogTransactionStart::Unsupported(reason) = &ctas else {
            panic!("CTAS must be refused on a Hadoop catalog, got {ctas:?}");
        };
        assert!(reason.message().contains("staged-create"));
        assert!(ctas.permits_cleanup());
        assert_eq!(
            std::fs::read_dir(warehouse.path())
                .expect("warehouse readable")
                .count(),
            before,
            "a refused CTAS must not create anything in the warehouse"
        );
    }

    #[tokio::test]
    async fn create_or_replace_is_refused_where_it_cannot_be_atomic() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .expect("catalog");
        let outcome = catalog
            .new_create_or_replace_table_transaction(create_request(
                "db",
                "t",
                CatalogCreateIntent::EmptyTable,
            ))
            .await;
        assert!(matches!(outcome, CatalogTransactionStart::Unsupported(_)));
    }

    #[tokio::test]
    async fn existing_table_transactions_are_admitted_on_every_catalog() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .expect("catalog");
        let start = catalog
            .new_transaction(TransactionRequest {
                identity: TransactionIdentity::new("test", [2u8; 16]),
                target: CatalogTableName::new("db", "t"),
                target_ref: Arc::from("main"),
                base_snapshot_id: None,
                expected_table_uuid: None,
                marker: None,
            })
            .await;
        assert!(matches!(start, CatalogTransactionStart::Ready(_)));
    }
}
