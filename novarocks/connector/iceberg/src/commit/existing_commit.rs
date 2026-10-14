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

//! Existing-table commit construction that always freezes the table UUID.
//!
//! Every direct-path commit against an existing table is built here. The UUID
//! comes from the same metadata that produced the updates, never from a second
//! load: a reload may already observe a dropped-and-recreated table whose
//! schema and spec IDs happen to match, and the commit must not land on it.

use std::fmt;

use uuid::Uuid;

use crate::iceberg::spec::TableMetadata;
use crate::iceberg::{TableCommit, TableIdent, TableRequirement, TableUpdate};

/// Refusal to build an existing-table commit. Raised before any dispatch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ExistingCommitError {
    /// A caller-supplied UUID assertion names another table incarnation than
    /// the metadata the updates were produced from.
    ConflictingTableUuid { base: Uuid, asserted: Uuid },
    /// An existing-table commit cannot assert that the table does not exist.
    CreateRequirement,
}

impl fmt::Display for ExistingCommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConflictingTableUuid { base, asserted } => write!(
                f,
                "existing-table commit asserts table UUID {asserted}, but its updates were \
                 produced from table UUID {base}"
            ),
            Self::CreateRequirement => {
                f.write_str("existing-table commit cannot carry a table-does-not-exist requirement")
            }
        }
    }
}

impl std::error::Error for ExistingCommitError {}

/// Build a commit against an existing table, freezing `base`'s UUID.
///
/// `base` must be the metadata the caller produced `updates` (and the other
/// `requirements`) from. A `UuidMatch` already present must equal its UUID;
/// otherwise the frozen assertion is placed first so a replaced table fails
/// before any other requirement is evaluated.
pub(crate) fn existing_table_commit(
    ident: TableIdent,
    base: &TableMetadata,
    mut requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
) -> Result<TableCommit, ExistingCommitError> {
    let uuid = base.uuid();
    let mut frozen = false;
    for requirement in &requirements {
        match requirement {
            TableRequirement::NotExist => return Err(ExistingCommitError::CreateRequirement),
            TableRequirement::UuidMatch { uuid: asserted } if *asserted != uuid => {
                return Err(ExistingCommitError::ConflictingTableUuid {
                    base: uuid,
                    asserted: *asserted,
                });
            }
            TableRequirement::UuidMatch { .. } => frozen = true,
            _ => {}
        }
    }
    if !frozen {
        requirements.insert(0, TableRequirement::UuidMatch { uuid });
    }
    Ok(TableCommit::builder()
        .ident(ident)
        .requirements(requirements)
        .updates(updates)
        .build())
}

/// Shared fixtures for call-site tests: a local Hadoop table whose catalog
/// records every dispatched commit's requirements before delegating.
#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use uuid::Uuid;

    use crate::iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, FormatVersion, NestedField,
        PrimitiveType, Schema, Struct, Type,
    };
    use crate::iceberg::table::Table;
    use crate::iceberg::transaction::{ApplyTransactionAction, Transaction};
    use crate::iceberg::{
        Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
        TableRequirement,
    };

    #[derive(Debug)]
    pub(crate) struct RecordingCatalog {
        inner: Arc<dyn Catalog>,
        commits: Mutex<Vec<Vec<TableRequirement>>>,
    }

    impl RecordingCatalog {
        pub(crate) fn commits(&self) -> Vec<Vec<TableRequirement>> {
            self.commits.lock().expect("recording catalog lock").clone()
        }

        /// UUID assertions of the only commit recorded since the last call.
        pub(crate) fn take_single_commit_uuids(&self) -> Vec<Uuid> {
            let mut commits = self.commits.lock().expect("recording catalog lock");
            assert_eq!(commits.len(), 1, "expected exactly one dispatched commit");
            commits
                .drain(..)
                .flatten()
                .filter_map(|requirement| match requirement {
                    TableRequirement::UuidMatch { uuid } => Some(uuid),
                    _ => None,
                })
                .collect()
        }
    }

    #[async_trait]
    impl Catalog for RecordingCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&NamespaceIdent>,
        ) -> crate::iceberg::Result<Vec<NamespaceIdent>> {
            self.inner.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> crate::iceberg::Result<Namespace> {
            self.inner.create_namespace(namespace, properties).await
        }

        async fn get_namespace(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<Namespace> {
            self.inner.get_namespace(namespace).await
        }

        async fn namespace_exists(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<bool> {
            self.inner.namespace_exists(namespace).await
        }

        async fn update_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> crate::iceberg::Result<()> {
            self.inner.update_namespace(namespace, properties).await
        }

        async fn drop_namespace(&self, namespace: &NamespaceIdent) -> crate::iceberg::Result<()> {
            self.inner.drop_namespace(namespace).await
        }

        async fn list_tables(
            &self,
            namespace: &NamespaceIdent,
        ) -> crate::iceberg::Result<Vec<TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            namespace: &NamespaceIdent,
            creation: TableCreation,
        ) -> crate::iceberg::Result<Table> {
            self.inner.create_table(namespace, creation).await
        }

        async fn load_table(&self, table: &TableIdent) -> crate::iceberg::Result<Table> {
            self.inner.load_table(table).await
        }

        async fn drop_table(&self, table: &TableIdent) -> crate::iceberg::Result<()> {
            self.inner.drop_table(table).await
        }

        async fn purge_table(&self, table: &TableIdent) -> crate::iceberg::Result<()> {
            self.inner.purge_table(table).await
        }

        async fn table_exists(&self, table: &TableIdent) -> crate::iceberg::Result<bool> {
            self.inner.table_exists(table).await
        }

        async fn rename_table(
            &self,
            src: &TableIdent,
            dest: &TableIdent,
        ) -> crate::iceberg::Result<()> {
            self.inner.rename_table(src, dest).await
        }

        async fn register_table(
            &self,
            table: &TableIdent,
            metadata_location: String,
        ) -> crate::iceberg::Result<Table> {
            self.inner.register_table(table, metadata_location).await
        }

        async fn update_table(&self, mut commit: TableCommit) -> crate::iceberg::Result<Table> {
            let requirements = commit.take_requirements();
            self.commits
                .lock()
                .expect("recording catalog lock")
                .push(requirements.clone());
            let forwarded = TableCommit::builder()
                .ident(commit.identifier().clone())
                .requirements(requirements)
                .updates(commit.take_updates())
                .build();
            self.inner.update_table(forwarded).await
        }
    }

    pub(crate) struct LocalTable {
        pub(crate) catalog: Arc<RecordingCatalog>,
        pub(crate) ident: TableIdent,
        _warehouse: tempfile::TempDir,
    }

    impl LocalTable {
        pub(crate) async fn load(&self) -> Table {
            self.catalog
                .load_table(&self.ident)
                .await
                .expect("load local table")
        }

        /// Fast-append one synthetic data file to `main`; returns its snapshot.
        pub(crate) async fn append(&self, name: &str) -> i64 {
            let table = self.load().await;
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!(
                    "{}/data/{name}.parquet",
                    table.metadata().location()
                ))
                .file_format(DataFileFormat::Parquet)
                .record_count(1)
                .file_size_in_bytes(10)
                .partition(Struct::empty())
                .build()
                .expect("synthetic data file");
            let transaction = Transaction::new(&table);
            let transaction = transaction
                .fast_append()
                .add_data_files([file])
                .apply(transaction)
                .expect("stage synthetic append");
            let table = transaction
                .commit(self.catalog.inner.as_ref())
                .await
                .expect("commit synthetic append");
            table
                .metadata()
                .current_snapshot_id()
                .expect("append produced a snapshot")
        }
    }

    /// Requires a current Tokio runtime (`#[tokio::test]`).
    pub(crate) async fn local_table() -> LocalTable {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let runtime = tokio::runtime::Handle::current();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime.clone())),
        );
        let inner: Arc<dyn Catalog> = Arc::new(
            crate::hadoop_catalog::HadoopFileSystemCatalog::new_with_binding(
                crate::fs_io::build_file_io_for_location(&warehouse_uri, binding.clone()),
                warehouse_uri,
                binding,
                crate::resources::IcebergCatalogRuntime::new(runtime)
                    .iceberg_runtime()
                    .clone(),
            ),
        );
        let namespace = NamespaceIdent::new("db".to_string());
        inner
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        inner
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .format_version(FormatVersion::V2)
                    .build(),
            )
            .await
            .expect("create table");
        LocalTable {
            catalog: Arc::new(RecordingCatalog {
                inner,
                commits: Mutex::new(Vec::new()),
            }),
            ident: TableIdent::new(namespace, "t".to_string()),
            _warehouse: warehouse,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };

    fn metadata(uuid: Uuid) -> TableMetadata {
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
            "memory://warehouse/db/t".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .assign_uuid(uuid)
        .build()
        .expect("metadata")
        .metadata
    }

    fn ident() -> TableIdent {
        TableIdent::from_strs(["db", "t"]).expect("table identifier")
    }

    fn updates() -> Vec<TableUpdate> {
        vec![TableUpdate::RemoveProperties {
            removals: vec!["k".to_string()],
        }]
    }

    #[test]
    fn freezes_the_uuid_of_the_metadata_that_produced_the_updates() {
        let base = metadata(Uuid::from_u128(7));
        let schema = TableRequirement::CurrentSchemaIdMatch {
            current_schema_id: base.current_schema_id(),
        };
        let mut commit = existing_table_commit(ident(), &base, vec![schema.clone()], updates())
            .expect("existing-table commit");

        assert_eq!(commit.identifier(), &ident());
        assert_eq!(
            commit.take_requirements(),
            vec![
                TableRequirement::UuidMatch {
                    uuid: Uuid::from_u128(7)
                },
                schema,
            ]
        );
        assert_eq!(commit.take_updates(), updates());
    }

    #[test]
    fn keeps_one_equal_caller_uuid_assertion_in_place() {
        let base = metadata(Uuid::from_u128(7));
        let requirements = vec![
            TableRequirement::DefaultSpecIdMatch { default_spec_id: 0 },
            TableRequirement::UuidMatch {
                uuid: Uuid::from_u128(7),
            },
        ];
        let mut commit = existing_table_commit(ident(), &base, requirements.clone(), updates())
            .expect("existing-table commit");

        assert_eq!(commit.take_requirements(), requirements);
    }

    #[test]
    fn rejects_a_uuid_assertion_for_another_table_incarnation() {
        let base = metadata(Uuid::from_u128(7));
        let error = existing_table_commit(
            ident(),
            &base,
            vec![TableRequirement::UuidMatch {
                uuid: Uuid::from_u128(8),
            }],
            updates(),
        )
        .expect_err("conflicting UUID assertion");

        assert_eq!(
            error,
            ExistingCommitError::ConflictingTableUuid {
                base: Uuid::from_u128(7),
                asserted: Uuid::from_u128(8),
            }
        );
    }

    #[test]
    fn rejects_a_table_does_not_exist_requirement() {
        let base = metadata(Uuid::from_u128(7));
        let error =
            existing_table_commit(ident(), &base, vec![TableRequirement::NotExist], updates())
                .expect_err("create requirement");

        assert_eq!(error, ExistingCommitError::CreateRequirement);
    }
}
