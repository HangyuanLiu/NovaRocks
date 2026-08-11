// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.

//! Provider-private REST staged-table preparation.
//!
//! One control generation retains the exact concrete REST client used for
//! ordinary metadata. Staging therefore neither rebuilds a client nor
//! downcasts the generic catalog surface.

use std::collections::HashMap;
use std::sync::Arc;

use novarocks_catalog::identifier::normalize_identifier;
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorError, ConnectorPartitionTransform,
};

use crate::control_runtime::IcebergControlRuntime;
use crate::iceberg::{Catalog, NamespaceIdent, TableCreation, TableUpdate};

#[derive(Clone)]
pub(crate) struct RestStagedTableCreate {
    pub(crate) catalog: Arc<crate::iceberg_catalog_rest::RestCatalog>,
    pub(crate) table: crate::iceberg::table::Table,
    pub(crate) initialization_updates: Vec<TableUpdate>,
}

#[derive(Debug)]
pub(crate) enum RestStagedPrepareFailure {
    Conflict(String),
    KnownUncommitted(String),
    CommitUnknown(String),
}

impl From<String> for RestStagedPrepareFailure {
    fn from(message: String) -> Self {
        Self::KnownUncommitted(message)
    }
}

impl From<ConnectorError> for RestStagedPrepareFailure {
    fn from(error: ConnectorError) -> Self {
        Self::KnownUncommitted(error.to_string())
    }
}

pub(crate) fn prepare_rest_staged_table(
    runtime: &IcebergControlRuntime,
    namespace_name: &str,
    table_name: &str,
    columns: &[ConnectorColumnDefinition],
    partitioning: &[ConnectorPartitionTransform],
    properties: &[(Arc<str>, Arc<str>)],
) -> Result<RestStagedTableCreate, RestStagedPrepareFailure> {
    let catalog = runtime.rest_catalog().cloned().ok_or_else(|| {
        RestStagedPrepareFailure::KnownUncommitted(
            "atomic staged table publication is unsupported by this Iceberg catalog".to_string(),
        )
    })?;
    let namespace_name = normalize_identifier(namespace_name)?;
    let table_name = normalize_identifier(table_name)?;
    let namespace = NamespaceIdent::new(namespace_name.clone());
    let namespace_catalog = Arc::clone(&catalog);
    let namespace_for_check = namespace.clone();
    let exists = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move {
            namespace_catalog
                .namespace_exists(&namespace_for_check)
                .await
        })
        .map_err(|error| {
            RestStagedPrepareFailure::KnownUncommitted(format!(
                "check REST namespace runtime: {error}"
            ))
        })?
        .map_err(|error| {
            RestStagedPrepareFailure::KnownUncommitted(format!("check REST namespace: {error}"))
        })?;
    if !exists {
        return Err(RestStagedPrepareFailure::KnownUncommitted(format!(
            "prepare staged Iceberg table failed: namespace {namespace_name} does not exist"
        )));
    }
    let (format_version, mut properties) =
        super::catalog_mutation::table_properties(columns, None, properties)?;
    if format_version != crate::iceberg::spec::FormatVersion::V3
        && columns.iter().any(|column| {
            column.default.as_ref().is_some_and(|value| {
                !matches!(value, novarocks_spi::connector::ConnectorDefaultValue::Null)
            })
        })
    {
        return Err(RestStagedPrepareFailure::KnownUncommitted(
            "Iceberg column defaults require format-version 3".to_string(),
        ));
    }
    let schema = crate::iceberg::spec::Schema::builder()
        .with_fields(super::type_mapping::schema_fields(columns)?)
        .build()
        .map_err(|error| format!("build staged Iceberg schema: {error}"))?;
    let partition_spec = super::catalog_mutation::initial_partition_spec(&schema, partitioning)?;
    properties.insert(
        "format-version".to_string(),
        (format_version as u8).to_string(),
    );
    let publication_properties = properties
        .iter()
        .filter(|(key, _)| !key.eq_ignore_ascii_case("format-version"))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let creation = TableCreation::builder()
        .name(table_name)
        .schema(schema)
        .properties(properties.into_iter())
        .format_version(format_version);
    let creation = if let Some(spec) = partition_spec {
        creation.partition_spec(spec).build()
    } else {
        creation.build()
    };
    let staging_catalog = Arc::clone(&catalog);
    let staged = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move {
            staging_catalog
                .stage_create_table_typed(&namespace, creation)
                .await
        })
        .map_err(|error| {
            RestStagedPrepareFailure::KnownUncommitted(format!(
                "prepare staged REST table runtime: {error}"
            ))
        })?
        .map_err(|error| match error {
            crate::iceberg_catalog_rest::StagedCreateError::Conflict(error) => {
                RestStagedPrepareFailure::Conflict(format!("prepare staged REST table: {error}"))
            }
            crate::iceberg_catalog_rest::StagedCreateError::KnownNotDispatched(error) => {
                RestStagedPrepareFailure::KnownUncommitted(format!(
                    "prepare staged REST table: {error}"
                ))
            }
            crate::iceberg_catalog_rest::StagedCreateError::PossiblyDispatched(error) => {
                RestStagedPrepareFailure::CommitUnknown(format!(
                    "prepare staged REST table: {error}"
                ))
            }
        })?;
    let (table, mut initialization_updates) = staged.into_parts();
    if !publication_properties.is_empty() {
        initialization_updates.push(TableUpdate::SetProperties {
            updates: publication_properties,
        });
    }
    Ok(RestStagedTableCreate {
        catalog,
        table,
        initialization_updates,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergControlResources;

    #[test]
    fn hadoop_generation_fails_closed_without_constructing_a_rest_client() {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = IcebergControlRuntime::try_new(
            IcebergCatalogControlState::new(configuration),
            IcebergControlResources::new(binding, executor.handle().clone()),
        )
        .expect("control runtime");
        assert!(runtime.rest_catalog().is_none());
        let failure = match prepare_rest_staged_table(&runtime, "db", "t", &[], &[], &[]) {
            Ok(_) => panic!("Hadoop must not expose a REST staged-create surface"),
            Err(failure) => failure,
        };
        assert!(matches!(
            failure,
            RestStagedPrepareFailure::KnownUncommitted(message)
                if message.contains("unsupported")
        ));
    }
}
