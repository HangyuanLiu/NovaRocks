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

//! Provider-owned Iceberg catalog client construction.
//!
//! The caller supplies execution of the returned futures. This module never
//! discovers a Tokio runtime, so a future control generation can bind all
//! catalog I/O to its role-local runtime.

use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog_config::{IcebergCatalogConfiguration, IcebergCatalogKind};

pub fn build_hadoop_catalog(
    configuration: &IcebergCatalogConfiguration,
) -> Result<crate::hadoop_catalog::HadoopFileSystemCatalog, String> {
    if configuration.kind != IcebergCatalogKind::Hadoop {
        return Err(format!(
            "build Hadoop Iceberg catalog called for {:?} configuration",
            configuration.kind
        ));
    }
    let file_io = crate::fs_io::build_file_io_for_location(
        &configuration.warehouse_uri,
        configuration.object_store_config.as_ref(),
    );
    Ok(crate::hadoop_catalog::HadoopFileSystemCatalog::new(
        file_io,
        configuration.warehouse_uri.clone(),
    ))
}

pub async fn build_rest_catalog(
    configuration: &IcebergCatalogConfiguration,
) -> Result<crate::iceberg_catalog_rest::RestCatalog, String> {
    use crate::iceberg::CatalogBuilder;
    use crate::iceberg_catalog_rest::{
        REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
    };

    if configuration.kind != IcebergCatalogKind::Rest {
        return Err(format!(
            "build REST Iceberg catalog called for {:?} configuration",
            configuration.kind
        ));
    }
    let uri = configuration.rest_uri.clone().ok_or_else(|| {
        "REST iceberg catalog entry missing rest_uri (CREATE EXTERNAL CATALOG must set `uri`)"
            .to_string()
    })?;
    let mut properties = configuration
        .properties
        .iter()
        .filter(|(key, _)| key != "type")
        .cloned()
        .collect::<HashMap<_, _>>();
    properties.insert(REST_CATALOG_PROP_URI.to_string(), uri);
    if !configuration.warehouse_uri.is_empty() {
        properties.insert(
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            configuration.warehouse_uri.clone(),
        );
    }
    RestCatalogBuilder::default()
        .with_storage_factory(storage_factory(configuration))
        .load("rest".to_string(), properties)
        .await
        .map_err(|error| format!("build REST iceberg catalog: {error}"))
}

pub async fn build_hms_catalog(
    configuration: &IcebergCatalogConfiguration,
) -> Result<crate::iceberg_catalog_hms::HmsCatalog, String> {
    use crate::iceberg::CatalogBuilder;
    use crate::iceberg_catalog_hms::{
        HMS_CATALOG_PROP_THRIFT_TRANSPORT, HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE,
        HmsCatalogBuilder, THRIFT_TRANSPORT_BUFFERED, THRIFT_TRANSPORT_FRAMED,
    };

    if configuration.kind != IcebergCatalogKind::Hive {
        return Err(format!(
            "build Hive Iceberg catalog called for {:?} configuration",
            configuration.kind
        ));
    }
    let hms_uri = configuration.hms_uris.clone().ok_or_else(|| {
        "hive iceberg catalog entry missing hms_uris (CREATE EXTERNAL CATALOG must set `hive.metastore.uris`)"
            .to_string()
    })?;
    let mut properties = HashMap::new();
    properties.insert(HMS_CATALOG_PROP_URI.to_string(), hms_uri);
    if !configuration.warehouse_uri.is_empty() {
        properties.insert(
            HMS_CATALOG_PROP_WAREHOUSE.to_string(),
            configuration.warehouse_uri.clone(),
        );
    }
    let framed = configuration
        .properties
        .iter()
        .find(|(key, _)| key == "hive.metastore.thrift.framed")
        .is_some_and(|(_, value)| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        });
    properties.insert(
        HMS_CATALOG_PROP_THRIFT_TRANSPORT.to_string(),
        if framed {
            THRIFT_TRANSPORT_FRAMED.to_string()
        } else {
            THRIFT_TRANSPORT_BUFFERED.to_string()
        },
    );
    HmsCatalogBuilder::default()
        .with_storage_factory(storage_factory(configuration))
        .load("hms".to_string(), properties)
        .await
        .map_err(|error| format!("build HMS iceberg catalog: {error}"))
}

/// Construct the concrete catalog client for one provider control generation.
///
/// The returned client remains provider-private.  In particular, the caller
/// supplies both the configuration and the runtime that polls this future;
/// this helper never discovers either from process-global state.
pub async fn build_catalog(
    configuration: &IcebergCatalogConfiguration,
) -> Result<Arc<dyn crate::iceberg::Catalog>, String> {
    match configuration.kind {
        IcebergCatalogKind::Hadoop => Ok(Arc::new(build_hadoop_catalog(configuration)?)),
        IcebergCatalogKind::Rest => Ok(Arc::new(build_rest_catalog(configuration).await?)),
        IcebergCatalogKind::Hive => Ok(Arc::new(build_hms_catalog(configuration).await?)),
    }
}

fn storage_factory(
    configuration: &IcebergCatalogConfiguration,
) -> Arc<dyn crate::iceberg::io::StorageFactory> {
    crate::fs_io::build_storage_factory_for_location(
        &configuration.warehouse_uri,
        configuration.object_store_config.as_ref(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dispatches_hadoop_catalog_from_provider_configuration() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");

        build_catalog(&configuration)
            .await
            .expect("provider catalog");
    }
}
