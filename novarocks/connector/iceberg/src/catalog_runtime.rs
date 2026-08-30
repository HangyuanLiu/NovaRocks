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

use crate::access_binding::IcebergReadBinding;
use crate::catalog_config::{IcebergCatalogConfiguration, IcebergCatalogKind};

/// One concrete catalog client with both its generic control view and the
/// typed REST surface needed for staged table publication. The REST view is an
/// `Arc` clone of the same client allocation, never a second client or a
/// runtime downcast.
pub struct IcebergCatalogClient {
    generic: Arc<dyn crate::iceberg::Catalog>,
    hadoop: Option<Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>>,
    rest: Option<Arc<crate::iceberg_catalog_rest::RestCatalog>>,
}

impl IcebergCatalogClient {
    pub fn generic(&self) -> &Arc<dyn crate::iceberg::Catalog> {
        &self.generic
    }

    pub fn rest(&self) -> Option<&Arc<crate::iceberg_catalog_rest::RestCatalog>> {
        self.rest.as_ref()
    }

    pub fn hadoop(&self) -> Option<&Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>> {
        self.hadoop.as_ref()
    }
}

pub fn build_hadoop_catalog(
    configuration: &IcebergCatalogConfiguration,
    binding: IcebergReadBinding,
) -> Result<crate::hadoop_catalog::HadoopFileSystemCatalog, String> {
    if configuration.kind != IcebergCatalogKind::Hadoop {
        return Err(format!(
            "build Hadoop Iceberg catalog called for {:?} configuration",
            configuration.kind
        ));
    }
    let file_io =
        crate::fs_io::build_file_io_for_location(&configuration.warehouse_uri, binding.clone());
    Ok(
        crate::hadoop_catalog::HadoopFileSystemCatalog::new_with_binding(
            file_io,
            configuration.warehouse_uri.clone(),
            binding,
        ),
    )
}

pub async fn build_rest_catalog(
    configuration: &IcebergCatalogConfiguration,
    binding: IcebergReadBinding,
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
        .with_storage_factory(storage_factory(&configuration.warehouse_uri, binding))
        .load("rest".to_string(), properties)
        .await
        .map_err(|error| format!("build REST iceberg catalog: {error}"))
}

pub async fn build_hms_catalog(
    configuration: &IcebergCatalogConfiguration,
    binding: IcebergReadBinding,
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
        .with_storage_factory(storage_factory(&configuration.warehouse_uri, binding))
        .load("hms".to_string(), properties)
        .await
        .map_err(|error| format!("build HMS iceberg catalog: {error}"))
}

/// Construct the single concrete client retained by one control generation.
pub async fn build_catalog_client(
    configuration: &IcebergCatalogConfiguration,
    binding: IcebergReadBinding,
) -> Result<IcebergCatalogClient, String> {
    match configuration.kind {
        IcebergCatalogKind::Hadoop => {
            let hadoop = Arc::new(build_hadoop_catalog(configuration, binding)?);
            let generic: Arc<dyn crate::iceberg::Catalog> = hadoop.clone();
            Ok(IcebergCatalogClient {
                generic,
                hadoop: Some(hadoop),
                rest: None,
            })
        }
        IcebergCatalogKind::Rest => {
            let rest = Arc::new(build_rest_catalog(configuration, binding).await?);
            let generic: Arc<dyn crate::iceberg::Catalog> = rest.clone();
            Ok(IcebergCatalogClient {
                generic,
                hadoop: None,
                rest: Some(rest),
            })
        }
        IcebergCatalogKind::Hive => Ok(IcebergCatalogClient {
            generic: Arc::new(build_hms_catalog(configuration, binding).await?),
            hadoop: None,
            rest: None,
        }),
    }
}

fn storage_factory(
    warehouse_uri: &str,
    binding: IcebergReadBinding,
) -> Arc<dyn crate::iceberg::io::StorageFactory> {
    crate::fs_io::build_storage_factory_for_location(warehouse_uri, binding)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};

    use super::*;

    fn local_binding() -> IcebergReadBinding {
        let runtime = tokio::runtime::Handle::current();
        IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
    }

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

        let client = build_catalog_client(&configuration, local_binding())
            .await
            .expect("provider catalog");
        assert!(client.rest().is_none());
        assert!(client.hadoop().is_some());
        assert!(Arc::strong_count(client.generic()) >= 1);
    }
}
