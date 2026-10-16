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

//! Complete catalog metadata for reclamation, without storage materialization.

use crate::iceberg::spec::TableMetadata;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use super::CatalogCommitBase;
pub(crate) use crate::rest_protocol::endpoints::CatalogMetadataValidator;

/// NotModified is useful only with the caller's retained complete baseline.
/// Neither result supplies or renews storage credentials.
#[derive(Debug)]
pub(crate) enum CatalogMetadataLoad {
    Complete {
        base: CatalogCommitBase,
        validator: Option<CatalogMetadataValidator>,
    },
    NotModified,
}

pub(crate) fn complete_metadata(
    metadata: TableMetadata,
    location: Option<&str>,
    validator: Option<CatalogMetadataValidator>,
) -> Result<CatalogMetadataLoad, ConnectorError> {
    let location = location
        .filter(|location| !location.is_empty())
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Complete catalog metadata has no exact metadata location",
            )
        })?;
    Ok(CatalogMetadataLoad::Complete {
        base: CatalogCommitBase {
            metadata,
            metadata_location: location.to_owned(),
        },
        validator,
    })
}
