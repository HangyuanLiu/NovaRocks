// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Delete-only planning on the exact full observation, without any reload.
use super::{keys, retention, view};
use crate::commit::staging::PreparedChange;
use crate::iceberg::TableUpdate;
use crate::iceberg::spec::TableMetadata;
use novarocks_spi::connector::{ConnectorError, KeyedDocumentObservationRequest, KeyedExpiry};

pub(crate) fn prepare(
    request: &KeyedDocumentObservationRequest,
    expiry: &KeyedExpiry,
) -> Result<(TableMetadata, PreparedChange), ConnectorError> {
    expiry.observation.validate_sealed()?;
    let metadata = view::metadata_from_token(&expiry.observation)?;
    let full = view::project_full_view(
        &metadata,
        request,
        expiry.observation.metadata_version().clone(),
    )
    .map_err(super::super::observation::keyed_read_error)?;
    let deletable = retention::deletable_versions(&full, &expiry.remove_snapshots)?;
    let mut updates = Vec::new();
    if !expiry.remove_snapshots.is_empty() {
        updates.push(TableUpdate::RemoveSnapshots {
            snapshot_ids: expiry.remove_snapshots.iter().copied().collect(),
        });
    }
    if !deletable.is_empty() {
        updates.push(TableUpdate::RemoveProperties {
            removals: deletable.into_iter().map(keys::version_key).collect(),
        });
    }
    Ok((
        metadata,
        PreparedChange {
            requirements: vec![],
            updates,
        },
    ))
}
