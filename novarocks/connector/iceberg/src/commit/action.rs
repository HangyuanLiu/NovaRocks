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

//! Trait abstraction over the three commit-action implementations
//! (FastAppend / Overwrite / RowDelta).
//!
//! `CommitCtx` carries everything an action needs to write manifests and
//! call `Catalog::update_table`. `FileIO` is supplied explicitly rather than
//! lifted from `table.file_io()` so that engine-side staging credentials and
//! catalog-default credentials can differ when needed.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::iceberg::Catalog;
use crate::iceberg::io::FileIO;
use crate::iceberg::table::Table;
use async_trait::async_trait;
use uuid::Uuid;

use super::collector::IcebergCommitCollector;
use crate::commit::CommitOutcome;
use crate::commit::abort::AbortLog;
use crate::commit::{
    MV_PUBLICATION_PROVENANCE_PROP, MV_REFRESH_ROW_COUNT_PROP, MvPublicationProvenanceV2,
};

pub struct CommitCtx<'a> {
    pub collector: &'a IcebergCommitCollector,
    pub table: &'a Table,
    pub catalog: &'a dyn Catalog,
    pub file_io: &'a FileIO,
    pub commit_uuid: Uuid,
    pub abort_handle: Arc<AbortLog>,
    /// Target ref for this commit. `"main"` is the default; non-`main`
    /// values are used for branch-qualified DML (`INSERT INTO t.branch_<x>`).
    pub target_ref: &'a str,
    pub snapshot_properties: &'a BTreeMap<String, String>,
}

pub(super) fn merge_snapshot_summary_properties(
    mut built_in: HashMap<String, String>,
    snapshot_properties: &BTreeMap<String, String>,
) -> Result<HashMap<String, String>, String> {
    let mut provider_properties = snapshot_properties.clone();
    if let Some(raw_provenance) = provider_properties
        .get(MV_PUBLICATION_PROVENANCE_PROP)
        .cloned()
    {
        let total_records = built_in
            .get("total-records")
            .ok_or_else(|| "MV commit summary is missing total-records".to_string())?
            .parse::<i64>()
            .map_err(|error| format!("MV commit summary has invalid total-records: {error}"))?;
        let provenance = MvPublicationProvenanceV2::from_json(&raw_provenance)?;
        let canonical = provenance.with_rows(total_records)?.to_canonical_json()?;
        provider_properties.insert(MV_PUBLICATION_PROVENANCE_PROP.to_string(), canonical);
        provider_properties.insert(
            MV_REFRESH_ROW_COUNT_PROP.to_string(),
            total_records.to_string(),
        );
    }
    for key in provider_properties.keys() {
        if built_in.contains_key(key) {
            return Err(format!(
                "snapshot property {key} conflicts with built-in summary field"
            ));
        }
    }
    built_in.extend(provider_properties);
    Ok(built_in)
}

#[async_trait]
pub trait IcebergCommitAction: Send + Sync {
    /// Stage any manifests required, build a `TableCommit`, and submit it via
    /// `Catalog::update_table`. Implementations must record every staged
    /// manifest path on `ctx.abort_handle` so that a later failure can clean
    /// them up. On the success path the orchestrator does not call
    /// `AbortLog::cleanup`, so the records are harmless.
    async fn commit(&self, ctx: CommitCtx<'_>) -> Result<CommitOutcome, String>;
}
