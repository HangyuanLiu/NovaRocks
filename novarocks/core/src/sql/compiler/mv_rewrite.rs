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

//! Immutable materialized-view rewrite facts frozen by application admission.
//!
//! The compiler uses this value as data only. Repository enumeration and
//! connector/catalog reads happen before construction, in the application
//! facade, so one statement never observes a changing MV definition set.

use std::collections::BTreeMap;

/// One captured base-table identity at statement admission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MvRewriteBaseTableState {
    Resolved {
        snapshot_id: Option<i64>,
        table_uuid: Option<String>,
    },
    Unavailable(String),
}

/// Immutable facts required to assess one persisted MV as a rewrite candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MvRewriteDefinition {
    pub(crate) mv_id: i64,
    pub(crate) select_sql: String,
    pub(crate) base_table_refs: Vec<String>,
    pub(crate) storage_engine: String,
    pub(crate) target_catalog: Option<String>,
    pub(crate) target_namespace: Option<String>,
    pub(crate) target_table: Option<String>,
    pub(crate) last_refresh_snapshots: BTreeMap<String, i64>,
    pub(crate) last_refresh_table_uuids: BTreeMap<String, String>,
    /// Per-base-table reads (including failures) captured while admission
    /// froze this definition. The map is keyed by canonical `cat.ns.tbl`.
    pub(crate) base_table_states: BTreeMap<String, MvRewriteBaseTableState>,
}

/// Repository-order-preserving MV definition snapshot for one compiler request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MvRewriteDefinitionIndex {
    definitions: Vec<MvRewriteDefinition>,
}

impl MvRewriteDefinitionIndex {
    pub(crate) fn new(definitions: Vec<MvRewriteDefinition>) -> Self {
        Self { definitions }
    }

    pub(crate) fn definitions(&self) -> &[MvRewriteDefinition] {
        &self.definitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlx1_mv_rewrite_definition_index_preserves_application_order() {
        let index = MvRewriteDefinitionIndex::new(vec![
            MvRewriteDefinition {
                mv_id: 7,
                select_sql: "select 1".to_string(),
                base_table_refs: Vec::new(),
                storage_engine: "iceberg".to_string(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                last_refresh_snapshots: BTreeMap::new(),
                last_refresh_table_uuids: BTreeMap::new(),
                base_table_states: BTreeMap::new(),
            },
            MvRewriteDefinition {
                mv_id: 3,
                select_sql: "select 2".to_string(),
                base_table_refs: Vec::new(),
                storage_engine: "iceberg".to_string(),
                target_catalog: None,
                target_namespace: None,
                target_table: None,
                last_refresh_snapshots: BTreeMap::new(),
                last_refresh_table_uuids: BTreeMap::new(),
                base_table_states: BTreeMap::new(),
            },
        ]);

        assert_eq!(
            index
                .definitions()
                .iter()
                .map(|definition| definition.mv_id)
                .collect::<Vec<_>>(),
            vec![7, 3]
        );
    }
}
