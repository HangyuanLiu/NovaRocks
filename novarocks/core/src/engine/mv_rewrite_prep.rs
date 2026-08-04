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

//! Application-side freezing for optional materialized-view rewrite.
//!
//! Repository enumeration and connector metadata reads belong here. The SQL
//! compiler receives the resulting immutable definition index and owns all
//! candidate parse/analyze/statistics/selection work.

use std::sync::Arc;

use crate::sql::compiler::mv_rewrite::{
    MvRewriteBaseTableState, MvRewriteDefinition, MvRewriteDefinitionIndex,
};

use super::StandaloneState;

/// Freeze repository order and every freshness observation once per request.
/// The compiler never observes repository or connector changes while it is
/// deciding optional rewrite candidates.
pub(crate) fn freeze_mv_rewrite_definition_index(
    state: &Arc<StandaloneState>,
) -> Result<MvRewriteDefinitionIndex, String> {
    let definitions = state
        .mv_repository
        .list_definitions()
        .map_err(|error| format!("list mv definitions: {error}"))?;

    Ok(MvRewriteDefinitionIndex::new(
        definitions
            .into_iter()
            .map(|definition| freeze_mv_rewrite_definition(state, definition))
            .collect(),
    ))
}

fn freeze_mv_rewrite_definition(
    state: &Arc<StandaloneState>,
    definition: crate::mv::persistence::definition::StoredMvDefinition,
) -> MvRewriteDefinition {
    let mut base_table_states = std::collections::BTreeMap::new();
    if definition.storage_engine == "iceberg" {
        for fqn in &definition.base_table_refs {
            let state = freeze_base_table_state(state, fqn)
                .unwrap_or_else(MvRewriteBaseTableState::Unavailable);
            base_table_states.insert(fqn.clone(), state);
        }
    }

    MvRewriteDefinition {
        mv_id: definition.mv_id,
        select_sql: definition.select_sql,
        base_table_refs: definition.base_table_refs,
        storage_engine: definition.storage_engine,
        target_catalog: definition.target_catalog,
        target_namespace: definition.target_namespace,
        target_table: definition.target_table,
        last_refresh_snapshots: definition.last_refresh_snapshots,
        last_refresh_table_uuids: definition.last_refresh_table_uuids,
        base_table_states,
    }
}

fn freeze_base_table_state(
    state: &Arc<StandaloneState>,
    fqn: &str,
) -> Result<MvRewriteBaseTableState, String> {
    let table_ref = crate::engine::mv::refresh_io::parse_iceberg_table_refs(&[fqn.to_string()])?
        .into_iter()
        .next()
        .expect("one table reference produces one parsed identity");
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .expect("iceberg catalogs read lock");
        registry.get(&table_ref.catalog)?
    };
    let loaded = crate::connector::iceberg::catalog::load_table(
        &entry,
        &table_ref.namespace,
        &table_ref.table,
    )?;
    Ok(MvRewriteBaseTableState::Resolved {
        snapshot_id: loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id()),
        table_uuid: Some(loaded.table.metadata().uuid().to_string()),
    })
}
