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

//! Dictionary maintenance hooks for write/drop flows.
//!
//! `mark_table_stale` is invoked after a successful INSERT / UPDATE / MERGE /
//! TRUNCATE / DELETE (or any other data-mutating operation that may invalidate
//! previously-built dictionaries) and flips every active dictionary for the
//! given owner into the STALE state. Subsequent queries observe the dictionary
//! as missing through [`DictionaryManager::load_active_snapshot`].
//!
//! `drop_table_dictionaries` is invoked from DROP TABLE / DROP DATABASE and
//! removes the lookup entries while marking the snapshot DROPPED.
//!
//! Both helpers are no-ops when the standalone state has no metadata provider
//! configured (test/embedding modes), so callers may invoke them
//! unconditionally.

use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::engine::backend_resolver::TargetBackend;
use crate::engine::dictionary::model::DictionaryOwner;
use crate::sql::planner::table::ScanSource;

/// Enumerate dictionary owners for every table in `namespace_target`. Used by
/// DROP DATABASE to invalidate dictionaries for all child tables before the
/// namespace itself is removed. Best-effort: missing or partially-registered
/// tables are silently skipped (the DROP path is already best-effort).
pub(crate) fn collect_namespace_owners(
    state: &Arc<StandaloneState>,
    namespace_target: &TargetBackend,
) -> Vec<DictionaryOwner> {
    let mut owners = Vec::new();
    match namespace_target.backend_name {
        "starrocks" => {
            let starrocks = state
                .starrocks_table
                .read()
                .expect("standalone StarRocks table read lock");
            let Ok(tables) = starrocks.list_tables_in_database(&namespace_target.namespace) else {
                return owners;
            };
            for table_name in tables {
                if let Ok(runtime) = starrocks.table(&namespace_target.namespace, &table_name) {
                    owners.push(DictionaryOwner::StarRocksTable {
                        database: runtime.database_name.clone(),
                        table: runtime.table.name.clone(),
                        db_id: runtime.table.db_id,
                        table_id: runtime.table.table_id,
                    });
                }
            }
        }
        "iceberg" => {
            // The standalone catalog mirrors registered iceberg tables under
            // the namespace; iterate everything that resolves there.
            let logical = state.catalog.read().expect("standalone catalog read lock");
            for table_name in logical.table_names_in_database(&namespace_target.namespace) {
                if let Ok(table_def) = logical.get(&namespace_target.namespace, &table_name)
                    && let ScanSource::IcebergDataFiles { table: info, .. } = &table_def.source
                {
                    owners.push(DictionaryOwner::IcebergTable {
                        catalog: info.catalog.clone(),
                        namespace: info.namespace.clone(),
                        table: info.table.clone(),
                        table_uuid: info.table_uuid.clone(),
                    });
                }
            }
        }
        _ => {}
    }
    owners
}

pub(crate) fn mark_table_stale(
    state: &StandaloneState,
    owner: &DictionaryOwner,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("mark dictionary stale")
        .map_err(|e| format!("open dictionary stale txn failed: {e}"))?;
    state
        .dictionary_manager
        .repo()
        .mark_owner_stale(txn.as_mut(), owner.kind(), &owner.stable_key())
        .map_err(|e| format!("mark dictionary stale failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit dictionary stale failed: {e}"))?;
    Ok(())
}

/// Convenience: derive the [`DictionaryOwner`] for a resolved write target
/// and invoke [`mark_table_stale`]. Returns `Ok(())` when the target backend
/// is unrelated to dictionary state (e.g. external local catalogs).
pub(crate) fn mark_target_stale(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
) -> Result<(), String> {
    if state.metadata_provider.is_none() {
        return Ok(());
    }
    let Some(owner) = resolve_owner_from_target(state, target)? else {
        return Ok(());
    };
    mark_table_stale(state, &owner)
}

/// Mark every active dictionary for a StarRocks table identified by
/// `(database, table)` stale. Used by the StarRocks txn post-commit hook so
/// every successful INSERT / UPDATE / DELETE invalidates dictionaries built
/// against the previous visible version. Returns silently when the table is
/// no longer registered (e.g. concurrent drop) — the DROP path covers
/// dictionary teardown.
pub(crate) fn mark_starrocks_table_stale(
    state: &Arc<StandaloneState>,
    database: &str,
    table: &str,
) -> Result<(), String> {
    if state.metadata_provider.is_none() {
        return Ok(());
    }
    let owner = {
        let starrocks = state
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        match starrocks.table(database, table) {
            Ok(runtime) => DictionaryOwner::StarRocksTable {
                database: runtime.database_name.clone(),
                table: runtime.table.name.clone(),
                db_id: runtime.table.db_id,
                table_id: runtime.table.table_id,
            },
            Err(_) => return Ok(()),
        }
    };
    mark_table_stale(state, &owner)
}

/// Best-effort lookup of the dictionary owner for a write target. Returns
/// `None` when the target is unrelated to dictionary state (e.g. local-only
/// catalog tables) or when the underlying catalog entry has already been
/// removed. This intentionally swallows resolution errors because callers
/// (DROP TABLE in particular) need to capture the owner *before* destroying
/// the catalog entry — failures here should fall through to a best-effort
/// teardown without aborting the drop.
pub(crate) fn resolve_target_owner(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
) -> Option<DictionaryOwner> {
    resolve_owner_from_target(state, target).ok().flatten()
}

fn resolve_owner_from_target(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
) -> Result<Option<DictionaryOwner>, String> {
    match target.backend_name {
        "starrocks" => {
            let starrocks = state
                .starrocks_table
                .read()
                .expect("standalone StarRocks table read lock");
            match starrocks.table(&target.namespace, &target.table) {
                Ok(runtime) => Ok(Some(DictionaryOwner::StarRocksTable {
                    database: runtime.database_name.clone(),
                    table: runtime.table.name.clone(),
                    db_id: runtime.table.db_id,
                    table_id: runtime.table.table_id,
                })),
                Err(_) => Ok(None),
            }
        }
        "iceberg" => {
            // Try to read the iceberg table info from the standalone catalog
            // first; query-prep registers tables there with full
            // IcebergTableInfo. If not registered yet, fall back to building
            // an owner with no UUID — invalidation/drop matches by the
            // stable key, so a UUID-less drop still removes everything that
            // was upserted without a UUID. ANALYZE FULL is the only path
            // that upserts, and it always registers the table first.
            let logical = state.catalog.read().expect("standalone catalog read lock");
            if let Ok(table_def) = logical.get(&target.namespace, &target.table)
                && let ScanSource::IcebergDataFiles { table: info, .. } = &table_def.source
            {
                return Ok(Some(DictionaryOwner::IcebergTable {
                    catalog: info.catalog.clone(),
                    namespace: info.namespace.clone(),
                    table: info.table.clone(),
                    table_uuid: info.table_uuid.clone(),
                }));
            }
            Ok(Some(DictionaryOwner::IcebergTable {
                catalog: target.catalog.clone(),
                namespace: target.namespace.clone(),
                table: target.table.clone(),
                table_uuid: None,
            }))
        }
        _ => Ok(None),
    }
}

pub(crate) fn drop_table_dictionaries(
    state: &StandaloneState,
    owner: &DictionaryOwner,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("drop dictionary entries")
        .map_err(|e| format!("open dictionary drop txn failed: {e}"))?;
    state
        .dictionary_manager
        .repo()
        .drop_owner(txn.as_mut(), owner.kind(), &owner.stable_key())
        .map_err(|e| format!("drop dictionary entries failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit dictionary drop failed: {e}"))?;
    Ok(())
}
