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

//! Engine dispatch for `ALTER TABLE … (CREATE|DROP) BRANCH|TAG`.
//!
//! Bridges parser AST → connector mutation DTO. The provider owns authoritative
//! ref/snapshot validation and the external catalog commit.

use std::sync::Arc;

use crate::engine::{StandaloneState, StatementResult};
use crate::sql::parser::ast::{
    AlterIcebergRefAction, AlterIcebergRefStmt, ObjectName, SnapshotAnchor,
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorInstanceId, ConnectorRefAction, ConnectorRefKind,
    ConnectorTableIdentity, CreateOrReplacePolicy, DropPolicy,
};

pub(crate) fn execute(
    state: &Arc<StandaloneState>,
    _current_database: &str,
    stmt: &AlterIcebergRefStmt,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    crate::connector::validate_request_context(connector_context)?;
    // 1. Resolve qualified name — must be 3-part (catalog.namespace.table).
    let (catalog_name, namespace, table_name) = resolve_table_parts(&stmt.table)?;

    // 2. Load iceberg catalog entry.
    let registry = state
        .iceberg_catalogs
        .read()
        .expect("iceberg catalogs read");
    let entry = registry.get(&catalog_name)?;

    // Read metadata only for the application-owned MV guard. Ref identity,
    // snapshot anchor, and compare-and-set validation belong to the provider.
    let loaded =
        crate::connector::iceberg::catalog::registry::load_table(&entry, &namespace, &table_name)?;
    crate::connector::validate_request_context(connector_context)?;
    let target = crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: catalog_name.clone(),
        namespace: namespace.clone(),
        table: table_name.clone(),
    };
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_properties(
        &target,
        loaded.table.metadata().properties(),
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::AlterTable,
    )?;
    let instance_id =
        ConnectorInstanceId::parse(&catalog_name).map_err(|error| error.to_string())?;
    crate::connector::mutation::execute_catalog_mutation(
        state.connector_control.as_ref(),
        &instance_id,
        ConnectorCatalogMutationOperation::AlterRef {
            table: ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from(namespace.as_str()),
                table: Arc::from(table_name.as_str()),
            },
            action: connector_ref_action(&stmt.action)?,
        },
        connector_context.clone(),
    )?;
    entry.invalidate_table_cache(&namespace, &table_name);

    Ok(StatementResult::Ok)
}

fn connector_ref_action(action: &AlterIcebergRefAction) -> Result<ConnectorRefAction, String> {
    let policy = |replace: bool, if_not_exists: bool| {
        if replace {
            CreateOrReplacePolicy::ReplaceIfExists
        } else if if_not_exists {
            CreateOrReplacePolicy::NoOpIfExists
        } else {
            CreateOrReplacePolicy::FailIfExists
        }
    };
    let snapshot_anchor = |anchor: &SnapshotAnchor| match anchor {
        SnapshotAnchor::SnapshotId(snapshot_id) => Some(*snapshot_id),
        SnapshotAnchor::CurrentMain => None,
    };
    Ok(match action {
        AlterIcebergRefAction::CreateBranch {
            name,
            anchor,
            if_not_exists,
            replace,
            ..
        } => ConnectorRefAction::Create {
            kind: ConnectorRefKind::Branch,
            name: Arc::from(name.as_str()),
            snapshot_id: snapshot_anchor(anchor),
            policy: policy(*replace, *if_not_exists),
        },
        AlterIcebergRefAction::CreateTag {
            name,
            anchor,
            if_not_exists,
            replace,
            ..
        } => ConnectorRefAction::Create {
            kind: ConnectorRefKind::Tag,
            name: Arc::from(name.as_str()),
            snapshot_id: snapshot_anchor(anchor),
            policy: policy(*replace, *if_not_exists),
        },
        AlterIcebergRefAction::DropBranch { name, if_exists } => ConnectorRefAction::Drop {
            kind: ConnectorRefKind::Branch,
            name: Arc::from(name.as_str()),
            policy: if *if_exists {
                DropPolicy::NoOpIfMissing
            } else {
                DropPolicy::FailIfMissing
            },
        },
        AlterIcebergRefAction::DropTag { name, if_exists } => ConnectorRefAction::Drop {
            kind: ConnectorRefKind::Tag,
            name: Arc::from(name.as_str()),
            policy: if *if_exists {
                DropPolicy::NoOpIfMissing
            } else {
                DropPolicy::FailIfMissing
            },
        },
    })
}

fn resolve_table_parts(name: &ObjectName) -> Result<(String, String, String), String> {
    let parts = &name.parts;
    match parts.len() {
        3 => Ok((parts[0].clone(), parts[1].clone(), parts[2].clone())),
        2 => Err(format!(
            "iceberg ref: qualify table with catalog (got '{}.{}')",
            parts[0], parts[1]
        )),
        1 => Err(format!(
            "iceberg ref: qualify table with catalog and namespace (got '{}')",
            parts[0]
        )),
        _ => Err(format!(
            "iceberg ref: invalid table name (parts: {})",
            parts.len()
        )),
    }
}
