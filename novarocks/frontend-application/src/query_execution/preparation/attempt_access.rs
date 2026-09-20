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

//! Process-local Connector access retained by one logical execution.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::query_execution::artifact::FragmentId;
use novarocks_spi::connector::{
    CatalogProperties, ConnectorControlPlanningLease, ConnectorReadAttemptAccess,
};

/// The single owner of everything that may open the immutable scans across
/// replacement attempts of one logical execution. It contains no attempt
/// identity, request-scoped credentials, or open split source; those are
/// acquired separately for each attempt.
pub(crate) struct LogicalExecutionAccessScope {
    entries: BTreeMap<(FragmentId, i32), Arc<ConnectorAttemptAccessEntry>>,
}

#[cfg(test)]
impl LogicalExecutionAccessScope {
    pub(super) fn get(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&ConnectorAttemptAccessEntry> {
        self.entries.get(&(fragment_id, node_id)).map(Arc::as_ref)
    }
}

pub(crate) type ConnectorAttemptAccessPlan = LogicalExecutionAccessScope;

pub(crate) struct ConnectorAttemptAccessEntry {
    catalog_properties: CatalogProperties,
    planning_lease: ConnectorControlPlanningLease,
    access: ConnectorReadAttemptAccess,
}

/// The per-attempt access for one completed plan, keyed the way its wire form
/// addresses each scan.
///
/// Nothing is re-derived here. Every scan of the plan already has exactly one
/// frozen read - the pairing the plan was published with says so - and the
/// physical fragment and node identities are the ones the wire carries, so the
/// key is a translation rather than a lookup that could miss.
pub(crate) type FrozenReadCapability = (
    novarocks_sql::binding::SqlTableBindingId,
    ConnectorReadAttemptAccess,
    ConnectorControlPlanningLease,
    CatalogProperties,
);

pub(crate) fn attempt_access_for_completed_plan(
    plan: &novarocks_physical_plan::PhysicalPlan,
    mut reads: BTreeMap<novarocks_physical_plan::ProviderReadOccurrenceId, FrozenReadCapability>,
) -> Result<ConnectorAttemptAccessPlan, String> {
    // A capability cannot be copied, so each is taken out as its scan claims
    // it. Two scans claiming one occurrence would leave the second with
    // nothing, which is what the absent entry below reports.
    let mut entries = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let novarocks_physical_plan::NodeKind::Scan { occurrence, .. } = &node.kind else {
                continue;
            };
            let (_, access, generation, catalog) = reads.remove(occurrence).ok_or_else(|| {
                format!(
                    "completed plan scans provider read occurrence {} with no frozen read",
                    occurrence.get()
                )
            })?;
            let node_id = i32::try_from(node.id.get()).map_err(|_| {
                format!("scan node {} exceeds the wire node identity", node.id.get())
            })?;
            let fragment_id = FragmentId::from(fragment.id().get());
            if entries
                .insert(
                    (fragment_id, node_id),
                    Arc::new(ConnectorAttemptAccessEntry {
                        catalog_properties: catalog,
                        planning_lease: generation,
                        access,
                    }),
                )
                .is_some()
            {
                return Err(format!(
                    "duplicate connector attempt access fragment_id={fragment_id} node_id={node_id}"
                ));
            }
        }
    }
    Ok(LogicalExecutionAccessScope { entries })
}

impl LogicalExecutionAccessScope {
    pub(crate) fn share(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<Arc<ConnectorAttemptAccessEntry>> {
        self.entries.get(&(fragment_id, node_id)).cloned()
    }

    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = (FragmentId, i32, &ConnectorAttemptAccessEntry)> {
        self.entries
            .iter()
            .map(|(&(fragment_id, node_id), entry)| (fragment_id, node_id, entry.as_ref()))
    }
}

impl ConnectorAttemptAccessEntry {
    pub(crate) const fn access(&self) -> &ConnectorReadAttemptAccess {
        &self.access
    }

    pub(crate) const fn catalog_properties(&self) -> &CatalogProperties {
        &self.catalog_properties
    }

    pub(crate) const fn planning_lease(&self) -> &ConnectorControlPlanningLease {
        &self.planning_lease
    }
}
