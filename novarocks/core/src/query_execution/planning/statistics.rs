#![allow(dead_code)]
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

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorControlResolver, StatisticsMetric, StatisticsMetricRequest,
};

use crate::connector::unified_statistics::{
    ResolvedStatisticsTable, StatisticsResolutionFailure, UnifiedStatisticsResolver,
};
use crate::query_execution::kernels::{
    DmlExecutionKernel, MvExecutionKernel, QueryPreparationKernel, StatisticsExecutionKernel,
};
use crate::query_execution::planning::bindings::{
    QueryScanMaterialization, QueryTableBinding, QueryTableBindingAdmission,
    QueryTableBindingStore, parse_time_travel_overlay_identity,
};
use crate::query_execution::planning::catalog_materializer::{
    QueryTableBindingLoader, connector_query_binding_from_materialization,
    load_connector_table_alias_materialization_with_lease,
    load_connector_table_materialization_with_lease,
};
use novarocks_sql::planning::catalog::materialization_statistics_facts;
use novarocks_sql::planning::dml::{
    DmlStatisticsEvidence, DmlStatisticsFailure, DmlStatisticsSnapshot,
};

#[derive(Clone, Default)]
/// Query-scoped handles for the one unified statistics resolver.  This is not
/// a provider registry: absent pins intentionally produce missing statistics
/// rather than a second latest-resolution path.
pub struct QueryStatisticsContext {
    snapshot: DmlStatisticsSnapshot,
}

impl QueryStatisticsContext {
    pub(crate) fn none() -> Self {
        Self::default()
    }

    pub(crate) fn unavailable() -> Self {
        Self::none()
    }

    pub(crate) fn from_statistics_resolver_with_bindings(
        resolver: &impl QueryStatisticsResolver,
        bindings: Arc<QueryTableBindingStore>,
    ) -> Self {
        Self {
            snapshot: DmlStatisticsSnapshot::from_evidence(project_statistics_evidence(
                resolver.unified_statistics(),
                &bindings,
            )),
        }
    }

    pub(crate) fn snapshot(&self) -> &DmlStatisticsSnapshot {
        &self.snapshot
    }
}

impl std::ops::Deref for QueryStatisticsContext {
    type Target = DmlStatisticsSnapshot;

    fn deref(&self) -> &Self::Target {
        self.snapshot()
    }
}

/// Query planning needs only frozen statistics evidence.  This trait avoids
/// taking the full application state while preserving the no-latest-lookup
/// rule in `QueryStatisticsContext`.
pub(crate) trait QueryStatisticsResolver {
    fn unified_statistics(&self) -> &UnifiedStatisticsResolver;
    fn unified_statistics_arc(&self) -> &Arc<UnifiedStatisticsResolver>;
}

macro_rules! impl_kernel_statistics_resolver {
    ($kernel:ty) => {
        impl QueryStatisticsResolver for $kernel {
            fn unified_statistics(&self) -> &UnifiedStatisticsResolver {
                self.unified_statistics().as_ref()
            }

            fn unified_statistics_arc(&self) -> &Arc<UnifiedStatisticsResolver> {
                self.unified_statistics()
            }
        }
    };
}

impl_kernel_statistics_resolver!(QueryPreparationKernel);
impl_kernel_statistics_resolver!(DmlExecutionKernel);
impl_kernel_statistics_resolver!(MvExecutionKernel);
impl_kernel_statistics_resolver!(StatisticsExecutionKernel);

/// Project every admission-frozen connector observation into SQL values before
/// optimization begins.  This is the one application boundary that may touch
/// a lease, a table handle, or a connector capability; `QueryStatisticsContext`
/// subsequently serves only the immutable snapshot below.
fn project_statistics_evidence(
    resolver: &UnifiedStatisticsResolver,
    bindings: &QueryTableBindingStore,
) -> Vec<DmlStatisticsEvidence> {
    let mut evidence = Vec::new();
    for (binding_id, binding) in bindings.captured_bindings() {
        let facts = materialization_statistics_facts(&binding.resolved);
        evidence.push(project_binding_statistics(
            resolver, binding_id, &facts, &binding,
        ));
    }
    evidence
}

fn project_binding_statistics(
    resolver: &UnifiedStatisticsResolver,
    binding_id: novarocks_sql::binding::SqlTableBindingId,
    facts: &novarocks_sql::planning::catalog::SqlCatalogStatisticsFacts,
    binding: &QueryTableBinding,
) -> DmlStatisticsEvidence {
    let label = facts.label();
    let Some(pin) = binding.statistics_pin.as_ref() else {
        return DmlStatisticsEvidence::Missing {
            binding: binding_id,
            label: label.to_string(),
            reason: "resolved table does not expose connector statistics".to_string(),
        };
    };
    let planning_lease = match binding.admission.exact_planning_lease() {
        Ok(lease) => lease,
        Err(_) => {
            return fatal_statistics_evidence(
                binding_id,
                label,
                DmlStatisticsFailure::BindingMissing,
            );
        }
    };
    let control_binding = planning_lease.binding();
    if control_binding.descriptor().instance_id != *pin.table.owner() {
        return fatal_statistics_evidence(binding_id, label, DmlStatisticsFailure::OwnerMismatch);
    }
    let Some(statistics) = control_binding.statistics() else {
        return DmlStatisticsEvidence::Missing {
            binding: binding_id,
            label: label.to_string(),
            reason: "resolved connector generation does not expose statistics".to_string(),
        };
    };
    let metrics = match metric_request(facts.columns()) {
        Ok(metrics) => metrics,
        Err(error) => {
            return fatal_statistics_evidence(
                binding_id,
                label,
                DmlStatisticsFailure::CorruptEvidence(format!("build metric request: {error}")),
            );
        }
    };
    let context = match crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    ) {
        Ok(context) => context,
        Err(error) => {
            return fatal_statistics_evidence(
                binding_id,
                label,
                DmlStatisticsFailure::CorruptEvidence(format!("build statistics context: {error}")),
            );
        }
    };
    let evidence = match resolver.resolve(
        &ResolvedStatisticsTable {
            table: pin.table.clone(),
            data_version: pin.data_version.clone(),
            incarnation: control_binding.incarnation(),
        },
        statistics.as_ref(),
        metrics,
        context,
    ) {
        Ok(evidence) => evidence,
        // A provider that cannot supply evidence remains the normal
        // conservative path.  Only a fact that contradicts the retained
        // binding is fatal to compilation.
        Err(StatisticsResolutionFailure::Connector(error)) => {
            return DmlStatisticsEvidence::Missing {
                binding: binding_id,
                label: label.to_string(),
                reason: error.to_string(),
            };
        }
        Err(error) => {
            return fatal_statistics_evidence(binding_id, label, map_resolution_failure(error));
        }
    };
    DmlStatisticsEvidence::Available {
        binding: binding_id,
        label: label.to_string(),
        columns: facts.columns().to_vec(),
        optimizer_usable: UnifiedStatisticsResolver::optimizer_usable(&evidence),
        evidence: (*evidence).clone(),
    }
}

fn fatal_statistics_evidence(
    binding: novarocks_sql::binding::SqlTableBindingId,
    label: &str,
    failure: DmlStatisticsFailure,
) -> DmlStatisticsEvidence {
    DmlStatisticsEvidence::Fatal {
        binding,
        label: label.to_string(),
        failure,
    }
}

fn map_resolution_failure(error: StatisticsResolutionFailure) -> DmlStatisticsFailure {
    match error {
        StatisticsResolutionFailure::OwnerMismatch => DmlStatisticsFailure::OwnerMismatch,
        StatisticsResolutionFailure::IncarnationMismatch => {
            DmlStatisticsFailure::IncarnationMismatch
        }
        StatisticsResolutionFailure::DataVersionMismatch => {
            DmlStatisticsFailure::DataVersionMismatch
        }
        StatisticsResolutionFailure::CorruptEvidence(message) => {
            DmlStatisticsFailure::CorruptEvidence(message)
        }
        StatisticsResolutionFailure::Connector(error) => DmlStatisticsFailure::CorruptEvidence(
            format!("unexpected connector error after conservative mapping: {error}"),
        ),
    }
}

/// Application adapter for the SQL catalog's provider-neutral materialization
/// seam.  The resulting binding carries the exact planning lease acquired for
/// metadata; SQL itself never names the Iceberg provider.
pub(crate) fn iceberg_table_binding_loader<'a>(
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
) -> Box<dyn QueryTableBindingLoader + 'a> {
    Box::new(IcebergTableBindingLoader {
        controls,
        connector_context,
    })
}

struct IcebergTableBindingLoader<'a> {
    controls: &'a dyn ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl QueryTableBindingLoader for IcebergTableBindingLoader<'_> {
    fn load_strict_base_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        binding_id: novarocks_sql::binding::SqlTableBindingId,
    ) -> Result<QueryTableBinding, String> {
        let (base_table, snapshot_id) = parse_time_travel_overlay_identity(table)
            .map(|(base_table, snapshot_id)| (base_table, Some(snapshot_id)))
            .unwrap_or((table, None));
        let mut materialization = load_connector_table_materialization_with_lease(
            self.controls,
            self.connector_context.clone(),
            catalog,
            namespace,
            base_table,
        )?;
        if let Some(snapshot_id) = snapshot_id {
            materialization.read_selector =
                novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot_id);
        }
        connector_query_binding_from_materialization(
            materialization,
            catalog,
            namespace,
            table,
            binding_id,
        )
    }

    fn load_metadata_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
        metadata_table_type: novarocks_sql::planning::catalog::MetadataTableKind,
        binding_id: novarocks_sql::binding::SqlTableBindingId,
    ) -> Result<QueryTableBinding, String> {
        let alias = format!(
            "{table}${}",
            metadata_table_alias_suffix(metadata_table_type)
        );
        let materialization = load_connector_table_alias_materialization_with_lease(
            self.controls,
            self.connector_context.clone(),
            catalog,
            namespace,
            &alias,
        )?;
        Ok(QueryTableBinding {
            resolved: novarocks_sql::planning::catalog::resolved_metadata_table(
                catalog,
                namespace,
                table,
                metadata_table_type,
                materialization.columns,
                materialization.row_lineage_metadata_columns,
                binding_id,
            ),
            statistics_pin: materialization.statistics_pin.clone(),
            admission: QueryTableBindingAdmission::Exact(materialization.planning_lease.clone()),
            scan_materialization: Some(QueryScanMaterialization {
                table: materialization.read_table,
                schema: materialization.read_schema,
                selector: materialization.read_selector,
                statistics_pin: materialization.statistics_pin,
                planning_lease: materialization.planning_lease,
            }),
            mv_target_read: None,
            write_target_admission: None,
            frozen_snapshot_materializations: std::collections::BTreeMap::new(),
            admitted_change_scans: std::collections::BTreeMap::new(),
        })
    }
}

fn metadata_table_alias_suffix(
    kind: novarocks_sql::planning::catalog::MetadataTableKind,
) -> &'static str {
    use novarocks_sql::planning::catalog::MetadataTableKind;

    match kind {
        MetadataTableKind::Snapshots => "SNAPSHOTS",
        MetadataTableKind::History => "HISTORY",
        MetadataTableKind::Refs => "REFS",
        MetadataTableKind::Files => "FILES",
        MetadataTableKind::Manifests => "MANIFESTS",
        MetadataTableKind::Partitions => "PARTITIONS",
        MetadataTableKind::LogicalIcebergMetadata => "LOGICAL_ICEBERG_METADATA",
    }
}

fn metric_request(
    columns: &[novarocks_catalog::schema::ColumnDef],
) -> Result<StatisticsMetricRequest, novarocks_spi::connector::ConnectorError> {
    let mut metrics = Vec::with_capacity(1 + columns.len() * 5);
    metrics.push(StatisticsMetric::RowCount);
    for column in columns {
        let column = Arc::<str>::from(column.name.as_str());
        metrics.extend([
            StatisticsMetric::NullCount {
                column: Arc::clone(&column),
            },
            StatisticsMetric::Minimum {
                column: Arc::clone(&column),
            },
            StatisticsMetric::Maximum {
                column: Arc::clone(&column),
            },
            StatisticsMetric::AverageSize {
                column: Arc::clone(&column),
            },
            StatisticsMetric::ThetaNdv { column },
        ]);
    }
    StatisticsMetricRequest::try_new(metrics)
}

#[cfg(test)]
mod unified_tests {
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;
    use novarocks_spi::connector::StatisticsMetric;

    use super::*;

    fn column(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    #[test]
    fn request_uses_stable_column_metric_names() {
        let request = metric_request(&[column("k")]).unwrap();
        assert_eq!(request.metrics().len(), 6);
        assert!(request.metrics().contains(&StatisticsMetric::ThetaNdv {
            column: Arc::from("k"),
        }));
    }

    #[test]
    fn sqlx1_resolution_time_travel_overlay_identity_is_canonical() {
        assert_eq!(
            parse_time_travel_overlay_identity("__sqlx1_tt_orders_42"),
            Some(("orders", 42))
        );
        assert_eq!(
            parse_time_travel_overlay_identity("__sqlx1_tt_sales_orders_-7"),
            Some(("sales_orders", -7))
        );
        assert_eq!(parse_time_travel_overlay_identity("orders"), None);
        assert_eq!(parse_time_travel_overlay_identity("__sqlx1_tt__bad"), None);
    }
}
