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

//! Concrete execution context for Iceberg MV refresh.
//!
//! This adapter owns catalog/table handles, affected-partition state, pruning
//! limits, and scan/file binding. Immutable rewrite metadata is held by the
//! canonical `mv::rewrite::context::IcebergMvRewriteContext`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use iceberg::spec::Schema;

use crate::catalog::identifier::TableIdentity;
use crate::connector::iceberg::catalog::registry::{IcebergCatalogEntry, IcebergCatalogRegistry};
use crate::connector::iceberg::scan_model::{
    IcebergDataFileInfo, IcebergPartitionFieldValue, IcebergPartitionValue, IcebergSchemaDef,
    IcebergSchemaFieldDef, IcebergTableInfo,
};
use crate::mv::persistence::definition::StoredMvDefinition;
use crate::mv::persistence::schema as mv_schema;
use crate::mv::refresh::pin::RefreshSnapshotPin;
use crate::mv::rewrite::context::IcebergMvRewriteContext;
use crate::sql::planner::table::{
    IcebergMvTargetLocatorScan, IcebergMvTargetStateScan, ScanSource,
};
use mv_schema::MvSchemaContract;

use super::iceberg_refresh::IcebergMvTarget;

/// Refresh-time context. Wraps `IcebergMvRewriteContext` and adds execution
/// handles only the refresh path needs.
pub(crate) struct IcebergMvRefreshContext {
    pub rewrite: Arc<IcebergMvRewriteContext>,
    /// Engine application DTO retained by this execution adapter until EBD-16
    /// moves the create/refresh application flow onto canonical identities.
    /// SQL and rewrite consumers must use `rewrite.target` instead.
    pub(super) application_target: IcebergMvTarget,
    pub target_entry: Arc<IcebergCatalogEntry>,
    pub base_catalog_entries: BTreeMap<String, IcebergCatalogEntry>,
    pub iceberg_catalog: Arc<dyn iceberg::Catalog>,
    pub target_table: iceberg::table::Table,
    pub affected_partitions: crate::mv::model::AffectedTargetPartitions,
    pub pruning_limits: MvRefreshPruningLimits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MvRefreshPruningLimits {
    pub max_touched_groups: usize,
    pub max_affected_partitions: usize,
}

impl Default for MvRefreshPruningLimits {
    fn default() -> Self {
        Self {
            max_touched_groups: 100_000,
            max_affected_partitions: 4_096,
        }
    }
}

impl MvRefreshPruningLimits {
    pub(crate) fn from_standalone_config(
        config: &crate::common::app_config::StandaloneServerConfig,
    ) -> Self {
        Self {
            max_touched_groups: config.mv_refresh_max_touched_groups,
            max_affected_partitions: config.mv_refresh_max_affected_partitions,
        }
    }

    pub(crate) fn affected_partition_count_exceeds_limit(&self, partition_count: usize) -> bool {
        partition_count > self.max_affected_partitions
    }

    pub(crate) fn touched_group_count_exceeds_limit(&self, touched_group_count: usize) -> bool {
        touched_group_count > self.max_touched_groups
    }
}

fn rewrite_target_identity(target: &IcebergMvTarget) -> TableIdentity {
    TableIdentity {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
    }
}

impl IcebergMvRefreshContext {
    /// Build the full refresh context from raw inputs. Extracts target
    /// snapshot id / uuid / schema from `target_table.metadata()` and forwards
    /// the rest to `IcebergMvRewriteContext::from_parts`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<&str>,
        current_database: &str,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[TableIdentity]>,
        pin: Arc<RefreshSnapshotPin>,
        iceberg_catalogs: &IcebergCatalogRegistry,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
    ) -> Result<Self, String> {
        Self::new_with_pruning_limits(
            target,
            mv_id,
            current_catalog,
            current_database,
            mv_definition,
            canonical_select_query,
            base_refs,
            pin,
            iceberg_catalogs,
            target_entry,
            iceberg_catalog,
            target_table,
            MvRefreshPruningLimits::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_pruning_limits(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<&str>,
        current_database: &str,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[TableIdentity]>,
        pin: Arc<RefreshSnapshotPin>,
        iceberg_catalogs: &IcebergCatalogRegistry,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
        pruning_limits: MvRefreshPruningLimits,
    ) -> Result<Self, String> {
        Self::new_with_affected_partitions_and_pruning_limits(
            target,
            mv_id,
            current_catalog,
            current_database,
            mv_definition,
            canonical_select_query,
            base_refs,
            pin,
            iceberg_catalogs,
            target_entry,
            iceberg_catalog,
            target_table,
            crate::mv::model::AffectedTargetPartitions::not_derived(
                "refresh context was constructed without planned affected partitions",
            ),
            pruning_limits,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_affected_partitions(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<&str>,
        current_database: &str,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[TableIdentity]>,
        pin: Arc<RefreshSnapshotPin>,
        iceberg_catalogs: &IcebergCatalogRegistry,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
        affected_partitions: crate::mv::model::AffectedTargetPartitions,
    ) -> Result<Self, String> {
        Self::new_with_affected_partitions_and_pruning_limits(
            target,
            mv_id,
            current_catalog,
            current_database,
            mv_definition,
            canonical_select_query,
            base_refs,
            pin,
            iceberg_catalogs,
            target_entry,
            iceberg_catalog,
            target_table,
            affected_partitions,
            MvRefreshPruningLimits::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_affected_partitions_and_pruning_limits(
        target: IcebergMvTarget,
        mv_id: i64,
        current_catalog: Option<&str>,
        current_database: &str,
        mv_definition: Arc<StoredMvDefinition>,
        canonical_select_query: Arc<sqlparser::ast::Query>,
        base_refs: Arc<[TableIdentity]>,
        pin: Arc<RefreshSnapshotPin>,
        iceberg_catalogs: &IcebergCatalogRegistry,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
        affected_partitions: crate::mv::model::AffectedTargetPartitions,
        pruning_limits: MvRefreshPruningLimits,
    ) -> Result<Self, String> {
        let metadata = target_table.metadata();
        let target_snapshot_id = metadata.current_snapshot().map(|s| s.snapshot_id());
        let target_table_uuid = metadata.uuid().to_string();
        let target_schema = metadata.current_schema().clone();
        let schema_contract = mv_definition.schema_contract.clone().map(Arc::new);

        let rewrite_target = rewrite_target_identity(&target);
        let rewrite = IcebergMvRewriteContext::from_parts(
            rewrite_target,
            mv_id,
            current_catalog.map(str::to_string),
            current_database.to_string(),
            mv_definition,
            canonical_select_query,
            base_refs.clone(),
            pin,
            target_snapshot_id,
            target_table_uuid,
            target_schema,
            schema_contract,
        )?;
        let base_catalog_entries = collect_base_catalog_entries(iceberg_catalogs, &base_refs)?;

        Ok(Self {
            rewrite: Arc::new(rewrite),
            application_target: target,
            target_entry,
            base_catalog_entries,
            iceberg_catalog,
            target_table,
            affected_partitions,
            pruning_limits,
        })
    }

    pub(crate) fn affected_partitions_to_target_partition_filter(
        &self,
    ) -> crate::mv::model::TargetPartitionFilter {
        match &self.affected_partitions {
            crate::mv::model::AffectedTargetPartitions::Known { partitions } => {
                if self
                    .pruning_limits
                    .affected_partition_count_exceeds_limit(partitions.len())
                {
                    tracing::warn!(
                        target = ?self.rewrite.target,
                        affected_partition_count = partitions.len(),
                        max_affected_partitions = self.pruning_limits.max_affected_partitions,
                        fallback_reason = "affected_partition_threshold",
                        "falling back to unpartitioned target scan because affected partition allow-list exceeds configured threshold"
                    );
                    crate::mv::model::TargetPartitionFilter::None
                } else {
                    crate::mv::model::TargetPartitionFilter::AllowList(partitions.clone())
                }
            }
            crate::mv::model::AffectedTargetPartitions::Unpartitioned
            | crate::mv::model::AffectedTargetPartitions::NotDerived { .. } => {
                crate::mv::model::TargetPartitionFilter::None
            }
        }
    }

    pub(crate) fn version_scan_source(
        &self,
        table: &IcebergTableInfo,
        snapshot_id: i64,
    ) -> Result<ScanSource, String> {
        let entry = self.base_catalog_entry_for_version_scan(&table.catalog)?;
        let ident =
            iceberg::TableIdent::from_strs([table.namespace.as_str(), table.table.as_str()])
                .map_err(|e| {
                    format!(
                        "build iceberg table ident for version scan {}.{}.{}: {e}",
                        table.catalog, table.namespace, table.table
                    )
                })?;
        let catalog = crate::connector::iceberg::catalog::registry::build_iceberg_catalog(entry)
            .map_err(|e| {
                format!(
                    "build iceberg catalog for version scan {}.{}.{}: {e}",
                    table.catalog, table.namespace, table.table
                )
            })?;
        let loaded = crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            catalog.load_table(&ident).await
        })
        .map_err(|e| format!("load iceberg table for version scan runtime failed: {e}"))?
        .map_err(|e| {
            format!(
                "load iceberg table for version scan {}.{}.{}: {e}",
                table.catalog, table.namespace, table.table
            )
        })?;
        let files = data_files_at_snapshot(&loaded, snapshot_id)?;
        Ok(ScanSource::IcebergDataFiles {
            table: table.clone(),
            files,
            cloud_properties: entry.cloud_properties_map(),
            binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
        })
    }

    fn base_catalog_entry_for_version_scan(
        &self,
        catalog: &str,
    ) -> Result<&IcebergCatalogEntry, String> {
        let key = crate::catalog::identifier::normalize_identifier(catalog)?;
        self.base_catalog_entries.get(&key).ok_or_else(|| {
            format!("Iceberg version scan requires base catalog {catalog} in MV refresh context")
        })
    }

    pub(crate) fn target_state_scan_source(
        &self,
        scan: &IcebergMvTargetStateScan,
    ) -> Result<ScanSource, String> {
        let target = &self.rewrite.target;
        if !scan.catalog.eq_ignore_ascii_case(&target.catalog)
            || !scan.database.eq_ignore_ascii_case(&target.namespace)
            || !scan.table.eq_ignore_ascii_case(&target.table)
        {
            return Err(format!(
                "Iceberg target-state scan {} does not match MV refresh target {}.{}.{}",
                scan.fqn(),
                target.catalog,
                target.namespace,
                target.table
            ));
        }
        if scan.target_table_uuid != self.rewrite.target_table_uuid {
            return Err(format!(
                "Iceberg target-state scan {} target uuid mismatch: scan={} context={}",
                scan.fqn(),
                scan.target_table_uuid,
                self.rewrite.target_table_uuid
            ));
        }
        if scan.target_snapshot_id != self.rewrite.target_snapshot_id {
            return Err(format!(
                "Iceberg target-state scan {} target snapshot mismatch: scan={:?} context={:?}",
                scan.fqn(),
                scan.target_snapshot_id,
                self.rewrite.target_snapshot_id
            ));
        }
        let target_partition_allow_list = self.target_state_partition_allow_list(scan)?;
        let aggregate_contract =
            self.rewrite
                .schema_contract
                .aggregate
                .as_ref()
                .ok_or_else(|| {
                    format!(
                        "Iceberg target-state scan {} requires aggregate state contract",
                        scan.fqn()
                    )
                })?;
        if scan.aggregate_state_layout_version != aggregate_contract.state_layout_version {
            return Err(format!(
                "Iceberg target-state scan {} aggregate layout version mismatch: scan={} contract={}",
                scan.fqn(),
                scan.aggregate_state_layout_version,
                aggregate_contract.state_layout_version
            ));
        }
        match &scan.row_filter {
            crate::sql::planner::table::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope,
            } if row_id_column_name.eq_ignore_ascii_case(&scan.row_id_column_name) => {
                validate_target_state_branch_scope(
                    scan,
                    branch_scope.as_ref(),
                    &self.rewrite.schema_contract,
                )?;
            }
            crate::sql::planner::table::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                ..
            } => {
                return Err(format!(
                    "Iceberg target-state scan {} row filter column mismatch: filter={} scan={}",
                    scan.fqn(),
                    row_id_column_name,
                    scan.row_id_column_name
                ));
            }
        }
        let (_, layout) = self.rewrite.aggregate_shape_and_layout_for_execution()?;
        let expected_physical_columns = layout
            .physical_columns
            .iter()
            .map(|column| column.column.name.clone())
            .collect::<Vec<_>>();
        if scan.physical_column_names != expected_physical_columns {
            return Err(format!(
                "Iceberg target-state scan {} physical column mismatch: scan={:?} expected={:?}",
                scan.fqn(),
                scan.physical_column_names,
                expected_physical_columns
            ));
        }

        let files = match self.rewrite.target_snapshot_id {
            Some(snapshot_id) => data_files_at_snapshot(&self.target_table, snapshot_id)?,
            None => Vec::new(),
        };
        let files = if let Some(allow_list) = target_partition_allow_list {
            filter_target_state_files_by_partition(
                self.rewrite.schema_contract.as_ref(),
                &allow_list,
                files,
                scan,
            )?
        } else {
            files
        };
        Ok(ScanSource::IcebergDataFiles {
            table: target_table_info(self, scan)?,
            files,
            cloud_properties: self.target_entry.cloud_properties_map(),
            binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
        })
    }

    pub(crate) fn target_locator_scan_source(
        &self,
        scan: &IcebergMvTargetLocatorScan,
    ) -> Result<ScanSource, String> {
        let target = &self.rewrite.target;
        if !scan.catalog.eq_ignore_ascii_case(&target.catalog)
            || !scan.database.eq_ignore_ascii_case(&target.namespace)
            || !scan.table.eq_ignore_ascii_case(&target.table)
        {
            return Err(format!(
                "Iceberg target-locator scan {} does not match MV refresh target {}.{}.{}",
                scan.fqn(),
                target.catalog,
                target.namespace,
                target.table
            ));
        }
        if scan.target_table_uuid != self.rewrite.target_table_uuid {
            return Err(format!(
                "Iceberg target-locator scan {} target uuid mismatch: scan={} context={}",
                scan.fqn(),
                scan.target_table_uuid,
                self.rewrite.target_table_uuid
            ));
        }
        if scan.target_snapshot_id != self.rewrite.target_snapshot_id {
            return Err(format!(
                "Iceberg target-locator scan {} target snapshot mismatch: scan={:?} context={:?}",
                scan.fqn(),
                scan.target_snapshot_id,
                self.rewrite.target_snapshot_id
            ));
        }
        let expected_apply_key = &self
            .rewrite
            .schema_contract
            .target
            .hidden_apply_key
            .column_name;
        if !scan
            .apply_key_column
            .eq_ignore_ascii_case(expected_apply_key)
        {
            return Err(format!(
                "Iceberg target-locator scan {} apply-key column mismatch: scan={} contract={}",
                scan.fqn(),
                scan.apply_key_column,
                expected_apply_key
            ));
        }
        match (
            scan.branch_id_column.as_deref(),
            self.rewrite
                .schema_contract
                .branch
                .as_ref()
                .map(|branch| branch.branch_id_column.column_name.as_str()),
        ) {
            (Some(scan_branch), Some(contract_branch))
                if scan_branch.eq_ignore_ascii_case(contract_branch) => {}
            (None, None) => {}
            (scan_branch, contract_branch) => {
                return Err(format!(
                    "Iceberg target-locator scan {} branch column mismatch: scan={:?} contract={:?}",
                    scan.fqn(),
                    scan_branch,
                    contract_branch
                ));
            }
        }

        let files = match self.rewrite.target_snapshot_id {
            Some(snapshot_id) => data_files_at_snapshot(&self.target_table, snapshot_id)?,
            None => Vec::new(),
        };
        Ok(ScanSource::IcebergDataFiles {
            table: target_locator_table_info(self, scan)?,
            files,
            cloud_properties: self.target_entry.cloud_properties_map(),
            binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
        })
    }

    fn target_state_partition_allow_list(
        &self,
        scan: &IcebergMvTargetStateScan,
    ) -> Result<Option<BTreeSet<crate::mv::model::MvPartitionKey>>, String> {
        match scan.partition_constraint {
            crate::sql::planner::table::IcebergMvTargetStatePartitionConstraint::Unpartitioned => {
                Ok(None)
            }
            crate::sql::planner::table::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired => {
                match &self.affected_partitions {
                    crate::mv::model::AffectedTargetPartitions::Unpartitioned => {
                        Ok(None)
                    }
                    crate::mv::model::AffectedTargetPartitions::Known {
                        partitions,
                    } => {
                        if self
                            .pruning_limits
                            .affected_partition_count_exceeds_limit(partitions.len())
                        {
                            tracing::warn!(
                                target = %scan.fqn(),
                                affected_partition_count = partitions.len(),
                                max_affected_partitions =
                                    self.pruning_limits.max_affected_partitions,
                                fallback_reason = "affected_partition_threshold",
                                "falling back to full target-state scan because affected partition allow-list exceeds configured threshold"
                            );
                            Ok(None)
                        } else {
                            Ok(Some(partitions.clone()))
                        }
                    }
                    crate::mv::model::AffectedTargetPartitions::NotDerived {
                        reason,
                    } => {
                        tracing::warn!(
                            target = %scan.fqn(),
                            reason = %reason,
                            "falling back to full target-state scan because affected partition planning is unknown"
                        );
                        Ok(None)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn bind_target_state_file_positions(
    mut source: ScanSource,
    matched_positions: &[crate::engine::mv::iceberg_target_apply::TargetRowPositionSet],
    target: &str,
) -> Result<ScanSource, String> {
    let ScanSource::IcebergDataFiles { files, .. } = &mut source else {
        return Err(format!(
            "Iceberg target-state position binding for {target} requires IcebergDataFiles source"
        ));
    };

    if matched_positions.is_empty() {
        files.clear();
        return Ok(source);
    }

    let mut by_file = BTreeMap::<String, Vec<i64>>::new();
    for set in matched_positions {
        if set.positions.is_empty() {
            continue;
        }
        by_file
            .entry(set.referenced_data_file.clone())
            .or_default()
            .extend(set.positions.iter().copied());
    }
    for positions in by_file.values_mut() {
        positions.sort_unstable();
        positions.dedup();
    }
    if by_file.is_empty() {
        files.clear();
        return Ok(source);
    }

    let mut bound_files = Vec::new();
    for mut file in std::mem::take(files) {
        if let Some(positions) = by_file.remove(&file.path) {
            file.included_positions = Some(positions);
            bound_files.push(file);
        }
    }
    if !by_file.is_empty() {
        let missing = by_file.keys().cloned().collect::<Vec<_>>().join(", ");
        return Err(format!(
            "Iceberg target-state scan {target} locator returned positions for files not present in scan source: [{missing}]"
        ));
    }
    *files = bound_files;
    Ok(source)
}

fn validate_target_state_branch_scope(
    scan: &IcebergMvTargetStateScan,
    scope: Option<&crate::sql::planner::table::BranchScope>,
    contract: &MvSchemaContract,
) -> Result<(), String> {
    let Some(scope) = scope else {
        return Ok(());
    };
    let branch = contract.branch.as_ref().ok_or_else(|| {
        format!(
            "Iceberg target-state scan {} has branch scope but schema contract has no branch contract",
            scan.fqn()
        )
    })?;
    if !scope
        .branch_id_column_name
        .eq_ignore_ascii_case(&branch.branch_id_column.column_name)
    {
        return Err(format!(
            "Iceberg target-state scan {} branch column mismatch: scope={} contract={}",
            scan.fqn(),
            scope.branch_id_column_name,
            branch.branch_id_column.column_name
        ));
    }
    if scope.branch_id < 0 || scope.branch_id as u32 >= branch.branch_count {
        return Err(format!(
            "Iceberg target-state scan {} branch id {} out of range 0..{}",
            scan.fqn(),
            scope.branch_id,
            branch.branch_count
        ));
    }
    Ok(())
}

fn data_files_at_snapshot(
    table: &iceberg::table::Table,
    snapshot_id: i64,
) -> Result<Vec<IcebergDataFileInfo>, String> {
    crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
        table,
        snapshot_id,
    )
    .map(|files| {
        files
            .into_iter()
            .map(data_file_with_stats_to_info)
            .collect()
    })
}

fn filter_target_state_files_by_partition(
    contract: &MvSchemaContract,
    allow_list: &BTreeSet<crate::mv::model::MvPartitionKey>,
    files: Vec<IcebergDataFileInfo>,
    scan: &IcebergMvTargetStateScan,
) -> Result<Vec<IcebergDataFileInfo>, String> {
    if allow_list.is_empty() {
        return Ok(Vec::new());
    }
    files
        .into_iter()
        .filter_map(|file| match target_file_partition_key(contract, &file) {
            Ok(Some(key)) if allow_list.contains(&key) => Some(Ok(file)),
            Ok(Some(_)) => None,
            Ok(None) => Some(Err(format!(
                "Iceberg target-state scan {} requires partition keys for target files",
                scan.fqn()
            ))),
            Err(err) => Some(Err(format!(
                "Iceberg target-state scan {} cannot map target file {} partition: {}",
                scan.fqn(),
                file.path,
                err
            ))),
        })
        .collect()
}

fn target_file_partition_key(
    contract: &MvSchemaContract,
    file: &IcebergDataFileInfo,
) -> Result<Option<crate::mv::model::MvPartitionKey>, String> {
    let Some(partition) = &contract.target.partition else {
        return Ok(None);
    };
    let Some(spec_id) = file.partition_spec_id else {
        return Err(format!(
            "target file {} is missing partition spec id",
            file.path
        ));
    };
    let mut fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        let expected_transform = target_contract_transform_text(&partition_field.transform)
            .ok_or_else(|| {
                format!(
                    "MV partition field {} uses unsupported void transform",
                    partition_field.partition_field_name
                )
            })?;
        let value = file
            .partition_values
            .iter()
            .find(|value| {
                value
                    .source_column
                    .eq_ignore_ascii_case(&partition_field.source_column_name)
                    && value.transform.eq_ignore_ascii_case(&expected_transform)
            })
            .or_else(|| {
                file.partition_values.iter().find(|value| {
                    value
                        .field_name
                        .eq_ignore_ascii_case(&partition_field.partition_field_name)
                        && value.transform.eq_ignore_ascii_case(&expected_transform)
                })
            })
            .ok_or_else(|| {
                format!(
                    "target file {} has no partition value for {} with transform {}",
                    file.path, partition_field.partition_field_name, expected_transform
                )
            })?;
        fields.push(crate::mv::model::MvPartitionKeyField::new(
            partition_field.partition_field_name.clone(),
            target_partition_value_to_mv_value(value)?,
        ));
    }

    Ok(Some(crate::mv::model::MvPartitionKey::new(spec_id, fields)))
}

fn target_partition_value_to_mv_value(
    value: &IcebergPartitionFieldValue,
) -> Result<crate::mv::model::MvPartitionValue, String> {
    match &value.value {
        None => Ok(crate::mv::model::MvPartitionValue::Null),
        Some(IcebergPartitionValue::Boolean(v)) => {
            Ok(crate::mv::model::MvPartitionValue::String(v.to_string()))
        }
        Some(IcebergPartitionValue::Int32(v)) => {
            Ok(crate::mv::model::MvPartitionValue::String(v.to_string()))
        }
        Some(IcebergPartitionValue::Int64(v)) => {
            Ok(crate::mv::model::MvPartitionValue::String(v.to_string()))
        }
        Some(IcebergPartitionValue::Float(v)) => {
            Ok(crate::mv::model::MvPartitionValue::String(v.to_string()))
        }
        Some(IcebergPartitionValue::Double(v)) => {
            Ok(crate::mv::model::MvPartitionValue::String(v.to_string()))
        }
        Some(IcebergPartitionValue::String(v)) => {
            Ok(crate::mv::model::MvPartitionValue::String(v.clone()))
        }
        Some(IcebergPartitionValue::Binary(_)) => Err(format!(
            "target partition field {} has unsupported binary value",
            value.field_name
        )),
    }
}

fn target_contract_transform_text(
    transform: &mv_schema::MvPartitionTransformContract,
) -> Option<String> {
    match transform {
        mv_schema::MvPartitionTransformContract::Identity => Some("identity".to_string()),
        mv_schema::MvPartitionTransformContract::Year => Some("year".to_string()),
        mv_schema::MvPartitionTransformContract::Month => Some("month".to_string()),
        mv_schema::MvPartitionTransformContract::Day => Some("day".to_string()),
        mv_schema::MvPartitionTransformContract::Hour => Some("hour".to_string()),
        mv_schema::MvPartitionTransformContract::Bucket { num_buckets } => {
            Some(format!("bucket({num_buckets})"))
        }
        mv_schema::MvPartitionTransformContract::Truncate { width } => {
            Some(format!("truncate({width})"))
        }
        mv_schema::MvPartitionTransformContract::Void => None,
    }
}

fn collect_base_catalog_entries(
    iceberg_catalogs: &IcebergCatalogRegistry,
    base_refs: &[TableIdentity],
) -> Result<BTreeMap<String, IcebergCatalogEntry>, String> {
    let mut entries = BTreeMap::new();
    for base_ref in base_refs {
        let key = crate::catalog::identifier::normalize_identifier(&base_ref.catalog)?;
        if entries.contains_key(&key) {
            continue;
        }
        let entry = iceberg_catalogs.get(&base_ref.catalog).map_err(|e| {
            format!(
                "collect iceberg MV refresh base catalog {} for {}: {e}",
                base_ref.catalog,
                base_ref.fqn()
            )
        })?;
        entries.insert(key, entry);
    }
    Ok(entries)
}

fn data_file_with_stats_to_info(
    file: crate::connector::iceberg::catalog::registry::DataFileWithStats,
) -> IcebergDataFileInfo {
    IcebergDataFileInfo {
        path: file.path,
        size: file.size,
        row_count: file.record_count,
        column_stats: file.column_stats,
        partition_spec_id: file.partition_spec_id,
        partition_key: file.partition_key,
        first_row_id: file.first_row_id,
        data_sequence_number: file.data_sequence_number,
        ivm_change_op: None,
        included_positions: None,
        delete_files: file.delete_files,
        manifest_path: file.manifest_path,
        partition_values: file.partition_field_values,
    }
}

fn target_table_info(
    ctx: &IcebergMvRefreshContext,
    scan: &IcebergMvTargetStateScan,
) -> Result<IcebergTableInfo, String> {
    target_table_info_for_target(ctx, &scan.catalog, &scan.database, &scan.table)
}

fn target_locator_table_info(
    ctx: &IcebergMvRefreshContext,
    scan: &IcebergMvTargetLocatorScan,
) -> Result<IcebergTableInfo, String> {
    target_table_info_for_target(ctx, &scan.catalog, &scan.database, &scan.table)
}

fn target_table_info_for_target(
    ctx: &IcebergMvRefreshContext,
    catalog: &str,
    database: &str,
    table: &str,
) -> Result<IcebergTableInfo, String> {
    let metadata = ctx.target_table.metadata();
    Ok(IcebergTableInfo {
        catalog: catalog.to_string(),
        namespace: database.to_string(),
        table: table.to_string(),
        table_uuid: Some(metadata.uuid().to_string()),
        current_snapshot_id: metadata.current_snapshot_id(),
        schema_id: metadata.current_schema_id(),
        location: metadata.location().to_string(),
        schema: iceberg_schema_def(metadata.current_schema()),
        serialized_metadata: Some(
            serde_json::to_string(metadata)
                .map_err(|err| format!("serialize iceberg target table metadata failed: {err}"))?,
        ),
        serialized_metadata_rows: None,
    })
}

fn iceberg_schema_def(schema: &iceberg::spec::Schema) -> IcebergSchemaDef {
    IcebergSchemaDef {
        fields: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| iceberg_field_def(field.as_ref()))
            .collect(),
    }
}

fn iceberg_field_def(field: &iceberg::spec::NestedField) -> IcebergSchemaFieldDef {
    let initial_default_json = field.initial_default.as_ref().and_then(|literal| {
        literal
            .clone()
            .try_into_json(field.field_type.as_ref())
            .ok()
            .map(|json| json.to_string())
    });
    let write_default_json = field.write_default.as_ref().and_then(|literal| {
        literal
            .clone()
            .try_into_json(field.field_type.as_ref())
            .ok()
            .map(|json| json.to_string())
    });
    IcebergSchemaFieldDef {
        field_id: field.id,
        name: field.name.clone(),
        initial_default: field.initial_default.clone(),
        write_default: field.write_default.clone(),
        initial_default_json,
        write_default_json,
        children: iceberg_type_children(field.field_type.as_ref()),
    }
}

fn iceberg_type_children(ty: &iceberg::spec::Type) -> Vec<IcebergSchemaFieldDef> {
    match ty {
        iceberg::spec::Type::Struct(struct_ty) => struct_ty
            .fields()
            .iter()
            .map(|field| iceberg_field_def(field.as_ref()))
            .collect(),
        iceberg::spec::Type::List(list_ty) => {
            vec![iceberg_field_def(list_ty.element_field.as_ref())]
        }
        iceberg::spec::Type::Map(map_ty) => vec![
            iceberg_field_def(map_ty.key_field.as_ref()),
            iceberg_field_def(map_ty.value_field.as_ref()),
        ],
        iceberg::spec::Type::Primitive(_) => vec![],
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use crate::catalog::identifier::TableIdentity;
    use crate::mv::rewrite::context::IcebergMvRewriteContext;
    pub(crate) use crate::mv::rewrite::context::tests_support::{
        dummy_rewrite_context, make_mv_definition, make_pin, make_ref, make_schema_contract,
        make_target, make_target_schema, parse_query,
    };
    use crate::sql::planner::table::{
        IcebergMvTargetStatePartitionConstraint, IcebergMvTargetStateRowFilter,
    };
    use mv_schema::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, JOIN_APPLY_KEY_COLUMN_NAME,
    };

    use super::*;

    pub(crate) struct TargetLocatorRefreshFixture {
        pub(crate) _warehouse: tempfile::TempDir,
        pub(crate) target_entry: Arc<IcebergCatalogEntry>,
        pub(crate) iceberg_catalog: Arc<dyn iceberg::Catalog>,
        pub(crate) target_table: iceberg::table::Table,
        pub(crate) target_snapshot_id: i64,
    }

    pub(crate) fn target_locator_refresh_fixture(test_name: &str) -> TargetLocatorRefreshFixture {
        let warehouse = tempfile::Builder::new()
            .prefix(&format!("novarocks_target_locator_{test_name}_"))
            .tempdir()
            .expect("warehouse tempdir");
        let warehouse_uri = format!("file://{}", warehouse.path().join("warehouse").display());
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse_uri),
                ],
            )
            .expect("target catalog entry"),
        );
        crate::connector::iceberg::catalog::registry::create_namespace(&target_entry, "db")
            .expect("create target namespace");
        crate::connector::iceberg::catalog::registry::create_table(
            &target_entry,
            "db",
            "mv",
            &[
                crate::sql::TableColumnDef {
                    name: "k".to_string(),
                    data_type: crate::catalog::schema::SqlType::BigInt,
                    nullable: false,
                    aggregation: None,
                    default: None,
                },
                crate::sql::TableColumnDef {
                    name: "v".to_string(),
                    data_type: crate::catalog::schema::SqlType::BigInt,
                    nullable: true,
                    aggregation: None,
                    default: None,
                },
            ],
            None,
            &[],
            &[],
        )
        .expect("create target table");
        crate::connector::iceberg::catalog::registry::insert_rows(
            &target_entry,
            "db",
            "mv",
            &[
                vec![crate::sql::Literal::Int(10), crate::sql::Literal::Int(100)],
                vec![crate::sql::Literal::Int(20), crate::sql::Literal::Int(200)],
            ],
        )
        .expect("insert target rows");
        let loaded =
            crate::connector::iceberg::catalog::registry::load_table(&target_entry, "db", "mv")
                .expect("load target table");
        let target_snapshot_id = loaded
            .table
            .metadata()
            .current_snapshot_id()
            .expect("target snapshot");
        let iceberg_catalog =
            crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&target_entry)
                .expect("build target iceberg catalog");

        TargetLocatorRefreshFixture {
            _warehouse: warehouse,
            target_entry,
            iceberg_catalog,
            target_table: loaded.table,
            target_snapshot_id,
        }
    }

    fn target_field_id(schema: &Schema, name: &str) -> i32 {
        schema
            .as_struct()
            .fields()
            .iter()
            .find(|field| field.name == name)
            .unwrap_or_else(|| panic!("missing target field {name}"))
            .id
    }

    fn rewrite_context_for_target_fixture(
        target_table: &iceberg::table::Table,
        target_snapshot_id: i64,
    ) -> Arc<IcebergMvRewriteContext> {
        let metadata = target_table.metadata();
        let target_schema = metadata.current_schema().clone();
        let mut contract = make_schema_contract();
        let k_id = target_field_id(target_schema.as_ref(), "k");
        let v_id = target_field_id(target_schema.as_ref(), "v");
        contract.target.table_uuid = metadata.uuid().to_string();
        contract.target.schema_id_at_create = metadata.current_schema_id();
        contract.target.visible_columns[0].target_field_id = k_id;
        contract.target.visible_columns[1].target_field_id = v_id;
        contract.target.hidden_apply_key.target_field_id = k_id;
        let target = make_target();
        let mv_def = Arc::new(make_mv_definition());
        let query = Arc::new(parse_query("SELECT k, v FROM ice.db.b"));
        let base_refs: Arc<[TableIdentity]> = Arc::from(vec![make_ref("ice", "db", "b")]);
        let pin = Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")]));

        Arc::new(
            IcebergMvRewriteContext::from_parts(
                target,
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                mv_def,
                query,
                base_refs,
                pin,
                Some(target_snapshot_id),
                metadata.uuid().to_string(),
                target_schema,
                Some(Arc::new(contract)),
            )
            .expect("target fixture rewrite context"),
        )
    }

    pub(crate) fn refresh_context_for_target_fixture(
        fixture: &TargetLocatorRefreshFixture,
    ) -> IcebergMvRefreshContext {
        IcebergMvRefreshContext {
            rewrite: rewrite_context_for_target_fixture(
                &fixture.target_table,
                fixture.target_snapshot_id,
            ),
            application_target: make_application_target(),
            target_entry: fixture.target_entry.clone(),
            base_catalog_entries: BTreeMap::new(),
            iceberg_catalog: fixture.iceberg_catalog.clone(),
            target_table: fixture.target_table.clone(),
            affected_partitions: crate::mv::model::AffectedTargetPartitions::not_derived(
                "test context",
            ),
            pruning_limits: MvRefreshPruningLimits::default(),
        }
    }

    pub(crate) fn target_fixture_table_info(ctx: &IcebergMvRefreshContext) -> IcebergTableInfo {
        target_table_info_for_target(ctx, "tgt", "db", "mv").expect("target fixture table info")
    }

    pub(crate) fn make_application_target() -> IcebergMvTarget {
        IcebergMvTarget {
            catalog: "tgt".to_string(),
            namespace: "db".to_string(),
            table: "mv".to_string(),
        }
    }

    pub(crate) fn refresh_context_for_handles(
        rewrite: Arc<IcebergMvRewriteContext>,
        target_entry: Arc<IcebergCatalogEntry>,
        iceberg_catalog: Arc<dyn iceberg::Catalog>,
        target_table: iceberg::table::Table,
    ) -> IcebergMvRefreshContext {
        IcebergMvRefreshContext {
            rewrite,
            application_target: make_application_target(),
            target_entry,
            base_catalog_entries: BTreeMap::new(),
            iceberg_catalog,
            target_table,
            affected_partitions: crate::mv::model::AffectedTargetPartitions::not_derived(
                "engine test context",
            ),
            pruning_limits: MvRefreshPruningLimits::default(),
        }
    }

    pub(crate) fn aggregate_target_state_refresh_fixture()
    -> (IcebergMvRefreshContext, IcebergMvTargetStateScan) {
        use iceberg::{NamespaceIdent, TableIdent};

        let warehouse_dir = tempfile::TempDir::new()
            .expect("aggregate target warehouse tempdir")
            .keep();
        let warehouse = format!("file://{}", warehouse_dir.join("warehouse").display());
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse.clone()),
                ],
            )
            .expect("aggregate target entry"),
        );
        let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&target_entry)
                .expect("build aggregate target catalog"),
        );
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(7)
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        100,
                        "k",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        101,
                        "v",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::required(
                        999,
                        "__row_id__",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        200,
                        "__agg_state_v",
                        Type::Primitive(PrimitiveType::Binary),
                    )),
                ])
                .build()
                .expect("aggregate target schema"),
        );
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec().into_unbound(),
            iceberg::spec::SortOrder::unsorted_order(),
            format!("{warehouse}/target/table"),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("aggregate metadata builder")
        .build()
        .expect("aggregate metadata")
        .metadata;
        let target_uuid = metadata.uuid().to_string();
        let target_table = iceberg::table::Table::builder()
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("db".to_string()),
                "mv".to_string(),
            ))
            .build()
            .expect("aggregate target table");

        let mut contract = make_schema_contract();
        contract.target.table_uuid = target_uuid.clone();
        contract.target.schema_id_at_create = 7;
        contract.target.hidden_apply_key.column_name = "__row_id__".to_string();
        contract.target.hidden_apply_key.target_field_id = 999;
        contract.target.hidden_apply_key.source = ApplyKeySource::GroupRowId;
        contract.aggregate = Some(AggregateStateContract {
            state_layout_version: 1,
            row_id_column_name: "__row_id__".to_string(),
            state_columns: vec![AggregateStateColumnContract {
                column_name: "__agg_state_v".to_string(),
                target_field_id: 200,
                type_signature: "varbinary".to_string(),
                nullable: false,
                role: AggregateStateRoleContract::Single,
            }],
        });
        let rewrite = Arc::new(
            IcebergMvRewriteContext::from_parts(
                make_target(),
                42,
                Some("sess_cat".to_string()),
                "sess_db".to_string(),
                Arc::new(make_mv_definition()),
                Arc::new(parse_query(
                    "SELECT k, COUNT(*) AS v FROM ice.db.b GROUP BY k",
                )),
                Arc::from(vec![make_ref("ice", "db", "b")]),
                Arc::new(make_pin(&[("ice.db.b", 22, "uuid-b")])),
                None,
                target_uuid.clone(),
                schema,
                Some(Arc::new(contract)),
            )
            .expect("aggregate rewrite context"),
        );
        let (_, layout) = rewrite
            .aggregate_shape_and_layout_for_execution()
            .expect("aggregate execution layout");
        let physical_column_names = layout
            .physical_columns
            .iter()
            .map(|column| column.column.name.clone())
            .collect();
        let ctx = IcebergMvRefreshContext {
            rewrite,
            application_target: make_application_target(),
            target_entry,
            base_catalog_entries: BTreeMap::new(),
            iceberg_catalog,
            target_table,
            affected_partitions: crate::mv::model::AffectedTargetPartitions::not_derived(
                "test context",
            ),
            pruning_limits: MvRefreshPruningLimits::default(),
        };
        let scan = IcebergMvTargetStateScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: target_uuid,
            target_snapshot_id: None,
            aggregate_state_layout_version: 1,
            columns: Vec::new(),
            group_key_names: vec!["k".to_string()],
            aggregate_state_names: vec!["__agg_state_v".to_string()],
            physical_column_names,
            row_id_column_name: "__row_id__".to_string(),
            row_filter: IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "__row_id__".to_string(),
                branch_scope: None,
            },
            partition_constraint: IcebergMvTargetStatePartitionConstraint::Unpartitioned,
        };
        (ctx, scan)
    }

    pub(crate) fn join_projection_refresh_context_for_test()
    -> (tempfile::TempDir, IcebergMvRefreshContext) {
        let warehouse = tempfile::Builder::new()
            .prefix("novarocks_join_projection_target_")
            .tempdir()
            .expect("target warehouse tempdir");
        let target_warehouse_uri = format!("file://{}", warehouse.path().join("target").display());
        let base_warehouse_uri = format!("file://{}", warehouse.path().join("base").display());
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    (
                        "iceberg.catalog.warehouse".to_string(),
                        target_warehouse_uri,
                    ),
                ],
            )
            .expect("target catalog entry"),
        );
        let base_entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "ice",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), base_warehouse_uri),
            ],
        )
        .expect("base catalog entry");
        crate::connector::iceberg::catalog::registry::create_namespace(&target_entry, "db")
            .expect("create target namespace");
        crate::connector::iceberg::catalog::registry::create_namespace(&base_entry, "db")
            .expect("create base namespace");
        for table in ["l", "r"] {
            crate::connector::iceberg::catalog::registry::create_table(
                &base_entry,
                "db",
                table,
                &[
                    crate::sql::TableColumnDef {
                        name: "k".to_string(),
                        data_type: crate::catalog::schema::SqlType::BigInt,
                        nullable: false,
                        aggregation: None,
                        default: None,
                    },
                    crate::sql::TableColumnDef {
                        name: "v".to_string(),
                        data_type: crate::catalog::schema::SqlType::BigInt,
                        nullable: true,
                        aggregation: None,
                        default: None,
                    },
                ],
                None,
                &[],
                &[
                    ("format-version".to_string(), "3".to_string()),
                    ("write.row-lineage".to_string(), "true".to_string()),
                ],
            )
            .expect("create base table");
        }
        let left_lineage = seed_join_projection_base_table(&base_entry, "l", 10, 11);
        let right_lineage = seed_join_projection_base_table(&base_entry, "r", 20, 21);
        crate::connector::iceberg::catalog::registry::create_table(
            &target_entry,
            "db",
            "mv",
            &[
                crate::sql::TableColumnDef {
                    name: "k".to_string(),
                    data_type: crate::catalog::schema::SqlType::BigInt,
                    nullable: false,
                    aggregation: None,
                    default: None,
                },
                crate::sql::TableColumnDef {
                    name: "v".to_string(),
                    data_type: crate::catalog::schema::SqlType::BigInt,
                    nullable: true,
                    aggregation: None,
                    default: None,
                },
                crate::sql::TableColumnDef {
                    name: JOIN_APPLY_KEY_COLUMN_NAME.to_string(),
                    data_type: crate::catalog::schema::SqlType::String,
                    nullable: false,
                    aggregation: None,
                    default: None,
                },
            ],
            None,
            &[],
            &[],
        )
        .expect("create target table");
        crate::connector::iceberg::catalog::registry::insert_rows(
            &target_entry,
            "db",
            "mv",
            &[vec![
                crate::sql::Literal::Int(1),
                crate::sql::Literal::Int(10),
                crate::sql::Literal::String("join-key-1".to_string()),
            ]],
        )
        .expect("insert target row");
        let loaded =
            crate::connector::iceberg::catalog::registry::load_table(&target_entry, "db", "mv")
                .expect("load target table");
        let target_snapshot_id = loaded
            .table
            .metadata()
            .current_snapshot_id()
            .expect("target snapshot");
        let iceberg_catalog =
            crate::connector::iceberg::catalog::registry::build_iceberg_catalog(&target_entry)
                .expect("build target iceberg catalog");

        let base = crate::mv::rewrite::context::tests_support::join_projection_rewrite_context();
        let mut mv_definition = (*base.mv_definition).clone();
        mv_definition.last_refresh_snapshots = [
            ("ice.db.l".to_string(), left_lineage.previous_snapshot_id),
            ("ice.db.r".to_string(), right_lineage.previous_snapshot_id),
        ]
        .into_iter()
        .collect();
        mv_definition.last_refresh_table_uuids = [
            ("ice.db.l".to_string(), left_lineage.table_uuid.clone()),
            ("ice.db.r".to_string(), right_lineage.table_uuid.clone()),
        ]
        .into_iter()
        .collect();
        let mut schema_contract = (*base.schema_contract).clone();
        for base_contract in &mut schema_contract.bases {
            if base_contract.table_fqn.eq_ignore_ascii_case("ice.db.l") {
                base_contract.table_uuid = left_lineage.table_uuid.clone();
            } else if base_contract.table_fqn.eq_ignore_ascii_case("ice.db.r") {
                base_contract.table_uuid = right_lineage.table_uuid.clone();
            }
        }
        mv_definition.schema_contract = Some(schema_contract.clone());
        let pin = make_pin(&[
            (
                "ice.db.l",
                left_lineage.current_snapshot_id,
                left_lineage.table_uuid.as_str(),
            ),
            (
                "ice.db.r",
                right_lineage.current_snapshot_id,
                right_lineage.table_uuid.as_str(),
            ),
        ]);
        let rewrite = Arc::new(
            IcebergMvRewriteContext::from_parts(
                base.target.clone(),
                base.mv_id,
                base.current_catalog.clone(),
                base.current_database.clone(),
                Arc::new(mv_definition),
                Arc::clone(&base.canonical_select_query),
                Arc::clone(&base.base_refs),
                Arc::new(pin),
                Some(target_snapshot_id),
                loaded.table.metadata().uuid().to_string(),
                Arc::clone(&base.target_schema),
                Some(Arc::new(schema_contract)),
            )
            .expect("join projection refresh rewrite context"),
        );

        let mut base_catalog_entries = BTreeMap::new();
        base_catalog_entries.insert("ice".to_string(), base_entry);

        (
            warehouse,
            IcebergMvRefreshContext {
                rewrite,
                application_target: make_application_target(),
                target_entry,
                base_catalog_entries,
                iceberg_catalog,
                target_table: loaded.table,
                affected_partitions: crate::mv::model::AffectedTargetPartitions::not_derived(
                    "test context",
                ),
                pruning_limits: MvRefreshPruningLimits::default(),
            },
        )
    }

    #[derive(Debug)]
    struct JoinProjectionBaseLineage {
        previous_snapshot_id: i64,
        current_snapshot_id: i64,
        table_uuid: String,
    }

    fn seed_join_projection_base_table(
        entry: &crate::connector::iceberg::catalog::registry::IcebergCatalogEntry,
        table: &str,
        previous_value: i64,
        current_value: i64,
    ) -> JoinProjectionBaseLineage {
        crate::connector::iceberg::catalog::registry::insert_rows(
            entry,
            "db",
            table,
            &[vec![
                crate::sql::Literal::Int(1),
                crate::sql::Literal::Int(previous_value),
            ]],
        )
        .expect("insert previous base row");
        let previous = crate::connector::iceberg::catalog::registry::load_table(entry, "db", table)
            .expect("load previous base table");
        let previous_snapshot_id = previous
            .table
            .metadata()
            .current_snapshot_id()
            .expect("previous base snapshot");

        crate::connector::iceberg::catalog::registry::insert_rows(
            entry,
            "db",
            table,
            &[vec![
                crate::sql::Literal::Int(2),
                crate::sql::Literal::Int(current_value),
            ]],
        )
        .expect("insert current base row");
        let current = crate::connector::iceberg::catalog::registry::load_table(entry, "db", table)
            .expect("load current base table");
        let current_snapshot_id = current
            .table
            .metadata()
            .current_snapshot_id()
            .expect("current base snapshot");

        JoinProjectionBaseLineage {
            previous_snapshot_id,
            current_snapshot_id,
            table_uuid: current.table.metadata().uuid().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

    use crate::catalog::identifier::TableIdentity;
    use crate::mv::persistence::schema as mv_schema;
    use crate::mv::refresh::pin::RefreshSnapshotPin;
    use mv_schema::{
        AggregateStateColumnContract, AggregateStateContract, AggregateStateRoleContract,
        ApplyKeySource, BRANCH_ID_COLUMN_NAME, BranchIdColumnContract, BranchUnionContract,
    };

    use super::tests_support::*;
    use super::*;

    #[test]
    fn pure_join_refresh_fragment_materialization_lowers_coalesce_plan() {
        std::thread::Builder::new()
            .name("imv-join-fragment-lowering-test".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let (_warehouse, refresh_ctx) = join_projection_refresh_context_for_test();
                let optimized_tree = crate::sql::planner::imv_rewrite::entrypoint::tests::tests_support::build_join_refresh_coalesce_plan_for_lowering(
                    &refresh_ctx.rewrite,
                );
                let mut connectors = crate::connector::ConnectorRegistry::default();
                connectors.register_scan_planner(Arc::new(
                    crate::connector::iceberg::IcebergConnectorScanPlanner::new(),
                ));

                let physical_plan =
                    crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)
                        .expect("convert optimizer physical plan");
                let distributed_plan =
                    crate::sql::planner::pipeline::build_distributed_plan(physical_plan)
                        .expect("build DistributedPlan");
                let prepared = crate::coordinator::prepare::prepare_fragments(
                    &distributed_plan,
                    &connectors,
                    Some(&refresh_ctx),
                )
                .expect("join projection coalesce plan must prepare");
                crate::protocol::native::encode::encode_native_fragment_bundle(
                    &distributed_plan,
                    &prepared,
                )
                .expect("join projection coalesce plan must lower");
            })
            .expect("spawn fragment lowering test")
            .join()
            .expect("fragment lowering test");
    }

    #[test]
    fn version_scan_source_does_not_reject_base_catalog_that_differs_from_target() {
        let mixed_case_target = IcebergMvTarget {
            catalog: "TargetCase".to_string(),
            namespace: "NameSpace".to_string(),
            table: "MvTable".to_string(),
        };
        let identity = rewrite_target_identity(&mixed_case_target);
        assert_eq!(identity.catalog, mixed_case_target.catalog);
        assert_eq!(identity.namespace, mixed_case_target.namespace);
        assert_eq!(identity.table, mixed_case_target.table);

        use iceberg::{NamespaceIdent, TableIdent};

        let warehouse_dir = tempfile::TempDir::new()
            .expect("target warehouse tempdir")
            .keep();
        let warehouse = format!("file://{}", warehouse_dir.join("warehouse").display());
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse.clone()),
                ],
            )
            .expect("catalog entry"),
        );
        let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&target_entry)
                .expect("build hadoop catalog"),
        );
        let base_warehouse = std::env::temp_dir()
            .join(format!(
                "novarocks-version-scan-base-catalog-test-{}",
                uuid::Uuid::new_v4()
            ))
            .to_string_lossy()
            .into_owned();
        let base_entry = crate::connector::iceberg::catalog::registry::build_catalog_entry(
            "ice",
            &[
                ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                ("iceberg.catalog.warehouse".to_string(), base_warehouse),
            ],
        )
        .expect("base catalog entry");
        let base_catalog_entries = [("ice".to_string(), base_entry)].into_iter().collect();
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    100,
                    "k",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    101,
                    "v",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .expect("schema");
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema,
            iceberg::spec::PartitionSpec::unpartition_spec().into_unbound(),
            iceberg::spec::SortOrder::unsorted_order(),
            format!("{warehouse}/target/table"),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let target_table = iceberg::table::Table::builder()
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("db".to_string()),
                "mv".to_string(),
            ))
            .build()
            .expect("target table");
        let ctx = IcebergMvRefreshContext {
            rewrite: dummy_rewrite_context(),
            application_target: make_application_target(),
            target_entry,
            base_catalog_entries,
            iceberg_catalog,
            target_table,
            affected_partitions: crate::mv::model::AffectedTargetPartitions::not_derived(
                "test context",
            ),
            pruning_limits: MvRefreshPruningLimits::default(),
        };
        let table = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "missing_base".to_string(),
            table_uuid: None,
            current_snapshot_id: None,
            schema_id: 0,
            location: String::new(),
            schema: IcebergSchemaDef { fields: Vec::new() },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };

        let err = ctx
            .version_scan_source(&table, 123)
            .expect_err("missing base table should fail after catalog resolution");
        assert!(
            !err.contains("requires catalog ice in MV refresh context, got tgt"),
            "version scan must resolve by base catalog, got: {err}"
        );
    }

    #[test]
    fn collect_base_catalog_entries_preserves_base_catalog_cloud_properties() {
        let mut registry = IcebergCatalogRegistry::default();
        let target_warehouse = std::env::temp_dir()
            .join(format!(
                "novarocks-version-scan-target-catalog-test-{}",
                uuid::Uuid::new_v4()
            ))
            .to_string_lossy()
            .into_owned();
        let base_warehouse = std::env::temp_dir()
            .join(format!(
                "novarocks-version-scan-base-entry-test-{}",
                uuid::Uuid::new_v4()
            ))
            .to_string_lossy()
            .into_owned();
        registry
            .create_catalog(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), target_warehouse),
                    ("aws.s3.endpoint".to_string(), "target-endpoint".to_string()),
                ],
            )
            .expect("target catalog");
        registry
            .create_catalog(
                "ice",
                &[
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), base_warehouse),
                    ("aws.s3.endpoint".to_string(), "base-endpoint".to_string()),
                ],
            )
            .expect("base catalog");
        let base_refs = vec![make_ref("ice", "db", "b")];

        let entries = collect_base_catalog_entries(&registry, &base_refs).expect("entries");
        let cloud = entries
            .get("ice")
            .expect("base entry")
            .cloud_properties_map();

        assert_eq!(
            cloud.get("aws.s3.endpoint").map(String::as_str),
            Some("base-endpoint")
        );
    }

    #[test]
    fn target_locator_scan_source_loads_snapshot_pinned_explicit_files() {
        let fixture = target_locator_refresh_fixture("explicit_files");
        let ctx = refresh_context_for_target_fixture(&fixture);
        let scan = IcebergMvTargetLocatorScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: ctx.rewrite.target_table_uuid.clone(),
            target_snapshot_id: Some(fixture.target_snapshot_id),
            apply_key_column: "k".to_string(),
            branch_id_column: None,
        };

        let source = ctx
            .target_locator_scan_source(&scan)
            .expect("target locator source");

        let ScanSource::IcebergDataFiles {
            table,
            files,
            binding,
            ..
        } = source
        else {
            panic!("expected target locator explicit IcebergDataFiles");
        };
        assert_eq!(
            binding,
            crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles
        );
        assert_eq!(table.catalog, "tgt");
        assert_eq!(table.namespace, "db");
        assert_eq!(table.table, "mv");
        assert_eq!(
            table.table_uuid.as_deref(),
            Some(ctx.rewrite.target_table_uuid.as_str())
        );
        assert_eq!(table.current_snapshot_id, Some(fixture.target_snapshot_id));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].row_count, Some(2));
        assert!(files[0].path.ends_with(".parquet"));
    }

    #[test]
    fn target_locator_scan_source_rejects_apply_key_mismatch() {
        let fixture = target_locator_refresh_fixture("apply_key_mismatch");
        let ctx = refresh_context_for_target_fixture(&fixture);
        let scan = IcebergMvTargetLocatorScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: ctx.rewrite.target_table_uuid.clone(),
            target_snapshot_id: Some(fixture.target_snapshot_id),
            apply_key_column: "wrong_apply_key".to_string(),
            branch_id_column: None,
        };

        let err = ctx
            .target_locator_scan_source(&scan)
            .expect_err("apply-key mismatch should fail");

        assert!(err.contains("apply-key column mismatch"), "got: {err}");
    }

    fn target_state_source_for_binding_test() -> ScanSource {
        ScanSource::IcebergDataFiles {
            table: IcebergTableInfo {
                catalog: "tgt".to_string(),
                namespace: "db".to_string(),
                table: "mv".to_string(),
                table_uuid: Some("uuid-tgt".to_string()),
                current_snapshot_id: Some(99),
                schema_id: 1,
                location: "s3://bucket/mv".to_string(),
                schema: IcebergSchemaDef { fields: Vec::new() },
                serialized_metadata: None,
                serialized_metadata_rows: None,
            },
            files: vec![
                IcebergDataFileInfo {
                    path: "s3://bucket/mv/data-a.parquet".to_string(),
                    size: 10,
                    row_count: Some(10),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    delete_files: Vec::new(),
                    manifest_path: None,
                    partition_values: Vec::new(),
                },
                IcebergDataFileInfo {
                    path: "s3://bucket/mv/data-b.parquet".to_string(),
                    size: 20,
                    row_count: Some(20),
                    column_stats: None,
                    partition_spec_id: None,
                    partition_key: None,
                    first_row_id: None,
                    data_sequence_number: None,
                    ivm_change_op: None,
                    included_positions: None,
                    delete_files: Vec::new(),
                    manifest_path: None,
                    partition_values: Vec::new(),
                },
            ],
            cloud_properties: BTreeMap::new(),
            binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::ExplicitFiles,
        }
    }

    #[test]
    fn bind_target_state_file_positions_keeps_only_matched_files() {
        let positions = vec![
            crate::engine::mv::iceberg_target_apply::TargetRowPositionSet {
                referenced_data_file: "s3://bucket/mv/data-b.parquet".to_string(),
                positions: vec![2, 8, 13],
            },
        ];

        let source = bind_target_state_file_positions(
            target_state_source_for_binding_test(),
            &positions,
            "tgt.db.mv",
        )
        .expect("bind positions");

        let ScanSource::IcebergDataFiles { files, .. } = source else {
            panic!("expected IcebergDataFiles");
        };
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "s3://bucket/mv/data-b.parquet");
        assert_eq!(files[0].included_positions, Some(vec![2, 8, 13]));
    }

    #[test]
    fn bind_target_state_file_positions_empty_matches_returns_empty_source() {
        let source = bind_target_state_file_positions(
            target_state_source_for_binding_test(),
            &[],
            "tgt.db.mv",
        )
        .expect("bind empty positions");

        let ScanSource::IcebergDataFiles { files, .. } = source else {
            panic!("expected IcebergDataFiles");
        };
        assert!(files.is_empty());
    }

    #[test]
    fn bind_target_state_file_positions_rejects_missing_files() {
        let positions = vec![
            crate::engine::mv::iceberg_target_apply::TargetRowPositionSet {
                referenced_data_file: "s3://bucket/mv/missing.parquet".to_string(),
                positions: vec![1],
            },
        ];

        let err = bind_target_state_file_positions(
            target_state_source_for_binding_test(),
            &positions,
            "tgt.db.mv",
        )
        .expect_err("missing target file should fail");

        assert!(err.contains("locator returned positions for files not present"));
        assert!(err.contains("s3://bucket/mv/missing.parquet"));
    }

    #[test]
    fn target_state_scan_falls_back_without_partition_allow_list() {
        use iceberg::{NamespaceIdent, TableIdent};

        let warehouse_dir = tempfile::TempDir::new()
            .expect("target warehouse tempdir")
            .keep();
        let warehouse = format!("file://{}", warehouse_dir.join("warehouse").display());
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse.clone()),
                ],
            )
            .expect("target entry"),
        );
        let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&target_entry)
                .expect("build hadoop catalog"),
        );
        let schema = make_target_schema();
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec().into_unbound(),
            iceberg::spec::SortOrder::unsorted_order(),
            format!("{warehouse}/target/table"),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let target_table = iceberg::table::Table::builder()
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("db".to_string()),
                "mv".to_string(),
            ))
            .build()
            .expect("target table");
        let mut ctx = IcebergMvRefreshContext {
            rewrite: dummy_rewrite_context(),
            application_target: make_application_target(),
            target_entry,
            base_catalog_entries: BTreeMap::new(),
            iceberg_catalog,
            target_table,
            affected_partitions: crate::mv::model::AffectedTargetPartitions::not_derived(
                "test context",
            ),
            pruning_limits: MvRefreshPruningLimits {
                max_touched_groups: 100_000,
                max_affected_partitions: 2,
            },
        };
        let scan = IcebergMvTargetStateScan {
            catalog: "tgt".to_string(),
            database: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: "uuid-tgt".to_string(),
            target_snapshot_id: Some(99),
            aggregate_state_layout_version: 1,
            columns: Vec::new(),
            group_key_names: Vec::new(),
            aggregate_state_names: Vec::new(),
            physical_column_names: Vec::new(),
            row_id_column_name: "__row_id__".to_string(),
            row_filter: crate::sql::planner::table::IcebergMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name: "__row_id__".to_string(),
                branch_scope: None,
            },
            partition_constraint:
                crate::sql::planner::table::IcebergMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired,
        };

        let unknown_filter = ctx
            .target_state_partition_allow_list(&scan)
            .expect("unknown affected partitions should fall back to full target scan");
        assert!(
            unknown_filter.is_none(),
            "unknown affected partitions should disable pruning"
        );

        let new_key = crate::mv::model::MvPartitionKey::new(1, Vec::new());
        let old_key = crate::mv::model::MvPartitionKey::new(2, Vec::new());
        ctx.affected_partitions =
            crate::mv::model::AffectedTargetPartitions::known([new_key.clone(), old_key.clone()]);
        let allow_list = ctx
            .target_state_partition_allow_list(&scan)
            .expect("known affected partitions should satisfy partition contract")
            .expect("partitioned scan should return an allow-list");
        assert!(allow_list.contains(&new_key));
        assert!(allow_list.contains(&old_key));

        ctx.pruning_limits.max_affected_partitions = 1;
        let threshold_filter = ctx
            .target_state_partition_allow_list(&scan)
            .expect("over-threshold affected partitions should fall back to full target scan");
        assert!(
            threshold_filter.is_none(),
            "over-threshold affected partitions should disable pruning"
        );
        assert_eq!(
            ctx.affected_partitions_to_target_partition_filter(),
            crate::mv::model::TargetPartitionFilter::None,
            "over-threshold affected partitions should disable merge-sink plan-time pruning"
        );
    }
}
