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

use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorPredicateDisposition,
    ConnectorPredicateDispositionKind, ConnectorReadSelector, ConnectorSplitPlanningRequest,
    ConnectorStaticPredicate, normalize_predicate_dispositions,
};

use crate::engine::query_planning::bindings::QueryScanMaterialization;
use crate::query_execution::preparation::scan::{PlannedConnectorRead, ResolvedScanExecution};
use crate::sql::analysis::TypedExpr;
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::table::ScanSource;

use super::projection::effective_scan_column_names;

pub(crate) fn build_iceberg_metadata_scan_range_params()
-> crate::runtime::scan_range::ScanRangeParams {
    use crate::runtime::scan_range::{FileFormat, FileScanRange, ScanRangeParams};

    ScanRangeParams::file(FileScanRange {
        file_format: FileFormat::Parquet,
        full_path: Some("iceberg-metadata".to_string()),
        relative_path: None,
        table_id: None,
        offset: 0,
        length: 0,
        file_length: 0,
        delete_files: Vec::new(),
        deletion_vector_descriptor: None,
        first_row_id: None,
        data_sequence_number: None,
        modification_time: None,
        datacache_options: None,
        candidate_node: None,
        included_positions: Vec::new(),
        serialized_split: Some(String::new()),
        use_iceberg_jni_metadata_reader: true,
        ivm_change_op: None,
        file_pruning_min_max_values: None,
    })
}

/// Plans executable opaque splits through the real Iceberg connector instance.
/// Native scheduling owns only the resulting SPI identities and byte-size
/// hints; it must not lower these splits back into `FileScanRange`.
pub(super) fn plan_iceberg_connector_read(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    exact_lease: Option<novarocks_spi::connector::ConnectorControlPlanningLease>,
    context: novarocks_spi::connector::ConnectorRequestContext,
    scan: &PlanScanNode,
    execution: &ResolvedScanExecution,
    static_predicates: Vec<ConnectorStaticPredicate>,
    target_parallelism: std::num::NonZeroUsize,
    max_split_bytes: Option<std::num::NonZeroU64>,
) -> Result<PlannedConnectorRead, String> {
    let ResolvedScanExecution::IcebergFiles(files) = execution else {
        return Err("Iceberg connector planning requires IcebergFiles execution".to_string());
    };
    let requested_projection = effective_scan_column_names(scan)
        .iter()
        .filter_map(|name| {
            files
                .table
                .schema
                .fields
                .iter()
                .position(|field| field.name.eq_ignore_ascii_case(name))
        })
        .collect::<Vec<_>>();
    // The connector treats an empty projection as all provider fields.  Make
    // that implicit choice explicit before sealing the read so every output
    // field has a stable ordinal for scan-domain evaluation.
    let projection = if requested_projection.is_empty() {
        (0..files.table.schema.fields.len()).collect()
    } else {
        requested_projection
    };
    let planned = match exact_lease {
        Some(lease) => crate::connector::iceberg::provider::plan_native_iceberg_read_with_lease(
            lease,
            context,
            &files.table,
            files.binding,
            &files.files,
            &projection,
            static_predicates.clone(),
            target_parallelism,
            max_split_bytes,
        )?,
        // Callers that construct plans outside SQL compilation (test-only
        // native encoding fixtures and legacy internal statistics plans) do
        // not own a query catalog binding. Production SQL preparation passes
        // an exact lease above and never reaches this fallback.
        None => crate::connector::iceberg::provider::plan_native_iceberg_read(
            controls,
            context,
            &files.table,
            files.binding,
            &files.files,
            &projection,
            static_predicates.clone(),
            target_parallelism,
            max_split_bytes,
        )?,
    };
    let predicate_dispositions =
        normalize_predicate_dispositions(&static_predicates, &planned.scan.predicate_dispositions)
            .map_err(|error| format!("Iceberg connector static predicate response: {error}"))?;
    let residual_predicates = residual_predicates(&scan.predicates, &predicate_dispositions)?;
    let provider_field_ordinals = projection
        .into_iter()
        .map(|ordinal| {
            u32::try_from(ordinal)
                .map_err(|_| "Iceberg provider field ordinal does not fit u32".to_string())
        })
        .collect::<Result<_, _>>()?;
    Ok(PlannedConnectorRead {
        declaration: planned.declaration,
        scan: planned.scan,
        provider_field_ordinals,
        splits: planned.splits,
        planning_metrics: planned.planning_metrics,
        static_predicates,
        predicate_dispositions,
        residual_predicates,
        batch: planned.batch,
        planning_lease: Some(planned.planning_lease),
        read_session: None,
    })
}

/// Plan an admitted connector read without decoding or reconstructing a
/// provider handle.  Projection ordinals are derived exclusively from the
/// schema frozen by `ConnectorMetadata::load_table`; a missing or ambiguous
/// column is a preparation error rather than an opportunity for Core to map
/// provider field identities.
pub(super) fn plan_connector_read(
    context: novarocks_spi::connector::ConnectorRequestContext,
    scan: &PlanScanNode,
    materialization: &QueryScanMaterialization,
    static_predicates: Vec<ConnectorStaticPredicate>,
    target_parallelism: std::num::NonZeroUsize,
    max_split_bytes: Option<std::num::NonZeroU64>,
) -> Result<PlannedConnectorRead, String> {
    let QueryScanMaterialization::ConnectorRead {
        table,
        schema,
        selector,
        planning_lease,
        ..
    } = materialization
    else {
        return Err(
            "generic connector planning requires a connector-read materialization".to_string(),
        );
    };
    let projection_names = effective_scan_column_names(scan);
    let mut projection = Vec::with_capacity(projection_names.len());
    for name in projection_names {
        let mut matching = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name().eq_ignore_ascii_case(&name));
        let Some((ordinal, _)) = matching.next() else {
            return Err(format!(
                "connector read schema is missing projected column '{name}'"
            ));
        };
        if matching.next().is_some() {
            return Err(format!(
                "connector read schema has ambiguous projected column '{name}'"
            ));
        }
        projection.push(ordinal);
    }
    let binding = planning_lease.binding();
    if table.owner() != &binding.descriptor().instance_id {
        return Err(
            "connector read table handle owner does not match its planning lease".to_string(),
        );
    }
    let declaration = binding
        .execution_declaration(&context)
        .map_err(|error| error.to_string())?;
    let batch = ConnectorBatchBudget {
        max_rows: std::num::NonZeroUsize::new(4096).expect("batch rows are nonzero"),
        max_bytes: std::num::NonZeroUsize::new(
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        )
        .expect("batch bytes are nonzero"),
    };
    let connector_scan = binding
        .planning()
        .begin_scan(
            table,
            ConnectorBeginScanRequest {
                projection: projection.clone(),
                static_predicates: static_predicates.clone(),
                selector: *selector,
                limit: None,
                batch,
                context: context.clone(),
            },
        )
        .map_err(|error| error.to_string())?;
    let expected_fields = projection
        .iter()
        .map(|ordinal| schema.fields()[*ordinal].clone())
        .collect::<Vec<_>>();
    if connector_scan.output_schema.fields().as_ref() != expected_fields.as_slice() {
        return Err(
            "connector read returned a schema that does not match the admitted projection"
                .to_string(),
        );
    }
    let predicate_dispositions = normalize_predicate_dispositions(
        &static_predicates,
        &connector_scan.predicate_dispositions,
    )
    .map_err(|error| format!("connector static predicate response: {error}"))?;
    let residual_predicates = residual_predicates(&scan.predicates, &predicate_dispositions)?;
    let split_result = binding
        .planning()
        .plan_splits(
            &connector_scan.handle,
            ConnectorSplitPlanningRequest {
                target_parallelism,
                max_split_bytes,
                context,
            },
        )
        .map_err(|error| error.to_string())?;
    if split_result
        .splits
        .iter()
        .any(|split| split.owner() != &binding.descriptor().instance_id)
    {
        return Err("connector read planned a split for another instance".to_string());
    }
    Ok(PlannedConnectorRead {
        declaration,
        scan: connector_scan,
        splits: split_result.splits,
        planning_metrics: split_result.metrics,
        static_predicates,
        predicate_dispositions,
        residual_predicates,
        batch,
        planning_lease: Some(planning_lease.clone()),
        read_session: split_result.session,
    })
}

/// Plans every Iceberg snapshot-delta role through opaque provider-owned
/// splits.  Core keeps the logical `IcebergDeltaTable` identity for planning,
/// but it does not retain a delta physical reader or a delete-side decoder.
pub(super) fn plan_iceberg_delta_connector_read(
    exact_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    context: novarocks_spi::connector::ConnectorRequestContext,
    table: &novarocks_connector_iceberg::scan_model::IcebergTableInfo,
    predicates: &[TypedExpr],
    execution: &ResolvedScanExecution,
    target_parallelism: std::num::NonZeroUsize,
    max_split_bytes: Option<std::num::NonZeroU64>,
) -> Result<PlannedConnectorRead, String> {
    let ResolvedScanExecution::IcebergDelta(delta) = execution else {
        return Err("Iceberg delta connector planning requires IcebergDelta execution".to_string());
    };
    let planned = crate::connector::iceberg::provider::plan_native_iceberg_delta_read_with_lease(
        exact_lease,
        context,
        table,
        &delta.runtime_plan.change_files,
        delta.runtime_plan.delete_side.as_ref(),
        target_parallelism,
        max_split_bytes,
    )?;
    let provider_field_ordinals = (0..planned.scan.output_schema.fields().len())
        .map(|ordinal| {
            u32::try_from(ordinal)
                .map_err(|_| "Iceberg delta provider field ordinal does not fit u32".to_string())
        })
        .collect::<Result<_, _>>()?;
    Ok(PlannedConnectorRead {
        declaration: planned.declaration,
        scan: planned.scan,
        provider_field_ordinals,
        splits: planned.splits,
        planning_metrics: planned.planning_metrics,
        static_predicates: Vec::new(),
        predicate_dispositions: Vec::new(),
        residual_predicates: predicates.to_vec(),
        batch: planned.batch,
        planning_lease: Some(planned.planning_lease),
        read_session: None,
    })
}

fn residual_predicates(
    predicates: &[TypedExpr],
    dispositions: &[ConnectorPredicateDisposition],
) -> Result<Vec<TypedExpr>, String> {
    let exact = dispositions
        .iter()
        .filter(|disposition| disposition.kind == ConnectorPredicateDispositionKind::Exact)
        .map(|disposition| usize::try_from(disposition.predicate_id.0))
        .collect::<Result<std::collections::BTreeSet<_>, _>>()
        .map_err(|_| "connector predicate ID does not fit the local ordinal".to_string())?;
    Ok(predicates
        .iter()
        .enumerate()
        .filter(|(ordinal, _)| !exact.contains(ordinal))
        .map(|(_, predicate)| predicate.clone())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue};
    use arrow::datatypes::DataType;
    use novarocks_spi::connector::{ConnectorPredicateDisposition, ConnectorStaticPredicateId};

    fn predicate(value: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(value)),
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    #[test]
    fn only_exact_dispositions_remove_ordered_core_residuals() {
        let predicates = vec![predicate(false), predicate(true), predicate(false)];
        let dispositions = vec![
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(0),
                kind: ConnectorPredicateDispositionKind::Exact,
            },
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(1),
                kind: ConnectorPredicateDispositionKind::PruningOnly,
            },
            ConnectorPredicateDisposition {
                predicate_id: ConnectorStaticPredicateId(2),
                kind: ConnectorPredicateDispositionKind::Exact,
            },
        ];

        let residual = residual_predicates(&predicates, &dispositions).unwrap();
        assert_eq!(residual.len(), 1);
        assert!(matches!(
            &residual[0].kind,
            ExprKind::Literal(LiteralValue::Bool(true))
        ));
    }
}
