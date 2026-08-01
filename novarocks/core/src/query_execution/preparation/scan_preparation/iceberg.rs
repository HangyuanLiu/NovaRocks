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
    ConnectorPredicateDisposition, ConnectorPredicateDispositionKind, ConnectorStaticPredicate,
    normalize_predicate_dispositions,
};

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
    context: novarocks_spi::connector::ConnectorRequestContext,
    scan: &PlanScanNode,
    execution: &ResolvedScanExecution,
    static_predicates: Vec<ConnectorStaticPredicate>,
) -> Result<PlannedConnectorRead, String> {
    let ResolvedScanExecution::IcebergFiles(files) = execution else {
        return Err("Iceberg connector planning requires IcebergFiles execution".to_string());
    };
    let projection = effective_scan_column_names(scan)
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
    let planned = crate::connector::iceberg::provider::plan_native_iceberg_read(
        controls,
        context,
        &files.table,
        files.binding,
        &files.files,
        &projection,
        static_predicates.clone(),
    )?;
    let predicate_dispositions =
        normalize_predicate_dispositions(&static_predicates, &planned.scan.predicate_dispositions)
            .map_err(|error| format!("Iceberg connector static predicate response: {error}"))?;
    let residual_predicates = residual_predicates(&scan.predicates, &predicate_dispositions)?;
    Ok(PlannedConnectorRead {
        declaration: planned.declaration,
        scan: planned.scan,
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

/// Plans every Iceberg snapshot-delta role through opaque provider-owned
/// splits.  Core keeps the logical `IcebergDeltaTable` identity for planning,
/// but it does not retain a delta physical reader or a delete-side decoder.
pub(super) fn plan_iceberg_delta_connector_read(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    scan: &PlanScanNode,
    execution: &ResolvedScanExecution,
) -> Result<PlannedConnectorRead, String> {
    let ResolvedScanExecution::IcebergDelta(delta) = execution else {
        return Err("Iceberg delta connector planning requires IcebergDelta execution".to_string());
    };
    let ScanSource::IcebergDeltaTable { table, .. } = &scan.table.source else {
        return Err(
            "Iceberg delta connector planning requires IcebergDeltaTable source".to_string(),
        );
    };
    let planned = crate::connector::iceberg::provider::plan_native_iceberg_delta_read(
        controls,
        context,
        table,
        &delta.runtime_plan.change_files,
        delta.runtime_plan.delete_side.as_ref(),
    )?;
    Ok(PlannedConnectorRead {
        declaration: planned.declaration,
        scan: planned.scan,
        splits: planned.splits,
        planning_metrics: planned.planning_metrics,
        static_predicates: Vec::new(),
        predicate_dispositions: Vec::new(),
        residual_predicates: scan.predicates.clone(),
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
