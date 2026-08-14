// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! SQL-owned construction for application-admitted frozen connector reads.
//!
//! Applications retain the provider lease, binding store, and execution
//! resolver.  This module owns the only synthetic SQL scan used to carry that
//! admitted read through SQL planning.  In particular, it does not expose the
//! physical planner tree to Core: callers retain only an opaque scan program
//! until a SQL-owned terminal-planning entry consumes it.

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;

use crate::analysis::OutputColumn;
use crate::binding::SqlTableBindingId;
use crate::catalog::ResolvedAnalyzerTable;
use crate::column_id::ColumnRefFactory;
use crate::plan_read::PlanScanNode;

/// Immutable SQL identity for a synthetic, application-admitted connector
/// scan.  It carries no catalog handle or provider capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrozenConnectorScanIdentity {
    catalog: String,
    namespace: String,
    table: String,
}

impl FrozenConnectorScanIdentity {
    pub fn new(
        catalog: impl Into<String>,
        namespace: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self {
            catalog: catalog.into(),
            namespace: namespace.into(),
            table: table.into(),
        }
    }

    pub fn catalog(&self) -> &str {
        &self.catalog
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    fn planner_identity(&self) -> crate::planner::table::SqlTableIdentity {
        crate::planner::table::SqlTableIdentity {
            catalog: self.catalog.clone(),
            namespace: self.namespace.clone(),
            table: self.table.clone(),
        }
    }
}

/// Opaque synthetic scan program for one admitted frozen connector read.
///
/// It can only be consumed by SQL-owned planning entry points.  `scan()` is a
/// read projection used by Core preparation to match the exact binding; it
/// does not permit construction or mutation of the physical planner graph.
#[derive(Clone, Debug)]
pub struct FrozenConnectorScanPlan(crate::planner::physical::PhysicalPlanNode);

impl FrozenConnectorScanPlan {
    pub fn scan(&self) -> &PlanScanNode {
        let crate::planner::physical::PhysicalPlanKind::Scan(scan) = &self.0.kind else {
            unreachable!("frozen connector scan plan is constructed as one scan")
        };
        scan
    }

    pub fn output_column_count(&self) -> usize {
        self.0.output_columns.len()
    }

    /// Attach SQL predicates before the opaque scan program is sealed into a
    /// distributed plan.  This is used for an already admitted frozen source:
    /// Core later retains these as execution residuals rather than negotiating
    /// them against a newer provider generation.
    pub fn with_predicates(mut self, predicates: Vec<crate::plan_read::TypedExpr>) -> Self {
        let crate::planner::physical::PhysicalPlanKind::Scan(scan) = &mut self.0.kind else {
            unreachable!("frozen connector scan plan is constructed as one scan")
        };
        scan.predicates = predicates;
        self
    }

    pub(crate) fn into_physical(self) -> crate::planner::physical::PhysicalPlanNode {
        self.0
    }
}

/// Build the query-local analyzer table for an admitted frozen connector
/// source.  The caller still owns the binding-store lifetime and all provider
/// authority; SQL receives only a token, static identity, and Arrow schema.
pub fn frozen_connector_resolved_analyzer_table(
    identity: &FrozenConnectorScanIdentity,
    input_schema: SchemaRef,
    binding: SqlTableBindingId,
) -> ResolvedAnalyzerTable {
    let columns = column_defs(&input_schema);
    let planner_identity = identity.planner_identity();
    ResolvedAnalyzerTable::from_planner(
        Some(identity.catalog()),
        identity.namespace(),
        crate::planner::table::TableDef {
            name: identity.table().to_string(),
            columns,
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: crate::planner::table::ScanSource::Sql(
                crate::planner::table::SqlScanSource::new(
                    binding,
                    planner_identity,
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            ),
        },
    )
}

/// Construct the sole synthetic scan carrier accepted for a frozen connector
/// source.  The physical tree stays opaque outside SQL.
pub fn build_frozen_connector_scan_plan(
    identity: &FrozenConnectorScanIdentity,
    input_schema: &SchemaRef,
    binding: SqlTableBindingId,
) -> FrozenConnectorScanPlan {
    let mut factory = ColumnRefFactory::new();
    let mut output_columns = Vec::with_capacity(input_schema.fields().len());
    for field in input_schema.fields() {
        output_columns.push(OutputColumn {
            column_id: factory.create(
                None,
                field.name().to_string(),
                field.data_type().clone(),
                field.is_nullable(),
            ),
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            is_internal: false,
        });
    }
    let table = crate::planner::table::TableDef {
        name: identity.table().to_string(),
        columns: column_defs(input_schema),
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: crate::planner::table::ScanSource::Sql(crate::planner::table::SqlScanSource::new(
            binding,
            identity.planner_identity(),
            crate::planner::table::SqlScanKind::ConnectorRead,
        )),
    };
    FrozenConnectorScanPlan(crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::Scan(PlanScanNode {
            database: identity.namespace().to_string(),
            table,
            alias: None,
            columns: output_columns.clone(),
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
        children: Vec::new(),
        output_columns,
        stats: crate::planner::physical::PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: crate::planner::physical::PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        },
        probe_runtime_filters: Vec::new(),
    })
}

/// Compare a sealed/read-only scan projection with the exact application
/// binding that admitted a frozen connector source.
pub fn matches_frozen_connector_scan(
    scan: &PlanScanNode,
    binding: SqlTableBindingId,
    identity: &FrozenConnectorScanIdentity,
) -> bool {
    let crate::planner::table::ScanSource::Sql(source) = &scan.table.source;
    source.kind == crate::planner::table::SqlScanKind::ConnectorRead
        && source.binding == binding
        && source.table == identity.planner_identity()
}

fn column_defs(input_schema: &SchemaRef) -> Vec<novarocks_catalog::schema::ColumnDef> {
    input_schema
        .fields()
        .iter()
        .map(|field| novarocks_catalog::schema::ColumnDef {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: None,
            logical_type: None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{
        build_frozen_connector_scan_plan, frozen_connector_resolved_analyzer_table,
        matches_frozen_connector_scan, FrozenConnectorScanIdentity,
    };
    use crate::binding::SqlTableBindingId;

    fn binding() -> SqlTableBindingId {
        SqlTableBindingId::new_for_test(7)
    }

    #[test]
    fn frozen_scan_program_preserves_only_static_identity_and_binding() {
        let identity = FrozenConnectorScanIdentity::new("__frozen", "operation", "cohort_7");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let plan = build_frozen_connector_scan_plan(&identity, &schema, binding());

        assert_eq!(plan.output_column_count(), 1);
        assert!(matches_frozen_connector_scan(
            plan.scan(),
            binding(),
            &identity
        ));
        assert!(!matches_frozen_connector_scan(
            plan.scan(),
            SqlTableBindingId::new_for_test(8),
            &identity,
        ));
    }

    #[test]
    fn frozen_analyzer_table_carries_the_same_sql_identity() {
        let identity = FrozenConnectorScanIdentity::new("__frozen", "operation", "cohort_7");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let _resolved = frozen_connector_resolved_analyzer_table(&identity, schema, binding());
    }
}
