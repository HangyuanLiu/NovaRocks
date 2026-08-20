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

use crate::catalog::{IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable};
use crate::planner::table::TableDef;
use novarocks_catalog::memory::MemoryCatalog;

const DEFAULT_CATALOG: &str = "default_catalog";

pub(crate) type PlannerMemoryCatalog = MemoryCatalog<TableDef>;

impl PlannerTableProvider for PlannerMemoryCatalog {
    fn resolve_table_for_analysis(
        &self,
        _catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        let planner = self.get(database, table)?;
        Ok(ResolvedAnalyzerTable::from_planner(
            Some(DEFAULT_CATALOG),
            database,
            planner,
        ))
    }

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        Some(self)
    }
}

impl IcebergMetadataTableProvider for PlannerMemoryCatalog {
    fn get_iceberg_metadata_table(
        &self,
        _catalog: Option<&str>,
        database: &str,
        table: &str,
        _metadata_table_type: crate::planner::table::SqlMetadataTableKind,
    ) -> Result<ResolvedAnalyzerTable, String> {
        Ok(ResolvedAnalyzerTable::from_planner(
            Some(DEFAULT_CATALOG),
            database,
            self.get(database, table)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::PlannerMemoryCatalog;
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::catalog::PlannerTableProvider;
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };
    use novarocks_catalog::memory::DEFAULT_DATABASE;
    use novarocks_catalog::schema::ColumnDef;
    use std::num::{NonZeroU32, NonZeroU64};

    fn column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    fn test_scan_source(name: &str, kind: SqlScanKind) -> ScanSource {
        ScanSource::Sql(SqlScanSource::new(
            SqlTableBindingId::new(
                SqlTableBindingScopeId::new(NonZeroU64::new(47).expect("non-zero scope")),
                NonZeroU32::new(
                    name.bytes()
                        .fold(0u32, |ordinal, byte| {
                            ordinal.wrapping_mul(31) + u32::from(byte)
                        })
                        .max(1),
                )
                .expect("non-zero binding ordinal"),
            ),
            SqlTableIdentity {
                catalog: "default_catalog".to_string(),
                namespace: DEFAULT_DATABASE.to_string(),
                table: name.to_string(),
            },
            kind,
        ))
    }

    fn test_table(name: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![column("id", DataType::Int32, false)],
            iceberg_row_lineage_metadata_columns: vec![],
            source: test_scan_source(
                name,
                SqlScanKind::Data {
                    version: SqlTableVersionSelector::Current,
                },
            ),
        }
    }

    fn test_metadata_table(name: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![column("id", DataType::Int64, false)],
            iceberg_row_lineage_metadata_columns: vec![],
            source: test_scan_source(
                name,
                SqlScanKind::Data {
                    version: SqlTableVersionSelector::Current,
                },
            ),
        }
    }

    #[test]
    fn register_overwrites_planner_table() {
        let mut catalog = PlannerMemoryCatalog::default();
        catalog
            .register(DEFAULT_DATABASE, test_table("connector_tbl"))
            .expect("register table");
        let mut replacement = test_table("connector_tbl");
        replacement.columns[0].nullable = true;

        catalog
            .register(DEFAULT_DATABASE, replacement)
            .expect("overwrite table");

        assert!(
            catalog
                .get(DEFAULT_DATABASE, "connector_tbl")
                .expect("overwritten table")
                .columns[0]
                .nullable
        );
    }

    #[test]
    fn sqlx2_local_catalog_accepts_sql_owned_test_binding() {
        let mut catalog = PlannerMemoryCatalog::default();
        catalog
            .register(DEFAULT_DATABASE, test_table("connector_tbl"))
            .expect("register table");

        let table = catalog
            .resolve_table_for_analysis(None, DEFAULT_DATABASE, "connector_tbl")
            .expect("SQL-owned test binding should remain analyzable");
        assert_eq!(table.planner.name, "connector_tbl");
    }

    #[test]
    fn local_metadata_lookup_preserves_analyzer_behavior_and_exact_errors() {
        let mut catalog = PlannerMemoryCatalog::default();
        catalog
            .register(DEFAULT_DATABASE, test_metadata_table("local_ice"))
            .expect("register local iceberg table");

        let statements = novarocks_parser::parse("SELECT * FROM local_ice$snapshots")
            .expect("parse metadata query");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        crate::analyzer::analyze(query, &catalog, DEFAULT_DATABASE)
            .expect("analyze local metadata query");

        let statements = novarocks_parser::parse("SELECT * FROM missing$snapshots")
            .expect("parse missing table query");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        assert_eq!(
            crate::analyzer::analyze(query, &catalog, DEFAULT_DATABASE)
                .expect_err("missing table must fail"),
            "unknown table: missing"
        );

        let statements = novarocks_parser::parse("SELECT * FROM missing_db.local_ice$snapshots")
            .expect("parse missing database query");
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        assert_eq!(
            crate::analyzer::analyze(query, &catalog, DEFAULT_DATABASE)
                .expect_err("missing database must fail"),
            "unknown database: missing_db"
        );
    }
}
