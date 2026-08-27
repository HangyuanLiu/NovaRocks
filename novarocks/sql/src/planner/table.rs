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

use crate::binding::SqlTableBindingId;
#[cfg(any(test, feature = "test-support"))]
use crate::binding::SqlTableBindingScopeId;
use arrow::datatypes::Schema;
use novarocks_types::schema::ColumnDef;

/// Immutable version selector attached to a query-local table binding.
///
/// The selector is a SQL planning fact, not a provider request.  The
/// application materializes the selected table once and assigns the binding
/// token that preparation later uses to recover the exact connector lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlTableVersionSelector {
    Current,
    Snapshot(i64),
    TimestampMillis(i64),
}

/// SQL-level metadata table identity.  Provider-specific metadata APIs are
/// deliberately not represented in the compiler vocabulary.
///
/// This is a closed set. Six of the seven variants are the wire's worker set
/// (`IcebergSystemTableType`: FILES, ENTRIES, SNAPSHOTS, HISTORY, REFS,
/// MANIFESTS); `Partitions` is a view over the same pinned `$files` snapshot
/// and never travels as a worker type. There is no alias, no `ALL_*` variant,
/// and no unknown variant, so every consumer must match exhaustively and a new
/// relation is a compile error rather than a silent gap.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SqlMetadataTableKind {
    Snapshots,
    History,
    Refs,
    Files,
    Manifests,
    Partitions,
    /// Trino `$entries`: the manifest entries of the pinned snapshot.
    ///
    /// This replaces the former `LogicalIcebergMetadata` alias. That alias
    /// accepted two spellings for one relation and described a flattened
    /// `$files` superset rather than Trino's nested `data_file` ROW; both are
    /// gone.
    Entries,
}

impl SqlMetadataTableKind {
    /// Resolve a `<table>$<suffix>` suffix. An unrecognized suffix is refused
    /// here — there is deliberately no catch-all relation to fall back to.
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "snapshots" => Ok(Self::Snapshots),
            "history" => Ok(Self::History),
            "refs" => Ok(Self::Refs),
            "files" => Ok(Self::Files),
            "manifests" => Ok(Self::Manifests),
            "partitions" => Ok(Self::Partitions),
            "entries" => Ok(Self::Entries),
            _ => Err(format!(
                "unsupported Iceberg metadata table type: {value} (expected one of \
                 snapshots, history, refs, files, manifests, partitions, entries)"
            )),
        }
    }
}

/// Immutable SQL facts that characterize a scan without carrying provider
/// metadata, files, credentials, or an executable connector handle.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlScanKind {
    /// A connector-neutral external scan. Its exact execution authority is
    /// recovered by `binding` at the application preparation boundary.
    ConnectorRead,
    /// A scan of one relation restricted to exactly the files a provider froze
    /// for one mutation or rewrite cohort.
    ///
    /// SQL carries no file here and never could: the set is minted by the
    /// connector's own preparation, and the application recovers it by
    /// `binding` at the preparation boundary. It is a kind of its own because
    /// a pinned read may not be narrowed -- neither by a predicate the
    /// optimizer pushes nor by anything the connector would re-derive -- while
    /// an ordinary `Data` scan may be.
    PinnedFileSet,
    /// A scan of the relation one distributed `ALTER TABLE ... EXECUTE`
    /// procedure instance reads.
    ///
    /// SQL carries no procedure facts here: the frozen group is minted by the
    /// connector's own rewrite preparation, and the application recovers it by
    /// `binding` at the preparation boundary. It is a kind of its own because
    /// what such a scan outputs is not the relation's rows -- rewriting delete
    /// artifacts produces the rows those artifacts remove -- so neither a
    /// `Data` predicate nor a pinned file set describes it.
    TableExecute,
    Data {
        version: SqlTableVersionSelector,
    },
    FrozenInputSet {
        version: SqlTableVersionSelector,
    },
    Metadata {
        kind: SqlMetadataTableKind,
        version: SqlTableVersionSelector,
    },
    Delta {
        from_snapshot_id: i64,
        to_snapshot_id: i64,
    },
    MvTargetState {
        facts: SqlMvTargetStateScan,
    },
    MvTargetLocator {
        facts: SqlMvTargetLocatorScan,
    },
}

/// Canonical table identity attached to a compiler scan fact.  This is kept
/// separate from an application catalog handle so the optimizer can reason
/// about identity without being able to reach a provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlTableIdentity {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) table: String,
}

impl SqlTableIdentity {
    /// Construct immutable table identity facts for a request-local SQL
    /// binding. This contains no catalog or connector handle.
    pub fn try_new(catalog: String, namespace: String, table: String) -> Result<Self, String> {
        if catalog.is_empty() || namespace.is_empty() || table.is_empty() {
            return Err("SQL table identity is incomplete".to_string());
        }
        Ok(Self {
            catalog,
            namespace,
            table,
        })
    }
}

/// Immutable SQL facts that make UK/FK rewrites sound for one admitted table
/// binding.  The application projects the two supported constraint properties
/// while it still owns provider metadata; the optimizer only observes this
/// normalized value attached to a `SqlScanSource`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SqlUkFkTableFacts {
    unique_constraints: Vec<Vec<String>>,
    foreign_key_constraints: Vec<SqlUkFkForeignKey>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SqlUkFkForeignKey {
    local_columns: Vec<String>,
    referenced_table: String,
    referenced_columns: Vec<String>,
}

impl SqlUkFkTableFacts {
    /// Project provider-neutral, schema-ordinal constraints into the SQL
    /// optimizer's name-based facts.  The materializer never inspects a
    /// connector table handle to recover these values.
    pub(crate) fn from_connector_planning_facts(
        schema: &Schema,
        facts: &novarocks_spi::connector::ConnectorTablePlanningFacts,
    ) -> Self {
        let column_name = |ordinal: u32| {
            schema
                .fields()
                .get(ordinal as usize)
                .map(|field| field.name().to_ascii_lowercase())
        };
        let unique_constraints = facts
            .unique_constraints()
            .iter()
            .filter_map(|constraint| {
                constraint
                    .column_ordinals()
                    .iter()
                    .map(|ordinal| column_name(*ordinal))
                    .collect::<Option<Vec<_>>>()
            })
            .collect();
        let foreign_key_constraints = facts
            .foreign_key_constraints()
            .iter()
            .filter_map(|constraint| {
                let local_columns = constraint
                    .local_column_ordinals()
                    .iter()
                    .map(|ordinal| column_name(*ordinal))
                    .collect::<Option<Vec<_>>>()?;
                let referenced = constraint.referenced_table();
                let referenced_table = format!("{}.{}", referenced.namespace, referenced.table,);
                Some(SqlUkFkForeignKey {
                    local_columns,
                    referenced_table,
                    referenced_columns: constraint
                        .referenced_column_names()
                        .iter()
                        .map(|column| column.to_ascii_lowercase())
                        .collect(),
                })
            })
            .collect();
        Self {
            unique_constraints,
            foreign_key_constraints,
        }
    }

    pub(crate) fn has_unique_key(&self, columns: &[String]) -> bool {
        self.unique_constraints
            .iter()
            .any(|constraint| same_constraint_columns(constraint, columns))
    }

    pub(crate) fn has_matching_foreign_key(
        &self,
        local_columns: &[String],
        referenced_table: &str,
        referenced_alias: Option<&str>,
        referenced_columns: &[String],
    ) -> bool {
        self.foreign_key_constraints.iter().any(|foreign_key| {
            same_constraint_columns(&foreign_key.local_columns, local_columns)
                && table_name_matches_identity(
                    &foreign_key.referenced_table,
                    referenced_table,
                    referenced_alias,
                )
                && same_constraint_columns(&foreign_key.referenced_columns, referenced_columns)
        })
    }
}

fn normalize_constraint_identifier(value: &str) -> String {
    value
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .to_ascii_lowercase()
}

fn normalize_constraint_table_name(value: &str) -> String {
    value
        .trim()
        .split('.')
        .map(normalize_constraint_identifier)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(".")
}

fn same_constraint_columns(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .all(|column| right.iter().any(|other| other.eq_ignore_ascii_case(column)))
}

fn table_name_matches_identity(expected: &str, table: &str, alias: Option<&str>) -> bool {
    let expected = normalize_constraint_table_name(expected);
    let table = normalize_constraint_table_name(table);
    if expected == table
        || expected.rsplit('.').next().unwrap_or_default() == table
        || expected == table.rsplit('.').next().unwrap_or_default()
    {
        return true;
    }
    alias
        .map(normalize_constraint_table_name)
        .is_some_and(|alias| expected == alias)
}

/// The only scan source a SQL compiler artifact may expose to application
/// preparation.  A token is valid exclusively in the paired
/// `QueryTableBindingStore`; attempts to use it with another request fail
/// before connector submission.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlScanSource {
    pub(crate) binding: SqlTableBindingId,
    pub(crate) table: SqlTableIdentity,
    pub(crate) kind: SqlScanKind,
    ukfk_facts: SqlUkFkTableFacts,
}

impl SqlScanSource {
    pub(crate) fn new(
        binding: SqlTableBindingId,
        table: SqlTableIdentity,
        kind: SqlScanKind,
    ) -> Self {
        Self {
            binding,
            table,
            kind,
            ukfk_facts: SqlUkFkTableFacts::default(),
        }
    }

    /// Attach normalized constraint facts captured from the exact admission
    /// materialization.  No later planner or optimizer phase can replace
    /// these facts by consulting a newer provider generation.
    pub(crate) fn with_ukfk_facts(mut self, facts: SqlUkFkTableFacts) -> Self {
        self.ukfk_facts = facts;
        self
    }

    pub(crate) fn ukfk_facts(&self) -> &SqlUkFkTableFacts {
        &self.ukfk_facts
    }
}

/// Metadata for an IMV target-state scan source. This struct carries only
/// planner-safe metadata for the MV's own target state — column definitions
/// and the aggregate/join logical contract. The scan's canonical table
/// identity and binding token live in the enclosing `SqlScanSource`. It has no
/// execution or catalog handles and is designed to be inspectable during
/// analyzer/optimizer phases without triggering runtime behavior. The
/// standalone refresh codegen lowers this source into the local target-state
/// scan used by aggregate-state merge execution.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlMvTargetStateScan {
    pub(crate) target_table_uuid: String,
    pub(crate) target_snapshot_id: Option<i64>,
    pub(crate) aggregate_state_layout_version: u16,
    pub(crate) columns: Vec<ColumnDef>,
    pub(crate) group_key_names: Vec<String>,
    pub(crate) aggregate_state_names: Vec<String>,
    pub(crate) physical_column_names: Vec<String>,
    pub(crate) row_id_column_name: String,
    pub(crate) row_filter: SqlMvTargetStateRowFilter,
    pub(crate) partition_constraint: SqlMvTargetStatePartitionConstraint,
}

/// Metadata for an IMV target-locator scan source. It is a refresh-only
/// placeholder that reads the MV target at the refresh-before snapshot and
/// projects the physical apply-key columns plus Iceberg `_file` / `_pos`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvTargetLocatorScan {
    pub(crate) target_table_uuid: String,
    pub(crate) target_snapshot_id: Option<i64>,
    pub(crate) apply_key_column: String,
    pub(crate) branch_id_column: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchScope {
    pub(crate) branch_id_column_name: String,
    pub(crate) branch_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlMvTargetStateRowFilter {
    DeltaInputRowIds {
        row_id_column_name: String,
        branch_scope: Option<BranchScope>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlMvTargetStatePartitionConstraint {
    Unpartitioned,
    AffectedPartitionAllowListRequired,
}

impl SqlMvTargetStateScan {
    /// Legacy resolver diagnostics do not carry table identity. Canonical SQL
    /// scans keep that identity on `SqlScanSource`; this label is deliberately
    /// diagnostic-only and must never be used for lookup.
    #[allow(
        dead_code,
        reason = "Target-state names are retained for planner diagnostics."
    )]
    pub(crate) fn fqn(&self) -> &'static str {
        "token-bound MV target"
    }

    pub(crate) fn constraint_summary(&self) -> String {
        let row_filter = match &self.row_filter {
            SqlMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope: None,
            } => {
                format!("row_filter=delta_input_row_ids({row_id_column_name})")
            }
            SqlMvTargetStateRowFilter::DeltaInputRowIds {
                row_id_column_name,
                branch_scope: Some(scope),
            } => format!(
                "row_filter=delta_input_row_ids({row_id_column_name}, {}={})",
                scope.branch_id_column_name, scope.branch_id
            ),
        };
        let partition = match self.partition_constraint {
            SqlMvTargetStatePartitionConstraint::Unpartitioned => "partition=unpartitioned",
            SqlMvTargetStatePartitionConstraint::AffectedPartitionAllowListRequired => {
                "partition=affected_allow_list_required"
            }
        };
        format!(
            "uuid={} snapshot={} layout={} {} {}",
            self.target_table_uuid,
            self.target_snapshot_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "none".to_string()),
            self.aggregate_state_layout_version,
            row_filter,
            partition
        )
    }
}

impl SqlMvTargetLocatorScan {
    /// See `SqlMvTargetStateScan::fqn`: identity belongs to the enclosing
    /// tokenized source, not to locator facts.
    #[allow(
        dead_code,
        reason = "Target-locator names are retained for planner diagnostics."
    )]
    pub(crate) fn fqn(&self) -> &'static str {
        "token-bound MV target"
    }
}

pub(crate) fn sql_mv_target_state_scan(source: &ScanSource) -> Option<&SqlMvTargetStateScan> {
    match source {
        ScanSource::Sql(SqlScanSource {
            kind: SqlScanKind::MvTargetState { facts },
            ..
        }) => Some(facts),
        _ => None,
    }
}

pub(crate) fn sql_mv_target_locator_scan(source: &ScanSource) -> Option<&SqlMvTargetLocatorScan> {
    match source {
        ScanSource::Sql(SqlScanSource {
            kind: SqlScanKind::MvTargetLocator { facts },
            ..
        }) => Some(facts),
        _ => None,
    }
}

/// SQL scan carrier.  Every compiler artifact carries a tokenized SQL source;
/// concrete provider materialization belongs exclusively to the paired
/// application `QueryTableBindingStore`.
#[derive(Clone, Debug)]
pub enum ScanSource {
    Sql(SqlScanSource),
}

/// Build a tokenized scan carrier for owner-side unit tests.  The token is
/// deliberately non-serializable and is only useful for tests that exercise
/// SQL/native shape projection without scan preparation.  Tests that prepare
/// a scan must instead allocate the token from a `QueryTableBindingStore` and
/// retain the matching materialization there.
#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_sql_scan_source(kind: SqlScanKind) -> ScanSource {
    use std::num::{NonZeroU32, NonZeroU64};

    ScanSource::Sql(SqlScanSource::new(
        SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("test scope")),
            NonZeroU32::new(1).expect("test ordinal"),
        ),
        SqlTableIdentity {
            catalog: "test_catalog".to_string(),
            namespace: "test_db".to_string(),
            table: "test_table".to_string(),
        },
        kind,
    ))
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// Iceberg metadata pseudo-columns. `_file` and `_pos` are available for
    /// Iceberg row-identity scans; `_row_id` and
    /// `_last_updated_sequence_number` are exposed only when the table
    /// satisfies v3 row-lineage preconditions. The analyzer registers these
    /// into the per-relation scope as resolvable pseudo-columns but **not**
    /// into `SELECT *` expansion.
    pub iceberg_row_lineage_metadata_columns: Vec<ColumnDef>,
    pub source: ScanSource,
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorRequestContext,
        ConnectorTableForeignKeyConstraint, ConnectorTableIdentity, ConnectorTablePlanningFacts,
        ConnectorTableUniqueConstraint,
    };

    use super::*;

    #[test]
    fn metadata_table_kind_parses_the_seven_relation_suffixes() {
        for (suffix, expected) in [
            ("snapshots", SqlMetadataTableKind::Snapshots),
            ("history", SqlMetadataTableKind::History),
            ("refs", SqlMetadataTableKind::Refs),
            ("files", SqlMetadataTableKind::Files),
            ("manifests", SqlMetadataTableKind::Manifests),
            ("partitions", SqlMetadataTableKind::Partitions),
            ("entries", SqlMetadataTableKind::Entries),
        ] {
            assert_eq!(
                SqlMetadataTableKind::parse(suffix),
                Ok(expected),
                "lowercase {suffix}"
            );
            assert_eq!(
                SqlMetadataTableKind::parse(&suffix.to_uppercase()),
                Ok(expected),
                "uppercase {suffix}"
            );
        }
    }

    #[test]
    fn entries_resolves_without_the_logical_iceberg_metadata_alias() {
        assert_eq!(
            SqlMetadataTableKind::parse("entries"),
            Ok(SqlMetadataTableKind::Entries)
        );
        // The retired alias must not resolve to anything at all.
        let error = SqlMetadataTableKind::parse("logical_iceberg_metadata")
            .expect_err("the LogicalIcebergMetadata alias is removed");
        assert!(error.contains("logical_iceberg_metadata"), "{error}");
    }

    #[test]
    fn unknown_metadata_suffix_is_refused_naming_the_closed_set() {
        // `all_entries` / `all_manifests` are Trino relations this stack does
        // not implement; they must be refused, never silently aliased.
        for unknown in ["all_entries", "all_manifests", "metadata_log_entries", ""] {
            let error = SqlMetadataTableKind::parse(unknown)
                .unwrap_err_or_relation_name(&format!("`{unknown}` must be refused"));
            assert!(
                error.contains("unsupported Iceberg metadata table type"),
                "{error}"
            );
            assert!(error.contains("entries"), "{error}");
        }
    }

    /// Small readability helper: a metadata suffix that resolves is a test
    /// failure, and the panic should say which suffix leaked through.
    trait UnwrapErrOrRelationName {
        fn unwrap_err_or_relation_name(self, context: &str) -> String;
    }

    impl UnwrapErrOrRelationName for Result<SqlMetadataTableKind, String> {
        fn unwrap_err_or_relation_name(self, context: &str) -> String {
            match self {
                Ok(kind) => panic!("{context}, but it resolved to {kind:?}"),
                Err(error) => error,
            }
        }
    }

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn typed_ukfk_facts() -> SqlUkFkTableFacts {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
        ]));
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            4_096,
            4_096,
        )
        .expect("valid connector context");
        let facts = ConnectorTablePlanningFacts::try_new(
            &schema,
            vec![],
            vec![ConnectorTableUniqueConstraint::new(vec![0])],
            vec![ConnectorTableForeignKeyConstraint::new(
                vec![1],
                ConnectorTableIdentity {
                    instance_id: ConnectorInstanceId::parse("iceberg").expect("valid instance ID"),
                    namespace: Arc::from("sales"),
                    table: Arc::from("dim_customer"),
                },
                vec![Arc::from("id")],
            )],
            vec![],
            &context,
        )
        .expect("valid typed planning facts");
        SqlUkFkTableFacts::from_connector_planning_facts(&schema, &facts)
    }

    #[test]
    fn sqlx2_scan_source_contains_only_binding_and_sql_facts() {
        let binding = SqlTableBindingId::new(
            crate::binding::SqlTableBindingScopeId::new(NonZeroU64::new(3).expect("scope")),
            NonZeroU32::new(7).expect("ordinal"),
        );
        let scan = SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
            },
            SqlScanKind::Metadata {
                kind: SqlMetadataTableKind::Snapshots,
                version: SqlTableVersionSelector::Snapshot(42),
            },
        );

        assert_eq!(scan.binding, binding);
        assert_eq!(scan.table.catalog, "ice");
        assert_eq!(scan.table.namespace, "sales");
        assert_eq!(scan.table.table, "orders");
        assert!(matches!(
            scan.kind,
            SqlScanKind::Metadata {
                kind: SqlMetadataTableKind::Snapshots,
                version: SqlTableVersionSelector::Snapshot(42),
            }
        ));
    }

    #[test]
    fn sqlx2_scan_source_keeps_typed_ukfk_facts_on_its_binding() {
        let binding = SqlTableBindingId::new(
            crate::binding::SqlTableBindingScopeId::new(NonZeroU64::new(5).expect("scope")),
            NonZeroU32::new(2).expect("ordinal"),
        );
        let scan = SqlScanSource::new(
            binding,
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "sales".to_string(),
                table: "orders".to_string(),
            },
            SqlScanKind::Data {
                version: SqlTableVersionSelector::Current,
            },
        )
        .with_ukfk_facts(typed_ukfk_facts());

        assert!(scan.ukfk_facts().has_unique_key(&["ID".to_string()]));
        assert!(scan.ukfk_facts().has_matching_foreign_key(
            &["customer_id".to_string()],
            "sales.dim_customer",
            Some("d"),
            &["id".to_string()],
        ));
    }
}

#[cfg(test)]
mod imv_target_state_tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use super::*;
    use crate::binding::SqlTableBindingScopeId;

    fn sample_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "region".to_string(),
                data_type: arrow::datatypes::DataType::Utf8,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "c".to_string(),
                data_type: arrow::datatypes::DataType::Int64,
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ]
    }

    #[test]
    fn sqlx2_mv_target_state_scan_source_carries_logical_contract() {
        let scope = SqlTableBindingScopeId::new(NonZeroU64::new(31).unwrap());
        let source = ScanSource::Sql(SqlScanSource::new(
            SqlTableBindingId::new(scope, NonZeroU32::new(1).unwrap()),
            SqlTableIdentity {
                catalog: "ice".to_string(),
                namespace: "ns".to_string(),
                table: "mv_sales".to_string(),
            },
            SqlScanKind::MvTargetState {
                facts: SqlMvTargetStateScan {
                    target_table_uuid: "target-uuid".to_string(),
                    target_snapshot_id: Some(42),
                    aggregate_state_layout_version: 1,
                    columns: sample_columns(),
                    group_key_names: vec!["region".to_string()],
                    aggregate_state_names: vec!["c".to_string()],
                    physical_column_names: vec!["region".to_string(), "c".to_string()],
                    row_id_column_name: "__row_id__".to_string(),
                    row_filter: SqlMvTargetStateRowFilter::DeltaInputRowIds {
                        row_id_column_name: "__row_id__".to_string(),
                        branch_scope: None,
                    },
                    partition_constraint: SqlMvTargetStatePartitionConstraint::Unpartitioned,
                },
            },
        ));

        let ScanSource::Sql(sql_source) = &source;
        let Some(scan) = sql_mv_target_state_scan(&source) else {
            panic!("expected SQL target-state scan source");
        };
        assert_eq!(sql_source.table.catalog, "ice");
        assert_eq!(sql_source.table.namespace, "ns");
        assert_eq!(sql_source.table.table, "mv_sales");
        assert_eq!(scan.group_key_names, vec!["region"]);
        assert_eq!(scan.aggregate_state_names, vec!["c"]);
        assert_eq!(scan.row_id_column_name, "__row_id__");
        assert!(
            scan.constraint_summary()
                .contains("row_filter=delta_input_row_ids(__row_id__)")
        );
    }

    #[test]
    fn target_state_row_filter_carries_branch_scope() {
        let filter = SqlMvTargetStateRowFilter::DeltaInputRowIds {
            row_id_column_name: "__row_id__".to_string(),
            branch_scope: Some(BranchScope {
                branch_id_column_name: "__branch_id__".to_string(),
                branch_id: 2,
            }),
        };

        let SqlMvTargetStateRowFilter::DeltaInputRowIds {
            branch_scope: Some(scope),
            ..
        } = filter
        else {
            panic!("expected branch scope");
        };
        assert_eq!(scope.branch_id_column_name, "__branch_id__");
        assert_eq!(scope.branch_id, 2);
    }
}
