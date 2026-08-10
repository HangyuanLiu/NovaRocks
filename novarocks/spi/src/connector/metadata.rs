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

use arrow::datatypes::SchemaRef;
use bytes::Bytes;

use super::{
    ConnectorError, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableHandle,
    StatisticsDataVersion,
};

/// Arrow field metadata key for connector fields that participate in a read
/// schema but must not be exposed as SQL target columns.  Core preserves the
/// field and its ordinal for connector scan planning, while generic DML
/// admission omits it from SQL-owned write shaping.
pub const CONNECTOR_FIELD_HIDDEN_FROM_SQL: &str = "novarocks.connector.hidden_from_sql";

/// Upper bounds for the provider-neutral facts returned together with one
/// connector table schema. These facts are request-local metadata, not a
/// durable connector contract or a table-handle payload.
pub const MAX_CONNECTOR_TABLE_PLANNING_FACT_COLUMNS: usize = 4_096;
pub const MAX_CONNECTOR_TABLE_PLANNING_FACT_UNIQUE_CONSTRAINTS: usize = 1_024;
pub const MAX_CONNECTOR_TABLE_PLANNING_FACT_FOREIGN_KEY_CONSTRAINTS: usize = 1_024;
pub const MAX_CONNECTOR_TABLE_PLANNING_FACT_CONSTRAINT_COLUMNS: usize = 256;

const TABLE_PLANNING_FACT_COLUMN_BYTES: usize = 16;
const TABLE_PLANNING_FACT_CONSTRAINT_BYTES: usize = 16;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorNamespaceIdentity {
    pub instance_id: ConnectorInstanceId,
    pub namespace: Arc<str>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorTableIdentity {
    pub instance_id: ConnectorInstanceId,
    pub namespace: Arc<str>,
    pub table: Arc<str>,
}

/// SQL exposure of one field in the frozen Arrow schema.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorTableColumnVisibility {
    #[default]
    Sql,
    Hidden,
}

/// SQL semantic kind whose meaning cannot be recovered from the Arrow storage
/// type alone.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorTableColumnSemanticKind {
    #[default]
    None,
    Bitmap,
    Hll,
}

/// Connector-owned role of one field in the frozen Arrow schema.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorTableColumnRole {
    #[default]
    Ordinary,
    RowLineageSystem,
}

/// Provider-neutral planning facts for one Arrow schema field. The ordinal is
/// deliberately explicit so Core can project facts without inspecting a
/// provider-private table-handle payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableColumnPlanningFact {
    field_ordinal: u32,
    visibility: ConnectorTableColumnVisibility,
    semantic_kind: ConnectorTableColumnSemanticKind,
    role: ConnectorTableColumnRole,
}

impl ConnectorTableColumnPlanningFact {
    pub const fn new(
        field_ordinal: u32,
        visibility: ConnectorTableColumnVisibility,
        semantic_kind: ConnectorTableColumnSemanticKind,
        role: ConnectorTableColumnRole,
    ) -> Self {
        Self {
            field_ordinal,
            visibility,
            semantic_kind,
            role,
        }
    }

    pub const fn field_ordinal(&self) -> u32 {
        self.field_ordinal
    }

    pub const fn visibility(&self) -> ConnectorTableColumnVisibility {
        self.visibility
    }

    pub const fn semantic_kind(&self) -> ConnectorTableColumnSemanticKind {
        self.semantic_kind
    }

    pub const fn role(&self) -> ConnectorTableColumnRole {
        self.role
    }
}

/// A canonical unique-key declaration expressed using fields of the frozen
/// Arrow schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableUniqueConstraint {
    column_ordinals: Vec<u32>,
}

impl ConnectorTableUniqueConstraint {
    pub fn new(column_ordinals: Vec<u32>) -> Self {
        Self { column_ordinals }
    }

    pub fn column_ordinals(&self) -> &[u32] {
        &self.column_ordinals
    }
}

/// A canonical foreign-key declaration. Local columns are schema ordinals;
/// the referenced table is a connector identity and its column names are
/// canonical SQL names. Provider-private IDs never cross this boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorTableForeignKeyConstraint {
    local_column_ordinals: Vec<u32>,
    referenced_table: ConnectorTableIdentity,
    referenced_column_names: Vec<Arc<str>>,
}

impl ConnectorTableForeignKeyConstraint {
    pub fn new(
        local_column_ordinals: Vec<u32>,
        referenced_table: ConnectorTableIdentity,
        referenced_column_names: Vec<Arc<str>>,
    ) -> Self {
        Self {
            local_column_ordinals,
            referenced_table,
            referenced_column_names,
        }
    }

    pub fn local_column_ordinals(&self) -> &[u32] {
        &self.local_column_ordinals
    }

    pub fn referenced_table(&self) -> &ConnectorTableIdentity {
        &self.referenced_table
    }

    pub fn referenced_column_names(&self) -> &[Arc<str>] {
        &self.referenced_column_names
    }
}

/// Bounded provider-neutral facts needed by Core to materialize SQL table
/// columns and optimizer UK/FK facts. Providers that have no additional facts
/// return [`Self::empty`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectorTablePlanningFacts {
    column_facts: Vec<ConnectorTableColumnPlanningFact>,
    unique_constraints: Vec<ConnectorTableUniqueConstraint>,
    foreign_key_constraints: Vec<ConnectorTableForeignKeyConstraint>,
}

impl ConnectorTablePlanningFacts {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn try_new(
        schema: &SchemaRef,
        column_facts: Vec<ConnectorTableColumnPlanningFact>,
        mut unique_constraints: Vec<ConnectorTableUniqueConstraint>,
        mut foreign_key_constraints: Vec<ConnectorTableForeignKeyConstraint>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_column_facts(schema, &column_facts)?;
        validate_unique_constraints(schema, &mut unique_constraints)?;
        validate_foreign_key_constraints(schema, &mut foreign_key_constraints)?;

        let bytes =
            planning_facts_bytes(&column_facts, &unique_constraints, &foreign_key_constraints);
        if bytes > context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::ResourceExhausted,
                "connector table planning facts exceed request total payload budget",
            ));
        }

        Ok(Self {
            column_facts,
            unique_constraints,
            foreign_key_constraints,
        })
    }

    pub fn column_facts(&self) -> &[ConnectorTableColumnPlanningFact] {
        &self.column_facts
    }

    pub fn unique_constraints(&self) -> &[ConnectorTableUniqueConstraint] {
        &self.unique_constraints
    }

    pub fn foreign_key_constraints(&self) -> &[ConnectorTableForeignKeyConstraint] {
        &self.foreign_key_constraints
    }
}

fn validate_column_facts(
    schema: &SchemaRef,
    column_facts: &[ConnectorTableColumnPlanningFact],
) -> Result<(), ConnectorError> {
    if column_facts.is_empty() {
        return Ok(());
    }
    if column_facts.len() > MAX_CONNECTOR_TABLE_PLANNING_FACT_COLUMNS {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts exceed the column fact limit",
        ));
    }
    if column_facts.len() != schema.fields().len() {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts do not cover the frozen schema",
        ));
    }
    for (expected, fact) in column_facts.iter().enumerate() {
        let expected = u32::try_from(expected).map_err(|_| {
            ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table schema ordinal does not fit u32",
            )
        })?;
        if fact.field_ordinal != expected {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts contain a duplicate or misaligned schema ordinal",
            ));
        }
    }
    Ok(())
}

fn validate_unique_constraints(
    schema: &SchemaRef,
    constraints: &mut Vec<ConnectorTableUniqueConstraint>,
) -> Result<(), ConnectorError> {
    if constraints.len() > MAX_CONNECTOR_TABLE_PLANNING_FACT_UNIQUE_CONSTRAINTS {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts exceed the unique constraint limit",
        ));
    }
    for constraint in constraints.iter_mut() {
        validate_local_constraint_columns(schema, &mut constraint.column_ordinals)?;
    }
    constraints.sort_by(|left, right| left.column_ordinals.cmp(&right.column_ordinals));
    if constraints.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts contain duplicate unique constraints",
        ));
    }
    Ok(())
}

fn validate_foreign_key_constraints(
    schema: &SchemaRef,
    constraints: &mut Vec<ConnectorTableForeignKeyConstraint>,
) -> Result<(), ConnectorError> {
    if constraints.len() > MAX_CONNECTOR_TABLE_PLANNING_FACT_FOREIGN_KEY_CONSTRAINTS {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts exceed the foreign key constraint limit",
        ));
    }
    for constraint in constraints.iter_mut() {
        if constraint.referenced_table.namespace.is_empty()
            || constraint.referenced_table.table.is_empty()
            || constraint.local_column_ordinals.len() != constraint.referenced_column_names.len()
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts contain an invalid foreign key constraint",
            ));
        }
        if constraint.local_column_ordinals.len()
            > MAX_CONNECTOR_TABLE_PLANNING_FACT_CONSTRAINT_COLUMNS
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts foreign key exceeds the column limit",
            ));
        }

        let mut pairs = constraint
            .local_column_ordinals
            .iter()
            .copied()
            .zip(constraint.referenced_column_names.iter().cloned())
            .collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(&right.0));
        if pairs.iter().any(|(_, column)| column.trim().is_empty())
            || pairs.windows(2).any(|pair| pair[0].0 == pair[1].0)
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts contain duplicate or empty foreign key columns",
            ));
        }
        if pairs
            .iter()
            .any(|(ordinal, _)| *ordinal as usize >= schema.fields().len())
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts foreign key references an unknown local column",
            ));
        }
        let mut referenced_names = pairs
            .iter()
            .map(|(_, name)| Arc::<str>::from(name.to_ascii_lowercase()))
            .collect::<Vec<_>>();
        referenced_names.sort();
        if referenced_names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector table planning facts foreign key repeats a referenced column",
            ));
        }
        constraint.local_column_ordinals = pairs.iter().map(|(ordinal, _)| *ordinal).collect();
        constraint.referenced_column_names = pairs
            .into_iter()
            .map(|(_, name)| Arc::<str>::from(name.to_ascii_lowercase()))
            .collect();
    }
    constraints.sort_by(compare_foreign_key_constraints);
    if constraints.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts contain duplicate foreign key constraints",
        ));
    }
    Ok(())
}

fn validate_local_constraint_columns(
    schema: &SchemaRef,
    columns: &mut Vec<u32>,
) -> Result<(), ConnectorError> {
    if columns.is_empty() || columns.len() > MAX_CONNECTOR_TABLE_PLANNING_FACT_CONSTRAINT_COLUMNS {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts contain an invalid constraint column count",
        ));
    }
    columns.sort_unstable();
    if columns.windows(2).any(|pair| pair[0] == pair[1])
        || columns
            .iter()
            .any(|ordinal| *ordinal as usize >= schema.fields().len())
    {
        return Err(ConnectorError::new(
            super::ConnectorErrorKind::CorruptData,
            "connector table planning facts reference an unknown or duplicate schema column",
        ));
    }
    Ok(())
}

fn compare_foreign_key_constraints(
    left: &ConnectorTableForeignKeyConstraint,
    right: &ConnectorTableForeignKeyConstraint,
) -> std::cmp::Ordering {
    left.local_column_ordinals
        .cmp(&right.local_column_ordinals)
        .then_with(|| {
            left.referenced_table
                .instance_id
                .cmp(&right.referenced_table.instance_id)
        })
        .then_with(|| {
            left.referenced_table
                .namespace
                .cmp(&right.referenced_table.namespace)
        })
        .then_with(|| {
            left.referenced_table
                .table
                .cmp(&right.referenced_table.table)
        })
        .then_with(|| {
            left.referenced_column_names
                .cmp(&right.referenced_column_names)
        })
}

fn planning_facts_bytes(
    column_facts: &[ConnectorTableColumnPlanningFact],
    unique_constraints: &[ConnectorTableUniqueConstraint],
    foreign_key_constraints: &[ConnectorTableForeignKeyConstraint],
) -> usize {
    column_facts
        .len()
        .saturating_mul(TABLE_PLANNING_FACT_COLUMN_BYTES)
        .saturating_add(unique_constraints.iter().fold(0usize, |bytes, constraint| {
            bytes
                .saturating_add(TABLE_PLANNING_FACT_CONSTRAINT_BYTES)
                .saturating_add(
                    constraint
                        .column_ordinals
                        .len()
                        .saturating_mul(std::mem::size_of::<u32>()),
                )
        }))
        .saturating_add(
            foreign_key_constraints
                .iter()
                .fold(0usize, |bytes, constraint| {
                    bytes
                        .saturating_add(TABLE_PLANNING_FACT_CONSTRAINT_BYTES)
                        .saturating_add(
                            constraint
                                .local_column_ordinals
                                .len()
                                .saturating_mul(std::mem::size_of::<u32>()),
                        )
                        .saturating_add(constraint.referenced_table.instance_id.as_str().len())
                        .saturating_add(constraint.referenced_table.namespace.len())
                        .saturating_add(constraint.referenced_table.table.len())
                        .saturating_add(
                            constraint
                                .referenced_column_names
                                .iter()
                                .map(|name| name.len())
                                .sum::<usize>(),
                        )
                }),
        )
}

#[derive(Clone)]
pub struct ConnectorTableMetadata {
    pub identity: ConnectorTableIdentity,
    pub schema: SchemaRef,
    /// Bounded provider-neutral facts aligned to `schema`. Empty facts retain
    /// the historical provider-neutral defaults.
    pub planning_facts: ConnectorTablePlanningFacts,
    /// Provider-owned schema identity. This remains deliberately distinct
    /// from the data-version pin used by statistics and scan planning.
    pub version: Option<Bytes>,
    /// Opaque data-version resolved together with this table metadata. Core
    /// must pass this exact pin to both scan and statistics consumers rather
    /// than resolving `latest` a second time.
    pub statistics_data_version: Option<StatisticsDataVersion>,
    pub table: ConnectorTableHandle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorTableResolution {
    StrictBaseTable,
    ProviderReadAlias,
}

#[derive(Clone)]
pub struct ConnectorNamespaceRequest {
    pub namespace: ConnectorNamespaceIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorTableRequest {
    pub table: ConnectorTableIdentity,
    pub resolution: ConnectorTableResolution,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorListTablesRequest {
    pub namespace: ConnectorNamespaceIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorListNamespacesRequest {
    pub instance_id: ConnectorInstanceId,
    pub context: ConnectorRequestContext,
}

#[derive(Clone)]
pub struct ConnectorReadReferenceFactsRequest {
    pub table: ConnectorTableIdentity,
    pub context: ConnectorRequestContext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadReferenceKind {
    Branch,
    Tag,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadNamedReference {
    pub name: Arc<str>,
    pub kind: ConnectorReadReferenceKind,
    pub snapshot_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadSnapshotLogEntry {
    pub snapshot_id: i64,
    pub timestamp_millis: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadReferenceFacts {
    snapshot_ids: Vec<i64>,
    snapshot_log: Vec<ConnectorReadSnapshotLogEntry>,
    named_references: Vec<ConnectorReadNamedReference>,
    current_snapshot_id: Option<i64>,
}

impl ConnectorReadReferenceFacts {
    pub fn try_new(
        mut snapshot_ids: Vec<i64>,
        mut snapshot_log: Vec<ConnectorReadSnapshotLogEntry>,
        mut named_references: Vec<ConnectorReadNamedReference>,
        current_snapshot_id: Option<i64>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        snapshot_ids.sort_unstable();
        if snapshot_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts contain duplicate snapshot IDs",
            ));
        }

        let contains_snapshot = |snapshot_id| snapshot_ids.binary_search(&snapshot_id).is_ok();
        if current_snapshot_id.is_some_and(|snapshot_id| !contains_snapshot(snapshot_id)) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts current snapshot is not listed",
            ));
        }

        snapshot_log.sort_by_key(|entry| (entry.timestamp_millis, entry.snapshot_id));
        if snapshot_log
            .iter()
            .any(|entry| !contains_snapshot(entry.snapshot_id))
        {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts snapshot log references an unknown snapshot",
            ));
        }
        if snapshot_log.windows(2).any(|pair| {
            pair[0].timestamp_millis == pair[1].timestamp_millis
                && pair[0].snapshot_id == pair[1].snapshot_id
        }) {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::CorruptData,
                "connector read reference facts contain duplicate snapshot-log entries",
            ));
        }

        named_references.sort_by(|left, right| left.name.cmp(&right.name));
        let mut previous_name: Option<&str> = None;
        for reference in &named_references {
            if reference.name.is_empty() || !contains_snapshot(reference.snapshot_id) {
                return Err(ConnectorError::new(
                    super::ConnectorErrorKind::CorruptData,
                    "connector read reference facts contain an invalid named reference",
                ));
            }
            if previous_name == Some(reference.name.as_ref()) {
                return Err(ConnectorError::new(
                    super::ConnectorErrorKind::CorruptData,
                    "connector read reference facts contain duplicate named references",
                ));
            }
            previous_name = Some(reference.name.as_ref());
        }

        let bytes = snapshot_ids
            .len()
            .saturating_mul(std::mem::size_of::<i64>())
            + snapshot_log
                .len()
                .saturating_mul(2 * std::mem::size_of::<i64>())
            + named_references.iter().fold(0usize, |total, reference| {
                total
                    .saturating_add(reference.name.len())
                    .saturating_add(std::mem::size_of::<i64>())
                    .saturating_add(1)
            })
            + usize::from(current_snapshot_id.is_some()) * std::mem::size_of::<i64>();
        if bytes > context.max_total_payload_bytes() {
            return Err(ConnectorError::new(
                super::ConnectorErrorKind::ResourceExhausted,
                "connector read reference facts exceed request total payload budget",
            ));
        }

        Ok(Self {
            snapshot_ids,
            snapshot_log,
            named_references,
            current_snapshot_id,
        })
    }

    pub fn snapshot_ids(&self) -> &[i64] {
        &self.snapshot_ids
    }

    pub fn snapshot_log(&self) -> &[ConnectorReadSnapshotLogEntry] {
        &self.snapshot_log
    }

    pub fn named_references(&self) -> &[ConnectorReadNamedReference] {
        &self.named_references
    }

    pub const fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }
}

pub trait ConnectorMetadata: Send + Sync {
    fn instance_id(&self) -> &ConnectorInstanceId;

    fn list_namespaces(
        &self,
        _request: ConnectorListNamespacesRequest,
    ) -> Result<Vec<ConnectorNamespaceIdentity>, ConnectorError> {
        Err(ConnectorError::new(
            super::ConnectorErrorKind::Unsupported,
            "connector metadata does not support namespace enumeration",
        ))
    }

    fn namespace_exists(&self, request: ConnectorNamespaceRequest) -> Result<bool, ConnectorError>;

    fn table_exists(&self, request: ConnectorTableRequest) -> Result<bool, ConnectorError>;

    fn list_tables(
        &self,
        request: ConnectorListTablesRequest,
    ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError>;

    fn read_reference_facts(
        &self,
        _request: ConnectorReadReferenceFactsRequest,
    ) -> Result<ConnectorReadReferenceFacts, ConnectorError> {
        Err(ConnectorError::new(
            super::ConnectorErrorKind::Unsupported,
            "connector metadata does not support read reference facts",
        ))
    }

    fn load_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    struct NeverCancelled;

    impl super::super::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context(total_payload_bytes: usize) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            Arc::new(NeverCancelled),
            total_payload_bytes,
            total_payload_bytes,
        )
        .expect("valid connector request context")
    }

    #[test]
    fn spi5b_reference_facts_are_canonicalized_deterministically() {
        let facts = ConnectorReadReferenceFacts::try_new(
            vec![30, 10, 20],
            vec![
                ConnectorReadSnapshotLogEntry {
                    snapshot_id: 30,
                    timestamp_millis: 200,
                },
                ConnectorReadSnapshotLogEntry {
                    snapshot_id: 10,
                    timestamp_millis: 100,
                },
            ],
            vec![
                ConnectorReadNamedReference {
                    name: Arc::from("release"),
                    kind: ConnectorReadReferenceKind::Tag,
                    snapshot_id: 30,
                },
                ConnectorReadNamedReference {
                    name: Arc::from("main"),
                    kind: ConnectorReadReferenceKind::Branch,
                    snapshot_id: 20,
                },
            ],
            Some(20),
            &context(1024),
        )
        .expect("facts are valid");

        assert_eq!(facts.snapshot_ids(), &[10, 20, 30]);
        assert_eq!(facts.snapshot_log()[0].snapshot_id, 10);
        assert_eq!(facts.named_references()[0].name.as_ref(), "main");
        assert_eq!(facts.current_snapshot_id(), Some(20));
    }

    #[test]
    fn spi5b_reference_facts_reject_unknown_named_reference_snapshot() {
        let error = ConnectorReadReferenceFacts::try_new(
            vec![10],
            Vec::new(),
            vec![ConnectorReadNamedReference {
                name: Arc::from("main"),
                kind: ConnectorReadReferenceKind::Branch,
                snapshot_id: 20,
            }],
            None,
            &context(1024),
        )
        .expect_err("unknown named-reference snapshot is corrupt provider data");

        assert_eq!(error.kind(), super::super::ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn spi5b_reference_facts_enforce_the_request_payload_budget() {
        let error = ConnectorReadReferenceFacts::try_new(
            vec![10],
            Vec::new(),
            vec![ConnectorReadNamedReference {
                name: Arc::from("main"),
                kind: ConnectorReadReferenceKind::Branch,
                snapshot_id: 10,
            }],
            None,
            &context(16),
        )
        .expect_err("facts larger than the request budget must fail");

        assert_eq!(
            error.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }

    fn planning_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("sketch", DataType::Binary, true),
            Field::new("_row_id", DataType::Int64, false),
        ]))
    }

    fn referenced_table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("iceberg").expect("valid instance ID"),
            namespace: Arc::from("analytics"),
            table: Arc::from("customers"),
        }
    }

    #[test]
    fn spi5ef_table_planning_facts_canonicalize_constraints() {
        let facts = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            vec![
                ConnectorTableColumnPlanningFact::new(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    1,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::Hll,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    2,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::RowLineageSystem,
                ),
            ],
            vec![
                ConnectorTableUniqueConstraint::new(vec![1, 0]),
                ConnectorTableUniqueConstraint::new(vec![2]),
            ],
            vec![ConnectorTableForeignKeyConstraint::new(
                vec![1, 0],
                referenced_table(),
                vec![Arc::from("CUSTOMER_SKETCH"), Arc::from("CUSTOMER_ID")],
            )],
            &context(4_096),
        )
        .expect("valid facts");

        assert_eq!(
            facts.column_facts()[1].semantic_kind(),
            ConnectorTableColumnSemanticKind::Hll
        );
        assert_eq!(facts.unique_constraints()[0].column_ordinals(), &[0, 1]);
        let foreign_key = &facts.foreign_key_constraints()[0];
        assert_eq!(foreign_key.local_column_ordinals(), &[0, 1]);
        assert_eq!(
            foreign_key.referenced_column_names(),
            &[
                Arc::<str>::from("customer_id"),
                Arc::<str>::from("customer_sketch")
            ]
        );
    }

    #[test]
    fn spi5ef_table_planning_facts_reject_misaligned_or_duplicate_ordinals() {
        let error = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            vec![
                ConnectorTableColumnPlanningFact::new(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    0,
                    ConnectorTableColumnVisibility::Sql,
                    ConnectorTableColumnSemanticKind::Bitmap,
                    ConnectorTableColumnRole::Ordinary,
                ),
                ConnectorTableColumnPlanningFact::new(
                    2,
                    ConnectorTableColumnVisibility::Hidden,
                    ConnectorTableColumnSemanticKind::None,
                    ConnectorTableColumnRole::RowLineageSystem,
                ),
            ],
            Vec::new(),
            Vec::new(),
            &context(4_096),
        )
        .expect_err("duplicate ordinal must be rejected");

        assert_eq!(error.kind(), super::super::ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn spi5ef_table_planning_facts_reject_unknown_constraint_columns_and_budget_overflow() {
        let unknown_column = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            Vec::new(),
            vec![ConnectorTableUniqueConstraint::new(vec![3])],
            Vec::new(),
            &context(4_096),
        )
        .expect_err("unique constraint must reference a schema field");
        assert_eq!(
            unknown_column.kind(),
            super::super::ConnectorErrorKind::CorruptData
        );

        let budget = ConnectorTablePlanningFacts::try_new(
            &planning_schema(),
            Vec::new(),
            Vec::new(),
            vec![ConnectorTableForeignKeyConstraint::new(
                vec![0],
                referenced_table(),
                vec![Arc::from("customer_id")],
            )],
            &context(16),
        )
        .expect_err("facts must respect request budget");
        assert_eq!(
            budget.kind(),
            super::super::ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn spi5ef_table_planning_facts_default_to_empty() {
        assert!(
            ConnectorTablePlanningFacts::empty()
                .column_facts()
                .is_empty()
        );
        assert!(
            ConnectorTablePlanningFacts::default()
                .foreign_key_constraints()
                .is_empty()
        );
    }
}
