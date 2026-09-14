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

//! Runtime-owned values used by the persistence codec.
//!
//! These types are not generated DTOs and contain no provider implementation,
//! catalog registry, I/O handle, task identity, owner, or process incarnation.

use crate::persistence::identity::{
    AggregateIdentity, ApplyKeyIdentity, BranchIdentity, ComputationIdentity, DocumentRevision,
    FieldIdentity, NativeDataVersion, ObjectIdentity, OutputIdentity, PartitionSpecVersion,
    PublicationIdentity, SchemaVersion, StateSlotIdentity,
};
use novarocks_query_application::persisted_query_definition::{
    PersistedQueryDefinition, PersistedQueryDialect,
};

pub const MV_PERSISTENCE_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefinitionDocument {
    pub query: QuerySource,
    /// Ordered syntactic occurrences. Duplicates are meaningful.
    pub relation_occurrences: Vec<RelationOccurrence>,
    /// Ordered result contract.
    pub outputs: Vec<OutputDefinition>,
    pub computation_identity: ComputationIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuerySource {
    pub effective_sql: String,
    pub dialect: QueryDialect,
    pub resolution: ResolutionContext,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryDialect {
    StarRocks,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolutionContext {
    pub default_catalog: String,
    pub default_namespace: String,
}

impl TryFrom<PersistedQueryDefinition> for QuerySource {
    type Error = String;

    fn try_from(value: PersistedQueryDefinition) -> Result<Self, Self::Error> {
        value.validate()?;
        let dialect = match value.dialect {
            PersistedQueryDialect::StarRocks => QueryDialect::StarRocks,
        };
        Ok(Self {
            effective_sql: value.raw_query_source,
            dialect,
            resolution: ResolutionContext {
                default_catalog: value.resolution.default_catalog,
                default_namespace: value.resolution.default_database,
            },
        })
    }
}

impl TryFrom<QuerySource> for PersistedQueryDefinition {
    type Error = String;

    fn try_from(value: QuerySource) -> Result<Self, Self::Error> {
        let dialect = match value.dialect {
            QueryDialect::StarRocks => PersistedQueryDialect::StarRocks,
        };
        PersistedQueryDefinition::new(
            value.effective_sql,
            dialect,
            &value.resolution.default_catalog,
            &value.resolution.default_namespace,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelationOccurrence {
    pub occurrence_id: u32,
    pub catalog_at_binding: String,
    pub namespace_at_binding: String,
    pub relation_at_binding: String,
    pub qualifier_at_binding: String,
    pub object_id: ObjectIdentity,
    pub schema_version: SchemaVersion,
    /// Set semantics, canonically sorted by stable field identity.
    pub fields: Vec<SourceFieldBinding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceFieldBinding {
    pub field_id: FieldIdentity,
    pub name_at_binding: String,
    pub type_signature: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputDefinition {
    pub output_id: OutputIdentity,
    pub name: String,
    pub type_signature: String,
    pub nullable: bool,
    pub expression: ExpressionShape,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpressionShape {
    pub kind: ExpressionKind,
    pub function_identity: Option<String>,
    /// Set semantics, canonically sorted by occurrence and field identity.
    pub source_fields: Vec<SourceFieldReference>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpressionKind {
    Field,
    Literal,
    Cast,
    Function,
    Mixed,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceFieldReference {
    pub occurrence_id: u32,
    pub field_id: FieldIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterpretationDocument {
    pub definition_revision: DocumentRevision,
    pub computation_identity: ComputationIdentity,
    /// Set semantics, canonically sorted by output identity.
    pub outputs: Vec<OutputBinding>,
    /// Set semantics, canonically sorted by slot identity.
    pub state_slots: Vec<StateSlot>,
    pub apply_key: ApplyKey,
    /// Set semantics, canonically sorted by aggregate identity.
    pub aggregates: Vec<AggregateInterpretation>,
    /// Ordered UNION branches. Their position is semantic.
    pub branches: Vec<BranchInterpretation>,
    pub target: TargetBinding,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputBinding {
    pub output_id: OutputIdentity,
    pub target_field_id: FieldIdentity,
    pub type_signature: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateSlot {
    pub slot_id: StateSlotIdentity,
    pub target_field_id: FieldIdentity,
    pub type_signature: String,
    pub nullable: bool,
    pub role: StateRole,
    pub encoding: StateEncoding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateRole {
    Single,
    AvgSum,
    AvgCount,
    RetractionCount,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StateEncoding {
    NativeColumnV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyKey {
    pub kind: ApplyKeyKind,
    /// Ordered composite-key components.
    pub components: Vec<ApplyKeyComponent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplyKeyComponent {
    pub logical_id: ApplyKeyIdentity,
    pub target_field_id: FieldIdentity,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyKeyKind {
    BaseRowId,
    JoinRowKey,
    GroupRowId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateInterpretation {
    pub aggregate_id: AggregateIdentity,
    pub function_identity: String,
    pub source_fields: Vec<SourceFieldReference>,
    /// Ordered algorithm state, e.g. AVG sum followed by count.
    pub state_slot_ids: Vec<StateSlotIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BranchInterpretation {
    pub branch_id: BranchIdentity,
    pub relation_occurrence_ids: Vec<u32>,
    pub output_ids: Vec<OutputIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetBinding {
    pub object_id: ObjectIdentity,
    pub schema_version: SchemaVersion,
    pub partition_spec_version: PartitionSpecVersion,
    /// Set semantics, canonically sorted by kind then logical identity.
    pub fields: Vec<PhysicalFieldBinding>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalFieldBinding {
    pub logical_identity: PhysicalFieldLogicalIdentity,
    pub target_field_id: FieldIdentity,
    pub type_signature: String,
    pub nullable: bool,
}

/// The logical owner of one physical target field.
///
/// Keeping the identity kind in the Rust type prevents equally shaped opaque
/// bytes from being accidentally accepted as an output, state slot, apply-key
/// component, or UNION branch.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PhysicalFieldLogicalIdentity {
    Output(OutputIdentity),
    State(StateSlotIdentity),
    ApplyKey(ApplyKeyIdentity),
    Branch(BranchIdentity),
}

impl PhysicalFieldLogicalIdentity {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Output(identity) => identity.as_bytes(),
            Self::State(identity) => identity.as_bytes(),
            Self::ApplyKey(identity) => identity.as_bytes(),
            Self::Branch(identity) => identity.as_bytes(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationDocument {
    pub publication_id: PublicationIdentity,
    pub definition_revision: DocumentRevision,
    pub interpretation_revision: DocumentRevision,
    /// Ordered by relation occurrence, retaining repeated physical objects.
    pub inputs: Vec<PublicationInput>,
    pub output: PublicationOutput,
    pub kind: PublicationKind,
    pub statistics: PublicationStatistics,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationInput {
    pub relation_occurrence_id: u32,
    pub object_id: ObjectIdentity,
    pub native_data_version: NativeDataVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationOutput {
    pub object_id: ObjectIdentity,
    pub empty_result: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationKind {
    FullRefresh,
    IncrementalRefresh,
    MetadataOnlyRefresh,
    Repartition,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublicationStatistics {
    pub logical_result_rows: Option<u64>,
    pub processed_input_rows: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigurationDocument {
    pub refresh_policy: RefreshPolicy,
    pub paused: bool,
    pub refresh_interval_ms: Option<u64>,
    pub max_staleness_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshPolicy {
    Manual,
    AsyncOnChange,
    AsyncInterval,
}
