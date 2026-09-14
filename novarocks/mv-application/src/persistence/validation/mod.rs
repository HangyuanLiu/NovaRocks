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

//! Pure semantic validation for durable MV documents.

pub mod runtime;

use std::collections::{BTreeMap, BTreeSet};

use crate::persistence::codec::{
    ApplyKeyKind, ConfigurationDocument, DefinitionDocument, ExpressionKind,
    InterpretationDocument, PhysicalFieldLogicalIdentity, PublicationDocument, RefreshPolicy,
    StateRole,
};
use crate::persistence::identity::{
    ComputationIdentity, DocumentRevision, FieldIdentity, ObjectIdentity,
};

pub const DEFAULT_MAX_DOCUMENT_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_MAX_DOCUMENT_SET_BYTES: usize = 32 * 1024 * 1024;
pub const DEFAULT_MAX_DECODE_WORKING_SET_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_DOCUMENT_ITEMS: usize = 4096;
pub const DEFAULT_MAX_STRUCTURE_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PersistenceDecodeBudget {
    pub max_document_bytes: usize,
    pub max_working_set_bytes: usize,
    pub max_items: usize,
    pub max_depth: usize,
}

impl Default for PersistenceDecodeBudget {
    fn default() -> Self {
        Self {
            max_document_bytes: DEFAULT_MAX_DOCUMENT_BYTES,
            max_working_set_bytes: DEFAULT_MAX_DECODE_WORKING_SET_BYTES,
            max_items: DEFAULT_MAX_DOCUMENT_ITEMS,
            max_depth: DEFAULT_MAX_STRUCTURE_DEPTH,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationError {
    path: String,
    message: String,
}

impl ValidationError {
    pub(crate) fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for ValidationError {}

pub fn validate_definition(document: &DefinitionDocument) -> Result<(), ValidationError> {
    nonempty_text(
        "definition.query.effective_sql",
        &document.query.effective_sql,
    )?;
    nonempty_text(
        "definition.query.resolution.default_catalog",
        &document.query.resolution.default_catalog,
    )?;
    nonempty_text(
        "definition.query.resolution.default_namespace",
        &document.query.resolution.default_namespace,
    )?;
    if document.relation_occurrences.is_empty() {
        return Err(ValidationError::new(
            "definition.relation_occurrences",
            "at least one relation occurrence is required",
        ));
    }
    if document.outputs.is_empty() {
        return Err(ValidationError::new(
            "definition.outputs",
            "at least one output is required",
        ));
    }

    let mut occurrences = BTreeMap::new();
    let mut item_count = document.relation_occurrences.len() + document.outputs.len();
    for relation in &document.relation_occurrences {
        if occurrences
            .insert(relation.occurrence_id, relation)
            .is_some()
        {
            return Err(ValidationError::new(
                "definition.relation_occurrences",
                format!("duplicate occurrence id {}", relation.occurrence_id),
            ));
        }
        for (field, value) in [
            ("catalog_at_binding", relation.catalog_at_binding.as_str()),
            (
                "namespace_at_binding",
                relation.namespace_at_binding.as_str(),
            ),
            ("relation_at_binding", relation.relation_at_binding.as_str()),
            (
                "qualifier_at_binding",
                relation.qualifier_at_binding.as_str(),
            ),
        ] {
            nonempty_text(
                format!(
                    "definition.relation_occurrences[{}].{field}",
                    relation.occurrence_id
                ),
                value,
            )?;
        }
        if relation.fields.is_empty() {
            return Err(ValidationError::new(
                format!(
                    "definition.relation_occurrences[{}].fields",
                    relation.occurrence_id
                ),
                "at least one stable field binding is required",
            ));
        }
        item_count = item_count.saturating_add(relation.fields.len());
        let mut fields = BTreeSet::new();
        for field in &relation.fields {
            if !fields.insert(&field.field_id) {
                return Err(ValidationError::new(
                    format!(
                        "definition.relation_occurrences[{}].fields",
                        relation.occurrence_id
                    ),
                    "duplicate stable field identity",
                ));
            }
            nonempty_text(
                "definition.relation.field.name_at_binding",
                &field.name_at_binding,
            )?;
            nonempty_text(
                "definition.relation.field.type_signature",
                &field.type_signature,
            )?;
        }
    }

    let mut output_ids = BTreeSet::new();
    for output in &document.outputs {
        if !output_ids.insert(&output.output_id) {
            return Err(ValidationError::new(
                "definition.outputs",
                "duplicate output identity",
            ));
        }
        nonempty_text("definition.output.name", &output.name)?;
        nonempty_text("definition.output.type_signature", &output.type_signature)?;
        match output.expression.kind {
            ExpressionKind::Function | ExpressionKind::Mixed
                if output
                    .expression
                    .function_identity
                    .as_deref()
                    .is_none_or(str::is_empty) =>
            {
                return Err(ValidationError::new(
                    "definition.output.expression.function_identity",
                    "function and mixed expressions require a function identity",
                ));
            }
            ExpressionKind::Literal if !output.expression.source_fields.is_empty() => {
                return Err(ValidationError::new(
                    "definition.output.expression.source_fields",
                    "literal expressions cannot reference source fields",
                ));
            }
            _ => {}
        }
        item_count = item_count.saturating_add(output.expression.source_fields.len());
        let mut refs = BTreeSet::new();
        for reference in &output.expression.source_fields {
            if !refs.insert((reference.occurrence_id, &reference.field_id)) {
                return Err(ValidationError::new(
                    "definition.output.expression.source_fields",
                    "duplicate stable source-field reference",
                ));
            }
            let Some(relation) = occurrences.get(&reference.occurrence_id) else {
                return Err(ValidationError::new(
                    "definition.output.expression.source_fields",
                    format!("unknown relation occurrence {}", reference.occurrence_id),
                ));
            };
            if !relation
                .fields
                .iter()
                .any(|field| field.field_id == reference.field_id)
            {
                return Err(ValidationError::new(
                    "definition.output.expression.source_fields",
                    format!(
                        "field is not bound by relation occurrence {}",
                        reference.occurrence_id
                    ),
                ));
            }
        }
    }
    enforce_item_budget("definition", item_count)
}

pub fn validate_interpretation(document: &InterpretationDocument) -> Result<(), ValidationError> {
    if document.outputs.is_empty() {
        return Err(ValidationError::new(
            "interpretation.outputs",
            "at least one output binding is required",
        ));
    }
    let mut outputs = BTreeMap::new();
    for output in &document.outputs {
        nonempty_text(
            "interpretation.output.type_signature",
            &output.type_signature,
        )?;
        if outputs.insert(&output.output_id, output).is_some() {
            return Err(ValidationError::new(
                "interpretation.outputs",
                "duplicate output identity",
            ));
        }
    }

    let mut slots = BTreeMap::new();
    for slot in &document.state_slots {
        nonempty_text(
            "interpretation.state_slot.type_signature",
            &slot.type_signature,
        )?;
        if slots.insert(&slot.slot_id, slot).is_some() {
            return Err(ValidationError::new(
                "interpretation.state_slots",
                "duplicate state-slot identity",
            ));
        }
    }
    match document.apply_key.kind {
        ApplyKeyKind::BaseRowId | ApplyKeyKind::JoinRowKey | ApplyKeyKind::GroupRowId => {}
    }
    if document.apply_key.kind == ApplyKeyKind::GroupRowId && document.aggregates.is_empty() {
        return Err(ValidationError::new(
            "interpretation.apply_key.kind",
            "GROUP_ROW_ID requires aggregate interpretation",
        ));
    }
    if document.apply_key.components.is_empty() {
        return Err(ValidationError::new(
            "interpretation.apply_key.components",
            "at least one apply-key field is required",
        ));
    }
    let mut apply_logical_ids = BTreeSet::new();
    let mut apply_target_ids = BTreeSet::new();
    for component in &document.apply_key.components {
        if !apply_logical_ids.insert(&component.logical_id) {
            return Err(ValidationError::new(
                "interpretation.apply_key.components",
                "contains a duplicate logical identity",
            ));
        }
        if !apply_target_ids.insert(&component.target_field_id) {
            return Err(ValidationError::new(
                "interpretation.apply_key.components",
                "contains a duplicate target field identity",
            ));
        }
    }

    let mut aggregate_ids = BTreeSet::new();
    for aggregate in &document.aggregates {
        if !aggregate_ids.insert(&aggregate.aggregate_id) {
            return Err(ValidationError::new(
                "interpretation.aggregates",
                "duplicate aggregate identity",
            ));
        }
        nonempty_text(
            "interpretation.aggregate.function_identity",
            &aggregate.function_identity,
        )?;
        let mut source_fields = BTreeSet::new();
        for reference in &aggregate.source_fields {
            if !source_fields.insert((reference.occurrence_id, &reference.field_id)) {
                return Err(ValidationError::new(
                    "interpretation.aggregate.source_fields",
                    "contains a duplicate source-field reference",
                ));
            }
        }
        if aggregate.state_slot_ids.is_empty() {
            return Err(ValidationError::new(
                "interpretation.aggregate.state_slot_ids",
                "aggregate interpretation requires at least one state slot",
            ));
        }
        unique_identities(
            "interpretation.aggregate.state_slot_ids",
            &aggregate.state_slot_ids,
        )?;
        for slot_id in &aggregate.state_slot_ids {
            if !slots.contains_key(slot_id) {
                return Err(ValidationError::new(
                    "interpretation.aggregate.state_slot_ids",
                    "aggregate references an unknown state slot",
                ));
            }
        }
        let referenced_roles = aggregate
            .state_slot_ids
            .iter()
            .filter_map(|id| slots.get(id).map(|slot| slot.role))
            .collect::<Vec<_>>();
        if aggregate.function_identity == "avg" {
            if referenced_roles.as_slice() != [StateRole::AvgSum, StateRole::AvgCount] {
                return Err(ValidationError::new(
                    "interpretation.aggregate.state_slot_ids",
                    "AVG interpretation requires exactly one sum slot followed by one distinct count slot",
                ));
            }
            let sum = slots
                .get(&aggregate.state_slot_ids[0])
                .expect("validated AVG sum slot");
            let count = slots
                .get(&aggregate.state_slot_ids[1])
                .expect("validated AVG count slot");
            if sum.target_field_id == count.target_field_id {
                return Err(ValidationError::new(
                    "interpretation.aggregate.state_slot_ids",
                    "AVG sum and count slots require distinct physical target fields",
                ));
            }
        } else if referenced_roles
            .iter()
            .any(|role| matches!(role, StateRole::AvgSum | StateRole::AvgCount))
        {
            return Err(ValidationError::new(
                "interpretation.aggregate.state_slot_ids",
                "non-AVG interpretation cannot reference AVG state roles",
            ));
        }
    }
    let referenced_slots = document
        .aggregates
        .iter()
        .flat_map(|aggregate| aggregate.state_slot_ids.iter())
        .collect::<BTreeSet<_>>();
    if slots
        .keys()
        .any(|slot_id| !referenced_slots.contains(slot_id))
    {
        return Err(ValidationError::new(
            "interpretation.state_slots",
            "contains a state slot not owned by any aggregate interpretation",
        ));
    }

    let mut branch_ids = BTreeSet::new();
    for branch in &document.branches {
        if !branch_ids.insert(&branch.branch_id) {
            return Err(ValidationError::new(
                "interpretation.branches",
                "duplicate branch identity",
            ));
        }
        if branch.relation_occurrence_ids.is_empty() || branch.output_ids.is_empty() {
            return Err(ValidationError::new(
                "interpretation.branches",
                "each branch requires relation occurrences and outputs",
            ));
        }
        let occurrence_count = branch
            .relation_occurrence_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .len();
        if occurrence_count != branch.relation_occurrence_ids.len() {
            return Err(ValidationError::new(
                "interpretation.branch.relation_occurrence_ids",
                "contains a duplicate occurrence",
            ));
        }
        unique_identities("interpretation.branch.output_ids", &branch.output_ids)?;
        for output_id in &branch.output_ids {
            if !outputs.contains_key(output_id) {
                return Err(ValidationError::new(
                    "interpretation.branch.output_ids",
                    "branch references an unknown output",
                ));
            }
        }
    }

    let mut physical = BTreeSet::new();
    for field in &document.target.fields {
        nonempty_text(
            "interpretation.target.field.type_signature",
            &field.type_signature,
        )?;
        if !physical.insert(&field.logical_identity) {
            return Err(ValidationError::new(
                "interpretation.target.fields",
                "duplicate physical binding for one logical identity",
            ));
        }
        match &field.logical_identity {
            PhysicalFieldLogicalIdentity::Output(output_id) if !outputs.contains_key(output_id) => {
                return Err(ValidationError::new(
                    "interpretation.target.fields",
                    "output physical binding references an unknown output",
                ));
            }
            PhysicalFieldLogicalIdentity::State(slot_id) if !slots.contains_key(slot_id) => {
                return Err(ValidationError::new(
                    "interpretation.target.fields",
                    "state physical binding references an unknown state slot",
                ));
            }
            PhysicalFieldLogicalIdentity::Branch(branch_id) if !branch_ids.contains(branch_id) => {
                return Err(ValidationError::new(
                    "interpretation.target.fields",
                    "branch physical binding references an unknown branch",
                ));
            }
            PhysicalFieldLogicalIdentity::ApplyKey(logical_id)
                if !document.apply_key.components.iter().any(|component| {
                    &component.logical_id == logical_id
                        && component.target_field_id == field.target_field_id
                }) =>
            {
                return Err(ValidationError::new(
                    "interpretation.target.fields",
                    "apply-key physical binding does not exactly match its declared logical key identity",
                ));
            }
            _ => {}
        }
    }
    for output in &document.outputs {
        let logical_identity = PhysicalFieldLogicalIdentity::Output(output.output_id.clone());
        require_physical_binding(
            &document.target.fields,
            &logical_identity,
            &output.target_field_id,
            &output.type_signature,
            output.nullable,
        )?;
    }
    for slot in &document.state_slots {
        let logical_identity = PhysicalFieldLogicalIdentity::State(slot.slot_id.clone());
        require_physical_binding(
            &document.target.fields,
            &logical_identity,
            &slot.target_field_id,
            &slot.type_signature,
            slot.nullable,
        )?;
    }
    for component in &document.apply_key.components {
        if !document.target.fields.iter().any(|field| {
            field.logical_identity
                == PhysicalFieldLogicalIdentity::ApplyKey(component.logical_id.clone())
                && field.target_field_id == component.target_field_id
        }) {
            return Err(ValidationError::new(
                "interpretation.target.fields",
                "a declared apply-key component has no exact physical binding",
            ));
        }
    }

    let item_count = document.outputs.len()
        + document.state_slots.len()
        + document.aggregates.len()
        + document.branches.len()
        + document.target.fields.len()
        + document.apply_key.components.len()
        + document
            .aggregates
            .iter()
            .map(|value| value.source_fields.len() + value.state_slot_ids.len())
            .sum::<usize>();
    enforce_item_budget("interpretation", item_count)
}

pub fn validate_publication(document: &PublicationDocument) -> Result<(), ValidationError> {
    if document.inputs.is_empty() {
        return Err(ValidationError::new(
            "publication.inputs",
            "at least one actual input occurrence is required",
        ));
    }
    let mut occurrences = BTreeSet::new();
    for input in &document.inputs {
        if !occurrences.insert(input.relation_occurrence_id) {
            return Err(ValidationError::new(
                "publication.inputs",
                format!(
                    "duplicate input occurrence {}",
                    input.relation_occurrence_id
                ),
            ));
        }
    }
    enforce_item_budget("publication", document.inputs.len())
}

pub fn validate_configuration(document: &ConfigurationDocument) -> Result<(), ValidationError> {
    match document.refresh_policy {
        RefreshPolicy::AsyncInterval
            if document.refresh_interval_ms.is_none_or(|value| value == 0) =>
        {
            return Err(ValidationError::new(
                "configuration.refresh_interval_ms",
                "ASYNC_INTERVAL requires a positive interval",
            ));
        }
        RefreshPolicy::Manual | RefreshPolicy::AsyncOnChange
            if document.refresh_interval_ms.is_some() =>
        {
            return Err(ValidationError::new(
                "configuration.refresh_interval_ms",
                "only ASYNC_INTERVAL may carry an interval",
            ));
        }
        _ => {}
    }
    if document.max_staleness_ms.is_some_and(|value| value == 0) {
        return Err(ValidationError::new(
            "configuration.max_staleness_ms",
            "maximum staleness must be positive when present",
        ));
    }
    Ok(())
}

/// Validates the immutable reference graph without reading Current catalog
/// state. Live-object rebinding is a separate caller-owned pure projection.
pub fn validate_document_set(
    definition: &DefinitionDocument,
    definition_revision: DocumentRevision,
    interpretation: &InterpretationDocument,
    interpretation_revision: DocumentRevision,
    publication: &PublicationDocument,
) -> Result<(), ValidationError> {
    validate_definition(definition)?;
    validate_interpretation(interpretation)?;
    validate_publication(publication)?;
    if interpretation.definition_revision != definition_revision {
        return Err(ValidationError::new(
            "interpretation.definition_revision",
            "does not reference the supplied definition document",
        ));
    }
    if interpretation.computation_identity != definition.computation_identity {
        return Err(ValidationError::new(
            "interpretation.computation_identity",
            "does not match the supplied definition semantics",
        ));
    }
    if publication.definition_revision != definition_revision {
        return Err(ValidationError::new(
            "publication.definition_revision",
            "does not reference the supplied definition document",
        ));
    }
    if publication.interpretation_revision != interpretation_revision {
        return Err(ValidationError::new(
            "publication.interpretation_revision",
            "does not reference the supplied interpretation document",
        ));
    }

    let definition_occurrences = definition
        .relation_occurrences
        .iter()
        .map(|value| (value.occurrence_id, value))
        .collect::<BTreeMap<_, _>>();
    let definition_outputs = definition
        .outputs
        .iter()
        .map(|value| (&value.output_id, value))
        .collect::<BTreeMap<_, _>>();
    if definition_outputs.len() != interpretation.outputs.len() {
        return Err(ValidationError::new(
            "interpretation.outputs",
            "does not bind every and only definition output",
        ));
    }
    for output in &interpretation.outputs {
        let Some(defined) = definition_outputs.get(&output.output_id) else {
            return Err(ValidationError::new(
                "interpretation.outputs",
                "binds an output absent from the referenced definition",
            ));
        };
        if defined.type_signature != output.type_signature || defined.nullable != output.nullable {
            return Err(ValidationError::new(
                "interpretation.outputs",
                "changes a definition output type or nullability",
            ));
        }
    }
    for aggregate in &interpretation.aggregates {
        for reference in &aggregate.source_fields {
            validate_definition_field_reference(&definition_occurrences, reference)?;
        }
    }
    for branch in &interpretation.branches {
        for occurrence_id in &branch.relation_occurrence_ids {
            if !definition_occurrences.contains_key(occurrence_id) {
                return Err(ValidationError::new(
                    "interpretation.branches.relation_occurrence_ids",
                    "branch references an occurrence absent from the definition",
                ));
            }
        }
    }
    let published_occurrences = publication
        .inputs
        .iter()
        .map(|value| value.relation_occurrence_id)
        .collect::<BTreeSet<_>>();
    let expected_occurrences = definition_occurrences
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    if published_occurrences != expected_occurrences {
        return Err(ValidationError::new(
            "publication.inputs",
            "must contain every and only actual definition occurrence",
        ));
    }
    if publication
        .inputs
        .iter()
        .map(|value| value.relation_occurrence_id)
        .ne(definition
            .relation_occurrences
            .iter()
            .map(|value| value.occurrence_id))
    {
        return Err(ValidationError::new(
            "publication.inputs",
            "must preserve definition occurrence order",
        ));
    }
    for input in &publication.inputs {
        let Some(relation) = definition_occurrences.get(&input.relation_occurrence_id) else {
            return Err(ValidationError::new(
                "publication.inputs",
                "publication references an unknown relation occurrence",
            ));
        };
        if input.object_id != relation.object_id {
            return Err(ValidationError::new(
                "publication.inputs.object_id",
                "same-name object replacement is not the bound source object",
            ));
        }
    }
    if publication.output.object_id != interpretation.target.object_id {
        return Err(ValidationError::new(
            "publication.output.object_id",
            "does not match the interpretation target object",
        ));
    }
    Ok(())
}

fn validate_definition_field_reference(
    occurrences: &BTreeMap<u32, &crate::persistence::codec::RelationOccurrence>,
    reference: &crate::persistence::codec::SourceFieldReference,
) -> Result<(), ValidationError> {
    let Some(relation) = occurrences.get(&reference.occurrence_id) else {
        return Err(ValidationError::new(
            "interpretation.aggregate.source_fields",
            "aggregate references an occurrence absent from the definition",
        ));
    };
    if !relation
        .fields
        .iter()
        .any(|field| field.field_id == reference.field_id)
    {
        return Err(ValidationError::new(
            "interpretation.aggregate.source_fields",
            "aggregate references a field absent from its definition occurrence",
        ));
    }
    Ok(())
}

/// Validates one live source rebinding. Names and field order are diagnostic;
/// stable object/field identities plus type/nullability decide compatibility.
pub fn validate_live_relation_binding(
    expected: &crate::persistence::codec::RelationOccurrence,
    actual_object_id: &ObjectIdentity,
    actual_fields: &[crate::persistence::codec::SourceFieldBinding],
) -> Result<(), ValidationError> {
    if &expected.object_id != actual_object_id {
        return Err(ValidationError::new(
            "live_relation.object_id",
            "same-name relation was rebuilt with a different object identity",
        ));
    }
    let mut actual = BTreeMap::new();
    for field in actual_fields {
        if actual.insert(&field.field_id, field).is_some() {
            return Err(ValidationError::new(
                "live_relation.fields",
                "live schema contains a duplicate stable field identity",
            ));
        }
    }
    for expected_field in &expected.fields {
        let Some(actual_field) = actual.get(&expected_field.field_id) else {
            return Err(ValidationError::new(
                "live_relation.fields",
                "a bound stable field no longer exists",
            ));
        };
        if expected_field.type_signature != actual_field.type_signature
            || expected_field.nullable != actual_field.nullable
        {
            return Err(ValidationError::new(
                "live_relation.fields",
                "a bound field has an incompatible type or nullability",
            ));
        }
    }
    Ok(())
}

pub(crate) fn verify_computation_identity(
    stored: ComputationIdentity,
    actual: ComputationIdentity,
) -> Result<(), ValidationError> {
    if stored != actual {
        return Err(ValidationError::new(
            "definition.computation_identity",
            "does not match canonical definition semantics",
        ));
    }
    Ok(())
}

fn require_physical_binding(
    fields: &[crate::persistence::codec::PhysicalFieldBinding],
    logical_identity: &PhysicalFieldLogicalIdentity,
    target_field_id: &FieldIdentity,
    type_signature: &str,
    nullable: bool,
) -> Result<(), ValidationError> {
    let Some(binding) = fields
        .iter()
        .find(|field| &field.logical_identity == logical_identity)
    else {
        return Err(ValidationError::new(
            "interpretation.target.fields",
            "a logical output or state slot has no physical binding",
        ));
    };
    if &binding.target_field_id != target_field_id
        || binding.type_signature != type_signature
        || binding.nullable != nullable
    {
        return Err(ValidationError::new(
            "interpretation.target.fields",
            "a duplicated physical binding fact disagrees with its logical binding",
        ));
    }
    Ok(())
}

fn nonempty_text(path: impl Into<String>, value: &str) -> Result<(), ValidationError> {
    if value.trim().is_empty() {
        return Err(ValidationError::new(path, "must not be empty"));
    }
    if value.contains('\0') {
        return Err(ValidationError::new(path, "must not contain NUL"));
    }
    Ok(())
}

fn unique_identities<T: Ord>(path: &str, values: &[T]) -> Result<(), ValidationError> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(ValidationError::new(path, "contains a duplicate identity"));
        }
    }
    Ok(())
}

fn enforce_item_budget(path: &str, count: usize) -> Result<(), ValidationError> {
    if count > DEFAULT_MAX_DOCUMENT_ITEMS {
        return Err(ValidationError::new(
            path,
            format!(
                "contains {count} expanded items, exceeding the {}-item budget",
                DEFAULT_MAX_DOCUMENT_ITEMS
            ),
        ));
    }
    Ok(())
}
