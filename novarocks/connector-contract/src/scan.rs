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

//! Public scan semantics paired by ordinal with a provider-validated relation
//! recipe. Dynamic splits are Task assignments and are never stored here.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;

use crate::{
    ConnectorExpression, ConnectorReadRelationKind, ConnectorReadRelationRecipe,
    ConnectorReadRelationRecipeDraft, ConnectorReadWorkSource, ConnectorValueType,
    MAX_CONNECTOR_RECIPE_COLUMNS, TupleDomain,
};

const MAX_SCAN_NAME_BYTES: usize = 256;
/// Total retained static scan material, including the provider recipe and all
/// public predicate/value backing. Individual algebra limits are insufficient:
/// thousands of individually valid ranges can otherwise retain gigabytes.
pub const MAX_STATIC_SCAN_RETAINED_BYTES: usize = 16 * 1024 * 1024;

/// Ordinal into both `assignments` and the recipe's canonical column payloads.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScanColumnId(usize);

impl ScanColumnId {
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticScanAssignment {
    variable: Arc<str>,
    value_type: ConnectorValueType,
}

impl StaticScanAssignment {
    pub fn new(variable: Arc<str>, value_type: ConnectorValueType) -> Self {
        Self {
            variable,
            value_type,
        }
    }

    pub fn variable(&self) -> &str {
        &self.variable
    }

    pub const fn value_type(&self) -> ConnectorValueType {
        self.value_type
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticScanDynamicFilter {
    filter_id: u32,
    variable: Arc<str>,
}

impl StaticScanDynamicFilter {
    pub fn new(filter_id: u32, variable: Arc<str>) -> Self {
        Self {
            filter_id,
            variable,
        }
    }

    pub const fn filter_id(&self) -> u32 {
        self.filter_id
    }

    pub fn variable(&self) -> &str {
        &self.variable
    }
}

/// Shared public scan facts. The recipe type states whether private provider
/// validation has run; neither form owns a reader or other live capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorScan<R> {
    recipe: R,
    facts: Arc<ConnectorScanFacts>,
    retained_bytes: usize,
}

// Validated once. Provider canonicalization shares these immutable public
// facts instead of cloning/revalidating predicate and expression trees.
#[derive(Debug, Eq, PartialEq)]
struct ConnectorScanFacts {
    assignments: Arc<[StaticScanAssignment]>,
    enforced_predicate: TupleDomain<ScanColumnId>,
    unenforced_predicate: TupleDomain<ScanColumnId>,
    remaining_expression: Option<ConnectorExpression>,
    dynamic_filters: Arc<[StaticScanDynamicFilter]>,
    max_batch_rows: NonZeroU64,
    max_batch_bytes: NonZeroU64,
    work_source: ConnectorReadWorkSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticConnectorScanError {
    EmptyAssignments,
    TooManyAssignments,
    RecipeColumnMismatch,
    InvalidVariable,
    DuplicateVariable,
    InvalidPredicateColumn,
    PredicateTypeMismatch,
    InvalidExpression,
    UnknownExpressionVariable,
    ExpressionTypeMismatch,
    DuplicateDynamicFilter,
    UnknownDynamicFilterVariable,
    WholeRelationRequiresSystemTable,
    TooManyRetainedBytes,
}

impl fmt::Display for StaticConnectorScanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid static connector scan: {self:?}")
    }
}

impl std::error::Error for StaticConnectorScanError {}

/// Structurally checked frozen input carried by a physical FragmentPackage.
pub type FrozenConnectorScan = ConnectorScan<ConnectorReadRelationRecipeDraft>;
/// Provider-validated input consumed by a local program.
pub type StaticConnectorScan = ConnectorScan<ConnectorReadRelationRecipe>;

impl<R: AsRef<ConnectorReadRelationRecipeDraft>> ConnectorScan<R> {
    #[expect(
        clippy::too_many_arguments,
        reason = "The frozen scan has independent public semantics and one provider recipe."
    )]
    pub fn try_new(
        recipe: R,
        assignments: Vec<StaticScanAssignment>,
        enforced_predicate: TupleDomain<ScanColumnId>,
        unenforced_predicate: TupleDomain<ScanColumnId>,
        remaining_expression: Option<ConnectorExpression>,
        dynamic_filters: Vec<StaticScanDynamicFilter>,
        max_batch_rows: NonZeroU64,
        max_batch_bytes: NonZeroU64,
        work_source: ConnectorReadWorkSource,
    ) -> Result<Self, StaticConnectorScanError> {
        if assignments.is_empty() {
            return Err(StaticConnectorScanError::EmptyAssignments);
        }
        if assignments.len() > MAX_CONNECTOR_RECIPE_COLUMNS {
            return Err(StaticConnectorScanError::TooManyAssignments);
        }
        if assignments.len() != recipe.as_ref().columns().len() {
            return Err(StaticConnectorScanError::RecipeColumnMismatch);
        }
        let mut variables = BTreeMap::new();
        for assignment in &assignments {
            if assignment.variable.is_empty() || assignment.variable.len() > MAX_SCAN_NAME_BYTES {
                return Err(StaticConnectorScanError::InvalidVariable);
            }
            if variables
                .insert(assignment.variable.as_ref(), assignment.value_type)
                .is_some()
            {
                return Err(StaticConnectorScanError::DuplicateVariable);
            }
        }
        for predicate in [&enforced_predicate, &unenforced_predicate] {
            if predicate
                .columns()
                .any(|column| column.index() >= assignments.len())
            {
                return Err(StaticConnectorScanError::InvalidPredicateColumn);
            }
            if predicate.domains().is_some_and(|domains| {
                domains.iter().any(|(column, domain)| {
                    domain.value_type() != assignments[column.index()].value_type
                })
            }) {
                return Err(StaticConnectorScanError::PredicateTypeMismatch);
            }
        }
        if let Some(expression) = &remaining_expression {
            expression
                .validate()
                .map_err(|_| StaticConnectorScanError::InvalidExpression)?;
            let mut names = Vec::new();
            expression.variable_names(&mut names);
            if names
                .iter()
                .any(|name| !variables.contains_key(name.as_ref()))
            {
                return Err(StaticConnectorScanError::UnknownExpressionVariable);
            }
            let mut pending = vec![expression];
            while let Some(node) = pending.pop() {
                match node {
                    ConnectorExpression::Variable { name, value_type }
                        if variables.get(name.as_ref()) != Some(value_type) =>
                    {
                        return Err(StaticConnectorScanError::ExpressionTypeMismatch);
                    }
                    ConnectorExpression::FieldDereference { target, .. } => pending.push(target),
                    ConnectorExpression::Call { arguments, .. } => pending.extend(arguments),
                    _ => {}
                }
            }
        }
        let mut filter_ids = BTreeSet::new();
        for filter in &dynamic_filters {
            if !filter_ids.insert(filter.filter_id) {
                return Err(StaticConnectorScanError::DuplicateDynamicFilter);
            }
            if !variables.contains_key(filter.variable.as_ref()) {
                return Err(StaticConnectorScanError::UnknownDynamicFilterVariable);
            }
        }
        if work_source == ConnectorReadWorkSource::WholeRelation
            && recipe.as_ref().relation().kind() != ConnectorReadRelationKind::SystemTable
        {
            return Err(StaticConnectorScanError::WholeRelationRequiresSystemTable);
        }
        let mut retained = recipe
            .as_ref()
            .charged_bytes()
            .checked_add(
                std::mem::size_of::<ConnectorScanFacts>() + 2 * std::mem::size_of::<usize>(),
            )
            .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
        let assignment_bytes = assignments
            .iter()
            .try_fold(0usize, |total, assignment| {
                total.checked_add(
                    std::mem::size_of::<StaticScanAssignment>() + assignment.variable.len(),
                )
            })
            .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
        let filter_bytes = dynamic_filters
            .iter()
            .try_fold(0usize, |total, filter| {
                total.checked_add(
                    std::mem::size_of::<StaticScanDynamicFilter>() + filter.variable.len(),
                )
            })
            .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
        retained = retained
            .checked_add(assignment_bytes)
            .and_then(|value| value.checked_add(filter_bytes))
            .and_then(|value| value.checked_add(tuple_domain_bytes(&enforced_predicate)?))
            .and_then(|value| value.checked_add(tuple_domain_bytes(&unenforced_predicate)?))
            .and_then(|value| {
                value.checked_add(
                    remaining_expression
                        .as_ref()
                        .map_or(Some(0), expression_bytes)?,
                )
            })
            .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
        if retained > MAX_STATIC_SCAN_RETAINED_BYTES {
            return Err(StaticConnectorScanError::TooManyRetainedBytes);
        }
        Ok(Self {
            recipe,
            facts: Arc::new(ConnectorScanFacts {
                assignments: assignments.into_boxed_slice().into(),
                enforced_predicate,
                unenforced_predicate,
                remaining_expression: remaining_expression.as_ref().map(owned_expression),
                dynamic_filters: dynamic_filters.into_boxed_slice().into(),
                max_batch_rows,
                max_batch_bytes,
                work_source,
            }),
            retained_bytes: retained,
        })
    }

    pub const fn recipe(&self) -> &R {
        &self.recipe
    }

    pub fn assignments(&self) -> &[StaticScanAssignment] {
        &self.facts.assignments
    }

    pub fn enforced_predicate(&self) -> &TupleDomain<ScanColumnId> {
        &self.facts.enforced_predicate
    }

    pub fn unenforced_predicate(&self) -> &TupleDomain<ScanColumnId> {
        &self.facts.unenforced_predicate
    }

    pub fn remaining_expression(&self) -> Option<&ConnectorExpression> {
        self.facts.remaining_expression.as_ref()
    }

    pub fn dynamic_filters(&self) -> &[StaticScanDynamicFilter] {
        &self.facts.dynamic_filters
    }

    pub fn max_batch_rows(&self) -> NonZeroU64 {
        self.facts.max_batch_rows
    }

    pub fn max_batch_bytes(&self) -> NonZeroU64 {
        self.facts.max_batch_bytes
    }

    pub fn work_source(&self) -> ConnectorReadWorkSource {
        self.facts.work_source
    }

    /// The checked conservative charge computed from these frozen facts.
    /// A containing package must still enforce its cumulative budget.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

impl FrozenConnectorScan {
    /// The caller first checks exact canonical public headers. Only private
    /// recipe bytes change; validated public backing remains shared.
    pub(crate) fn try_replace_private_recipe(
        &self,
        recipe: ConnectorReadRelationRecipeDraft,
    ) -> Result<Self, StaticConnectorScanError> {
        let retained_bytes = self
            .retained_bytes
            .checked_sub(self.recipe.charged_bytes())
            .and_then(|bytes| bytes.checked_add(recipe.charged_bytes()))
            .ok_or(StaticConnectorScanError::TooManyRetainedBytes)?;
        if retained_bytes > MAX_STATIC_SCAN_RETAINED_BYTES {
            return Err(StaticConnectorScanError::TooManyRetainedBytes);
        }
        Ok(Self {
            recipe,
            facts: self.facts.clone(),
            retained_bytes,
        })
    }
}

// Node/depth and cumulative byte preflight precede this bounded recursive copy.
// In particular, a Call must not retain an input Vec's unused capacity.
fn owned_expression(expression: &ConnectorExpression) -> ConnectorExpression {
    match expression {
        ConnectorExpression::Constant { value, value_type } => ConnectorExpression::Constant {
            value: value.clone(),
            value_type: *value_type,
        },
        ConnectorExpression::Variable { name, value_type } => ConnectorExpression::Variable {
            name: name.clone(),
            value_type: *value_type,
        },
        ConnectorExpression::FieldDereference {
            target,
            field_index,
            value_type,
        } => ConnectorExpression::FieldDereference {
            target: Box::new(owned_expression(target)),
            field_index: *field_index,
            value_type: *value_type,
        },
        ConnectorExpression::Call {
            function,
            value_type,
            arguments,
        } => ConnectorExpression::Call {
            function: function.clone(),
            value_type: *value_type,
            arguments: arguments
                .iter()
                .map(owned_expression)
                .collect::<Vec<_>>()
                .into_boxed_slice()
                .into_vec(),
        },
    }
}

fn tuple_domain_bytes(domain: &TupleDomain<ScanColumnId>) -> Option<usize> {
    let mut total = std::mem::size_of::<TupleDomain<ScanColumnId>>();
    let Some(domains) = domain.domains() else {
        return Some(total);
    };
    for values in domains.values() {
        total = total
            .checked_add(std::mem::size_of::<ScanColumnId>() + std::mem::size_of_val(values))?;
        for range in values.values().ranges() {
            total = total.checked_add(std::mem::size_of_val(range))?;
            for bound in [range.low(), range.high()] {
                total =
                    total.checked_add(bound.value().map_or(0, |value| value.payload_bytes()))?;
            }
        }
    }
    Some(total)
}

fn expression_bytes(expression: &ConnectorExpression) -> Option<usize> {
    let mut total = 0usize;
    let mut pending = vec![expression];
    while let Some(node) = pending.pop() {
        total = total.checked_add(std::mem::size_of::<ConnectorExpression>())?;
        match node {
            ConnectorExpression::Constant { value, .. } => {
                total =
                    total.checked_add(value.as_ref().map_or(0, |value| value.payload_bytes()))?
            }
            ConnectorExpression::Variable { name, .. } => total = total.checked_add(name.len())?,
            ConnectorExpression::FieldDereference { target, .. } => pending.push(target),
            ConnectorExpression::Call {
                function,
                arguments,
                ..
            } => {
                total = total.checked_add(function.as_str().len())?;
                pending.extend(arguments);
            }
        }
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding,
        ConnectorReadRecipeSplitDraft, ConnectorReadRelationPayload,
        ConnectorReadRelationRecipeCompiler, ConnectorReadRelationRecipeDraft,
        ConnectorReadRelationRecipeError, ConnectorValue, Domain, ValueSet,
    };
    use bytes::Bytes;

    use super::*;

    struct IdentityCompiler;

    impl ConnectorReadRelationRecipeCompiler for IdentityCompiler {
        type Error = ConnectorReadRelationRecipeError;

        fn compile_private(
            &self,
            draft: &ConnectorReadRelationRecipeDraft,
        ) -> Result<ConnectorReadRelationRecipeDraft, Self::Error> {
            Ok(draft.clone())
        }

        fn compile_split_private(
            &self,
            _binding: &ConnectorReadBinding,
            draft: &ConnectorReadRecipeSplitDraft,
        ) -> Result<ConnectorReadRecipeSplitDraft, Self::Error> {
            Ok(draft.clone())
        }
    }

    fn recipe() -> ConnectorReadRelationRecipe {
        let instance = ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: instance.clone(),
            },
            CatalogHandle::new(instance, CatalogVersion::from_bytes([1; 32])),
        );
        let payload = |category| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    binding.descriptor().provider_id.clone(),
                    binding.catalog_handle().clone(),
                    category,
                    ConnectorCodecRevision::try_new(1).unwrap(),
                ),
                Bytes::from_static(b"test"),
            )
        };
        let draft = ConnectorReadRelationRecipeDraft::try_new(
            binding.clone(),
            ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                payload(ConnectorCodecCategory::ReadTable),
                payload(ConnectorCodecCategory::ReadView),
            ),
            vec![payload(ConnectorCodecCategory::ReadColumn)],
        )
        .unwrap();
        ConnectorReadRelationRecipe::try_compile_with_provider(&draft, &IdentityCompiler).unwrap()
    }

    #[test]
    fn frozen_expression_trims_unused_call_capacity() {
        let mut arguments = Vec::with_capacity(100_000);
        arguments.push(ConnectorExpression::constant_true());
        let scan = FrozenConnectorScan::try_new(
            recipe().draft().clone(),
            vec![StaticScanAssignment::new(
                Arc::from("v"),
                ConnectorValueType::BigInt,
            )],
            TupleDomain::all(),
            TupleDomain::all(),
            Some(ConnectorExpression::Call {
                function: crate::ConnectorFunctionName::try_new("fixture").unwrap(),
                value_type: ConnectorValueType::Boolean,
                arguments,
            }),
            vec![],
            NonZeroU64::new(1).unwrap(),
            NonZeroU64::new(1).unwrap(),
            ConnectorReadWorkSource::RuntimeSplits,
        )
        .unwrap();
        let Some(ConnectorExpression::Call { arguments, .. }) = scan.remaining_expression() else {
            panic!("expected call")
        };
        assert_eq!(arguments.capacity(), 1);
        let canonical = scan
            .try_replace_private_recipe(scan.recipe().clone())
            .unwrap();
        assert!(Arc::ptr_eq(&scan.facts, &canonical.facts));
    }

    #[test]
    fn rejects_aggregate_predicate_backing_over_scan_cap() {
        let values = (0..512u16)
            .map(|index| {
                let mut bytes = vec![0u8; 32 * 1024];
                bytes[..2].copy_from_slice(&index.to_be_bytes());
                ConnectorValue::Varbinary(Arc::from(bytes))
            })
            .collect();
        let domain = Domain::new(
            ValueSet::of_values(ConnectorValueType::Varbinary, values).unwrap(),
            false,
        );
        let predicate =
            TupleDomain::with_column_domains(BTreeMap::from([(ScanColumnId::new(0), domain)]))
                .unwrap();
        assert!(matches!(
            StaticConnectorScan::try_new(
                recipe(),
                vec![StaticScanAssignment::new(
                    Arc::from("v"),
                    ConnectorValueType::Varbinary,
                )],
                predicate,
                TupleDomain::all(),
                None,
                vec![],
                NonZeroU64::new(1).unwrap(),
                NonZeroU64::new(1).unwrap(),
                ConnectorReadWorkSource::RuntimeSplits,
            ),
            Err(StaticConnectorScanError::TooManyRetainedBytes)
        ));
    }

    #[test]
    fn rejects_predicate_type_mismatch_before_binding() {
        let predicate = TupleDomain::with_column_domains(BTreeMap::from([(
            ScanColumnId::new(0),
            Domain::single_value(ConnectorValue::BigInt(1)).unwrap(),
        )]))
        .unwrap();
        assert!(matches!(
            StaticConnectorScan::try_new(
                recipe(),
                vec![StaticScanAssignment::new(
                    Arc::from("v"),
                    ConnectorValueType::Varbinary,
                )],
                predicate,
                TupleDomain::all(),
                None,
                vec![],
                NonZeroU64::new(1).unwrap(),
                NonZeroU64::new(1).unwrap(),
                ConnectorReadWorkSource::RuntimeSplits,
            ),
            Err(StaticConnectorScanError::PredicateTypeMismatch)
        ));
    }

    #[test]
    fn frozen_and_compiled_scans_share_one_public_contract() {
        let compiled_recipe = recipe();
        let assignment = vec![StaticScanAssignment::new(
            Arc::from("v"),
            ConnectorValueType::BigInt,
        )];
        let frozen = FrozenConnectorScan::try_new(
            compiled_recipe.draft().clone(),
            assignment.clone(),
            TupleDomain::all(),
            TupleDomain::all(),
            None,
            vec![StaticScanDynamicFilter::new(7, Arc::from("v"))],
            NonZeroU64::new(17).unwrap(),
            NonZeroU64::new(8192).unwrap(),
            ConnectorReadWorkSource::RuntimeSplits,
        )
        .unwrap();
        let compiled = StaticConnectorScan::try_new(
            compiled_recipe,
            assignment,
            frozen.enforced_predicate().clone(),
            frozen.unenforced_predicate().clone(),
            frozen.remaining_expression().cloned(),
            frozen.dynamic_filters().to_vec(),
            frozen.max_batch_rows(),
            frozen.max_batch_bytes(),
            frozen.work_source(),
        )
        .unwrap();
        assert_eq!(frozen.recipe(), compiled.recipe().draft());
        assert_eq!(frozen.assignments(), compiled.assignments());
        assert_eq!(frozen.dynamic_filters(), compiled.dynamic_filters());
        assert_eq!(frozen.max_batch_rows().get(), 17);
        assert_eq!(frozen.max_batch_bytes().get(), 8192);
        assert!(matches!(
            FrozenConnectorScan::try_new(
                frozen.recipe().clone(),
                frozen.assignments().to_vec(),
                TupleDomain::all(),
                TupleDomain::all(),
                None,
                vec![StaticScanDynamicFilter::new(7, Arc::from("unknown"))],
                frozen.max_batch_rows(),
                frozen.max_batch_bytes(),
                frozen.work_source(),
            ),
            Err(StaticConnectorScanError::UnknownDynamicFilterVariable)
        ));
    }
}
