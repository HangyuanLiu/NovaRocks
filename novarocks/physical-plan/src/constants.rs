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

//! Sparse references into the plan's sole checked immutable backing table.
//! Address identity is not selected-value equality. No scalar is reconstructed
//! and borrowing an admitted source does not consume a new backing policy.

use crate::ConstantPoolId;
use novarocks_constant_contract::{ConstantError, ConstantPool, ConstantValue};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, FunctionValueType};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConstantReference {
    pub pool: ConstantPoolId,
    pub ordinal: u32,
}

/// This map deliberately has no Eq or Hash implementation: two addresses do
/// not prove value equality and checked backings are not raw-byte values.
#[derive(Clone, Debug)]
pub struct ConstantPools {
    entries: BTreeMap<ConstantPoolId, ConstantPool>,
}

impl ConstantPools {
    pub fn empty() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
    pub fn entries(&self) -> &BTreeMap<ConstantPoolId, ConstantPool> {
        &self.entries
    }
    pub fn insert(
        &mut self,
        id: ConstantPoolId,
        pool: ConstantPool,
    ) -> Result<(), ConstantReferenceError> {
        use std::collections::btree_map::Entry;
        match self.entries.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(pool);
                Ok(())
            }
            Entry::Occupied(_) => Err(ConstantReferenceError::DuplicatePool(id)),
        }
    }
    pub fn resolve_observed(
        &self,
        reference: ConstantReference,
        expected: &FunctionValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantValue, ConstantReferenceError> {
        // The caller owns the encompassing finish; an originating control or
        // resource refusal must never be replaced by a later observation.
        work.step()?;
        let pool = self
            .entries
            .get(&reference.pool)
            .ok_or(ConstantReferenceError::MissingPool(reference.pool))?;
        if !expected.exactly_equals_observed(pool.value_type(), || {
            work.step().map_err(ConstantReferenceError::from)
        })? {
            return Err(ConstantReferenceError::SourceTypeMismatch(reference));
        }
        work.step()?;
        pool.value(reference.ordinal)
            .map_err(ConstantReferenceError::from)
    }
    /// Project only the already admitted backings referenced by this closure.
    /// All references are checked, including repeat ordinals and full types;
    /// one pool handle is retained once per sparse key.
    pub fn project_observed<'a>(
        &self,
        references: impl IntoIterator<Item = (ConstantReference, &'a FunctionValueType)>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantReferenceError> {
        self.project_optional_references_observed(references.into_iter().map(Some), work)
    }
    /// Observe every visited definition, including definitions without a
    /// constant reference. Filtering before this boundary would hide work.
    pub fn project_optional_references_observed<'a>(
        &self,
        references: impl IntoIterator<Item = Option<(ConstantReference, &'a FunctionValueType)>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ConstantReferenceError> {
        let mut projected = Self::empty();
        for definition in references {
            work.step()?;
            let Some((reference, expected)) = definition else {
                continue;
            };
            let value = self.resolve_observed(reference, expected, work)?;
            work.step()?;
            projected
                .entries
                .entry(reference.pool)
                .or_insert_with(|| value.pool().clone());
        }
        Ok(projected)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstantReferenceError {
    Control(CompileControlError),
    Constant(ConstantError),
    Structure(crate::ValidationErrors),
    MissingPool(ConstantPoolId),
    DuplicatePool(ConstantPoolId),
    SourceTypeMismatch(ConstantReference),
    UnusedPools,
    InvalidConsumer(&'static str),
}
impl From<novarocks_type_contract::ValueTypeError> for ConstantReferenceError {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Constant(ConstantError::Type(error))
    }
}
impl From<CompileControlError> for ConstantReferenceError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ConstantError> for ConstantReferenceError {
    fn from(error: ConstantError) -> Self {
        match error {
            ConstantError::Control(error) => Self::Control(error),
            ConstantError::Limit(_) => Self::Control(CompileControlError::ResourceExhausted),
            error => Self::Constant(error),
        }
    }
}
impl fmt::Display for ConstantReferenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::Constant(error) => error.fmt(formatter),
            Self::Structure(error) => error.fmt(formatter),
            Self::MissingPool(id) => write!(formatter, "missing constant pool {}", id.get()),
            Self::DuplicatePool(id) => write!(formatter, "duplicate constant pool {}", id.get()),
            Self::UnusedPools => {
                formatter.write_str("constant pool table contains unused definitions")
            }
            Self::InvalidConsumer(message) => formatter.write_str(message),
            Self::SourceTypeMismatch(reference) => write!(
                formatter,
                "constant {}:{} differs from its expected source type",
                reference.pool.get(),
                reference.ordinal
            ),
        }
    }
}
impl std::error::Error for ConstantReferenceError {}

/// Source-address/type validation and actual scalar consumer facts. A mutable
/// fragment definition is still a construction input; this check is mandatory
/// before publishing its enclosing plan or local package.
pub(crate) fn validate_fragment_constants_observed(
    fragment: &crate::Fragment,
    pools: &ConstantPools,
    require_closed: bool,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), ConstantReferenceError> {
    use novarocks_type_contract::CompilePhase;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        // A whole plan has one table spanning peer fragments; its enclosing
        // plan gate accounts that table. Only a closed local package owns it
        // within this fragment envelope.
        if require_closed {
            let mut errors = crate::validation::ValidationContext::new();
            let mut usage = crate::resource::CutResourcePreflight::new();
            usage.add_fragment(fragment, &mut errors);
            usage.add_constants_observed(pools, &mut work)?;
            usage.validate("package.constants.resources", &mut errors);
            if !errors.is_empty() {
                return Err(ConstantReferenceError::Structure(
                    crate::ValidationErrors::from_collector(errors),
                ));
            }
        }
        let mut used = std::collections::BTreeSet::new();
        for (_, expression) in fragment.expressions().iter() {
            work.step()?;
            if let crate::ExprKind::Constant(reference) = expression.kind {
                pools.resolve_observed(reference, &expression.ty, &mut work)?;
                if require_closed {
                    used.insert(reference.pool);
                }
            }
        }
        if require_closed && used.len() != pools.entries.len() {
            return Err(ConstantReferenceError::UnusedPools);
        }
        for (_, expression) in fragment.expressions().iter() {
            work.step()?;
            if let crate::ExprKind::WindowCall {
                frame: Some(frame), ..
            } = &expression.kind
            {
                validate_window_constants(fragment, pools, frame, &mut work)?;
            }
        }
        validate_unpivot_constants(fragment, pools, &mut work)?;
        Ok(())
    })();
    if matches!(result, Err(ConstantReferenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn window_offset(
    fragment: &crate::Fragment,
    pools: &ConstantPools,
    id: crate::ExprId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<u64>, ConstantReferenceError> {
    work.step()?;
    let Some(node) = fragment.expressions().get(id) else {
        return Ok(None);
    };
    let crate::ExprKind::Constant(reference) = node.kind else {
        return Ok(crate::validation::window_row_offset(fragment, id));
    };
    let value = pools.resolve_observed(reference, &node.ty, work)?;
    if node.ty.nullable
        || node.ty.logical_type != novarocks_type_contract::ValueLogicalType::Physical
    {
        return Err(ConstantReferenceError::InvalidConsumer(
            "window constant offset must be an exact non-null physical integer",
        ));
    }
    let offset = match node.ty.data_type {
        arrow_schema::DataType::UInt64 => value.try_u64()?,
        arrow_schema::DataType::Int64 => {
            value.try_i64()?.and_then(|value| u64::try_from(value).ok())
        }
        _ => None,
    };
    if offset.is_none() || offset == Some(0) {
        return Err(ConstantReferenceError::InvalidConsumer(
            "window constant offset must be a non-negative non-null exact I64/U64; zero requires CURRENT ROW",
        ));
    }
    Ok(offset)
}
fn validate_window_constants(
    fragment: &crate::Fragment,
    pools: &ConstantPools,
    frame: &crate::WindowFrame,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantReferenceError> {
    let mut offsets = [None, None];
    for (index, bound) in [&frame.start, &frame.end].into_iter().enumerate() {
        work.step()?;
        if let crate::WindowBound::Preceding(id) | crate::WindowBound::Following(id) = bound {
            offsets[index] = window_offset(fragment, pools, *id, work)?;
        }
    }
    let ordered = match (&frame.start, &frame.end, offsets) {
        (
            crate::WindowBound::Preceding(_),
            crate::WindowBound::Preceding(_),
            [Some(start), Some(end)],
        ) => start >= end,
        (
            crate::WindowBound::Following(_),
            crate::WindowBound::Following(_),
            [Some(start), Some(end)],
        ) => start <= end,
        _ => true,
    };
    if !ordered {
        return Err(ConstantReferenceError::InvalidConsumer(
            "window frame start follows its end after comparing exact constant offsets",
        ));
    }
    Ok(())
}
fn validate_unpivot_constants(
    fragment: &crate::Fragment,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ConstantReferenceError> {
    use crate::{NodeKind, UnpivotConstant};
    for node in fragment.nodes().values() {
        work.step()?;
        let mut bytes = 0u64;
        let ordinary = match &node.kind {
            NodeKind::Unpivot { spec } => Some(spec),
            _ => None,
        };
        let grouped = match &node.kind {
            NodeKind::TableFinish(spec) => spec.grouped_unpivot.as_ref(),
            _ => None,
        };
        let lists = ordinary
            .into_iter()
            .flat_map(|spec| {
                spec.mappings
                    .iter()
                    .map(|mapping| mapping.constants.as_ref())
            })
            .chain(grouped.into_iter().flat_map(|spec| {
                spec.mappings
                    .iter()
                    .map(|mapping| mapping.constants.as_ref())
            }));
        for list in lists {
            for constant in list {
                work.step()?;
                let selected = match constant {
                    UnpivotConstant::Scalar(id) => {
                        let Some(expression) = fragment.expressions().get(*id) else {
                            continue;
                        };
                        if let crate::ExprKind::Constant(reference) = expression.kind {
                            let value = pools.resolve_observed(reference, &expression.ty, work)?;
                            work.flush()?;
                            let bytes = value.selected_payload_bytes_observed(
                                novarocks_type_contract::CompilePhase::Validate,
                                work.control(),
                            )?;
                            work.flush()?;
                            bytes
                        } else {
                            crate::validation::unpivot_scalar_literal_bytes(fragment, *id) as u64
                        }
                    }
                    UnpivotConstant::Int32List(values) => (values.len() as u64).saturating_mul(4),
                    UnpivotConstant::Utf8Map(entries) => {
                        let mut bytes = 0u64;
                        for (key, value) in entries {
                            work.step()?;
                            bytes = bytes
                                .saturating_add(key.len() as u64)
                                .saturating_add(value.len() as u64);
                        }
                        bytes
                    }
                };
                bytes = bytes.saturating_add(selected);
                if bytes > crate::MAX_UNPIVOT_LITERAL_BYTES as u64 {
                    return Err(ConstantReferenceError::Control(
                        CompileControlError::ResourceExhausted,
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_plan_constants_observed(
    plan: &crate::PhysicalPlan,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<(), ConstantReferenceError> {
    use novarocks_type_contract::CompilePhase;
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = (|| {
        let mut errors = crate::validation::ValidationContext::new();
        crate::resource::validate_plan_resources_observed(plan, &mut errors, &mut work)?;
        if !errors.is_empty() {
            return Err(ConstantReferenceError::Structure(
                crate::ValidationErrors::from_collector(errors),
            ));
        }
        let mut used = std::collections::BTreeSet::new();
        for fragment in plan.fragments().values() {
            work.step()?;
            work.flush()?;
            validate_fragment_constants_observed(fragment, plan.constants(), false, control)?;
            work.flush()?;
            for (_, expression) in fragment.expressions().iter() {
                work.step()?;
                if let crate::ExprKind::Constant(reference) = expression.kind {
                    used.insert(reference.pool);
                }
            }
        }
        if used.len() != plan.constants().entries().len() {
            return Err(ConstantReferenceError::UnusedPools);
        }
        Ok(())
    })();
    if matches!(result, Err(ConstantReferenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[cfg(test)]
mod tests;
