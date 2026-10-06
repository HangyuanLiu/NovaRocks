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

//! Original checked Package roots and authored occurrence IDs. These views
//! own only loan/ID buffers; stable writer inputs are built in a second stage.
//! All new backing requests and actual visitor steps are admitted before work.
//! Source union invoices and complete-package/host-memory admission remain
//! with the composer. Request bounds do not grant opaque allocator CPU.

use super::type_sources::{
    PackageTypeChannel, PackageTypeOccurrence, PackageTypeOwner, PackageTypeSource,
    PackageTypeSourceSink, visit_package_type_sources_admitted_in,
};
use crate::physical_type_v2::WriterTypeSource;
use arrow::datatypes::Field;
use novarocks_connector_contract::ConnectorWriteRecipeDraft;
use novarocks_physical_plan as p;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ControlResourceError, PureCompileControl,
    owned_resources::vec::{self as growth, VecPushGrowthFacts},
};
use std::{fmt, mem::size_of, ops::Range, sync::Arc};

#[derive(Debug)]
pub(crate) enum TypeViewError {
    Control(CompileControlError),
    SourceModel(&'static str),
    InvalidSource(&'static str),
}
impl From<CompileControlError> for TypeViewError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<ControlResourceError> for TypeViewError {
    fn from(error: ControlResourceError) -> Self {
        match error {
            ControlResourceError::Control(cause) => Self::Control(cause),
            ControlResourceError::SourceModel(message) => Self::SourceModel(message),
        }
    }
}
impl fmt::Display for TypeViewError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::SourceModel(message) | Self::InvalidSource(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for TypeViewError {}
fn add(a: usize, b: usize) -> Result<usize, TypeViewError> {
    a.checked_add(b)
        .ok_or(CompileControlError::ResourceExhausted.into())
}
fn mul(a: usize, b: usize) -> Result<usize, TypeViewError> {
    a.checked_mul(b)
        .ok_or(CompileControlError::ResourceExhausted.into())
}
fn id(position: usize) -> Result<u32, TypeViewError> {
    u32::try_from(position).map_err(|_| CompileControlError::ResourceExhausted.into())
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TypeViewLimits {
    pub max_occurrences: usize,
    pub max_value_roots: usize,
    pub max_field_roots: usize,
    pub max_writer_recipes: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct TypeViewFacts {
    pub occurrence_count: usize,
    pub value_root_count: usize,
    pub field_root_count: usize,
    pub writer_recipe_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}

/// One child contribution, retained across collection and input loan creation.
/// Prefix facts replace this child's previous snapshot at the real parent;
/// source B is the parent's exact original union, never an independent wallet.
pub(crate) struct TypeViewBudget<'source, 'control, 'parent> {
    package: &'source p::FragmentPackage,
    control: &'control dyn PureCompileControl,
    limits: TypeViewLimits,
    source: usize,
    facts: TypeViewFacts,
    admit: &'parent mut dyn FnMut(&TypeViewFacts) -> Result<(), CompileControlError>,
}
impl<'source, 'control, 'parent> TypeViewBudget<'source, 'control, 'parent> {
    pub(crate) fn new_in(
        package: &'source p::FragmentPackage,
        source_retained_bytes: usize,
        limits: TypeViewLimits,
        admit: &'parent mut dyn FnMut(&TypeViewFacts) -> Result<(), CompileControlError>,
        work: &CompileCheckpoints<'control>,
    ) -> Result<Self, TypeViewError> {
        if source_retained_bytes < size_of::<p::FragmentPackage>() {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        let mut model = Self {
            package,
            control: work.control(),
            limits,
            source: source_retained_bytes,
            facts: TypeViewFacts {
                coexisting_source_and_request_bytes_upper_bound: source_retained_bytes,
                ..TypeViewFacts::default()
            },
            admit,
        };
        model.gate()?;
        Ok(model)
    }
    pub(crate) fn facts(&self) -> TypeViewFacts {
        self.facts
    }
    fn require_collection_floor(&self, floor: TypeViewFacts) -> Result<(), TypeViewError> {
        let current = self.facts;
        if current.occurrence_count < floor.occurrence_count
            || current.value_root_count < floor.value_root_count
            || current.field_root_count < floor.field_root_count
            || current.writer_recipe_count < floor.writer_recipe_count
            || current.allocation_requests_upper_bound < floor.allocation_requests_upper_bound
            || current.allocation_request_bytes_upper_bound
                < floor.allocation_request_bytes_upper_bound
            || current.coexisting_source_and_request_bytes_upper_bound
                < floor.coexisting_source_and_request_bytes_upper_bound
            || current.cumulative_work_upper_bound < floor.cumulative_work_upper_bound
        {
            return Err(TypeViewError::InvalidSource(
                "writer type loans omit the captured collection contribution",
            ));
        }
        Ok(())
    }
    fn gate(&mut self) -> Result<(), TypeViewError> {
        let f = self.facts;
        let l = self.limits;
        if f.occurrence_count > l.max_occurrences
            || f.value_root_count > l.max_value_roots
            || f.field_root_count > l.max_field_roots
            || f.writer_recipe_count > l.max_writer_recipes
            || f.allocation_requests_upper_bound > l.max_allocation_requests
            || f.allocation_request_bytes_upper_bound > l.max_allocation_request_bytes
            || f.coexisting_source_and_request_bytes_upper_bound
                > l.max_coexisting_source_and_request_bytes
            || f.cumulative_work_upper_bound > l.max_work
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        (self.admit)(&f)?;
        Ok(())
    }
    fn check(
        &self,
        package: &p::FragmentPackage,
        work: &CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        if !std::ptr::eq(self.package, package) || !std::ptr::addr_eq(self.control, work.control())
        {
            return Err(TypeViewError::InvalidSource(
                "type views use a different package or caller control",
            ));
        }
        Ok(())
    }
    fn request(&mut self, facts: VecPushGrowthFacts) -> Result<(), TypeViewError> {
        if let Some(layout) = facts.requested_backing {
            let bytes = layout.size();
            self.facts.allocation_requests_upper_bound =
                add(self.facts.allocation_requests_upper_bound, 1)?;
            self.facts.allocation_request_bytes_upper_bound =
                add(self.facts.allocation_request_bytes_upper_bound, bytes)?;
            // Each real grow may move the old buffer. The new request, its
            // initialization/movement and borrowed-element cleanup are captured
            // before reserve. All these view elements have no owned Drop body.
            let old = facts.old_backing.map_or(0, |layout| layout.size());
            self.facts.cumulative_work_upper_bound = add(
                self.facts.cumulative_work_upper_bound,
                add(128, mul(add(bytes, old)?, 4)?)?,
            )?;
        }
        self.facts.coexisting_source_and_request_bytes_upper_bound =
            add(self.source, self.facts.allocation_request_bytes_upper_bound)?;
        Ok(())
    }
    fn before_steps(&mut self, count: usize) -> Result<(), TypeViewError> {
        self.facts.cumulative_work_upper_bound =
            add(self.facts.cumulative_work_upper_bound, count)?;
        self.gate()
    }
    fn reserve<T>(
        &mut self,
        rows: &mut Vec<T>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        // The real growth geometry has already been added together with all
        // other requests for this capture before any completed callback.
        growth::reserve_for_push_in(rows, &mut |_| self.gate(), work)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TypeRootReference {
    Value(u32),
    Field(u32),
    /// A Cast borrows only its target carrier. Its ID is the previously
    /// captured expression Value root; the original Cast validator owns equality.
    CastCarrier {
        expression_value_type_id: u32,
    },
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct TypeOccurrenceRow<'source> {
    pub occurrence: PackageTypeOccurrence,
    pub root: TypeRootReference,
    pub source: PackageTypeSource<'source>,
}
struct WriterRoots<'source> {
    node: p::NodeId,
    recipe: &'source ConnectorWriteRecipeDraft,
    field_ids: Range<usize>,
}

/// Movable owned ID/loan buffers, with no references into their own storage.
/// The package objects remain original loans, including each repeated FVT.
pub(crate) struct PackageTypeViews<'source> {
    package: &'source p::FragmentPackage,
    control: &'source dyn PureCompileControl,
    collection_floor: TypeViewFacts,
    occurrences: Vec<TypeOccurrenceRow<'source>>,
    values: Vec<(u32, &'source p::ValueType)>,
    fields: Vec<(u32, &'source Arc<Field>)>,
    writer_field_ids: Vec<u32>,
    writer_roots: Vec<WriterRoots<'source>>,
}
impl<'source> PackageTypeViews<'source> {
    pub(crate) fn occurrences(&self) -> &[TypeOccurrenceRow<'source>] {
        &self.occurrences
    }
    pub(crate) fn values(&self) -> &[(u32, &'source p::ValueType)] {
        &self.values
    }
    pub(crate) fn fields(&self) -> &[(u32, &'source Arc<Field>)] {
        &self.fields
    }
    /// A separate owned input Vec borrows stable ID slices from these views.
    /// It is a local compose input, never stored back in its borrowed owner.
    pub(crate) fn writer_sources_in<'views>(
        &'views self,
        budget: &mut TypeViewBudget<'source, '_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<WriterTypeSource<'views>>, TypeViewError> {
        budget.check(self.package, work)?;
        budget.require_collection_floor(self.collection_floor)?;
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(TypeViewError::InvalidSource(
                "writer type loans use a different caller control",
            ));
        }
        let mut sources = Vec::new();
        let mut position = 0;
        for (node, recipe) in self.package.writes() {
            // Header comparisons and the loan-buffer push are known before
            // the first completed observation for this input.
            let request = growth::push_growth(&sources)?;
            budget.request(request)?;
            budget.before_steps(4)?;
            let range = if let Some(group) = self.writer_roots.get(position)
                && group.node == *node
            {
                if !std::ptr::eq(group.recipe, recipe)
                    || group.field_ids.len() != recipe.input().field_count()
                {
                    return Err(TypeViewError::InvalidSource(
                        "writer type occurrence group differs from its original recipe",
                    ));
                }
                position += 1;
                group.field_ids.clone()
            } else {
                if recipe.input().field_count() != 0 {
                    return Err(TypeViewError::InvalidSource(
                        "writer type occurrence group is absent",
                    ));
                }
                0..0
            };
            work.step()?;
            budget.reserve(&mut sources, work)?;
            sources.push(WriterTypeSource::new(recipe, &self.writer_field_ids[range]));
            work.step()?;
        }
        budget.before_steps(1)?;
        let complete = position == self.writer_roots.len();
        work.step()?;
        if !complete {
            return Err(TypeViewError::InvalidSource(
                "writer type occurrence group is unconsumed",
            ));
        }
        Ok(sources)
    }
}

struct Capture<'source, 'budget, 'control, 'parent> {
    views: PackageTypeViews<'source>,
    budget: &'budget mut TypeViewBudget<'source, 'control, 'parent>,
    next_field_id: usize,
}
impl<'source> PackageTypeSourceSink<'source, TypeViewError> for Capture<'source, '_, '_, '_> {
    fn capture(
        &mut self,
        occurrence: PackageTypeOccurrence,
        source: PackageTypeSource<'source>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeViewError> {
        let row_growth = growth::push_growth(&self.views.occurrences)?;
        self.budget.request(row_growth)?;
        self.budget.facts.occurrence_count = row_growth.next_len;
        let root;
        match source {
            PackageTypeSource::Value(value) => {
                let request = growth::push_growth(&self.views.values)?;
                root = TypeRootReference::Value(id(self.views.values.len())?);
                self.budget.request(request)?;
                self.budget.facts.value_root_count = request.next_len;
                self.budget.before_steps(2)?;
                self.budget.reserve(&mut self.views.occurrences, work)?;
                self.budget.reserve(&mut self.views.values, work)?;
                self.views
                    .values
                    .push((id(self.views.values.len())?, value));
            }
            PackageTypeSource::StrictField(field) => {
                let request = growth::push_growth(&self.views.fields)?;
                let field_id = id(self.next_field_id)?;
                root = TypeRootReference::Field(field_id);
                self.budget.request(request)?;
                self.budget.facts.field_root_count = add(self.budget.facts.field_root_count, 1)?;
                self.budget.before_steps(2)?;
                self.budget.reserve(&mut self.views.occurrences, work)?;
                self.budget.reserve(&mut self.views.fields, work)?;
                self.views.fields.push((field_id, field));
                self.next_field_id = add(self.next_field_id, 1)?;
            }
            PackageTypeSource::Carrier(_) => {
                let previous =
                    self.views
                        .occurrences
                        .last()
                        .ok_or(TypeViewError::InvalidSource(
                            "Cast expression Value root is absent",
                        ))?;
                let TypeRootReference::Value(expression_value_type_id) = previous.root else {
                    return Err(TypeViewError::InvalidSource(
                        "Cast expression root is not a complete Value type",
                    ));
                };
                if previous.occurrence.owner != occurrence.owner
                    || previous.occurrence.channel != PackageTypeChannel::Value
                    || !matches!(occurrence.owner, PackageTypeOwner::Expression(_))
                    || occurrence.channel != PackageTypeChannel::CastTarget
                {
                    return Err(TypeViewError::InvalidSource(
                        "Cast target is not adjacent to its original expression Value root",
                    ));
                }
                root = TypeRootReference::CastCarrier {
                    expression_value_type_id,
                };
                self.budget.before_steps(2)?;
                self.budget.reserve(&mut self.views.occurrences, work)?;
            }
            PackageTypeSource::WriterField { recipe, .. } => {
                let PackageTypeOwner::Recipe(node) = occurrence.owner else {
                    return Err(TypeViewError::InvalidSource(
                        "writer field has no original recipe owner",
                    ));
                };
                let field_id = id(self.next_field_id)?;
                root = TypeRootReference::Field(field_id);
                let is_new = self
                    .views
                    .writer_roots
                    .last()
                    .is_none_or(|group| group.node != node);
                let ids_growth = growth::push_growth(&self.views.writer_field_ids)?;
                self.budget.request(ids_growth)?;
                if is_new {
                    let groups_growth = growth::push_growth(&self.views.writer_roots)?;
                    self.budget.request(groups_growth)?;
                }
                self.budget.facts.field_root_count = add(self.budget.facts.field_root_count, 1)?;
                // The full checked writes map includes empty shapes as well;
                // reserve its original namespace cardinality once, no guessed count.
                self.budget.facts.writer_recipe_count = self.views.package.writes().len();
                self.budget.before_steps(4)?;
                self.budget.reserve(&mut self.views.occurrences, work)?;
                self.budget
                    .reserve(&mut self.views.writer_field_ids, work)?;
                if is_new {
                    self.budget.reserve(&mut self.views.writer_roots, work)?;
                    let begin = self.views.writer_field_ids.len();
                    self.views.writer_roots.push(WriterRoots {
                        node,
                        recipe,
                        field_ids: begin..begin,
                    });
                }
                let group =
                    self.views
                        .writer_roots
                        .last_mut()
                        .ok_or(TypeViewError::InvalidSource(
                            "writer group was not captured",
                        ))?;
                if !std::ptr::eq(group.recipe, recipe) {
                    return Err(TypeViewError::InvalidSource(
                        "writer field changed its original recipe owner",
                    ));
                }
                self.views.writer_field_ids.push(field_id);
                group.field_ids.end = self.views.writer_field_ids.len();
                self.next_field_id = add(self.next_field_id, 1)?;
            }
        }
        self.views.occurrences.push(TypeOccurrenceRow {
            occurrence,
            root,
            source,
        });
        work.step()?;
        Ok(())
    }
    fn before_completed(&mut self) -> Result<(), TypeViewError> {
        self.budget.before_steps(1)
    }
}

pub(crate) fn collect_package_type_views_in<'source>(
    budget: &mut TypeViewBudget<'source, 'source, '_>,
    work: &mut CompileCheckpoints<'source>,
) -> Result<PackageTypeViews<'source>, TypeViewError> {
    budget.check(budget.package, work)?;
    // This full namespace count is known even for zero-field writer recipes,
    // before the first namespace or root is observed by the original visitor.
    budget.facts.writer_recipe_count = budget.package.writes().len();
    budget.gate()?;
    let views = PackageTypeViews {
        package: budget.package,
        control: work.control(),
        collection_floor: TypeViewFacts::default(),
        occurrences: Vec::new(),
        values: Vec::new(),
        fields: Vec::new(),
        writer_field_ids: Vec::new(),
        writer_roots: Vec::new(),
    };
    let package = budget.package;
    let mut capture = Capture {
        views,
        budget,
        next_field_id: 0,
    };
    visit_package_type_sources_admitted_in(package, &mut capture, work)?;
    capture.views.collection_floor = capture.budget.facts();
    Ok(capture.views)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_type_v2::{
        PackageTypeProjectionLimits, encode_borrowed_type_table_writer_sources_in,
    };
    use arrow::datatypes::DataType;
    use novarocks_connector_contract as c;
    use novarocks_type_contract::CompilePhase;
    use std::{collections::HashMap, sync::Mutex};

    // The bounded fresh original fixture and its new view loans fit this
    // conservative invoice. It is not private allocator introspection or MEM.
    const SOURCE: usize = 128 * 1024 * 1024;
    const CAUSES: [CompileControlError; 3] = [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ];
    #[derive(Default)]
    struct Control {
        events: Mutex<Vec<u32>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Encode);
            let mut events = self.events.lock().unwrap();
            let at = events.len();
            if let Some((stop, _)) = self.stop {
                assert!(at <= stop, "callback after refusal");
            }
            events.push(units);
            if let Some((stop, cause)) = self.stop
                && at == stop
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn limits() -> TypeViewLimits {
        TypeViewLimits {
            max_occurrences: 1000,
            max_value_roots: 1000,
            max_field_roots: 1000,
            max_writer_recipes: 100,
            max_allocation_requests: 1000,
            max_allocation_request_bytes: 1024 * 1024,
            max_coexisting_source_and_request_bytes: SOURCE + 1024 * 1024,
            max_work: 16 * 1024 * 1024,
        }
    }
    fn package() -> p::FragmentPackage {
        let instance = c::ConnectorInstanceId::try_from_canonical("lake").unwrap();
        let catalog =
            c::CatalogHandle::new(instance.clone(), c::CatalogVersion::from_bytes([7; 32]));
        let provider = c::ConnectorProviderId::parse("iceberg").unwrap();
        let binding = c::ConnectorWriteBinding::new(
            c::ConnectorInstanceDescriptor {
                provider_id: provider.clone(),
                instance_id: instance,
            },
            catalog.clone(),
        );
        let payload = c::ConnectorEncodedPayload::new(
            c::ConnectorEnvelopeHeader::new(
                provider,
                catalog,
                c::ConnectorCodecCategory::WriteHandle,
                c::ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![7u8].into(),
        );
        let recipe = c::ConnectorWriteRecipeDraft::try_new(
            binding,
            payload,
            c::ConnectorWriteInputShape::Data {
                fields: vec![c::ConnectorWriteFieldBinding::new(
                    c::ConnectorWriteFieldToken::from_bytes([1; 32]),
                    Field::new("v", DataType::Int64, false).with_metadata(HashMap::from([
                        ("large".into(), "雪".repeat(6826) + "ab"),
                        ("embedded".into(), "a\0b".into()),
                    ])),
                )],
            },
        )
        .unwrap();
        crate::physical_type_v2::sender_tests::checked_writer_package(recipe)
    }
    #[test]
    fn checked_package_views_feed_original_borrowed_type_emitter_without_rebuilding_roots() {
        let package = package();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut prefix = TypeViewFacts::default();
        let mut admit = |facts: &TypeViewFacts| {
            prefix = *facts;
            Ok(())
        };
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work).unwrap();
        let views = collect_package_type_views_in(&mut budget, &mut work).unwrap();
        let writers = views.writer_sources_in(&mut budget, &mut work).unwrap();
        assert_eq!(
            (
                views.occurrences().len(),
                views.values().len(),
                views.fields().len(),
                writers.len()
            ),
            (24, 23, 0, 1)
        );
        assert_eq!(
            (
                budget.facts().occurrence_count,
                budget.facts().value_root_count,
                budget.facts().field_root_count
            ),
            (24, 23, 1)
        );
        let encoded = encode_borrowed_type_table_writer_sources_in(
            views.values(),
            views.fields(),
            &writers,
            SOURCE,
            PackageTypeProjectionLimits {
                max_definitions: 100_000,
                max_expanded_nodes: 1_000_000,
                max_string_bytes: 64 * 1024 * 1024,
                max_allocation_requests: 1_000_000,
                max_allocation_request_bytes: 512 * 1024 * 1024,
                max_coexisting_source_and_request_bytes: 1024 * 1024 * 1024,
                max_work: usize::MAX / 4,
            },
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        assert_eq!(
            (
                encoded.as_wire().value_types.len(),
                encoded.as_wire().fields.len()
            ),
            (23, 1)
        );
        for row in views.occurrences() {
            match (row.root, row.source) {
                (TypeRootReference::Value(id), PackageTypeSource::Value(original)) => {
                    assert!(std::ptr::eq(
                        encoded.value_type_observed(id, &mut work).unwrap().unwrap(),
                        original
                    ));
                }
                (TypeRootReference::Field(id), PackageTypeSource::WriterField { binding, .. }) => {
                    assert!(std::ptr::eq(
                        encoded
                            .field_source_observed(id, &mut work)
                            .unwrap()
                            .unwrap(),
                        binding.field()
                    ));
                }
                _ => panic!("unexpected root in original Data fixture"),
            }
        }
        assert_eq!(
            encoded.as_wire().fields[0].metadata[1].value.len(),
            20 * 1024
        );
        work.finish().unwrap();
        assert!(budget.facts().allocation_requests_upper_bound > 0);
    }
    #[test]
    fn views_every_actual_callback_returns_primary_control_and_source_guard_precedes_parent() {
        let package = package();
        let run = |stop| {
            let control = Control {
                stop,
                ..Default::default()
            };
            let result = (|| {
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode)?;
                let mut admit = |_: &TypeViewFacts| Ok(());
                let mut budget =
                    TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work)?;
                let views = collect_package_type_views_in(&mut budget, &mut work)?;
                let _loans = views.writer_sources_in(&mut budget, &mut work)?;
                work.finish()?;
                Ok::<_, TypeViewError>(())
            })();
            (result, control.events.into_inner().unwrap())
        };
        let (result, trace) = run(None);
        assert!(result.is_ok());
        assert!(trace.len() > 2);
        for at in 0..trace.len() {
            for cause in CAUSES {
                let (result, stopped) = run(Some((at, cause)));
                assert!(matches!(result, Err(TypeViewError::Control(actual)) if actual == cause));
                assert_eq!(stopped, trace[..=at]);
            }
        }
        let owner = Control::default();
        let foreign = Control::default();
        let work = CompileCheckpoints::try_new(&owner, CompilePhase::Encode).unwrap();
        let mut seen = 0;
        let mut admit = |_: &TypeViewFacts| {
            seen += 1;
            Ok(())
        };
        let mut budget =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut admit, &work).unwrap();
        let mut foreign_work = CompileCheckpoints::try_new(&foreign, CompilePhase::Encode).unwrap();
        assert!(matches!(
            collect_package_type_views_in(&mut budget, &mut foreign_work),
            Err(TypeViewError::InvalidSource(_))
        ));
        assert_eq!(foreign.events.lock().unwrap().as_slice(), &[0]);
        assert_eq!(seen, 1);
    }
    #[test]
    fn writer_loans_refuse_a_reset_collection_contribution_before_parent_or_observation() {
        let package = package();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut original_admit = |_: &TypeViewFacts| Ok(());
        let mut original =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut original_admit, &work).unwrap();
        let views = collect_package_type_views_in(&mut original, &mut work).unwrap();
        let trace = control.events.lock().unwrap().clone();
        let mut called = 0;
        let mut reset_admit = |_: &TypeViewFacts| {
            called += 1;
            Ok(())
        };
        let mut reset =
            TypeViewBudget::new_in(&package, SOURCE, limits(), &mut reset_admit, &work).unwrap();
        assert!(matches!(
            views.writer_sources_in(&mut reset, &mut work),
            Err(TypeViewError::InvalidSource(_))
        ));
        assert_eq!(control.events.lock().unwrap().as_slice(), trace);
        assert_eq!(reset.facts().allocation_requests_upper_bound, 0);
        assert_eq!(called, 1);
        let loans = views.writer_sources_in(&mut original, &mut work).unwrap();
        assert_eq!(loans.len(), 1);
        work.finish().unwrap();
    }
    #[test]
    fn all_first_capture_growth_is_admitted_before_pending_copy_callback() {
        let package = package();
        let row = growth::push_growth(&Vec::<TypeOccurrenceRow<'_>>::new()).unwrap();
        let value = growth::push_growth(&Vec::<(u32, &p::ValueType)>::new()).unwrap();
        let first_bytes =
            row.requested_backing.unwrap().size() + value.requested_backing.unwrap().size();
        for cause in CAUSES {
            let control = Control {
                stop: Some((3, cause)),
                ..Default::default()
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let spelling = "x".repeat(255);
            let copied = novarocks_type_contract::owned_resources::copy::copy_string::<
                CompileControlError,
            >(&spelling, &mut work)
            .unwrap();
            assert_eq!(copied, spelling);
            let mut caps = limits();
            caps.max_allocation_request_bytes = first_bytes - 1;
            let mut admit = |_: &TypeViewFacts| Ok(());
            let mut budget =
                TypeViewBudget::new_in(&package, SOURCE, caps, &mut admit, &work).unwrap();
            assert!(matches!(
                collect_package_type_views_in(&mut budget, &mut work),
                Err(TypeViewError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(budget.facts().allocation_requests_upper_bound, 2);
            assert_eq!(
                budget.facts().allocation_request_bytes_upper_bound,
                first_bytes
            );
            assert_eq!(control.events.lock().unwrap().as_slice(), &[0, 0, 1]);
        }
    }
    #[test]
    fn exact_view_envelopes_pass_and_each_actual_axis_one_under_refuses() {
        let package = package();
        let run = |caps| {
            let control = Control::default();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            let mut admit = |_: &TypeViewFacts| Ok(());
            let mut budget =
                TypeViewBudget::new_in(&package, SOURCE, caps, &mut admit, &work).unwrap();
            let result = collect_package_type_views_in(&mut budget, &mut work)
                .and_then(|views| views.writer_sources_in(&mut budget, &mut work).map(|_| ()));
            (result, budget.facts())
        };
        let (result, facts) = run(limits());
        assert!(result.is_ok());
        let exact = TypeViewLimits {
            max_occurrences: facts.occurrence_count,
            max_value_roots: facts.value_root_count,
            max_field_roots: facts.field_root_count,
            max_writer_recipes: facts.writer_recipe_count,
            max_allocation_requests: facts.allocation_requests_upper_bound,
            max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
            max_coexisting_source_and_request_bytes: facts
                .coexisting_source_and_request_bytes_upper_bound,
            max_work: facts.cumulative_work_upper_bound,
        };
        let (result, repeated) = run(exact);
        assert!(result.is_ok());
        assert_eq!(repeated, facts);
        for axis in 0..8 {
            let mut caps = exact;
            match axis {
                0 => caps.max_occurrences -= 1,
                1 => caps.max_value_roots -= 1,
                2 => caps.max_field_roots -= 1,
                3 => caps.max_writer_recipes -= 1,
                4 => caps.max_allocation_requests -= 1,
                5 => caps.max_allocation_request_bytes -= 1,
                6 => caps.max_coexisting_source_and_request_bytes -= 1,
                7 => caps.max_work -= 1,
                _ => unreachable!(),
            }
            let (result, _) = run(caps);
            assert!(
                matches!(
                    result,
                    Err(TypeViewError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ),
                "axis {axis}"
            );
        }
    }
}
