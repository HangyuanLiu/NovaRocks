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

//! Complete borrowed expression namespace. Every wire kind is inspected, but
//! no legacy Physical binding, expression arena or executable package is
//! manufactured. Fragment graph, lexical scope, operator typing and occurrence
//! effects remain with their original checked owners.

use super::namespace::{
    ExpressionNamespaceWriteFacts, add, bytes, cap, charge, check, finish, mul, shape,
    tree_lookup_work, vector,
};
use super::{ExpressionCodecError as Error, ExpressionProjectionLimits};
use crate::{
    binding_index_v2::BindingIndex,
    borrowed_type_resources::{preflight_type_binding, verify_type_binding},
    physical_aggregate_binding_v2::PreparedAggregateBindingHeaders,
    physical_binding_v2::PreparedFunctionBindingHeaders,
    physical_semantics_v2::{SemanticsCodecError, decode_decimal_policy, decode_reference},
    physical_type_v2::{DecodedTypeTable, TypeCodecError},
    physical_value_v2::DecodedValues,
};
use novarocks_physical_plan::{ConstantPoolId, ConstantPools, ConstantReference};
use novarocks_proto_models::{physical_package_v2 as wire, physical_semantics_v2 as semantics};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl, SemanticParameterKey,
    SemanticParameters, arrow_data_types_exact_borrowed_observed,
};
use std::mem::size_of;

/// Same seven numerical quantities as the emission author. The only new heap
/// request in this receiving component is the original sparse usize index.
pub type ExpressionNamespaceReadFacts = ExpressionNamespaceWriteFacts;

/// Immutable receiving correspondence, retaining every original dependency.
/// Parameters and pools are borrowed checked owners; this token does not
/// invent provenance for their construction or replace Fragment validation.
pub struct DecodedExpressions<'loan, 'wire, 'control> {
    definitions: &'wire [wire::ExpressionDefinition],
    values: &'loan DecodedValues<'loan, 'wire, 'control>,
    functions: &'loan PreparedFunctionBindingHeaders<'wire>,
    aggregates: &'loan PreparedAggregateBindingHeaders<'loan, 'wire>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    indices: BindingIndex,
    facts: ExpressionNamespaceReadFacts,
    source_retained_bytes: usize,
    control: &'control dyn PureCompileControl,
}
impl<'loan, 'wire, 'control> DecodedExpressions<'loan, 'wire, 'control> {
    pub fn as_wire(&self) -> &'wire [wire::ExpressionDefinition] {
        self.definitions
    }
    pub const fn facts(&self) -> &ExpressionNamespaceReadFacts {
        &self.facts
    }
    pub fn source_count(&self) -> usize {
        self.definitions.len()
    }
    pub fn values(&self) -> &'loan DecodedValues<'loan, 'wire, 'control> {
        self.values
    }
    pub fn types(&self) -> &DecodedTypeTable {
        self.values.types()
    }
    pub fn functions(&self) -> &'loan PreparedFunctionBindingHeaders<'wire> {
        self.functions
    }
    pub fn aggregates(&self) -> &'loan PreparedAggregateBindingHeaders<'loan, 'wire> {
        self.aggregates
    }
    pub fn parameters(&self) -> &'loan SemanticParameters {
        self.parameters
    }
    pub fn pools(&self) -> &'loan ConstantPools {
        self.pools
    }
    pub(crate) fn original_control(&self) -> &'control dyn PureCompileControl {
        self.control
    }
    pub fn lookup_work_upper_bound(&self) -> Result<usize, Error> {
        tree_lookup_work(self.definitions.len())
    }
    pub fn definition(&self, id: u32) -> Result<Option<&'wire wire::ExpressionDefinition>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.definition_observed(id, &mut work);
        finish(work, result)
    }
    pub(crate) fn definition_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'wire wire::ExpressionDefinition>, Error> {
        Ok(self
            .indices
            .find(id, |at| self.definitions[at].id, work)?
            .map(|at| &self.definitions[at]))
    }
    pub fn source_id(&self, source: &wire::ExpressionDefinition) -> Result<u32, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = (|| {
            let actual = self.definition_observed(source.id, &mut work)?;
            let same = actual.is_some_and(|actual| std::ptr::eq(actual, source));
            work.step()?;
            if !same {
                return Err(shape("expression DTO is not from this receiving namespace"));
            }
            Ok(source.id)
        })();
        finish(work, result)
    }
    pub fn value_type(&self, id: u32) -> Result<Option<&FunctionValueType>, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.value_type_observed(id, &mut work);
        finish(work, result)
    }
    pub(crate) fn value_type_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&FunctionValueType>, Error> {
        let Some(definition) = self.definition_observed(id, work)? else {
            return Ok(None);
        };
        let type_id = required(definition.value_type_id, "expression value type is absent")?;
        work.flush()?;
        let value = self.types().value_type(type_id);
        work.step()?;
        work.flush()?;
        Ok(value)
    }
    /// Necessary floor, not a whole retained-size measurement or a MEM grant.
    /// The original whole-source invoice includes all borrowed dependencies.
    pub fn retained_invoice_floor(&self) -> Result<usize, Error> {
        let mut work = CompileCheckpoints::try_new(self.original_control(), CompilePhase::Decode)?;
        let result = self.retained_floor_observed(&mut work);
        finish(work, result)
    }
    pub(crate) fn retained_floor_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, Error> {
        let floor = add(
            self.source_retained_bytes,
            add(size_of::<Self>(), self.indices.backing_bytes()?)?,
        );
        work.step()?;
        floor
    }
}

/// Borrow all seventeen closed wire kinds. Types and control come from the
/// original value namespace; function and aggregate tokens must retain that
/// same immutable correspondence. Source is a mandatory whole coexistence
/// invoice, not encoded length, a guessed capacity or allocation authority.
#[allow(clippy::too_many_arguments)]
pub fn decode_expression_definitions<'loan, 'wire, 'control>(
    definitions: &'wire [wire::ExpressionDefinition],
    values: &'loan DecodedValues<'loan, 'wire, 'control>,
    functions: &'loan PreparedFunctionBindingHeaders<'wire>,
    aggregates: &'loan PreparedAggregateBindingHeaders<'loan, 'wire>,
    parameters: &'loan SemanticParameters,
    pools: &'loan ConstantPools,
    source_retained_bytes: usize,
    limits: ExpressionProjectionLimits,
) -> Result<DecodedExpressions<'loan, 'wire, 'control>, Error> {
    let control = values.original_control();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let same = std::ptr::eq(values.types(), functions.type_table())
            && std::ptr::eq(functions, aggregates.functions())
            && std::ptr::eq(control, functions.original_control());
        work.step()?;
        if !same {
            return Err(shape(
                "receiving expression dependencies are not the same original namespaces",
            ));
        }
        let facts = preflight(
            definitions,
            values,
            functions,
            aggregates,
            source_retained_bytes,
            limits,
            &mut work,
        )?;
        let indices = BindingIndex::prepare(definitions.len(), |at| definitions[at].id, &mut work)?;
        let mut output = DecodedExpressions {
            definitions,
            values,
            functions,
            aggregates,
            parameters,
            pools,
            indices,
            facts,
            source_retained_bytes,
            control,
        };
        output.validate(limits, &mut work)?;
        Ok(output)
    })();
    finish(work, result)
}

fn required(id: Option<u32>, message: &'static str) -> Result<u32, Error> {
    id.ok_or_else(|| shape(message))
}

/// Visit actual ordered references without scratch, filtering or cloning.
/// Scope references are included independently of kind dependencies.
fn references(
    definition: &wire::ExpressionDefinition,
    mut visit: impl FnMut(u32) -> Result<(), Error>,
) -> Result<(), Error> {
    use wire::expression_definition::Kind as K;
    if let Some(scope) = definition.lambda_scope_expr_id {
        visit(scope)?;
    }
    match definition
        .kind
        .as_ref()
        .ok_or_else(|| shape("expression kind is absent"))?
    {
        K::ValueId(_) | K::Literal(_) => {}
        K::LambdaParameter(value) => visit(required(
            value.lambda_expr_id,
            "lambda parameter owner is absent",
        )?)?,
        K::Unary(value) => visit(required(value.expr_id, "unary operand is absent")?)?,
        K::Binary(value) => {
            visit(required(
                value.left_expr_id,
                "binary left operand is absent",
            )?)?;
            visit(required(
                value.right_expr_id,
                "binary right operand is absent",
            )?)?;
        }
        K::Conjunction(value) | K::Disjunction(value) => {
            for id in &value.expr_ids {
                visit(*id)?;
            }
        }
        K::FunctionCall(value) => {
            for id in &value.argument_expr_ids {
                visit(*id)?;
            }
        }
        K::Lambda(value) => visit(required(value.body_expr_id, "lambda body is absent")?)?,
        K::Cast(value) => visit(required(value.expr_id, "cast operand is absent")?)?,
        K::IsNull(value) => visit(required(value.expr_id, "null-test operand is absent")?)?,
        K::InList(value) => {
            visit(required(value.expr_id, "in-list operand is absent")?)?;
            for id in &value.list_expr_ids {
                visit(*id)?;
            }
        }
        K::Between(value) => {
            visit(required(value.expr_id, "between operand is absent")?)?;
            visit(required(
                value.low_expr_id,
                "between low operand is absent",
            )?)?;
            visit(required(
                value.high_expr_id,
                "between high operand is absent",
            )?)?;
        }
        K::Like(value) => {
            visit(required(value.expr_id, "like operand is absent")?)?;
            visit(required(value.pattern_expr_id, "like pattern is absent")?)?;
        }
        K::CaseExpression(value) => {
            if let Some(id) = value.operand_expr_id {
                visit(id)?;
            }
            for arm in &value.arms {
                visit(required(arm.when_expr_id, "case when operand is absent")?)?;
                visit(required(arm.then_expr_id, "case then operand is absent")?)?;
            }
            if let Some(id) = value.else_expr_id {
                visit(id)?;
            }
        }
        K::IsTruthValue(value) => visit(required(value.expr_id, "truth-test operand is absent")?)?,
        K::WindowCall(value) => {
            for id in &value.argument_expr_ids {
                visit(*id)?;
            }
            for key in &value.function_order_by {
                visit(required(key.expr_id, "window sort operand is absent")?)?;
            }
            if let Some(frame) = &value.frame {
                for bound in [&frame.start, &frame.end] {
                    let bound = bound
                        .as_ref()
                        .ok_or_else(|| shape("window frame bound is absent"))?;
                    match super::receiving_grammar::decode_window_bound(bound)? {
                        novarocks_physical_plan::WindowBound::Preceding(id)
                        | novarocks_physical_plan::WindowBound::Following(id) => visit(id.get())?,
                        novarocks_physical_plan::WindowBound::UnboundedPreceding
                        | novarocks_physical_plan::WindowBound::CurrentRow
                        | novarocks_physical_plan::WindowBound::UnboundedFollowing => {}
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn preflight(
    definitions: &[wire::ExpressionDefinition],
    values: &DecodedValues<'_, '_, '_>,
    functions: &PreparedFunctionBindingHeaders<'_>,
    aggregates: &PreparedAggregateBindingHeaders<'_, '_>,
    source: usize,
    limits: ExpressionProjectionLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ExpressionNamespaceReadFacts, Error> {
    let count = definitions.len();
    cap(count, limits.max_definitions, work)?;
    // Admit the count walk and index build before any source traversal/reserve.
    let mut facts = ExpressionNamespaceReadFacts {
        definition_count: count,
        type_reference_count: count,
        cumulative_work_upper_bound: add(128, mul(count, add(96, tree_lookup_work(count)?)?)?)?,
        ..Default::default()
    };
    vector::<usize>(&mut facts, count)?;
    facts.coexisting_source_and_request_bytes_upper_bound =
        add(source, facts.new_allocation_request_bytes_upper_bound)?;
    check(&facts, limits, work)?;
    let mut floor = bytes::<wire::ExpressionDefinition>(count)?;
    // These are lower floors of one declared whole-source invoice. The caller
    // must include independent backings too; max does not invent that proof.
    let dependency_floor = values
        .retained_floor_observed(work)?
        .max(functions.retained_invoice_floor()?)
        .max(aggregates.retained_invoice_floor()?);
    work.step()?;
    cap(floor.max(dependency_floor), source, work)?;
    for definition in definitions {
        use wire::expression_definition::Kind as K;
        let kind = definition
            .kind
            .as_ref()
            .ok_or_else(|| shape("expression kind is absent"))?;
        let owned = match kind {
            K::Conjunction(value) | K::Disjunction(value) => {
                bytes::<u32>(value.expr_ids.capacity())?
            }
            K::FunctionCall(value) => bytes::<u32>(value.argument_expr_ids.capacity())?,
            K::Lambda(value) => bytes::<u32>(value.parameter_value_type_ids.capacity())?,
            K::InList(value) => bytes::<u32>(value.list_expr_ids.capacity())?,
            K::CaseExpression(value) => bytes::<wire::WhenThen>(value.arms.capacity())?,
            K::WindowCall(value) => add(
                bytes::<u32>(value.argument_expr_ids.capacity())?,
                bytes::<wire::SortExpression>(value.function_order_by.capacity())?,
            )?,
            _ => 0,
        };
        floor = add(floor, owned)?;
        cap(floor, source, work)?;
        references(definition, |_| {
            facts.expression_reference_count = add(facts.expression_reference_count, 1)?;
            charge(&mut facts, add(16, tree_lookup_work(count)?)?, limits, work)?;
            cap(
                facts.expression_reference_count,
                limits.max_expression_references,
                work,
            )
        })?;
        match kind {
            K::Lambda(value) => {
                facts.type_reference_count = add(
                    facts.type_reference_count,
                    value.parameter_value_type_ids.len(),
                )?;
                charge(
                    &mut facts,
                    mul(value.parameter_value_type_ids.len(), 16)?,
                    limits,
                    work,
                )?;
            }
            K::Cast(_) => facts.type_reference_count = add(facts.type_reference_count, 1)?,
            _ => {}
        }
        work.step()?;
        cap(facts.type_reference_count, limits.max_type_references, work)?;
    }
    // Header and lambda carrier lookups use the original BTree owner. The
    // reference count measures source IDs; repeated delegate costs accumulate
    // before each ensuing operation on this same numerical author.
    let type_lookup_work = mul(
        facts.type_reference_count,
        tree_lookup_work(values.types().value_types().len())?,
    )?;
    charge(&mut facts, type_lookup_work, limits, work)?;
    check(&facts, limits, work)?;
    Ok(facts)
}

impl<'loan, 'wire, 'control> DecodedExpressions<'loan, 'wire, 'control> {
    fn charge(
        &mut self,
        amount: usize,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        charge(&mut self.facts, amount, limits, work)
    }
    fn type_root(
        &mut self,
        id: u32,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'loan FunctionValueType, Error> {
        let lookup_work = tree_lookup_work(self.types().value_types().len())?;
        self.charge(lookup_work, limits, work)?;
        work.flush()?;
        let values = self.values;
        let root = values.types().value_type(id);
        work.step()?;
        work.flush()?;
        root.ok_or_else(|| shape("expression value type is unknown"))
    }
    fn match_types(
        &mut self,
        left: &FunctionValueType,
        right: &FunctionValueType,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let verified = verify_type_binding(
            left,
            right,
            self.source_retained_bytes,
            limits.max_cumulative_work - self.facts.cumulative_work_upper_bound,
            work,
        )?;
        self.charge(verified.work_upper_bound(), limits, work)?;
        if !verified.matches() {
            return Err(shape("expression full value type differs from its source"));
        }
        Ok(())
    }
    fn required_expression(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'wire wire::ExpressionDefinition, Error> {
        self.definition_observed(id, work)?
            .ok_or_else(|| shape("expression reference is unknown"))
    }
    fn validate(
        &mut self,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let definitions = self.definitions;
        for definition in definitions {
            required(definition.owner_node_id, "expression owner node is absent")?;
            let type_id = required(definition.value_type_id, "expression value type is absent")?;
            self.type_root(type_id, limits, work)?;
            references(definition, |id| {
                self.required_expression(id, work)?;
                Ok(())
            })?;
            self.validate_kind(definition, limits, work)?;
            work.step()?;
        }
        check(&self.facts, limits, work)
    }
    fn validate_kind(
        &mut self,
        definition: &wire::ExpressionDefinition,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        use wire::expression_definition::Kind as K;
        let values = self.values;
        let types = values.types();
        let header = self.type_root(
            required(definition.value_type_id, "expression value type is absent")?,
            limits,
            work,
        )?;
        match definition
            .kind
            .as_ref()
            .ok_or_else(|| shape("expression kind is absent"))?
        {
            K::ValueId(id) => {
                self.charge(tree_lookup_work(self.values.source_count())?, limits, work)?;
                let value = self
                    .values
                    .value_observed(*id, work)?
                    .ok_or_else(|| shape("expression value reference is unknown"))?;
                self.match_types(header, &value.ty, limits, work)?;
            }
            K::Literal(reference) => {
                let reference = ConstantReference {
                    pool: ConstantPoolId::new(required(
                        reference.pool_id,
                        "constant pool ID is absent",
                    )?),
                    ordinal: reference.row_ordinal,
                };
                self.charge(
                    mul(2, tree_lookup_work(self.pools.entries().len())?)?,
                    limits,
                    work,
                )?;
                work.flush()?;
                let pool = self.pools.entries().get(&reference.pool);
                work.step()?;
                work.flush()?;
                let pool = pool.ok_or_else(|| shape("expression constant pool is unknown"))?;
                let retained =
                    usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
                        .map_err(|_| shape("constant retained extent is unrepresentable"))?;
                cap(retained, self.source_retained_bytes, work)?;
                let verified = verify_type_binding(
                    header,
                    pool.value_type(),
                    self.source_retained_bytes,
                    limits.max_cumulative_work - self.facts.cumulative_work_upper_bound,
                    work,
                )?;
                self.charge(verified.work_upper_bound(), limits, work)?;
                if !verified.matches() {
                    return Err(
                        novarocks_physical_plan::ConstantReferenceError::SourceTypeMismatch(
                            reference,
                        )
                        .into(),
                    );
                }
                work.flush()?;
                let selected = self
                    .pools
                    .resolve_source_observed(reference, work)
                    .map_err(Error::from);
                if matches!(&selected, Err(Error::Control(_))) {
                    return selected.map(|_| ());
                }
                work.flush()?;
                let selected = selected?;
                // Drop only the temporary shared selected handle, never its
                // original Field/type/backing. No payload is read or rebuilt.
                drop(selected);
            }
            K::LambdaParameter(value) => {
                let owner = self.required_expression(
                    required(value.lambda_expr_id, "lambda parameter owner is absent")?,
                    work,
                )?;
                let Some(K::Lambda(lambda)) = owner.kind.as_ref() else {
                    return Err(shape("lambda parameter owner is not a lambda"));
                };
                let ordinal = usize::try_from(value.ordinal)
                    .map_err(|_| shape("lambda ordinal is unrepresentable"))?;
                let parameter = *lambda
                    .parameter_value_type_ids
                    .get(ordinal)
                    .ok_or_else(|| shape("lambda parameter ordinal is out of bounds"))?;
                let expected = self.type_root(parameter, limits, work)?;
                self.match_types(header, expected, limits, work)?;
            }
            K::Unary(value) => {
                super::receiving_grammar::decode_unary(value.op)?;
            }
            K::Binary(value) => {
                let op = super::receiving_grammar::decode_binary(value.op)?;
                let arithmetic = op.arithmetic_operator().is_some();
                decode_decimal_policy(value.decimal_overflow_policy)?;
                let same_presence = arithmetic == value.allow_throw_exception.is_some();
                work.step()?;
                if !same_presence {
                    return Err(shape(
                        "binary ALLOW reference presence differs from its operator",
                    ));
                }
                if let Some(reference) = &value.allow_throw_exception {
                    self.allow_throw(reference, limits, work)?;
                }
            }
            K::Conjunction(_) | K::Disjunction(_) => {}
            K::FunctionCall(value) => {
                self.call(
                    definition,
                    required(value.function_binding_id, "function binding ID is absent")?,
                    &value.argument_expr_ids,
                    wire::FunctionKind::Scalar,
                    limits,
                    work,
                )?;
            }
            K::Lambda(value) => {
                for id in &value.parameter_value_type_ids {
                    self.type_root(*id, limits, work)?;
                    work.step()?;
                }
                let body = self.required_expression(
                    required(value.body_expr_id, "lambda body is absent")?,
                    work,
                )?;
                let body_type = self.type_root(
                    required(body.value_type_id, "lambda body type is absent")?,
                    limits,
                    work,
                )?;
                self.match_types(header, body_type, limits, work)?;
            }
            K::Cast(value) => {
                decode_decimal_policy(value.decimal_overflow_policy)?;
                self.allow_throw(
                    value
                        .allow_throw_exception
                        .as_ref()
                        .ok_or_else(|| shape("cast ALLOW reference is absent"))?,
                    limits,
                    work,
                )?;
                let carrier_id =
                    required(value.target_carrier_type_id, "cast carrier ID is absent")?;
                self.charge(tree_lookup_work(types.carriers.len())?, limits, work)?;
                work.flush()?;
                let carrier = types.carrier(carrier_id);
                work.step()?;
                work.flush()?;
                let carrier = carrier.ok_or_else(|| shape("cast carrier type is unknown"))?;
                // Raw carriers have no independent root flags. The same
                // complete-type numerical author bounds this datatype walk.
                let proof = preflight_type_binding(
                    header,
                    header,
                    self.source_retained_bytes,
                    limits.max_cumulative_work - self.facts.cumulative_work_upper_bound,
                    work,
                )?;
                self.charge(proof.work_upper_bound(), limits, work)?;
                work.flush()?;
                let same =
                    arrow_data_types_exact_borrowed_observed(&header.data_type, carrier, || {
                        work.step().map_err(TypeCodecError::from)
                    })
                    .map_err(Error::from)?;
                work.flush()?;
                if !same {
                    return Err(shape("cast carrier differs from its complete result type"));
                }
            }
            K::IsNull(_)
            | K::InList(_)
            | K::Between(_)
            | K::Like(_)
            | K::CaseExpression(_)
            | K::IsTruthValue(_) => {}
            K::WindowCall(value) => self.window(definition, value, limits, work)?,
        }
        work.step()?;
        Ok(())
    }
    fn allow_throw(
        &mut self,
        reference: &semantics::SemanticParameterRef,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        self.charge(
            tree_lookup_work(self.parameters.entries().len())?,
            limits,
            work,
        )?;
        let reference = decode_reference(reference, work)?;
        let valid = reference.expected_key == SemanticParameterKey::AllowThrowException;
        work.step()?;
        if !valid {
            return Err(shape(
                "intrinsic ALLOW reference has the wrong expected key",
            ));
        }
        work.flush()?;
        let resolved = self
            .parameters
            .require(reference)
            .map_err(SemanticsCodecError::from);
        work.step()?;
        work.flush()?;
        resolved?;
        Ok(())
    }
    fn function_header(
        &mut self,
        id: u32,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<&'wire wire::FunctionBindingDefinition, Error> {
        self.charge(
            tree_lookup_work(self.functions.as_wire().len())?,
            limits,
            work,
        )?;
        self.functions
            .definition_observed(id, work)?
            .ok_or_else(|| shape("expression function binding is unknown"))
    }
    fn call_result(
        &mut self,
        definition: &wire::ExpressionDefinition,
        function: &wire::FunctionBindingDefinition,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let Some(wire::function_binding_definition::Result::ScalarValueTypeId(result_id)) =
            function.result.as_ref()
        else {
            return Err(shape("expression function does not have a scalar result"));
        };
        let types = self.values.types();
        self.charge(
            mul(2, tree_lookup_work(types.value_types().len())?)?,
            limits,
            work,
        )?;
        work.flush()?;
        let result = types.value_type(*result_id);
        let header = types.value_type(required(
            definition.value_type_id,
            "call result type is absent",
        )?);
        work.step()?;
        work.flush()?;
        self.match_types(
            header.ok_or_else(|| shape("call result header type is unknown"))?,
            result.ok_or_else(|| shape("function result type is unknown"))?,
            limits,
            work,
        )
    }
    fn arguments(
        &mut self,
        expected: &[wire::FunctionArgumentType],
        arguments: &[u32],
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let same_count = expected.len() == arguments.len();
        work.step()?;
        if !same_count {
            return Err(shape(
                "call arguments differ from its frozen signature arity",
            ));
        }
        self.charge(
            mul(
                expected.len(),
                add(16, tree_lookup_work(self.definitions.len())?)?,
            )?,
            limits,
            work,
        )?;
        let types = self.values.types();
        for (expected, id) in expected.iter().zip(arguments) {
            let actual = self.required_expression(*id, work)?;
            match expected
                .kind
                .as_ref()
                .ok_or_else(|| shape("function argument shape is absent"))?
            {
                wire::function_argument_type::Kind::ValueTypeId(_) => {
                    // Exact Value-domain compatibility includes lawful nested
                    // nullability widening. Its sole fits author requires the
                    // final Fragment; this component proves only the shape.
                    if matches!(
                        actual.kind,
                        Some(wire::expression_definition::Kind::Lambda(_))
                    ) {
                        return Err(shape("value argument references a lambda"));
                    }
                }
                wire::function_argument_type::Kind::Lambda(expected) => {
                    let Some(wire::expression_definition::Kind::Lambda(actual_lambda)) =
                        actual.kind.as_ref()
                    else {
                        return Err(shape("lambda argument references a value expression"));
                    };
                    let same_count = expected.parameter_value_type_ids.len()
                        == actual_lambda.parameter_value_type_ids.len();
                    work.step()?;
                    if !same_count {
                        return Err(shape(
                            "lambda argument parameter arity differs from its signature",
                        ));
                    }
                    self.charge(
                        mul(
                            add(expected.parameter_value_type_ids.len(), 1)?,
                            mul(2, tree_lookup_work(types.value_types().len())?)?,
                        )?,
                        limits,
                        work,
                    )?;
                    for (left, right) in actual_lambda
                        .parameter_value_type_ids
                        .iter()
                        .zip(&expected.parameter_value_type_ids)
                    {
                        work.flush()?;
                        let left = types.value_type(*left);
                        let right = types.value_type(*right);
                        work.step()?;
                        work.flush()?;
                        self.match_types(
                            left.ok_or_else(|| shape("lambda actual parameter type is unknown"))?,
                            right.ok_or_else(|| {
                                shape("lambda expected parameter type is unknown")
                            })?,
                            limits,
                            work,
                        )?;
                    }
                    work.flush()?;
                    let left = types.value_type(required(
                        actual.value_type_id,
                        "lambda argument result header is absent",
                    )?);
                    let right = types.value_type(required(
                        expected.result_value_type_id,
                        "lambda signature result is absent",
                    )?);
                    work.step()?;
                    work.flush()?;
                    self.match_types(
                        left.ok_or_else(|| shape("lambda actual result type is unknown"))?,
                        right.ok_or_else(|| shape("lambda expected result type is unknown"))?,
                        limits,
                        work,
                    )?;
                }
            }
            work.step()?;
        }
        Ok(())
    }
    fn call(
        &mut self,
        definition: &wire::ExpressionDefinition,
        id: u32,
        arguments: &[u32],
        kind: wire::FunctionKind,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let function = self.function_header(id, limits, work)?;
        let valid_kind = function.kind == kind as i32;
        work.step()?;
        if !valid_kind {
            return Err(shape("expression function binding has the wrong kind"));
        }
        self.call_result(definition, function, limits, work)?;
        self.arguments(&function.arguments, arguments, limits, work)
    }
    fn signatures(
        &mut self,
        left: &wire::FunctionBindingDefinition,
        right: &wire::FunctionBindingDefinition,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        // Header identities have already passed the sole <=1024-byte grammar.
        // Compare the actual strings at an honest opaque boundary; IDs in the
        // sparse namespace may alias one exact selected signature.
        self.charge(
            add(
                add(left.function_id.len(), right.function_id.len())?,
                add(left.overload_id.len(), right.overload_id.len())?,
            )?,
            limits,
            work,
        )?;
        work.flush()?;
        let same = left.function_id == right.function_id
            && left.overload_id == right.overload_id
            && left.kind == right.kind
            && left.arguments.len() == right.arguments.len();
        work.step()?;
        work.flush()?;
        if !same {
            return Err(shape("window aggregate and function signatures differ"));
        }
        let types = self.values.types();
        self.charge(
            mul(
                add(left.arguments.len(), 1)?,
                add(16, mul(2, tree_lookup_work(types.value_types().len())?)?)?,
            )?,
            limits,
            work,
        )?;
        for (left, right) in left.arguments.iter().zip(&right.arguments) {
            match (left.kind.as_ref(), right.kind.as_ref()) {
                (
                    Some(wire::function_argument_type::Kind::ValueTypeId(left)),
                    Some(wire::function_argument_type::Kind::ValueTypeId(right)),
                ) => self.signature_types(*left, *right, limits, work)?,
                (
                    Some(wire::function_argument_type::Kind::Lambda(left)),
                    Some(wire::function_argument_type::Kind::Lambda(right)),
                ) => {
                    let same =
                        left.parameter_value_type_ids.len() == right.parameter_value_type_ids.len();
                    work.step()?;
                    if !same {
                        return Err(shape("aggregate lambda signature arity differs"));
                    }
                    self.charge(
                        mul(
                            left.parameter_value_type_ids.len(),
                            mul(2, tree_lookup_work(types.value_types().len())?)?,
                        )?,
                        limits,
                        work,
                    )?;
                    for (left, right) in left
                        .parameter_value_type_ids
                        .iter()
                        .zip(&right.parameter_value_type_ids)
                    {
                        self.signature_types(*left, *right, limits, work)?;
                    }
                    self.signature_types(
                        required(
                            left.result_value_type_id,
                            "aggregate lambda result is absent",
                        )?,
                        required(right.result_value_type_id, "window lambda result is absent")?,
                        limits,
                        work,
                    )?;
                }
                _ => return Err(shape("window aggregate argument shapes differ")),
            }
            work.step()?;
        }
        match (left.result.as_ref(), right.result.as_ref()) {
            (
                Some(wire::function_binding_definition::Result::ScalarValueTypeId(left)),
                Some(wire::function_binding_definition::Result::ScalarValueTypeId(right)),
            ) => self.signature_types(*left, *right, limits, work)?,
            _ => return Err(shape("window aggregate result is not scalar")),
        }
        Ok(())
    }
    fn signature_types(
        &mut self,
        left: u32,
        right: u32,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let types = self.values.types();
        work.flush()?;
        let left = types.value_type(left);
        let right = types.value_type(right);
        work.step()?;
        work.flush()?;
        self.match_types(
            left.ok_or_else(|| shape("signature type is unknown"))?,
            right.ok_or_else(|| shape("signature type is unknown"))?,
            limits,
            work,
        )
    }
    fn window(
        &mut self,
        definition: &wire::ExpressionDefinition,
        value: &wire::WindowCall,
        limits: ExpressionProjectionLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), Error> {
        let id = required(
            value.function_binding_id,
            "window function binding ID is absent",
        )?;
        let function = self.function_header(id, limits, work)?;
        self.call_result(definition, function, limits, work)?;
        match wire::FunctionKind::try_from(function.kind) {
            Ok(wire::FunctionKind::Window) => {
                let ordinary = value.aggregate_binding_id.is_none()
                    && !value.distinct
                    && value.function_order_by.is_empty();
                work.step()?;
                if !ordinary {
                    return Err(shape("window function carries aggregate-only fields"));
                }
                self.arguments(&function.arguments, &value.argument_expr_ids, limits, work)?;
            }
            Ok(wire::FunctionKind::Aggregate) => {
                self.charge(
                    tree_lookup_work(self.aggregates.as_wire().len())?,
                    limits,
                    work,
                )?;
                let aggregate = self
                    .aggregates
                    .definition_observed(
                        required(
                            value.aggregate_binding_id,
                            "aggregate window binding is absent",
                        )?,
                        work,
                    )?
                    .ok_or_else(|| shape("aggregate window binding is unknown"))?;
                let aggregate_function = self.function_header(
                    required(
                        aggregate.function_binding_id,
                        "aggregate function ID is absent",
                    )?,
                    limits,
                    work,
                )?;
                self.signatures(function, aggregate_function, limits, work)?;
                // Phase/channel and logical arguments plus ORDER BY inputs
                // are validated by the sole Fragment aggregate author. This
                // borrowed component preserves them without a scalar zip.
            }
            _ => return Err(shape("window binding is neither Window nor Aggregate")),
        }
        self.charge(mul(value.function_order_by.len(), 8)?, limits, work)?;
        for sort in &value.function_order_by {
            let closed = crate::physical_properties_v2::decode_direction(sort.direction).is_ok()
                && crate::physical_properties_v2::decode_nulls(sort.null_ordering).is_ok();
            work.step()?;
            if !closed {
                return Err(shape(
                    "window sort direction or null order is unknown or unspecified",
                ));
            }
        }
        if let Some(frame) = &value.frame {
            let closed = super::receiving_grammar::decode_window_units(frame.units).is_ok()
                && super::receiving_grammar::decode_window_exclusion(frame.exclusion).is_ok();
            work.step()?;
            if !closed {
                return Err(shape(
                    "window frame unit or exclusion is unknown or unspecified",
                ));
            }
            // Ordered bounds and their actual expression references were
            // already visited. Absence of the optional whole frame is legal.
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
