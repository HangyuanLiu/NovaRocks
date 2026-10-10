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

use super::*;
use crate::{
    FunctionArgument, FunctionSpecializationFailure, FunctionValueType, ScopedExpressionEffects,
    specialize_scalar,
};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext,
    ExpressionUseId, SemanticParameters, ValueLogicalType,
};
fn owner(name: &str) -> MapElementAtOwner {
    let signatures = super::super::registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(candidate, _)| candidate == name)
        .unwrap()
        .1;
    let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
        name,
        &signatures,
        novarocks_type_contract::FunctionKind::Scalar,
    )
    .unwrap();
    MapElementAtOwner::new(name, declaration, resolver).unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    }
}

fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        expected_result_type: None,
        arguments,
        logical_argument_count: arguments.len(),
    }
}

fn input<'a>(
    owner: &'a MapElementAtOwner,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    parameters: &'a SemanticParameters,
    uses: &'a [Option<ExpressionUseId>],
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: context(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected,
        request: request(arguments),
        environment: &[],
        parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Unconditional,
    }
}

fn argument(ty: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}

pub(crate) fn prepared_for_test(
    name: &str,
    sources: &[FunctionValueType],
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    prepared_with_control(name, sources, crate::binding_test_control())
}
pub(crate) fn prepared_with_control(
    name: &str,
    sources: &[FunctionValueType],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<Arc<dyn PreparedScalarKernel>, FunctionSpecializationFailure> {
    if !operation(name) {
        return Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction,
        ));
    }
    let owner = owner(name);
    let arguments = sources.iter().cloned().map(argument).collect::<Vec<_>>();
    let selected = Arc::new(owner.resolve(request(&arguments), control)?);
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses = sources
        .iter()
        .enumerate()
        .map(|(i, _)| Some(ExpressionUseId::new(i as u32 + 42)))
        .collect::<Vec<_>>();
    specialize_scalar(
        &owner,
        input(&owner, &selected, &arguments, &parameters, &uses),
        selected.clone(),
        ScopedExpressionEffects::pure_value(context()),
        control,
    )
    .map(|call| call.into_prepared())
}

#[test]
fn map_lookup_real_public_arity_and_nullable_result_match_original_binding() {
    let fields = vec![
        Arc::new(Field::new("key", DataType::Int32, true)),
        Arc::new(Field::new("value", DataType::Utf8, true)),
    ];
    let map = FunctionValueType::new(
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(fields.into()),
                false,
            )),
            false,
        ),
        true,
    );
    let key = FunctionValueType::new(DataType::Int32, true);
    let p = prepared_for_test("__map_element_at", &[map.clone(), key.clone()]).unwrap();
    assert_eq!(
        p.contract().result_type(),
        &FunctionValueType::new(DataType::Utf8, true)
    );
    assert_eq!(
        p.contract().effects().null_behavior,
        FunctionNullBehavior::CalledOnNull
    );
    assert_eq!(
        p.contract().effects().own_row_error,
        FunctionIntrinsicRowError::MayRaise
    );
    assert!(
        prepared_for_test(
            "__map_element_at",
            &[map, key, FunctionValueType::new(DataType::Boolean, true)]
        )
        .is_err()
    );
}

#[test]
fn map_lookup_null_result_profile_is_named_refusal_for_both_source_nullabilities() {
    for nullable in [false, true] {
        let fields = vec![
            Arc::new(Field::new("key", DataType::Int32, true)),
            Arc::new(Field::new("value", DataType::Null, true)),
        ];
        let map = FunctionValueType::new(
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(fields.into()),
                    false,
                )),
                false,
            ),
            nullable,
        );
        let error = prepared_for_test(
            "__map_element_at",
            &[map, FunctionValueType::new(DataType::Int32, nullable)],
        )
        .err()
        .unwrap();
        match error {
            FunctionSpecializationFailure::Kernel(crate::KernelFailure::InvalidProgram(
                message,
            )) => assert_eq!(
                message.message(),
                "__map_element_at has no installed Null result profile: original nullable-index overlay panics"
            ),
            other => panic!("unexpected Null-result refusal: {other:?}"),
        }
    }
}
