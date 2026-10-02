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
use novarocks_physical_plan::{
    Distribution, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackageError,
    FragmentPackageInput, FragmentSink, NodeId, NodeKind, OutputPort, PhysicalNode,
    PhysicalProperties, PipelineDopDomain, PlanVersionId, RequiredContracts, RowMultiplicity,
};
use novarocks_type_contract::{ExpressionControlFlow, SemanticParameterId, SemanticParameterValue};
use std::{collections::BTreeMap, sync::Mutex};

#[derive(Default)]
struct Control {
    observations: Mutex<Vec<(CompilePhase, u32)>>,
    failure: Option<(CompilePhase, usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        let mut observed = self.observations.lock().unwrap();
        let ordinal = observed.iter().filter(|(seen, _)| *seen == phase).count();
        observed.push((phase, units));
        match self.failure {
            Some((target, at, cause)) if target == phase && ordinal == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn input() -> FragmentPackageInput {
    let control = Control::default();
    let mut builder = FragmentBuilder::new(FragmentId::new(u32::MAX));
    let node = NodeId::new(0);
    builder
        .insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output: OutputPort {
                node,
                columns: Box::default(),
            },
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::default()]),
            },
        })
        .unwrap();
    let fragment = builder
        .finish_definition(
            node,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    let flow = ExpressionControlFlow::try_new(
        vec![],
        vec![],
        fragment.expressions(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let expression_uses = PhysicalRootUses::try_new(&fragment, flow, vec![], &control).unwrap();
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &expression_uses, vec![], &control).unwrap();
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &control).unwrap();
    FragmentPackageInput {
        version: PlanVersionId::try_new([1; 16]).unwrap(),
        required: RequiredContracts::default(),
        fragment,
        expression_uses,
        calls,
        pruning,
        cuts: FragmentCuts::default(),
        result: None,
        parameters: SemanticParameters::default(),
        scans: BTreeMap::new(),
        writes: BTreeMap::new(),
        annotations: Box::default(),
    }
}

#[test]
fn public_semantics_projection_preserves_explicit_empty_components_and_same_package() {
    let original = input();
    let package = FragmentPackage::try_new(original.clone(), &Control::default()).unwrap();
    let encoded = encode_fragment_semantics(&package, &Control::default()).unwrap();
    assert!(encoded.parameters.entries.is_empty());
    assert!(encoded.calls.entries.is_empty());
    assert!(encoded.pruning.witnesses.is_empty());
    let decoded = decode_fragment_semantics(
        package.fragment(),
        package.expression_uses(),
        &encoded,
        &Control::default(),
    )
    .unwrap();
    let (parameters, calls, pruning) = decoded.into_parts();
    let reconstructed = FragmentPackage::try_new(
        FragmentPackageInput {
            parameters,
            calls,
            pruning,
            ..original
        },
        &Control::default(),
    )
    .unwrap();
    assert_eq!(reconstructed, package);
}

#[test]
fn decoded_semantic_parts_require_actual_package_parameter_closure() {
    let original = input();
    let package = FragmentPackage::try_new(original.clone(), &Control::default()).unwrap();
    let mut encoded = encode_fragment_semantics(&package, &Control::default()).unwrap();
    encoded.parameters.entries.push(wire::SemanticParameter {
        id: u32::MAX,
        value: Some(wire::semantic_parameter::Value::AllowThrowException(false)),
    });
    let (parameters, calls, pruning) = decode_fragment_semantics(
        package.fragment(),
        package.expression_uses(),
        &encoded,
        &Control::default(),
    )
    .unwrap()
    .into_parts();
    assert_eq!(
        parameters.get(SemanticParameterId::new(u32::MAX)),
        Some(&SemanticParameterValue::AllowThrowException(false))
    );
    assert_eq!(
        FragmentPackage::try_new(
            FragmentPackageInput {
                parameters,
                calls,
                pruning,
                ..original
            },
            &Control::default()
        ),
        Err(FragmentPackageError::UnusedParameters)
    );
}

#[test]
fn public_semantics_control_refusal_precedes_result_publication_in_both_directions() {
    let package = FragmentPackage::try_new(input(), &Control::default()).unwrap();
    let encoded = encode_fragment_semantics(&package, &Control::default()).unwrap();
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for phase in [CompilePhase::Encode, CompilePhase::Decode] {
            for at in [0, 1] {
                let control = Control {
                    failure: Some((phase, at, error)),
                    ..Default::default()
                };
                let result = if phase == CompilePhase::Encode {
                    encode_fragment_semantics(&package, &control).map(|_| ())
                } else {
                    decode_fragment_semantics(
                        package.fragment(),
                        package.expression_uses(),
                        &encoded,
                        &control,
                    )
                    .map(|_| ())
                };
                assert_eq!(result, Err(SemanticsCodecError::Control(error)));
                assert_eq!(
                    control
                        .observations
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(seen, _)| *seen == phase)
                        .count(),
                    at + 1
                );
            }
        }
    }
}

#[test]
fn ordinary_decode_failure_observes_completed_tail_and_preserves_control_category() {
    struct PositiveRefusal(CompileControlError);
    impl PureCompileControl for PositiveRefusal {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            if phase == CompilePhase::Decode && units > 0 {
                Err(self.0)
            } else {
                Ok(())
            }
        }
    }
    let package = FragmentPackage::try_new(input(), &Control::default()).unwrap();
    let encoded = encode_fragment_semantics(&package, &Control::default()).unwrap();
    for malformed_parameters in [false, true] {
        let mut bad = encoded.clone();
        if malformed_parameters {
            bad.parameters
                .entries
                .push(wire::SemanticParameter { id: 0, value: None });
        } else {
            bad.pruning.witnesses.push(wire::PruningDomainWitness {
                target: None,
                sources: vec![],
            });
        }
        assert!(matches!(
            decode_fragment_semantics(
                package.fragment(),
                package.expression_uses(),
                &bad,
                &Control::default()
            ),
            Err(SemanticsCodecError::InvalidShape(_))
        ));
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let result = decode_fragment_semantics(
                package.fragment(),
                package.expression_uses(),
                &bad,
                &PositiveRefusal(error),
            )
            .map(|_| ());
            assert_eq!(result, Err(SemanticsCodecError::Control(error)));
        }
    }
}

#[test]
fn nested_owner_control_failures_keep_the_original_semantics_codec_category() {
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let root = RootUseBindingError::Roots(ExpressionRootError::Control(error));
        for projected in [
            SemanticsCodecError::from(FrozenCallError::Roots(root)),
            SemanticsCodecError::from(ControlCodecError::Roots(root)),
            SemanticsCodecError::from(FrozenPruningError::Structure(
                PruningStructureError::Source(PredicateSourceError::Control(error)),
            )),
        ] {
            assert_eq!(projected, SemanticsCodecError::Control(error));
        }
    }
}
