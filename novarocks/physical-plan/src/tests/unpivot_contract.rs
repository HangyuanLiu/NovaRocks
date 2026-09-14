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

#[derive(Clone, Copy)]
enum UnpivotShape {
    Valid,
    MappingOutsideChild,
    MappingTypeDrift,
    ConstantTypeDrift,
}

fn finish_unpivot(shape: UnpivotShape) -> Result<Fragment, ValidationErrors> {
    let mut builder = FragmentBuilder::new(FragmentId::new(812));
    let source = builder.reserve_node_id().unwrap();
    let number_expr = builder
        .add_expression(
            source,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(7)),
        )
        .unwrap();
    let text_expr = builder
        .add_expression(
            source,
            ty(DataType::Utf8, false),
            ExprKind::Literal(LiteralValue::Utf8("source".into())),
        )
        .unwrap();
    let number = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let text = builder
        .add_value(
            ty(DataType::Utf8, false),
            ValueOrigin::NodeOutput {
                node: source,
                output_ordinal: 1,
            },
        )
        .unwrap();
    builder
        .insert_node_unchecked(PhysicalNode {
            id: source,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node: source,
                columns: Box::from([number, text]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([number_expr, text_expr])]),
            },
        })
        .unwrap();

    let unpivot = builder.reserve_node_id().unwrap();
    let constant = match shape {
        UnpivotShape::ConstantTypeDrift => builder
            .add_expression(
                unpivot,
                ty(DataType::Int64, false),
                ExprKind::Literal(LiteralValue::Int64(9)),
            )
            .unwrap(),
        _ => builder
            .add_expression(
                unpivot,
                ty(DataType::Utf8, false),
                ExprKind::Literal(LiteralValue::Utf8("label".into())),
            )
            .unwrap(),
    };
    let value_output = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node: unpivot,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let literal_output = builder
        .add_value(
            ty(DataType::Utf8, false),
            ValueOrigin::NodeOutput {
                node: unpivot,
                output_ordinal: 2,
            },
        )
        .unwrap();
    let mapping_input = match shape {
        UnpivotShape::MappingOutsideChild => value_output,
        UnpivotShape::MappingTypeDrift => text,
        UnpivotShape::Valid | UnpivotShape::ConstantTypeDrift => number,
    };
    builder
        .insert_node_unchecked(PhysicalNode {
            id: unpivot,
            inputs: Box::from([source]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: singleton(),
            output: OutputPort {
                node: unpivot,
                columns: Box::from([text, value_output, literal_output]),
            },
            kind: NodeKind::Unpivot {
                spec: UnpivotSpec {
                    passthrough: Box::from([(text, text)]),
                    value_output,
                    literal_outputs: Box::from([literal_output]),
                    mappings: Box::from([UnpivotValueMapping {
                        input: mapping_input,
                        constants: Box::from([UnpivotConstant::Scalar(constant)]),
                    }]),
                    max_output_rows: 1024,
                    max_output_bytes: 1024 * 1024,
                },
            },
        })
        .unwrap();
    builder.finish_definition(unpivot, FragmentSink::Noop, dop())
}

#[test]
fn ordinary_unpivot_accepts_one_closed_input_and_output_shape() {
    finish_unpivot(UnpivotShape::Valid).expect("the exact Unpivot contract must validate");
}

#[test]
fn ordinary_unpivot_rejects_a_mapping_input_outside_its_child_port() {
    let errors = finish_unpivot(UnpivotShape::MappingOutsideChild)
        .expect_err("an Unpivot mapping must consume its exact child output");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("mapping input is absent from its exact child output")
    }));
}

#[test]
fn ordinary_unpivot_rejects_mapping_type_drift() {
    let errors = finish_unpivot(UnpivotShape::MappingTypeDrift)
        .expect_err("all mapping inputs must share the value output type");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("mapping input type differs from its value output")
    }));
}

#[test]
fn ordinary_unpivot_rejects_literal_constant_type_drift() {
    let errors = finish_unpivot(UnpivotShape::ConstantTypeDrift)
        .expect_err("a constant must match its literal output type");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("constant type differs from its literal output")
    }));
}
