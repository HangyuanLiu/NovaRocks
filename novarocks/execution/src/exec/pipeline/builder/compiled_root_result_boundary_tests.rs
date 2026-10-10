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

//! RootResult boundary probes using the original checked package fixture.
use super::*;

fn boundary_program(project: Option<bool>) -> Arc<LocalProgram> {
    let mut builder = FragmentBuilder::new(FragmentId::new(57));
    let values = super::super::family_fixture::values(
        &mut builder,
        VALUES,
        &[FunctionValueType::new(DataType::Int64, false)],
        &[vec![LiteralValue::Int64(7)]],
    );
    let root = if let Some(computed) = project {
        let ty = FunctionValueType::new(DataType::Int64, false);
        let expr = builder
            .add_expression(
                PROJECT,
                ty.clone(),
                if computed {
                    ExprKind::Literal(LiteralValue::Int64(19))
                } else {
                    ExprKind::Value(values[0])
                },
            )
            .unwrap();
        let output = builder
            .add_value(
                ty,
                ValueOrigin::Expr {
                    node: PROJECT,
                    expr,
                },
            )
            .unwrap();
        builder
            .add_project(
                PROJECT,
                VALUES,
                Box::from([(expr, output)]),
                Box::from([output]),
            )
            .unwrap();
        PROJECT
    } else {
        VALUES
    };
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::RootResult(Box::new(
                novarocks_result_contract::RootOutputContract::new(
                    novarocks_result_contract::RootProfileId::V1,
                    novarocks_result_contract::FrozenRootOutput::CountOnly,
                ),
            )),
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    compile(
        package_with_parameters(
            fragment,
            ConstantPools::empty(),
            SemanticParameters::try_new([]).unwrap(),
        ),
        1,
    )
}

#[test]
fn compiled_root_result_boundary_computes_nonidentity_project_before_validation() {
    let program = boundary_program(Some(true));
    let node = &program.graph().nodes()[program.graph().root().index()];
    assert!(matches!(
        node.kind(),
        ProgramNodeKind::Project {
            validate_final_result_input: false,
            ..
        }
    ));
    assert_eq!(int64_rows(&run(&program)), vec![vec![Some(19)]]);
}

#[test]
fn compiled_root_result_boundary_preserves_identity_project_output() {
    let program = boundary_program(Some(false));
    assert_eq!(int64_rows(&run(&program)), vec![vec![Some(7)]]);
}

#[test]
fn compiled_root_result_boundary_covers_nonproject_values_root() {
    let program = boundary_program(None);
    assert!(matches!(
        program.graph().nodes()[program.graph().root().index()].kind(),
        ProgramNodeKind::Values { .. }
    ));
    assert_eq!(int64_rows(&run(&program)), vec![vec![Some(7)]]);
}
