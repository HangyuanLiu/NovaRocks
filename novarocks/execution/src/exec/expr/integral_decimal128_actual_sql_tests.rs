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

//! Actual required signed source binding; no name-based re-resolution or fake provider.
use super::compiled_program::CompiledExpressionInstance;
use super::legacy_integral_decimal128_baseline_tests::{actual, integral_input, values};
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source;
use super::numeric_unary_owned_transaction_tests::programs_with_catalogue;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_functions::{KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::{ProgramRootInput, StaticExprKind, root_input_layout};
use novarocks_sql::compiler::SqlPhysicalEmissionMode;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, PureCompileControl,
};
use std::time::Duration;
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("integral Decimal CAST never waits")
    }
}
#[test]
fn integral_decimal128_actual_sql_required_i64_to_four_zero_bound_source_and_frame() {
    let target = DataType::Decimal128(4, 0);
    let source = sql_source(
        "SELECT CAST(k AS DECIMAL(4,0)) FROM fixture.source",
        DataType::Int64,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    );
    let mut count = 0;
    for fragment in source.plan().fragments().values() {
        for (_, node) in fragment.expressions().iter() {
            if let novarocks_physical_plan::ExprKind::Cast {
                expr,
                target: bound,
                decimal_overflow_policy,
                ..
            } = &node.kind
            {
                if *bound == target
                    && fragment.expressions().get(*expr).unwrap().ty.data_type == DataType::Int64
                {
                    count += 1;
                    assert_eq!(*decimal_overflow_policy, DecimalOverflowPolicy::OutputNull);
                }
            }
        }
    }
    assert_eq!(count, 1);
    let functions =
        super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue();
    // Permanent RED before the genuine integral Decimal recipe is installed.
    let programs = programs_with_catalogue(&source, &functions);
    let mut found = None;
    for program in programs.values() {
        let snapshot = program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot();
        for (&site, &root) in snapshot.bindings() {
            let invocation = &snapshot.flows()[&site.arena()].uses()[&root];
            if matches!(
                snapshot.roots().arenas()[&site.arena()]
                    .node(invocation.definition)
                    .unwrap()
                    .kind(),
                StaticExprKind::PreparedCast { .. }
            ) {
                assert!(found.is_none());
                found = Some((program.clone(), site));
            }
        }
    }
    let (program, site) = found.expect("actual SQL CAST root");
    let ProgramRootInput::Layout { node, role } = root_input_layout(program.graph(), site).unwrap()
    else {
        panic!("actual scan input")
    };
    let schema = program
        .checked()
        .channels()
        .channel_layout(node, role)
        .unwrap()
        .schema()
        .clone();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    let input = integral_input(&DataType::Int64, vec![Some(-100), Some(0), Some(150)]);
    let data = RecordBatch::try_new(schema, vec![input.clone()]).unwrap();
    for rows in [vec![0, 1, 2], vec![0, 2], Vec::new()] {
        let mut frame =
            CompiledExpressionInstance::try_new(program.clone(), site, &Control).unwrap();
        let output = frame
            .evaluate(&data, Selection::try_sparse(3, &rows).unwrap(), &Control)
            .unwrap();
        assert!(output.errors().is_empty());
        assert_eq!(output.values().data_type(), &target);
        let expected = rows
            .iter()
            .map(|row| {
                values(
                    &actual(
                        input.slice(*row, 1),
                        target.clone(),
                        DecimalOverflowPolicy::OutputNull,
                        false,
                    )
                    .unwrap(),
                )[0]
            })
            .collect::<Vec<_>>();
        assert_eq!(values(output.values()), expected);
    }
}
