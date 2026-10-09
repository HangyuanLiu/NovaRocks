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

//! Ordered original Scan residuals use the existing conjunction consumer.
//! This isolated required predicate preserves an explicit optimizer variant;
//! it does not claim native whole-case or provider execution acceptance.
use super::compiled_program::CompiledFilterConjunctionInstance;
use super::numeric_unary_original_nonnull_sql_baseline_tests::sql_source_with_single_field_and_core_residuals;
use super::numeric_unary_owned_transaction_tests::programs_with_catalogue_and_original_inlist_source;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use arrow::{
    array::{ArrayRef, BooleanArray, Int32Array},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use novarocks_functions::{KernelEvaluationControl, KernelFailure, Selection};
use novarocks_local_program::ProgramNodeKind;
use novarocks_physical_plan::NodeKind;
use novarocks_sql::compiler::{SqlAuthoredPhysicalPlan, SqlPhysicalEmissionMode};
use std::{sync::Arc, time::Duration};
struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("scan residuals never wait")
    }
}
fn source() -> SqlAuthoredPhysicalPlan {
    sql_source_with_single_field_and_core_residuals(
        "SELECT k FROM t_filter_is_not_null_and_range WHERE k IS NOT NULL AND k >= 10 AND k < 20",
        Field::new("k", DataType::Int32, true),
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    )
}
fn inspect(source: &SqlAuthoredPhysicalPlan) -> usize {
    let mut counts = Vec::new();
    for fragment in source.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Scan {
                residuals,
                relation,
                ..
            } = &node.kind
            {
                assert_eq!(relation.schema().len(), 1);
                assert_eq!(relation.schema()[0].ty.data_type, DataType::Int32);
                assert!(relation.schema()[0].ty.nullable);
                assert!(relation.predicate_guarantees().is_empty());
                for (ordinal, id) in residuals.iter().enumerate() {
                    let e = fragment.expressions().get(*id).unwrap();
                    println!(
                        "original scan residual ordinal={ordinal} id={id:?} type={:?} kind={:?}",
                        e.ty, e.kind
                    );
                    assert_eq!(e.ty.data_type, DataType::Boolean);
                }
                counts.push(residuals.len());
            }
        }
    }
    assert_eq!(counts.len(), 1);
    assert!(
        counts[0] >= 2,
        "original optimizer must retain multiple residuals"
    );
    counts[0]
}
#[test]
fn scan_ordered_required_source_keeps_original_nullable_field_and_residuals() {
    inspect(&source());
}
#[test]
fn scan_ordered_required_compiler_conjunction_preserves_true_only_null_and_boundaries() {
    let source = source();
    inspect(&source);
    let functions = installed_builtin_owner_catalogue();
    let programs = programs_with_catalogue_and_original_inlist_source(&source, &functions);
    let mut scans = Vec::new();
    for program in programs.values() {
        for node in program.graph().nodes() {
            if matches!(node.kind(), ProgramNodeKind::Scan { .. }) {
                scans.push((program.clone(), node.local_id().unwrap()));
            }
        }
    }
    assert_eq!(scans.len(), 1);
    let (program, node) = scans.pop().unwrap();
    let schema = program.graph().nodes()[node.index()]
        .output_layout()
        .schema()
        .clone();
    let values = Arc::new(Int32Array::from(vec![
        None,
        Some(5),
        Some(10),
        Some(11),
        Some(19),
        Some(20),
    ])) as ArrayRef;
    let batch = RecordBatch::try_new(schema, vec![values]).unwrap();
    let mut instance = CompiledFilterConjunctionInstance::try_new(program, node, &Control).unwrap();
    let result = instance
        .evaluate_required(&batch, Selection::all(6), &Control)
        .unwrap();
    let truth = result.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(
        truth.iter().collect::<Vec<_>>(),
        vec![
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false)
        ]
    );
}
