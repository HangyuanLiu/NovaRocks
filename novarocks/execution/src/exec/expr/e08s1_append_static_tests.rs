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
//! Static support shares the CPU's type predicates; suffix payload is never inspected.
use super::e08s1_append_original_static_tests::bindings;
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    FunctionArgumentType, FunctionBindingError, FunctionResultType, FunctionValueType,
    PureCallLifecycle,
};
use novarocks_sql::compiler::{
    SqlCompileControl, SqlPhysicalEmissionMode, builtin_sql_function_catalog,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::sync::{Arc, Mutex};

#[test]
fn e08s1_append_static_full_original_source_nullability_and_delayed_suffix_are_admitted() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        for binding in bindings(mode) {
            // The retained corpus includes empty, two-byte, multibyte and NULL suffixes.
            // Support admission has no ConstantValue or row-payload access.
            scoped
                .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
                .unwrap();
            original
                .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
                .unwrap();
            for left in [false, true] {
                for right in [false, true] {
                    let mut full = binding.clone();
                    for (index, nullable) in [left, right].into_iter().enumerate() {
                        let FunctionArgumentType::Value(ty) =
                            &mut full.selected.argument_types[index]
                        else {
                            unreachable!()
                        };
                        ty.nullable = nullable;
                    }
                    // Selected type-only probe, not a replacement source or an execution contract.
                    scoped
                        .admit_bound_lifecycle_observed(&full, PureCallLifecycle::Scalar, &control)
                        .unwrap();
                }
            }
        }
    }
}

#[test]
fn e08s1_append_static_refuses_stale_complete_profile_before_any_preparation() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    let binding = bindings(SqlPhysicalEmissionMode::OriginalNativeV1).remove(0);
    let mut stale = Vec::new();
    for index in 0..2 {
        for ty in [
            FunctionValueType::new(DataType::Binary, true),
            FunctionValueType::new(DataType::LargeUtf8, true),
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(
                DataType::List(Arc::new(Field::new("original-child", DataType::Utf8, true))),
                true,
            ),
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ] {
            let mut changed = binding.clone();
            changed.selected.argument_types[index] = FunctionArgumentType::Value(ty);
            stale.push(changed);
        }
    }
    for output in [
        FunctionValueType::new(DataType::Utf8, false),
        FunctionValueType::new(DataType::Binary, true),
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
    ] {
        let mut changed = binding.clone();
        changed.selected.result_type = FunctionResultType::Scalar(output);
        stale.push(changed);
    }
    let mut relation = binding.clone();
    relation.selected.result_type =
        FunctionResultType::Relation(Box::new([FunctionValueType::new(DataType::Utf8, true)]));
    stale.push(relation);
    let mut one = binding.clone();
    one.selected.argument_types = one.selected.argument_types[..1].to_vec().into_boxed_slice();
    one.logical_argument_count = 1;
    stale.push(one);
    let mut three = binding.clone();
    let mut types = three.selected.argument_types.to_vec();
    types.push(types[0].clone());
    three.selected.argument_types = types.into_boxed_slice();
    three.logical_argument_count = 3;
    stale.push(three);
    for changed in stale {
        original
            .admit_bound_lifecycle_observed(&changed, PureCallLifecycle::Scalar, &control)
            .unwrap();
        assert!(
            matches!(scoped.admit_bound_lifecycle_observed(&changed, PureCallLifecycle::Scalar, &control),
            Err(FunctionBindingError::UnavailableImplementation(id)) if id == binding.selected.overload)
        );
    }
}

struct Trace {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, steps: u32) -> Result<(), CompileControlError> {
        let mut events = self.events.lock().unwrap();
        let index = events.len();
        if let Some((refuse, _)) = self.refuse {
            assert!(index <= refuse, "no callback after first refusal")
        }
        events.push((phase, steps));
        if let Some((refuse, cause)) = self.refuse {
            if index == refuse {
                return Err(cause);
            }
        }
        Ok(())
    }
}
#[test]
fn e08s1_append_static_all_actual_compile_prefixes_preserve_three_causes_no_tail() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let binding = bindings(SqlPhysicalEmissionMode::OriginalNativeV1).remove(0);
    let good = Trace {
        events: Mutex::new(Vec::new()),
        refuse: None,
    };
    scoped
        .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &good)
        .unwrap();
    let trace = good.events.into_inner().unwrap();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Trace {
                events: Mutex::new(Vec::new()),
                refuse: Some((at, cause)),
            };
            original
                .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
                .unwrap();
            assert!(
                control.events.lock().unwrap().is_empty(),
                "Original remains no-work"
            );
            assert!(
                matches!(scoped.admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control), Err(FunctionBindingError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.events.lock().unwrap(), trace[..=at]);
        }
    }
}
