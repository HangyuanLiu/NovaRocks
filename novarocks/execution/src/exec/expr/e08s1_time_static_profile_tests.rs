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
//! Source-independent exact TIME types do not replace the original emitted-source refiner.
use super::e08s1_time_original_profile_tests::{bindings, domains, full_binding};
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_functions::{
    FunctionArgumentType, FunctionBindingError, FunctionResultType, FunctionValueType,
    PureCallLifecycle,
};
use novarocks_sql::compiler::{
    SqlCompileControl, SqlPhysicalEmissionMode, builtin_sql_function_catalog,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl, ValueLogicalType};
use std::sync::Mutex;
#[test]
fn e08s1_time_static_profile_actual_source_and_all_six_nullable_profiles_use_same_original_author()
{
    let original = builtin_sql_function_catalog().snapshot();
    let exact = original.snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    for mode in [
        SqlPhysicalEmissionMode::OriginalNativeV1,
        SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
    ] {
        for binding in bindings(mode) {
            exact
                .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
                .unwrap();
        }
    }
    for name in ["time_to_sec", "time_format"] {
        for domain in domains() {
            for mask in 0..if name == "time_format" { 4 } else { 2 } {
                let binding = full_binding(name, domain.clone(), mask);
                exact
                    .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
                    .unwrap();
            }
        }
    }
}
#[test]
fn e08s1_time_static_profile_refuses_stale_full_type_nominal_arity_and_result_without_preparing_data()
 {
    let original = builtin_sql_function_catalog().snapshot();
    let exact = original.snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    for name in ["time_to_sec", "time_format"] {
        for domain in domains() {
            let binding = full_binding(name, domain, 0);
            let mut stale = Vec::new();
            for index in 0..binding.selected.argument_types.len() {
                for carrier in [
                    DataType::Boolean,
                    DataType::LargeUtf8,
                    DataType::Date64,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                ] {
                    let mut bad = binding.clone();
                    bad.selected.argument_types[index] =
                        FunctionArgumentType::Value(FunctionValueType::new(carrier, true));
                    stale.push(bad);
                }
                let mut bad = binding.clone();
                bad.selected.argument_types[index] = FunctionArgumentType::Value(
                    FunctionValueType::try_with_logical_type(
                        DataType::Utf8,
                        true,
                        ValueLogicalType::Json,
                    )
                    .unwrap(),
                );
                stale.push(bad);
            }
            let mut bad = binding.clone();
            let FunctionResultType::Scalar(out) = &mut bad.selected.result_type else {
                unreachable!()
            };
            out.nullable = false;
            stale.push(bad);
            let mut bad = binding.clone();
            let FunctionResultType::Scalar(out) = &mut bad.selected.result_type else {
                unreachable!()
            };
            out.data_type = DataType::Binary;
            stale.push(bad);
            let mut bad = binding.clone();
            bad.selected.result_type =
                FunctionResultType::Relation(Box::new([FunctionValueType::new(
                    DataType::Utf8,
                    true,
                )]));
            stale.push(bad);
            let mut bad = binding.clone();
            bad.logical_argument_count = 0;
            bad.selected.argument_types = Box::new([]);
            stale.push(bad);
            let mut bad = binding.clone();
            let mut t = bad.selected.argument_types.to_vec();
            t.push(t[0].clone());
            bad.selected.argument_types = t.into_boxed_slice();
            bad.logical_argument_count += 1;
            stale.push(bad);
            for bad in stale {
                original
                    .admit_bound_lifecycle_observed(&bad, PureCallLifecycle::Scalar, &control)
                    .unwrap();
                assert!(
                    matches!(exact.admit_bound_lifecycle_observed(&bad,PureCallLifecycle::Scalar,&control),Err(FunctionBindingError::UnavailableImplementation(id)) if id==binding.selected.overload),
                    "{name} stale selected={:?}",
                    bad.selected
                );
            }
        }
    }
}
struct Trace {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut events = self.events.lock().unwrap();
        let i = events.len();
        if let Some((stop, _)) = self.refuse {
            assert!(i <= stop, "callback after first originating cause")
        }
        events.push((phase, units));
        if let Some((stop, cause)) = self.refuse {
            if i == stop {
                return Err(cause);
            }
        }
        Ok(())
    }
}
#[test]
fn e08s1_time_static_profile_compile_three_causes_preserve_original_prefix_without_original_lookup()
{
    let original = builtin_sql_function_catalog().snapshot();
    let exact = original.snapshot_for_scalar_presence();
    for name in ["time_to_sec", "time_format"] {
        for domain in domains() {
            let binding = full_binding(name, domain, 0);
            let trace = Trace {
                events: Mutex::new(Vec::new()),
                refuse: None,
            };
            exact
                .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &trace)
                .unwrap();
            let expected = trace.events.into_inner().unwrap();
            assert!(!expected.is_empty());
            for stop in 0..expected.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let trace = Trace {
                        events: Mutex::new(Vec::new()),
                        refuse: Some((stop, cause)),
                    };
                    original
                        .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &trace)
                        .unwrap();
                    assert!(trace.events.lock().unwrap().is_empty());
                    assert!(
                        matches!(exact.admit_bound_lifecycle_observed(&binding,PureCallLifecycle::Scalar,&trace),Err(FunctionBindingError::Control(c)) if c==cause)
                    );
                    assert_eq!(*trace.events.lock().unwrap(), expected[..=stop]);
                }
            }
        }
    }
}
