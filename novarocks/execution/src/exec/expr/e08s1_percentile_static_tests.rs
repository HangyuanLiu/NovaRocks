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
//! Static support borrows the sole original percentile profile without preparation/data.
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    FunctionArgument, FunctionBindingError, FunctionResultType, FunctionValueType,
    PureCallLifecycle,
};
use novarocks_sql::compiler::{SqlCompileControl, builtin_sql_function_catalog};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn arg(ty: DataType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: FunctionValueType::new(ty, false),
        constant: None,
    }
}
#[test]
fn e08s1_percentile_static_full_fields_any_rate_remains_delayed_without_value_work() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let c = SqlCompileControl::unbounded();
    // Full fields/nominal metadata remain with the original resolver. This is
    // contract admission, not evidence of successful interpolation for ANY.
    let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    for name in ["percentile_cont", "percentile_disc", "percentile_disc_lc"] {
        for ty in [
            DataType::Int64,
            DataType::Decimal128(38, -38),
            DataType::Decimal256(76, 76),
            list.clone(),
        ] {
            for rate in [
                DataType::Float64,
                DataType::Decimal128(1, 1),
                DataType::Utf8,
            ] {
                let args = [arg(ty.clone()), arg(rate)];
                let binding = original
                    .resolve_aggregate_binding(name, 2, &args, &c)
                    .unwrap();
                scoped
                    .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Aggregate, &c)
                    .unwrap();
                original
                    .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Aggregate, &c)
                    .unwrap();
            }
        }
    }
}
#[test]
fn e08s1_percentile_static_rejects_unprepared_wrong_full_fields_and_logical_channels() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let c = SqlCompileControl::unbounded();
    for name in ["percentile_cont", "percentile_disc", "percentile_disc_lc"] {
        let args = [arg(DataType::Int32), arg(DataType::Float64)];
        let binding = original
            .resolve_aggregate_binding(name, 2, &args, &c)
            .unwrap();
        let mut bad_result = binding.clone();
        let FunctionResultType::Scalar(output) = &mut bad_result.selected.result_type else {
            unreachable!()
        };
        output.nullable = false;
        let mut bad_state = binding.clone();
        bad_state
            .selected
            .aggregate
            .as_mut()
            .unwrap()
            .intermediate_type
            .nullable = false;
        let mut no_state = binding.clone();
        no_state.selected.aggregate = None;
        let mut bad_logical = binding.clone();
        bad_logical.logical_argument_count = 1;
        for bad in [bad_result, bad_state, no_state, bad_logical] {
            // These are deliberate stale-field probes from an actual selection,
            // not fabricated source plans or successful execution contracts.
            original
                .admit_bound_lifecycle_observed(&bad, PureCallLifecycle::Aggregate, &c)
                .unwrap();
            assert!(
                matches!(scoped.admit_bound_lifecycle_observed(&bad,PureCallLifecycle::Aggregate,&c),
                Err(FunctionBindingError::UnavailableImplementation(id)) if id==binding.selected.overload)
            );
        }
    }
}
struct Refuse {
    cause: CompileControlError,
    calls: AtomicUsize,
}
impl PureCompileControl for Refuse {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        assert_eq!(
            self.calls.fetch_add(1, Ordering::Relaxed),
            0,
            "no callback after first refusal"
        );
        Err(self.cause)
    }
}
#[test]
fn e08s1_percentile_static_preserves_three_compile_causes_before_profile_work() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let binding = original
        .resolve_aggregate_binding(
            "percentile_cont",
            2,
            &[arg(DataType::Int64), arg(DataType::Float64)],
            &SqlCompileControl::unbounded(),
        )
        .unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let c = Refuse {
            cause,
            calls: AtomicUsize::new(0),
        };
        assert!(
            matches!(scoped.admit_bound_lifecycle_observed(&binding,PureCallLifecycle::Aggregate,&c),Err(FunctionBindingError::Control(actual)) if actual==cause)
        );
        assert_eq!(c.calls.load(Ordering::Relaxed), 1);
    }
}
