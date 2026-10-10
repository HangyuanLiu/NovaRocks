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
use arrow_array::{Int64Array, StructArray, UInt64Array};
use std::collections::HashMap;
use std::sync::Mutex;

const PHASE: CompilePhase = CompilePhase::Decode;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

struct CallbackControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl CallbackControl {
    fn recording() -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal: None,
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for CallbackControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((refusal, _)) = self.refusal {
            assert!(at <= refusal, "the rejected owner was called again");
        }
        trace.push((phase, units));
        match self.refusal {
            Some((refusal, cause)) if refusal == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn policy() -> ConstantPolicy {
    // Explicit fixture policy for already allocated Arrow input, not a grant.
    ConstantPolicy {
        max_rows: 1_000_000,
        max_array_nodes: 4096,
        max_logical_elements: 100_000_000,
        max_retained_buffer_bytes: 64 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1024 * 1024,
        max_library_validation_work: 100_000_000,
        max_library_validation_bytes: 100_000_000,
    }
}

struct Input {
    field: Arc<Field>,
    value_type: FunctionValueType,
    data: ArrayData,
    policy: ConstantPolicy,
}
impl Input {
    fn new(array: ArrayRef, nullable: bool) -> Self {
        Self {
            field: Arc::new(Field::new("constant", array.data_type().clone(), nullable)),
            value_type: FunctionValueType::new(array.data_type().clone(), nullable),
            data: array.to_data(),
            policy: policy(),
        }
    }
    fn construct(&self, control: &dyn PureCompileControl) -> Result<ConstantPool, ConstantError> {
        ConstantPool::try_new(
            self.field.clone(),
            self.value_type.clone(),
            self.data.clone(),
            self.policy,
            PHASE,
            control,
        )
    }
}

fn failure_inputs() -> Vec<(Input, ConstantError)> {
    let mut nullable_mismatch = Input::new(Arc::new(Int64Array::from(vec![1])), false);
    nullable_mismatch.value_type = FunctionValueType::new(DataType::Int64, true);

    let mut carrier_mismatch = Input::new(Arc::new(Int64Array::from(vec![1])), true);
    carrier_mismatch.data = UInt64Array::from(vec![1]).to_data();

    let mut row_limit = Input::new(Arc::new(Int64Array::from(vec![1, 2])), false);
    row_limit.policy.max_rows = 1;

    // A safe standard Arrow array is structurally valid. Its SQL NULL violates
    // the exact nonnullable source only in the second semantic validation stage.
    let null_value = Input::new(Arc::new(Int64Array::from(vec![None::<i64>])), false);

    vec![
        (
            nullable_mismatch,
            ConstantError::Invalid("constant field differs from exact value nullability"),
        ),
        (
            carrier_mismatch,
            ConstantError::Invalid("constant ArrayData differs from exact field carrier"),
        ),
        (
            row_limit,
            ConstantError::Limit("constant row limit exceeded"),
        ),
        (
            null_value,
            ConstantError::Invalid("non-null constant contains SQL NULL"),
        ),
    ]
}

fn assert_every_refusal(input: &Input, baseline: &[(CompilePhase, u32)]) {
    assert!(!baseline.is_empty());
    assert!(baseline.iter().all(|(phase, _)| *phase == PHASE));
    for at in 0..baseline.len() {
        for cause in CAUSES {
            let control = CallbackControl::refusing(at, cause);
            assert_eq!(
                input.construct(&control).unwrap_err(),
                ConstantError::Control(cause)
            );
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
}

#[test]
fn ordinary_preflight_and_semantic_rejections_observe_completed_tails() {
    for (input, expected) in failure_inputs() {
        let control = CallbackControl::recording();
        assert_eq!(input.construct(&control).unwrap_err(), expected);
        let trace = control.trace();
        assert_eq!(trace.first(), Some(&(PHASE, 0)));
        // These four small inputs each complete actual work before rejecting.
        // The first mismatch must reach a tail despite failing before preflight.
        assert!(
            trace.len() >= 2,
            "missing ordinary failure completion: {expected:?}"
        );
        assert!(
            trace.last().unwrap().1 > 0,
            "completed work was lost: {expected:?}"
        );
        assert_every_refusal(&input, &trace);
    }
}

#[test]
fn successful_sliced_pool_preserves_exact_source_and_every_control_prefix() {
    let backing = Int64Array::from(vec![Some(99), None, Some(-7), Some(42), Some(88)]);
    let mut input = Input::new(Arc::new(backing.slice(1, 3)), true);
    input.field = Arc::new(
        Field::new("selected_constant", DataType::Int64, true).with_metadata(HashMap::from([
            ("provider_field_id".to_owned(), "71".to_owned()),
            ("source_note".to_owned(), "preserved".to_owned()),
        ])),
    );
    let control = CallbackControl::recording();
    let pool = input.construct(&control).unwrap();
    assert_eq!(pool.field(), input.field.as_ref());
    assert_eq!(pool.value_type(), &input.value_type);
    assert_eq!(pool.resource_facts().rows, 3);
    let values = pool.array().as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![None, Some(-7), Some(42)]
    );
    assert!(pool.value(3).is_err());
    let trace = control.trace();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    assert!(trace.last().unwrap().1 > 0);
    // The opaque library boundary and second-stage entry remain observable;
    // this test does not claim checkpoints inside Arrow validation.
    assert!(trace.windows(3).any(|window| window == [(PHASE, 0); 3]));
    assert_every_refusal(&input, &trace);
}

#[test]
fn lawful_wide_metadata_reaches_actual_quantum_and_preserves_control_causes() {
    let columns: Vec<(Arc<Field>, ArrayRef)> = (0..320)
        .map(|ordinal| {
            let field = Arc::new(
                Field::new(format!("source_{ordinal}"), DataType::Int64, false).with_metadata(
                    HashMap::from([
                        ("provider_field_id".to_owned(), ordinal.to_string()),
                        ("source_note".to_owned(), "retained".to_owned()),
                    ]),
                ),
            );
            let array: ArrayRef =
                Arc::new(Int64Array::from(vec![ordinal as i64, -(ordinal as i64)]));
            (field, array)
        })
        .collect();
    let input = Input::new(Arc::new(StructArray::from(columns)), false);
    let control = CallbackControl::recording();
    let pool = input.construct(&control).unwrap();
    assert_eq!(pool.value_type(), &input.value_type);
    assert_eq!(pool.field(), input.field.as_ref());
    assert_eq!(pool.resource_facts().rows, 2);
    let values = pool.array().as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(values.num_columns(), 320);
    for ordinal in [0, 255, 319] {
        let child = values
            .column(ordinal)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            child.values().as_ref(),
            &[ordinal as i64, -(ordinal as i64)]
        );
        assert_eq!(
            values.fields()[ordinal].metadata()["provider_field_id"],
            ordinal.to_string()
        );
    }
    let trace = control.trace();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    assert!(trace.iter().all(|(_, units)| *units <= 256));
    assert_every_refusal(&input, &trace);
}
