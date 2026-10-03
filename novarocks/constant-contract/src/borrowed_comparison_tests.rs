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
use arrow_array::{Int64Array, StructArray};
use std::{collections::HashMap, sync::Mutex};

const PHASE: CompilePhase = CompilePhase::Validate;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        if let Some((at, _)) = self.refusal {
            assert!(index <= at, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.refusal {
            Some((at, cause)) if index == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    // The existing constant-contract test profile, without new metadata caps.
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
fn child(reverse: bool, changed_value: bool, changed_key: bool) -> Arc<Field> {
    let first = (
        "a".repeat(2051),
        if changed_value {
            "changed".into()
        } else {
            "v".repeat(3073)
        },
    );
    let second = (
        if changed_key {
            "c".repeat(4099)
        } else {
            "b".repeat(4099)
        },
        "second".into(),
    );
    let entries = if reverse {
        [second, first]
    } else {
        [first, second]
    };
    Arc::new(
        Field::new("number", DataType::Int64, false)
            .with_metadata(entries.into_iter().collect::<HashMap<_, _>>()),
    )
}
fn array(child: Arc<Field>, rows: &[i64]) -> ArrayData {
    StructArray::new(
        vec![child].into(),
        vec![Arc::new(Int64Array::from(rows.to_vec())) as ArrayRef],
        None,
    )
    .to_data()
}
fn root(child: Arc<Field>) -> Arc<Field> {
    Arc::new(
        Field::new("literal", DataType::Struct(vec![child].into()), false)
            .with_metadata(HashMap::from([("source".into(), "exact".into())])),
    )
}
fn admit(
    field: Arc<Field>,
    data: ArrayData,
    control: &Control,
) -> Result<ConstantPool, ConstantError> {
    let ty = FunctionValueType::new(field.data_type().clone(), false);
    ConstantPool::try_new(field, ty, data, policy(), PHASE, control)
}
fn prefixes<T>(run: impl Fn(&Control) -> Result<T, ConstantError>) {
    let control = Control::default();
    let _ = run(&control);
    let trace = control.trace.lock().unwrap().clone();
    assert!(trace.len() >= 2);
    assert!(trace.iter().all(|(_, units)| *units <= 256));
    assert!(trace.iter().any(|(_, units)| *units > 0));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((at, cause)),
            };
            assert!(
                matches!(run(&control), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn nested_long_metadata_remains_unordered_exact_and_selected_value_equality_is_not_pool_identity() {
    let left_child = child(false, false, false);
    let right_child = child(true, false, false);
    let left = admit(
        root(left_child.clone()),
        array(left_child, &[111, 7, 999]),
        &Control::default(),
    )
    .unwrap();
    let right = admit(
        root(right_child.clone()),
        array(right_child, &[7, -500]),
        &Control::default(),
    )
    .unwrap();
    assert!(left.resource_facts().metadata_bytes > 1024);
    let left = left.value(1).unwrap();
    let right = right.value(0).unwrap();
    assert!(
        left.equals_observed(&right, PHASE, &Control::default())
            .unwrap()
    );
    prefixes(|control| left.equals_observed(&right, PHASE, control));

    for (changed_value, changed_key) in [(true, false), (false, true)] {
        let other_child = child(true, changed_value, changed_key);
        let other = admit(
            root(other_child.clone()),
            array(other_child, &[7]),
            &Control::default(),
        )
        .unwrap()
        .value(0)
        .unwrap();
        assert!(
            !left
                .equals_observed(&other, PHASE, &Control::default())
                .unwrap()
        );
        prefixes(|control| left.equals_observed(&other, PHASE, control));
    }
    let other_child = child(true, false, false);
    let other = admit(
        root(other_child.clone()),
        array(other_child, &[8]),
        &Control::default(),
    )
    .unwrap()
    .value(0)
    .unwrap();
    assert!(
        !left
            .equals_observed(&other, PHASE, &Control::default())
            .unwrap()
    );
}

#[test]
fn borrowed_constructor_comparisons_keep_both_exact_type_gates_and_original_control_prefixes() {
    let actual_child = child(false, false, false);
    let matching_child = child(true, false, false);
    let data = array(actual_child.clone(), &[7, 8]);
    let matching = root(matching_child);
    assert!(admit(matching.clone(), data.clone(), &Control::default()).is_ok());
    prefixes(|control| admit(matching.clone(), data.clone(), control));

    let different = root(child(true, true, false));
    assert!(matches!(
        admit(different.clone(), data.clone(), &Control::default()),
        Err(ConstantError::Invalid(
            "constant ArrayData differs from exact field carrier"
        ))
    ));
    prefixes(|control| admit(different.clone(), data.clone(), control));

    let actual = root(actual_child);
    let wrong_type = FunctionValueType::new(different.data_type().clone(), false);
    let run = |control: &Control| {
        ConstantPool::try_new(
            actual.clone(),
            wrong_type.clone(),
            data.clone(),
            policy(),
            PHASE,
            control,
        )
    };
    assert!(matches!(
        run(&Control::default()),
        Err(ConstantError::Invalid(
            "constant field differs from exact value carrier"
        ))
    ));
    prefixes(run);
}
