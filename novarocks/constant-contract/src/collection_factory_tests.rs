// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use arrow_array::{Array, Int32Array, ListArray, MapArray, StringArray};
use std::{collections::HashMap, sync::Mutex};

const PHASE: CompilePhase = CompilePhase::Validate;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push(units);
        match self.stop {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1,
        max_array_nodes: 16,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 8 << 20,
        max_type_depth: 16,
        max_type_nodes: 64,
        max_dictionary_depth: 4,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 64 << 20,
        max_library_validation_bytes: 64 << 20,
    }
}
fn source(data_type: DataType, nullable: bool) -> (Arc<Field>, FunctionValueType) {
    let field = Arc::new(
        Field::new("original_collection", data_type, nullable)
            .with_metadata(HashMap::from([("source.field-id".into(), "79".into())])),
    );
    let ty = FunctionValueType::try_from_field(&field).unwrap();
    (field, ty)
}
fn list_source(nullable: bool) -> (Arc<Field>, FunctionValueType) {
    source(
        DataType::List(Arc::new(
            Field::new("authored_item", DataType::Int32, true)
                .with_metadata(HashMap::from([("child.annotation".into(), "int32".into())])),
        )),
        nullable,
    )
}
fn map_source(nullable: bool) -> (Arc<Field>, FunctionValueType) {
    let fields = vec![
        Arc::new(
            Field::new("authored_key", DataType::Utf8, true).with_metadata(HashMap::from([(
                "key.annotation".into(),
                "original".into(),
            )])),
        ),
        Arc::new(
            Field::new("authored_value", DataType::Utf8, true).with_metadata(HashMap::from([(
                "value.annotation".into(),
                "original".into(),
            )])),
        ),
    ]
    .into();
    source(
        DataType::Map(
            Arc::new(
                Field::new("authored_entries", DataType::Struct(fields), false).with_metadata(
                    HashMap::from([("entries.annotation".into(), "original".into())]),
                ),
            ),
            false,
        ),
        nullable,
    )
}
fn entries() -> Vec<(String, String)> {
    vec![
        ("z".into(), "雪\0☃".into()),
        ("".into(), "".into()),
        ("z".into(), "λ".into()),
    ]
}

#[test]
fn collection_factories_preserve_original_fields_types_ordinal_and_exact_payload_order() {
    for nullable in [false, true] {
        let (field, ty) = list_source(nullable);
        let value = ConstantValue::from_int32_list(
            field.clone(),
            ty.clone(),
            &[i32::MIN, -1, 0, i32::MAX],
            policy(),
            PHASE,
            &Control::default(),
        )
        .unwrap();
        assert!(Arc::ptr_eq(value.pool().field_ref(), &field));
        assert_eq!(value.value_type(), &ty);
        assert_eq!(value.ordinal(), 0);
        let array = value
            .pool()
            .array()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value_offsets(), &[0, 4]);
        assert_eq!(
            array
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[i32::MIN, -1, 0, i32::MAX]
        );
        let view = value
            .int32_list_observed(PHASE, &Control::default())
            .unwrap()
            .unwrap();
        assert_eq!(
            view.item(3, PHASE, &Control::default()).unwrap(),
            Some(i32::MAX)
        );
        assert_eq!(value.pool().resource_facts().array_nodes, 2);
        assert_eq!(
            value.pool().resource_facts().logical_elements_upper_bound,
            5
        );

        let (field, ty) = map_source(nullable);
        let value = ConstantValue::from_utf8_map(
            field.clone(),
            ty.clone(),
            &entries(),
            policy(),
            PHASE,
            &Control::default(),
        )
        .unwrap();
        assert!(Arc::ptr_eq(value.pool().field_ref(), &field));
        assert_eq!(value.value_type(), &ty);
        assert_eq!(value.ordinal(), 0);
        let array = value
            .pool()
            .array()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert_eq!(array.value_offsets(), &[0, 3]);
        assert_eq!(
            array
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "z"
        );
        assert_eq!(
            array
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            ""
        );
        assert_eq!(
            array
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(2),
            "z"
        );
        let view = value
            .utf8_map_observed(PHASE, &Control::default())
            .unwrap()
            .unwrap();
        for (index, (key, payload)) in entries().iter().enumerate() {
            assert_eq!(
                view.item(index, PHASE, &Control::default()).unwrap(),
                (Some(key.as_str()), Some(payload.as_str()))
            );
        }
        assert_eq!(value.pool().resource_facts().array_nodes, 4);
        assert_eq!(
            value.pool().resource_facts().logical_elements_upper_bound,
            10
        );
    }
}

#[test]
fn collection_factories_empty_rows_remain_non_null_with_declared_nullable_source() {
    let (field, ty) = list_source(true);
    let value =
        ConstantValue::from_int32_list(field, ty, &[], policy(), PHASE, &Control::default())
            .unwrap();
    assert!(
        value
            .int32_list_observed(PHASE, &Control::default())
            .unwrap()
            .unwrap()
            .is_empty()
    );
    assert_eq!(value.pool().data().buffers()[0].as_slice(), &[0; 8]);
    let (field, ty) = map_source(true);
    let value =
        ConstantValue::from_utf8_map(field, ty, &[], policy(), PHASE, &Control::default()).unwrap();
    assert!(
        value
            .utf8_map_observed(PHASE, &Control::default())
            .unwrap()
            .unwrap()
            .is_empty()
    );
    assert_eq!(value.pool().resource_facts().rows, 1);
}

fn prefixes(invoke: impl Fn(&Control) -> Result<ConstantValue, ConstantError>, ordinary: bool) {
    let good = Control::default();
    let result = invoke(&good);
    if ordinary {
        assert!(matches!(
            result,
            Err(ConstantError::Invalid(_) | ConstantError::Type(_) | ConstantError::Arrow(_))
        ));
    } else {
        assert!(result.is_ok());
    }
    let trace = good.trace.lock().unwrap().clone();
    assert_eq!(trace[0], 0);
    assert!(
        trace.len() > 1,
        "ordinary/success owner must publish its tail"
    );
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Default::default(),
                stop: Some((at, cause)),
            };
            assert!(
                matches!(invoke(&control), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn collection_factories_success_and_ordinary_errors_preserve_every_callback_first_cause() {
    let (lf, lt) = list_source(false);
    let (mf, mt) = map_source(false);
    prefixes(
        |c| ConstantValue::from_int32_list(lf.clone(), lt.clone(), &[7, -9], policy(), PHASE, c),
        false,
    );
    prefixes(
        |c| ConstantValue::from_utf8_map(mf.clone(), mt.clone(), &entries(), policy(), PHASE, c),
        false,
    );
    let wrong = FunctionValueType::new(DataType::Int64, false);
    prefixes(
        |c| ConstantValue::from_int32_list(lf.clone(), wrong.clone(), &[7], policy(), PHASE, c),
        true,
    );
    prefixes(
        |c| ConstantValue::from_utf8_map(mf.clone(), wrong.clone(), &entries(), policy(), PHASE, c),
        true,
    );
    let wrong_field = Arc::new(Field::new("wrong_root", lt.data_type.clone(), true));
    prefixes(
        |c| {
            ConstantValue::from_int32_list(
                wrong_field.clone(),
                lt.clone(),
                &[7],
                policy(),
                PHASE,
                c,
            )
        },
        true,
    );
    let wrong_field = Arc::new(Field::new("wrong_carrier", DataType::Int64, false));
    prefixes(
        |c| {
            ConstantValue::from_utf8_map(
                wrong_field.clone(),
                mt.clone(),
                &entries(),
                policy(),
                PHASE,
                c,
            )
        },
        true,
    );
}

#[test]
fn collection_factory_row_node_and_logical_element_limits_match_actual_owner_facts() {
    let (lf, lt) = list_source(false);
    let (mf, mt) = map_source(false);
    for map in [false, true] {
        let invoke = |p| {
            if map {
                ConstantValue::from_utf8_map(
                    mf.clone(),
                    mt.clone(),
                    &entries(),
                    p,
                    PHASE,
                    &Control::default(),
                )
            } else {
                ConstantValue::from_int32_list(
                    lf.clone(),
                    lt.clone(),
                    &[7, -9, 0],
                    p,
                    PHASE,
                    &Control::default(),
                )
            }
        };
        let facts = invoke(policy()).unwrap().pool().resource_facts();
        for (index, exact) in [
            (0, facts.rows),
            (1, facts.array_nodes),
            (2, facts.logical_elements_upper_bound),
        ] {
            let set = |mut p: ConstantPolicy, v| {
                match index {
                    0 => p.max_rows = v,
                    1 => p.max_array_nodes = v,
                    _ => p.max_logical_elements = v,
                };
                p
            };
            assert!(invoke(set(policy(), exact)).is_ok());
            assert!(matches!(
                invoke(set(policy(), exact - 1)),
                Err(ConstantError::Limit(_))
            ));
        }
    }
}

#[test]
fn collection_factory_policy_limits_return_without_a_later_callback_or_tail() {
    let (field, ty) = list_source(false);
    let p = ConstantPolicy {
        max_logical_elements: 1,
        ..policy()
    };
    let control = Control::default();
    assert!(matches!(
        ConstantValue::from_int32_list(field.clone(), ty.clone(), &[7], p, PHASE, &control),
        Err(ConstantError::Limit(_))
    ));
    // Completed preflight type work remains pending when the originating limit
    // refuses. It must not flush an ordinary-error tail after that refusal.
    assert_eq!(*control.trace.lock().unwrap(), vec![0]);
    for cause in CAUSES {
        let control = Control {
            trace: Default::default(),
            stop: Some((0, cause)),
        };
        assert!(
            matches!(ConstantValue::from_int32_list(field.clone(), ty.clone(), &[7], p, PHASE, &control), Err(ConstantError::Control(actual)) if actual == cause)
        );
        assert_eq!(*control.trace.lock().unwrap(), vec![0]);
    }
}

#[test]
fn collection_factory_retained_metadata_and_library_limits_use_actual_admission_boundary() {
    let (field, ty) = map_source(false);
    let invoke = |p| {
        ConstantValue::from_utf8_map(
            field.clone(),
            ty.clone(),
            &entries(),
            p,
            PHASE,
            &Control::default(),
        )
    };
    let facts = invoke(policy()).unwrap().pool().resource_facts();
    // Locate the original owner's smallest accepted ceiling, including its
    // conservative pre-construction envelope. Do not guess allocator rounding,
    // mirror the private numerical formula, or weaken it to post-array facts.
    for index in [0, 2] {
        let get = |p: ConstantPolicy| match index {
            0 => p.max_retained_buffer_bytes,
            1 => p.max_metadata_bytes,
            2 => p.max_library_validation_work,
            _ => p.max_library_validation_bytes,
        };
        let set = |mut p: ConstantPolicy, v| {
            match index {
                0 => p.max_retained_buffer_bytes = v,
                1 => p.max_metadata_bytes = v,
                2 => p.max_library_validation_work = v,
                _ => p.max_library_validation_bytes = v,
            };
            p
        };
        let mut low = 0;
        let mut high = get(policy());
        while low < high {
            let middle = low + (high - low) / 2;
            match invoke(set(policy(), middle)) {
                Ok(_) => high = middle,
                Err(ConstantError::Limit(_)) => low = middle + 1,
                Err(error) => panic!("unexpected admission result: {error}"),
            }
        }
        let floor = match index {
            0 => facts.retained_buffer_capacity_bytes,
            1 => facts.metadata_bytes,
            2 => facts.library_validation_work_upper_bound,
            _ => facts.library_validation_bytes_upper_bound,
        };
        assert!(low >= floor && low > 0);
        assert!(invoke(set(policy(), low)).is_ok());
        assert!(matches!(
            invoke(set(policy(), low - 1)),
            Err(ConstantError::Limit(_))
        ));
    }
    // Metadata is the exact same source-type owner fact in both stages.
    assert!(
        invoke(ConstantPolicy {
            max_metadata_bytes: facts.metadata_bytes,
            ..policy()
        })
        .is_ok()
    );
    assert!(matches!(
        invoke(ConstantPolicy {
            max_metadata_bytes: facts.metadata_bytes - 1,
            ..policy()
        }),
        Err(ConstantError::Limit(_))
    ));
    // The factory's earlier multi-pass byte envelope can be stronger than the
    // post-pool floor. A ceiling below that floor is necessarily insufficient;
    // the explicit generous policy above remains the independently tested upper.
    assert!(matches!(
        invoke(ConstantPolicy {
            max_library_validation_bytes: facts.library_validation_bytes_upper_bound - 1,
            ..policy()
        }),
        Err(ConstantError::Limit(_))
    ));
}

#[test]
fn collection_factory_long_unicode_bytes_and_wide_list_have_actual_quantum_observations() {
    let (field, ty) = map_source(true);
    let text = "雪☃λ\0".repeat(4096);
    let input = vec![("z".into(), text.clone())];
    let good = Control::default();
    let value =
        ConstantValue::from_utf8_map(field.clone(), ty.clone(), &input, policy(), PHASE, &good)
            .unwrap();
    let view = value
        .utf8_map_observed(PHASE, &Control::default())
        .unwrap()
        .unwrap();
    assert_eq!(
        view.item(0, PHASE, &Control::default()).unwrap(),
        (Some("z"), Some(text.as_str()))
    );
    let trace = good.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    // Sample the first and last actual quantum plus entry/tail, without replaying
    // every long-output callback. All small paths above cover every position.
    let first = trace.iter().position(|units| *units == 256).unwrap();
    let last = trace.iter().rposition(|units| *units == 256).unwrap();
    for at in [0, first, last, trace.len() - 1] {
        for cause in CAUSES {
            let control = Control {
                trace: Default::default(),
                stop: Some((at, cause)),
            };
            assert!(
                matches!(ConstantValue::from_utf8_map(field.clone(), ty.clone(), &input, policy(), PHASE, &control), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    let (field, ty) = list_source(false);
    let values = (0..320).map(|i| i - 160).collect::<Vec<i32>>();
    let good = Control::default();
    let value = ConstantValue::from_int32_list(field, ty, &values, policy(), PHASE, &good).unwrap();
    assert!(good.trace.lock().unwrap().contains(&256));
    assert_eq!(
        value
            .int32_list_observed(PHASE, &Control::default())
            .unwrap()
            .unwrap()
            .item(319, PHASE, &Control::default())
            .unwrap(),
        Some(159)
    );
}
