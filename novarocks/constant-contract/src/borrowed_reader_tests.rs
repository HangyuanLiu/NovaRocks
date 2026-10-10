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

const CALLER_PHASE: CompilePhase = CompilePhase::Decode;
#[derive(Default)]
struct Caller {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Caller {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CALLER_PHASE, "reader created another phase");
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "observation after first refusal");
        }
        trace.push(units);
        match self.refusal {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn caller_run<T>(
    control: &Caller,
    body: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, ConstantError>,
) -> Result<T, ConstantError> {
    let mut work = CompileCheckpoints::try_new(control, CALLER_PHASE)?;
    let result = body(&mut work);
    // This is the actual caller policy, not a tail published by an _in reader.
    finish(work, result)
}
fn prefixes(invoke: impl Fn(&Caller) -> Result<(), ConstantError>, ordinary: bool) {
    let good = Caller::default();
    let result = invoke(&good);
    if ordinary {
        assert!(matches!(result, Err(ConstantError::Invalid(_))));
    } else {
        result.unwrap();
    }
    let trace = good.trace.lock().unwrap().clone();
    assert_eq!(trace[0], 0);
    assert!(trace.last().is_some_and(|units| *units > 0));
    for at in 0..trace.len() {
        for cause in CAUSES {
            let failed = Caller {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause)),
            };
            assert!(
                matches!(invoke(&failed), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(*failed.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn borrowed_readers_keep_nonzero_ordinals_offsets_nulls_and_original_source() {
    let p = pool(Arc::new(list()));
    let selected = p.value(1).unwrap();
    let caller = Caller::default();
    let mut work = CompileCheckpoints::try_new(&caller, CALLER_PHASE).unwrap();
    let view = selected.int32_list_in(&mut work).unwrap().unwrap();
    assert!(std::ptr::eq(view.source(), &selected));
    assert_eq!(view.source().ordinal(), 1);
    assert!(std::ptr::eq(view.child, &p.0.data.child_data()[0]));
    assert_eq!(view.len(), 3);
    assert_eq!(view.item_observed(0, &mut work).unwrap(), Some(i32::MIN));
    assert_eq!(view.item_observed(1, &mut work).unwrap(), None);
    assert_eq!(view.item_observed(2, &mut work).unwrap(), Some(i32::MAX));
    assert_eq!(selected.selected_payload_bytes_in(&mut work).unwrap(), 8);
    // These small reads did not publish a nested entry or collection tail.
    assert_eq!(*caller.trace.lock().unwrap(), [0]);
    work.finish().unwrap();
    let sliced = pool(Arc::new(list().slice(1, 2)));
    for (ordinal, expected) in [(0, 8), (1, 0)] {
        let v = sliced.value(ordinal).unwrap();
        caller_run(&Caller::default(), |work| {
            assert_eq!(v.selected_payload_bytes_in(work)?, expected);
            assert_eq!(v.int32_list_in(work)?.is_none(), ordinal == 1);
            Ok(())
        })
        .unwrap();
    }
    let empty = p.value(3).unwrap();
    caller_run(&Caller::default(), |work| {
        assert!(empty.int32_list_in(work)?.unwrap().is_empty());
        assert_eq!(empty.selected_payload_bytes_in(work)?, 0);
        Ok(())
    })
    .unwrap();

    let original = map();
    let maps = pool(Arc::new(original.clone()));
    let selected = maps.value(1).unwrap();
    caller_run(&Caller::default(), |work| {
        let view = selected.utf8_map_in(work)?.unwrap();
        assert!(std::ptr::eq(view.source(), &selected));
        assert!(std::ptr::eq(
            view.keys,
            &maps.0.data.child_data()[0].child_data()[0]
        ));
        assert_eq!(view.len(), 4);
        assert_eq!(view.item_observed(0, work)?, (None, Some("雪☃")));
        assert_eq!(view.item_observed(1, work)?, (Some(""), None));
        assert_eq!(view.item_observed(2, work)?, (Some("dup"), Some("left")));
        assert_eq!(view.item_observed(3, work)?, (Some("dup"), Some("right")));
        assert_eq!(selected.selected_payload_bytes_in(work)?, 21);
        let (_, text) = view.item_observed(0, work)?;
        let values = original
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text.unwrap().as_ptr(), values.value(1).as_ptr());
        Ok(())
    })
    .unwrap();
    let foreign = pool(Arc::new(original.slice(1, 1)));
    let other = foreign.value(0).unwrap();
    caller_run(&Caller::default(), |work| {
        let view = other.utf8_map_in(work)?.unwrap();
        assert!(std::ptr::eq(view.source(), &other));
        assert!(!std::ptr::eq(view.source(), &selected));
        assert!(!std::ptr::eq(
            view.keys,
            &maps.0.data.child_data()[0].child_data()[0]
        ));
        assert_eq!(other.selected_payload_bytes_in(work)?, 21);
        Ok(())
    })
    .unwrap();
}

#[test]
fn borrowed_dictionary_and_run_roots_keep_encoded_null_and_selected_payload() {
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), None, Some(2)]),
        Arc::new(list()),
    )
    .unwrap();
    let p = pool(Arc::new(dictionary));
    for (ordinal, bytes, is_null) in [(0, 4, false), (1, 8, false), (2, 0, true), (3, 0, true)] {
        let value = p.value(ordinal).unwrap();
        prefixes(
            |c| {
                caller_run(c, |work| {
                    assert_eq!(value.selected_payload_bytes_in(work)?, bytes);
                    let view = value.int32_list_in(work)?;
                    assert_eq!(view.is_none(), is_null);
                    if let Some(view) = view {
                        assert!(std::ptr::eq(view.source(), &value));
                    }
                    Ok(())
                })
            },
            false,
        );
    }
    let values =
        ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(-7), None]), None]);
    let run = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 5]), &values).unwrap();
    let p = pool(Arc::new(run));
    for (ordinal, expected) in [(1, 4), (4, 0)] {
        let value = p.value(ordinal).unwrap();
        prefixes(
            |c| {
                caller_run(c, |work| {
                    assert_eq!(value.selected_payload_bytes_in(work)?, expected);
                    if let Some(view) = value.int32_list_in(work)? {
                        assert_eq!(view.item_observed(0, work)?, Some(-7));
                        assert_eq!(view.item_observed(1, work)?, None);
                    } else {
                        assert_eq!(ordinal, 4);
                    }
                    Ok(())
                })
            },
            false,
        );
    }
    let p = pool(Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None]),
            Arc::new(map()),
        )
        .unwrap(),
    ));
    for (ordinal, expected) in [(0, 6), (1, 21), (2, 0)] {
        let value = p.value(ordinal).unwrap();
        prefixes(
            |c| {
                caller_run(c, |work| {
                    assert_eq!(value.selected_payload_bytes_in(work)?, expected);
                    assert_eq!(value.utf8_map_in(work)?.is_none(), ordinal == 2);
                    Ok(())
                })
            },
            false,
        );
    }
}

#[test]
fn borrowed_selected_bytes_exclude_unselected_rows_and_null_child_backing() {
    let hidden = "x".repeat(320 * 1024);
    let mut bytes = b"one".to_vec();
    bytes.extend_from_slice(hidden.as_bytes());
    let text: ArrayRef = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, 3, i32::try_from(bytes.len()).unwrap()].into()),
        Buffer::from(bytes),
        Some(NullBuffer::from(vec![true, false])),
    ));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ]
        .into(),
        vec![keys, text],
        None,
    );
    let array = MapArray::try_new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0i32, 1, 2].into()),
        entries,
        None,
        false,
    )
    .unwrap();
    let p = pool(Arc::new(array));
    for (ordinal, bytes, item) in [(0, 4, (Some("a"), Some("one"))), (1, 1, (Some("b"), None))] {
        let value = p.value(ordinal).unwrap();
        let c = Caller::default();
        caller_run(&c, |work| {
            assert_eq!(value.selected_payload_bytes_in(work)?, bytes);
            assert_eq!(
                value.utf8_map_in(work)?.unwrap().item_observed(0, work)?,
                item
            );
            Ok(())
        })
        .unwrap();
        assert!(!c.trace.lock().unwrap().contains(&256));
    }
    assert!(
        p.resource_facts().retained_buffer_capacity_bytes > u64::try_from(hidden.len()).unwrap()
    );
}

#[test]
fn borrowed_success_and_ordinary_reads_preserve_every_actual_caller_prefix() {
    let lists = pool(Arc::new(list()));
    for ordinal in [1, 2, 3] {
        let value = lists.value(ordinal).unwrap();
        prefixes(
            |c| {
                caller_run(c, |work| {
                    value.selected_payload_bytes_in(work)?;
                    if let Some(view) = value.int32_list_in(work)? {
                        for i in 0..view.len() {
                            view.item_observed(i, work)?;
                        }
                    }
                    Ok(())
                })
            },
            false,
        );
    }
    let value = pool(Arc::new(map())).value(1).unwrap();
    prefixes(
        |c| {
            caller_run(c, |work| {
                value.selected_payload_bytes_in(work)?;
                let view = value.utf8_map_in(work)?.unwrap();
                for i in 0..view.len() {
                    view.item_observed(i, work)?;
                }
                Ok(())
            })
        },
        false,
    );
    let wrong = pool(Arc::new(Int64Array::from(vec![1]))).value(0).unwrap();
    for list_reader in [true, false] {
        prefixes(
            |c| {
                caller_run(c, |work| {
                    if list_reader {
                        wrong.int32_list_in(work)?;
                    } else {
                        wrong.utf8_map_in(work)?;
                    }
                    Ok(())
                })
            },
            true,
        );
    }
    let value = lists.value(1).unwrap();
    prefixes(
        |c| {
            caller_run(c, |work| {
                value.int32_list_in(work)?.unwrap().item_observed(3, work)?;
                Ok(())
            })
        },
        true,
    );
    prefixes(
        |c| {
            caller_run(c, |work| {
                value.utf8_map_in(work)?;
                Ok(())
            })
        },
        true,
    );
}

#[test]
fn borrowed_readers_preserve_original_facade_trace_and_caller_tail_ownership() {
    for value in [
        pool(Arc::new(list())).value(1).unwrap(),
        pool(Arc::new(map())).value(1).unwrap(),
    ] {
        let old = Control::default();
        let expected = value.selected_payload_bytes_observed(PHASE, &old).unwrap();
        let c = Caller::default();
        let mut work = CompileCheckpoints::try_new(&c, CALLER_PHASE).unwrap();
        assert_eq!(
            value.selected_payload_bytes_in(&mut work).unwrap(),
            expected
        );
        assert_eq!(*c.trace.lock().unwrap(), [0]);
        work.finish().unwrap();
        assert_eq!(*c.trace.lock().unwrap(), *old.trace.lock().unwrap());
    }
    let list_value = pool(Arc::new(list())).value(1).unwrap();
    let old = Control::default();
    assert_eq!(
        list_value
            .int32_list_observed(PHASE, &old)
            .unwrap()
            .unwrap()
            .len(),
        3
    );
    let c = Caller::default();
    let mut work = CompileCheckpoints::try_new(&c, CALLER_PHASE).unwrap();
    assert_eq!(
        list_value.int32_list_in(&mut work).unwrap().unwrap().len(),
        3
    );
    assert_eq!(*c.trace.lock().unwrap(), [0]);
    work.finish().unwrap();
    assert_eq!(*c.trace.lock().unwrap(), *old.trace.lock().unwrap());
    let map_value = pool(Arc::new(map())).value(1).unwrap();
    let old = Control::default();
    assert_eq!(
        map_value
            .utf8_map_observed(PHASE, &old)
            .unwrap()
            .unwrap()
            .len(),
        4
    );
    let c = Caller::default();
    let mut work = CompileCheckpoints::try_new(&c, CALLER_PHASE).unwrap();
    assert_eq!(map_value.utf8_map_in(&mut work).unwrap().unwrap().len(), 4);
    assert_eq!(*c.trace.lock().unwrap(), [0]);
    work.finish().unwrap();
    assert_eq!(*c.trace.lock().unwrap(), *old.trace.lock().unwrap());
    let wrong = pool(Arc::new(Int64Array::from(vec![1]))).value(0).unwrap();
    let old = Control::default();
    assert!(matches!(
        wrong.int32_list_observed(PHASE, &old),
        Err(ConstantError::Invalid(_))
    ));
    let c = Caller::default();
    let mut work = CompileCheckpoints::try_new(&c, CALLER_PHASE).unwrap();
    assert!(matches!(
        wrong.int32_list_in(&mut work),
        Err(ConstantError::Invalid(_))
    ));
    assert_eq!(*c.trace.lock().unwrap(), [0]);
    work.finish().unwrap();
    assert_eq!(*c.trace.lock().unwrap(), *old.trace.lock().unwrap());
}

#[test]
fn borrowed_wide_list_and_payload_share_actual_quantum_without_nested_tails() {
    let value = pool(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        [Some((0..320).map(Some).collect::<Vec<_>>())],
    )))
    .value(0)
    .unwrap();
    prefixes(
        |c| {
            caller_run(c, |work| {
                let view = value.int32_list_in(work)?.unwrap();
                assert_eq!(view.len(), 320);
                for i in 0..view.len() {
                    assert_eq!(
                        view.item_observed(i, work)?,
                        Some(i32::try_from(i).unwrap())
                    );
                }
                assert_eq!(value.selected_payload_bytes_in(work)?, 1280);
                Ok(())
            })
        },
        false,
    );
    let c = Caller::default();
    caller_run(&c, |work| {
        let view = value.int32_list_in(work)?.unwrap();
        for i in 0..view.len() {
            view.item_observed(i, work)?;
        }
        value.selected_payload_bytes_in(work)?;
        Ok(())
    })
    .unwrap();
    let trace = c.trace.lock().unwrap();
    assert!(trace.contains(&256));
    assert_eq!(trace.iter().filter(|&&units| units == 0).count(), 1);
}
