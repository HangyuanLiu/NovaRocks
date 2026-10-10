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

//! Actual borrowed geometry and all typed refusal positions of source custody.
//! The Execution companion proves the real original Chunk accounting lifetime.
use super::*;
use crate::arrow_result_custody::{retain_source_backing, SourceBackingOwner};
use crate::array_backing_geometry::source_metadata_bytes;
use arrow_array::StringViewArray;
use arrow_buffer::Buffer;

#[test]
fn by_window_source_custody_actual_view_tables_are_admitted_before_to_data() {
    let base = StringViewArray::from(vec!["x"]);
    let many = StringViewArray::try_new(
        base.views().clone(),
        vec![Buffer::from_vec(Vec::<u8>::new()); 512],
        None,
    )
    .unwrap();
    assert_eq!(base.get_array_memory_size(), many.get_array_memory_size());
    let control = Control::default();
    let small = source_metadata_bytes(&base, &mut EvaluationCheckpoints::new(&control)).unwrap();
    let envelope = source_metadata_bytes(&many, &mut EvaluationCheckpoints::new(&control)).unwrap();
    assert!(envelope > small);
    let host = Arc::new(WindowHost::default());
    let array = Arc::new(many) as ArrayRef;
    let original =
        SourceBackingOwner::try_from_owned(array.clone(), host.clone(), &control).unwrap();
    let result = retain_source_backing(array, &original, &control).unwrap();
    assert!(
        host.gate
            .lock()
            .unwrap()
            .events
            .iter()
            .any(|(opaque, bytes)| *opaque && *bytes == envelope)
    );
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(0),
        "x"
    );
    let data = result.to_data();
    let empty_buffer = data.buffers()[512].clone();
    drop(original);
    drop(result);
    drop(data);
    assert!(
        host.gate.lock().unwrap().opaque > 0,
        "even a real empty Buffer loan holds its original owner"
    );
    drop(empty_buffer);
    host.zero();
}

fn source_custody_run(host: Arc<WindowHost>, control: &Control) -> Result<(), KernelFailure> {
    let array = Arc::new(StringArray::from(vec!["actual", "borrowed"])) as ArrayRef;
    let source = SourceBackingOwner::try_from_owned(array.clone(), host, control)?;
    let result = retain_source_backing(array, &source, control)?;
    drop(source);
    drop(result);
    Ok(())
}

#[test]
fn by_window_source_custody_actual_every_host_and_callback_seven_causes_no_footer() {
    let host = Arc::new(WindowHost::default());
    let control = Control::default();
    source_custody_run(host.clone(), &control).unwrap();
    let requests = host.gate.lock().unwrap().events.len();
    let checkpoints = control.trace.lock().unwrap().len();
    host.zero();
    assert!(requests > 0 && checkpoints > 0);
    for cause in causes() {
        for stop in 0..requests {
            let host = Arc::new(WindowHost::default());
            host.gate.lock().unwrap().refusal = Some((stop, cause.clone()));
            assert_eq!(
                source_custody_run(host.clone(), &Control::default()),
                Err(cause.clone())
            );
            assert_eq!(host.gate.lock().unwrap().events.len(), stop + 1);
            host.zero();
        }
        for stop in 0..checkpoints {
            let host = Arc::new(WindowHost::default());
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert_eq!(
                source_custody_run(host.clone(), &control),
                Err(cause.clone())
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            host.zero();
        }
    }
}

fn source_union(id: i8) -> ArrayRef {
    use arrow_array::UnionArray;
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::{Field, UnionFields};
    let fields =
        UnionFields::try_new([id], [Field::new("original", DataType::Int32, false)]).unwrap();
    Arc::new(
        UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![id]),
            None,
            vec![Arc::new(Int32Array::from(vec![42])) as ArrayRef],
        )
        .unwrap(),
    )
}

#[test]
fn by_window_source_custody_union_sparse_ids_use_actual_indexed_table_request() {
    use arrow_array::UnionArray;
    let small = source_union(0);
    let high = source_union(127);
    let control = Control::default();
    let low_envelope =
        source_metadata_bytes(small.as_ref(), &mut EvaluationCheckpoints::new(&control)).unwrap();
    let high_envelope =
        source_metadata_bytes(high.as_ref(), &mut EvaluationCheckpoints::new(&control)).unwrap();
    assert_eq!(
        high_envelope - low_envelope,
        4 * 2 * 127 * size_of::<Option<ArrayRef>>()
    );
    for (array, id, envelope) in [(small, 0, low_envelope), (high, 127, high_envelope)] {
        let host = Arc::new(WindowHost::default());
        let source =
            SourceBackingOwner::try_from_owned(array.clone(), host.clone(), &control).unwrap();
        let start = host.gate.lock().unwrap().events.len();
        let result = retain_source_backing(array.clone(), &source, &control).unwrap();
        assert_eq!(host.gate.lock().unwrap().events[start], (true, envelope));
        assert_eq!(result.to_data(), array.to_data());
        let union = result.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(union.type_ids().as_ref(), &[id]);
        assert_eq!(
            union
                .child(id)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            42
        );
        let data = result.to_data();
        let last_buffer = data.buffers()[0].clone();
        drop(data);
        drop(result);
        drop(source);
        assert!(host.gate.lock().unwrap().opaque > 0);
        drop(last_buffer);
        host.zero();
    }
}

#[test]
fn by_window_source_custody_union_sparse_ids_actual_request_refusal_preserves_cause() {
    for id in [0, 127] {
        for cause in causes() {
            let array = source_union(id);
            let host = Arc::new(WindowHost::default());
            let control = Control::default();
            let source =
                SourceBackingOwner::try_from_owned(array.clone(), host.clone(), &control).unwrap();
            let next = host.gate.lock().unwrap().events.len();
            let envelope =
                source_metadata_bytes(array.as_ref(), &mut EvaluationCheckpoints::new(&control))
                    .unwrap();
            host.gate.lock().unwrap().refusal = Some((next, cause.clone()));
            let actual = retain_source_backing(array, &source, &control).unwrap_err();
            assert_eq!(actual, cause);
            let gate = host.gate.lock().unwrap();
            assert_eq!(gate.events.len(), next + 1);
            assert_eq!(gate.events[next], (true, envelope));
            assert_eq!(gate.opaque, 0);
            drop(gate);
            drop(source);
            host.zero();
        }
    }
}
