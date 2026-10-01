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
use arrow::datatypes::{Field, Schema};
use novarocks_result_contract::RenderColumn;
fn make(value: String, with_float: bool) -> ArrowMysqlTextEncoder {
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![value.as_str()]))];
    let mut fields = vec![RenderField {
        native_type: N::String,
        presentation: P::ScalarText,
        nullable: false,
    }];
    if with_float {
        arrays.push(Arc::new(Float64Array::from(vec![1.25])));
        fields.push(RenderField {
            native_type: N::Float64,
            presentation: P::ScalarText,
            nullable: false,
        });
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(
            arrays
                .iter()
                .map(|a| Field::new("x", a.data_type().clone(), false))
                .collect::<Vec<_>>(),
        )),
        arrays,
    )
    .unwrap();
    let cols = fields
        .into_iter()
        .enumerate()
        .map(|(i, field)| RenderColumn {
            source_ordinal: i as u32,
            source_slot: None,
            name: "x".into(),
            field,
        })
        .collect();
    ArrowMysqlTextEncoder::try_new(
        Arc::new(ClientRenderSchema::try_new(cols, batch.num_columns()).unwrap()),
        batch,
    )
    .unwrap()
}
#[test]
fn exact_threshold_path_and_float_once() {
    for (n, large) in [(65532, false), (65533, false), (65534, true)] {
        let mut e = make("x".repeat(n), false);
        let mut out = [0; 65536];
        let mut counted = false;
        loop {
            let t = e.step(&mut out).unwrap();
            counted |= e.phase == Phase::Count || e.phase == Phase::Emit;
            if t.status == RenderTurnStatus::InputComplete {
                break;
            }
        }
        assert_eq!(counted, large, "{n}");
    }
    FLOAT_FORMATS.with(|n| n.set(0));
    let mut e = make("small".into(), true);
    let mut out = [0; 65536];
    loop {
        if e.step(&mut out).unwrap().status == RenderTurnStatus::InputComplete {
            break;
        }
    }
    assert_eq!(FLOAT_FORMATS.with(std::cell::Cell::get), 1);
}
#[test]
fn u32_prefix_not_published_on_exhausted_cell_budget() {
    let mut e = make("x".repeat(65534), false);
    let mut out = [0; 100];
    while e.phase != Phase::Emit {
        e.step(&mut out).unwrap();
    }
    e.reset_row(Phase::Emit);
    e.row_length = 65538;
    e.lengths[0] = 65534;
    let mut budget = Budget::new();
    budget.cells = V::CELLS_PER_TURN;
    assert!(!e.start_cell(&mut budget).unwrap());
    assert_eq!(e.prefix_at, 0);
    assert!(!e.cell_started);
    let t = e.step(&mut out[..5]).unwrap();
    assert_eq!(t.emitted_bytes, 5);
    assert_eq!(out[4], 0xfc);
}

#[test]
fn actual_scratch_capacity_layout() {
    let e = make("x".into(), false);
    let capacity = e.scratch_capacity_bytes();
    eprintln!(
        "SCRATCH capacity_bytes={capacity} staging_bytes={} cell_lengths_bytes={} task_slots={} task_size={} stack_capacity_bytes={} control_bytes={}",
        size_of::<[u8; 65536]>(),
        size_of::<[u32; 4096]>(),
        e.cursor.stack.capacity(),
        size_of::<Task>(),
        e.cursor.stack.capacity() * size_of::<Task>(),
        size_of::<ArrowMysqlTextEncoder>()
    );
    assert_eq!(
        capacity,
        size_of::<ArrowMysqlTextEncoder>() + 65536 + 16384 + 196 * size_of::<Task>()
    );
    assert!(capacity < 2 * 1024 * 1024);
}
