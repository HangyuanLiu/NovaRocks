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

fn split_encode(
    original: &ConstantPool,
    bounds: RecursivePoolWriteLimits,
    control: &Control,
) -> Result<Vec<u8>, TypeCodecError> {
    prepare_recursive_pool_write(original, invoice(original), bounds, control)?.emit(control)
}

fn positive(trace: &[(CompilePhase, u32)]) -> Vec<(CompilePhase, u32)> {
    trace
        .iter()
        .copied()
        .filter(|(_, units)| *units != 0)
        .collect()
}

#[test]
fn prepared_recursive_writer_snapshot_emits_original_fields_bits_and_ordinals() {
    let bits = [
        0x0000_0000_0000_0000,
        0x8000_0000_0000_0000,
        0x7ff0_0000_0000_0042,
        0x7ff0_0000_0000_0000,
        (-3.25_f64).to_bits(),
    ];
    let values: ArrayRef = Arc::new(Float64Array::from_iter_values(bits.map(f64::from_bits)));
    let item = Arc::new(
        Field::new("original-item", DataType::Float64, false).with_metadata(HashMap::from([(
            "nested-provider".into(),
            "original".into(),
        )])),
    );
    let array = ListArray::new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 5])),
        values,
        Some(NullBuffer::from(vec![true, false, true])),
    )
    .slice(1, 2);
    let original = pool(Arc::new(array));
    let control = Control::good(CompilePhase::Encode);
    let prepared =
        prepare_recursive_pool_write(&original, invoice(&original), limits(), &control).unwrap();
    assert!(std::ptr::eq(prepared.pool, &original));
    let expected = preflight_recursive_pool_write(
        &original,
        invoice(&original),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    assert_eq!(*prepared.facts(), expected);
    let bytes = prepared.emit(&control).unwrap();
    let output = agree_with_standard(&original, &bytes);
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output.value_offsets(), &[0, 2, 3]);
    assert!(output.is_null(0));
    assert!(!output.is_null(1));
    let child = output
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(child.len(), 3);
    assert_eq!(child.value(0).to_bits(), bits[2]);
    assert_eq!(child.value(1).to_bits(), bits[3]);
    assert_eq!(child.value(2).to_bits(), bits[4]);
    let decoded = ConstantPool::try_new(
        Arc::clone(original.field_ref()),
        original.value_type().clone(),
        output.to_data(),
        policy(),
        CompilePhase::Decode,
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    assert!(Arc::ptr_eq(decoded.field_ref(), original.field_ref()));
    assert_eq!(decoded.value_type(), original.value_type());
    assert!(
        original
            .value(1)
            .unwrap()
            .equals_observed(
                &decoded.value(1).unwrap(),
                CompilePhase::Decode,
                &Control::good(CompilePhase::Decode),
            )
            .unwrap()
    );
}

#[test]
fn prepared_recursive_writer_split_keeps_single_phase_positive_work_and_bytes() {
    // Parent offsets provide real wide work while the checked source's complete
    // child-elements bound stays within the unchanged finite fixture policy.
    let mut offsets = vec![0; 321];
    offsets[320] = 1;
    let original = pool(Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(Int32Array::from(vec![71])),
        None,
    )));
    let single_control = Control::good(CompilePhase::Encode);
    let single =
        encode_recursive_pool(&original, invoice(&original), limits(), &single_control).unwrap();
    let split_control = Control::good(CompilePhase::Encode);
    let split = split_encode(&original, limits(), &split_control).unwrap();
    assert_eq!(split, single);
    let single_work = positive(&single_control.trace());
    let split_work = positive(&split_control.trace());
    assert!(single_work.iter().any(|(_, units)| *units == 256));
    assert_eq!(split_work, single_work);
    // Splitting creates scope entry/tail observations, not another model walk.
    assert!(split_control.trace().len() > single_control.trace().len());
    let output = agree_with_standard(&original, &split);
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output.len(), 320);
    assert!(output.value(0).is_empty());
    let last = output.value(319);
    assert_eq!(
        last.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
        71
    );
}

#[test]
fn prepared_recursive_writer_prepare_and_emit_preserve_every_primary_control_prefix() {
    let original = list_fixture(2);
    for ordinary_failure in [false, true] {
        let mut bounds = limits();
        if ordinary_failure {
            bounds.max_total_rows = 1;
        }
        let good = Control::good(CompilePhase::Encode);
        let result = split_encode(&original, bounds, &good);
        if ordinary_failure {
            assert!(matches!(result, Err(TypeCodecError::InvalidShape(_))));
        } else {
            assert!(result.is_ok());
        }
        let trace = good.trace();
        for cause in CAUSES {
            for at in 0..trace.len() {
                let refusing = Control::refusing(at, cause);
                assert!(matches!(
                    split_encode(&original, bounds, &refusing),
                    Err(TypeCodecError::Control(actual)) if actual == cause
                ));
                assert_eq!(refusing.trace(), trace[..=at]);
            }
        }
    }
    // An already prepared owner's independent emission scope also preserves
    // its original callback cause at entry, opaque boundaries and publication.
    let baseline = Control::good(CompilePhase::Encode);
    prepare_recursive_pool_write(
        &original,
        invoice(&original),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap()
    .emit(&baseline)
    .unwrap();
    let trace = baseline.trace();
    for cause in CAUSES {
        for at in 0..trace.len() {
            let prepared = prepare_recursive_pool_write(
                &original,
                invoice(&original),
                limits(),
                &Control::good(CompilePhase::Encode),
            )
            .unwrap();
            let refusing = Control::refusing(at, cause);
            assert!(matches!(prepared.emit(&refusing),
                Err(TypeCodecError::Control(actual)) if actual == cause));
            assert_eq!(refusing.trace(), trace[..=at]);
        }
    }
}
