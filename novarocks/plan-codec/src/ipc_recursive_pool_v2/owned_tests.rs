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
fn source() -> ConstantPool {
    let fields = vec![
        Arc::new(Field::new("a", DataType::Int32, true)),
        Arc::new(Field::new("b", DataType::Utf8, true)),
    ];
    let values: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(7), None])),
        Arc::new(StringArray::from(vec![Some("x"), Some("yz")])),
    ];
    pool(Arc::new(StructArray::new(fields.into(), values, None)))
}
fn controller(at: Option<(usize, CompileControlError)>) -> Control {
    Control {
        phase: CompilePhase::Validate,
        refusal: at,
        trace: Mutex::new(Vec::new()),
    }
}
fn run(
    pool: &ConstantPool,
    control: &Control,
) -> Result<(Vec<u8>, RecursivePoolWriteFacts), TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let token =
        prepare_recursive_pool_write_in(pool, 1 << 20, limits(), &mut |_| Ok(()), &mut work)?;
    let facts = *token.facts();
    let bytes = token.emit_in(&mut |_| Ok(()), &mut work)?;
    work.finish()?;
    Ok((bytes, facts))
}
#[test]
fn parent_recursive_complete_children_and_all_real_prefixes() {
    let source = source();
    let control = controller(None);
    let (bytes, facts) = run(&source, &control).unwrap();
    let old = encode_recursive_pool(
        &source,
        1 << 20,
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    assert_eq!(bytes, old);
    let decoded = agree_with_standard(&source, &bytes);
    assert_eq!(decoded.to_data(), source.data().clone());
    assert_eq!(facts.field_nodes, 3);
    assert_eq!(facts.total_rows, 6);
    assert_eq!(facts.flat.buffer_descriptors, 6);
    let baseline = control.trace();
    for at in 0..baseline.len() {
        for cause in CAUSES {
            let control = controller(Some((at, cause)));
            assert!(
                matches!(run(&source,&control),Err(TypeCodecError::Control(actual))if actual==cause)
            );
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
}
#[test]
fn parent_recursive_root_field_limit_known_before_pending_callback() {
    let source = source();
    for cause in CAUSES {
        let control = controller(Some((1, cause)));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let mut l = limits();
        l.max_field_nodes = 0;
        assert!(matches!(
            prepare_recursive_pool_write_in(&source, 1 << 20, l, &mut |_| Ok(()), &mut work),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), [(CompilePhase::Validate, 0)]);
    }
}
#[test]
fn parent_recursive_wide_actual_quantum_and_exact_complete_replay() {
    let source = pool(Arc::new(StringArray::from(vec!["x"; 320])));
    let control = controller(None);
    let (_, facts) = run(&source, &control).unwrap();
    let baseline = control.trace();
    let quantum = baseline
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("actual owned initialization quantum");
    for at in [0, quantum, baseline.len() - 1] {
        for cause in CAUSES {
            let control = controller(Some((at, cause)));
            assert!(
                matches!(run(&source,&control),Err(TypeCodecError::Control(actual))if actual==cause)
            );
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
    let control = controller(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut tight = limits();
    tight.flat.max_new_allocation_request_bytes =
        facts.flat.new_allocation_request_bytes_upper_bound;
    tight.flat.max_coexisting_source_and_request_bytes =
        facts.flat.coexisting_source_and_request_bytes_upper_bound;
    tight.flat.max_cumulative_library_work = facts.flat.cumulative_library_work_upper_bound;
    let token = prepare_recursive_pool_write_in(
        &source,
        1 << 20,
        tight,
        &mut |prefix| {
            assert!(
                prefix.flat.new_allocation_request_bytes_upper_bound
                    <= facts.flat.new_allocation_request_bytes_upper_bound
            );
            assert!(
                prefix.flat.cumulative_library_work_upper_bound
                    <= facts.flat.cumulative_library_work_upper_bound
            );
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    token.emit_in(&mut |_| Ok(()), &mut work).unwrap();
}
