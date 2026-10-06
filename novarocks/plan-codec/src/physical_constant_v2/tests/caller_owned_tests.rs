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
use crate::physical_type_v2::encode_type_table_sources;
use novarocks_physical_plan::ConstantPools;

fn namespace_limits() -> ConstantNamespaceProjectionLimits {
    ConstantNamespaceProjectionLimits {
        max_records: 16,
        max_preparation_request_bytes: 32 * 1024 * 1024,
        max_new_allocation_request_bytes: 512 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 512 * 1024 * 1024,
        max_cumulative_library_work: 1024 * 1024 * 1024,
    }
}
fn duplicate_sources(original: &ConstantPool) -> ConstantPools {
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(0), original.clone())
        .unwrap();
    pools
        .insert(ConstantPoolId::new(u32::MAX), original.clone())
        .unwrap();
    pools
}
fn bindings() -> [ConstantRecordTypeIds; 2] {
    [
        ConstantRecordTypeIds {
            pool: ConstantPoolId::new(0),
            value_type_id: 0,
            field_id: 0,
        },
        ConstantRecordTypeIds {
            pool: ConstantPoolId::new(u32::MAX),
            value_type_id: u32::MAX,
            field_id: u32::MAX,
        },
    ]
}

#[test]
fn caller_constant_namespace_actual_aliases_keep_sparse_ids_and_source_once() {
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(7), None]))];
    arrays.push(nested_arrays().remove(0));
    for array in arrays {
        let original = pool(array);
        let pools = duplicate_sources(&original);
        let values = [
            (0, original.value_type().clone()),
            (u32::MAX, original.value_type().clone()),
        ];
        let fields = [
            (0, Arc::clone(original.field_ref())),
            (u32::MAX, Arc::clone(original.field_ref())),
        ];
        let typed = encode_type_table_sources(
            &values,
            &fields,
            type_limits(),
            &Control::good(CompilePhase::Encode),
        )
        .unwrap();
        let ids = bindings();
        let control = Control::good(CompilePhase::Encode);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut last = ConstantNamespaceWriteFacts::default();
        let prepared = prepare_constant_namespace_write_in(
            &pools,
            &ids,
            &typed,
            SOURCE,
            write_limits(),
            namespace_limits(),
            &mut |facts| {
                assert_eq!(facts.source_retained_bytes, SOURCE);
                assert_eq!(
                    facts.coexisting_source_and_request_bytes_upper_bound,
                    SOURCE + facts.new_allocation_request_bytes_upper_bound
                );
                assert!(
                    facts.new_allocation_request_bytes_upper_bound
                        >= last.new_allocation_request_bytes_upper_bound
                );
                last = *facts;
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        assert_eq!(*prepared.facts(), last);
        let records = prepared
            .emit_in(
                &mut |facts| {
                    assert_eq!(*facts, last);
                    Ok(())
                },
                &mut work,
            )
            .unwrap();
        work.finish().unwrap();
        assert_eq!(
            records.iter().map(|record| record.id).collect::<Vec<_>>(),
            vec![0, u32::MAX]
        );
        let decoded = decode_type_table(
            typed.as_wire(),
            type_limits(),
            &Control::good(CompilePhase::Decode),
        )
        .unwrap();
        let control = Control::good(CompilePhase::Decode);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut last = ConstantNamespaceResourceFacts::default();
        let prepared = prepare_constant_namespace_in(
            &records,
            &decoded,
            SOURCE,
            policy(),
            decode_limits(),
            namespace_limits(),
            &verifier(),
            &mut |facts| {
                assert_eq!(facts.source_retained_bytes, SOURCE);
                assert_eq!(
                    facts.coexisting_source_and_request_bytes_upper_bound,
                    SOURCE + facts.new_allocation_request_bytes_upper_bound
                );
                assert_eq!(
                    facts.preparation_request_bytes,
                    facts.prepared_storage_request_bytes + facts.geometry_scratch_request_bytes
                );
                assert_eq!(
                    facts.new_allocation_request_bytes_upper_bound,
                    facts.prepared_storage_request_bytes
                        + facts.pool_table_request_bytes_upper_bound
                        + facts.reader_request_bytes_upper_bound
                );
                assert!(
                    facts.new_allocation_request_bytes_upper_bound
                        >= last.new_allocation_request_bytes_upper_bound
                );
                last = *facts;
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        assert_eq!(*prepared.facts(), last);
        let result = prepared
            .materialize_in(
                &mut |facts| {
                    assert_eq!(*facts, last);
                    Ok(())
                },
                &mut work,
            )
            .unwrap();
        work.finish().unwrap();
        assert_eq!(result.entries().len(), 2);
        for id in [0, u32::MAX] {
            let decoded_pool = result.entries().get(&ConstantPoolId::new(id)).unwrap();
            assert_eq!(decoded_pool.value_type(), original.value_type());
            assert_eq!(decoded_pool.field(), original.field());
            assert_eq!(decoded_pool.array().len(), original.array().len());
            for ordinal in 0..original.array().len() {
                assert!(
                    original
                        .value(ordinal as u32)
                        .unwrap()
                        .equals_observed(
                            &decoded_pool.value(ordinal as u32).unwrap(),
                            CompilePhase::Validate,
                            &Control::good(CompilePhase::Validate)
                        )
                        .unwrap()
                );
            }
        }
    }
}

#[test]
fn caller_constant_namespace_known_initial_requests_precede_all_late_causes() {
    let original = pool(Arc::new(Int64Array::from(vec![7, 11])));
    let pools = duplicate_sources(&original);
    let values = [
        (0, original.value_type().clone()),
        (u32::MAX, original.value_type().clone()),
    ];
    let fields = [
        (0, Arc::clone(original.field_ref())),
        (u32::MAX, Arc::clone(original.field_ref())),
    ];
    let typed = encode_type_table_sources(
        &values,
        &fields,
        type_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let ids = bindings();
    let mut limits = namespace_limits();
    limits.max_new_allocation_request_bytes = 0;
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            let control = Control::refusing(CompilePhase::Encode, 1, cause);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            let result = prepare_constant_namespace_write_in(
                &pools,
                &ids,
                &typed,
                SOURCE,
                write_limits(),
                limits,
                &mut |_| panic!("parent after known local request refusal"),
                &mut work,
            );
            assert!(matches!(
                result,
                Err(PhysicalConstantCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(control.trace(), vec![(CompilePhase::Encode, 0)]);
        }
    }
}

fn caller_write(
    pools: &ConstantPools,
    ids: &[ConstantRecordTypeIds],
    typed: &crate::physical_type_v2::EncodedTypeTable<'_>,
    control: &Control,
) -> Result<
    Vec<novarocks_proto_models::physical_package_v2::IpcConstantPool>,
    PhysicalConstantCodecError,
> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        prepare_constant_namespace_write_in(
            pools,
            ids,
            typed,
            SOURCE,
            write_limits(),
            namespace_limits(),
            &mut |_| Ok(()),
            &mut work,
        )?
        .emit_in(&mut |_| Ok(()), &mut work)
    })();
    finish(work, result)
}
fn caller_read(
    records: &[novarocks_proto_models::physical_package_v2::IpcConstantPool],
    typed: &crate::physical_type_v2::DecodedTypeTable,
    control: &Control,
) -> Result<ConstantPools, PhysicalConstantCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        prepare_constant_namespace_in(
            records,
            typed,
            SOURCE,
            policy(),
            decode_limits(),
            namespace_limits(),
            &verifier(),
            &mut |_| Ok(()),
            &mut work,
        )?
        .materialize_in(&mut |_| Ok(()), &mut work)
    })();
    finish(work, result)
}

#[test]
fn caller_constant_namespace_actual_success_and_ordinary_tails_keep_every_first_cause() {
    let original = pool(Arc::new(Int64Array::from(vec![Some(7), None])));
    let pools = duplicate_sources(&original);
    let values = [
        (0, original.value_type().clone()),
        (u32::MAX, original.value_type().clone()),
    ];
    let fields = [
        (0, Arc::clone(original.field_ref())),
        (u32::MAX, Arc::clone(original.field_ref())),
    ];
    let typed = encode_type_table_sources(
        &values,
        &fields,
        type_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let ids = bindings();
    let good = Control::good(CompilePhase::Encode);
    let records = caller_write(&pools, &ids, &typed, &good).unwrap();
    let decoded = decode_type_table(
        typed.as_wire(),
        type_limits(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    let mut bad_ids = ids;
    bad_ids.swap(0, 1);
    for ids in [&ids, &bad_ids] {
        let baseline = Control::good(CompilePhase::Encode);
        let result = caller_write(&pools, ids, &typed, &baseline);
        assert_eq!(result.is_ok(), ids[0].pool == ConstantPoolId::new(0));
        let trace = baseline.trace();
        for stop in 0..trace.len() {
            for cause in CAUSES {
                let refusing = Control::refusing(CompilePhase::Encode, stop, cause);
                assert!(
                    matches!(caller_write(&pools, ids, &typed, &refusing), Err(PhysicalConstantCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(refusing.trace(), trace[..=stop]);
            }
        }
    }
    let mut bad_records = records.clone();
    bad_records[0].compression = i32::MAX;
    for (records, succeeds) in [(&records, true), (&bad_records, false)] {
        let baseline = Control::good(CompilePhase::Decode);
        assert_eq!(caller_read(records, &decoded, &baseline).is_ok(), succeeds);
        let trace = baseline.trace();
        for stop in 0..trace.len() {
            for cause in CAUSES {
                let refusing = Control::refusing(CompilePhase::Decode, stop, cause);
                assert!(
                    matches!(caller_read(records, &decoded, &refusing), Err(PhysicalConstantCodecError::Control(actual)) if actual == cause)
                );
                assert_eq!(refusing.trace(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn caller_constant_namespace_foreign_work_cannot_reach_parent_or_materialization() {
    let original = pool(Arc::new(Int64Array::from(vec![7])));
    let pools = duplicate_sources(&original);
    let values = [
        (0, original.value_type().clone()),
        (u32::MAX, original.value_type().clone()),
    ];
    let fields = [
        (0, Arc::clone(original.field_ref())),
        (u32::MAX, Arc::clone(original.field_ref())),
    ];
    let typed = encode_type_table_sources(
        &values,
        &fields,
        type_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let ids = bindings();
    let owner = Control::good(CompilePhase::Encode);
    let mut work = CompileCheckpoints::try_new(&owner, CompilePhase::Encode).unwrap();
    let token = prepare_constant_namespace_write_in(
        &pools,
        &ids,
        &typed,
        SOURCE,
        write_limits(),
        namespace_limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let foreign = Control::good(CompilePhase::Encode);
    let mut other = CompileCheckpoints::try_new(&foreign, CompilePhase::Encode).unwrap();
    assert!(matches!(
        token.emit_in(&mut |_| panic!("foreign emit reached parent"), &mut other),
        Err(PhysicalConstantCodecError::InvalidShape(_))
    ));
    assert_eq!(foreign.trace(), vec![(CompilePhase::Encode, 0)]);
    let records = caller_write(&pools, &ids, &typed, &Control::good(CompilePhase::Encode)).unwrap();
    let decoded = decode_type_table(
        typed.as_wire(),
        type_limits(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    let owner = Control::good(CompilePhase::Decode);
    let mut work = CompileCheckpoints::try_new(&owner, CompilePhase::Decode).unwrap();
    let token = prepare_constant_namespace_in(
        &records,
        &decoded,
        SOURCE,
        policy(),
        decode_limits(),
        namespace_limits(),
        &verifier(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let foreign = Control::good(CompilePhase::Decode);
    let mut other = CompileCheckpoints::try_new(&foreign, CompilePhase::Decode).unwrap();
    assert!(matches!(
        token.materialize_in(
            &mut |_| panic!("foreign materialize reached parent"),
            &mut other
        ),
        Err(PhysicalConstantCodecError::InvalidShape(_))
    ));
    assert_eq!(foreign.trace(), vec![(CompilePhase::Decode, 0)]);
}

#[test]
fn caller_constant_namespace_receiver_known_header_overflow_precedes_pending_causes() {
    let original = pool(Arc::new(Int64Array::from(vec![7])));
    let pools = duplicate_sources(&original);
    let values = [
        (0, original.value_type().clone()),
        (u32::MAX, original.value_type().clone()),
    ];
    let fields = [
        (0, Arc::clone(original.field_ref())),
        (u32::MAX, Arc::clone(original.field_ref())),
    ];
    let typed = encode_type_table_sources(
        &values,
        &fields,
        type_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let records = caller_write(
        &pools,
        &bindings(),
        &typed,
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let decoded = decode_type_table(
        typed.as_wire(),
        type_limits(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    for pending in [0, 254, 255] {
        for cause in CAUSES {
            let control = Control::refusing(CompilePhase::Decode, 1, cause);
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            let result = prepare_constant_namespace_in(
                &records,
                &decoded,
                usize::MAX,
                policy(),
                decode_limits(),
                namespace_limits(),
                &verifier(),
                &mut |_| panic!("parent after unrepresentable header"),
                &mut work,
            );
            assert!(matches!(
                result,
                Err(PhysicalConstantCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(control.trace(), vec![(CompilePhase::Decode, 0)]);
        }
    }
}
