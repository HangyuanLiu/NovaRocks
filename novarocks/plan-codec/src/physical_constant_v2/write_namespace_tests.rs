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

fn namespace_limits() -> ConstantNamespaceProjectionLimits {
    ConstantNamespaceProjectionLimits {
        max_records: 4096,
        max_preparation_request_bytes: 8 * 1024 * 1024,
        max_new_allocation_request_bytes: 128 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
        max_cumulative_library_work: 2 * 1024 * 1024 * 1024,
    }
}
fn binding(pool: u32, value_type_id: u32, field_id: u32) -> ConstantRecordTypeIds {
    ConstantRecordTypeIds {
        pool: ConstantPoolId::new(pool),
        value_type_id,
        field_id,
    }
}
fn alias_pools(original: &ConstantPool) -> novarocks_physical_plan::ConstantPools {
    let mut pools = novarocks_physical_plan::ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(u32::MAX), original.clone())
        .unwrap();
    pools
        .insert(ConstantPoolId::new(0), original.clone())
        .unwrap();
    pools
}

#[test]
fn write_namespace_binds_same_type_emission_and_counts_each_alias_output() {
    let original = pool(Arc::new(Float32Array::from(vec![
        Some(f32::from_bits(0x7fc0_1234)),
        None,
        Some(-0.0),
    ])));
    let pools = alias_pools(&original);
    let roots = [(0, original.value_type().clone())];
    let fields = [(u32::MAX, Arc::clone(original.field_ref()))];
    let table = encode_type_table_sources(
        &roots,
        &fields,
        type_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let bindings = [binding(0, 0, u32::MAX), binding(u32::MAX, 0, u32::MAX)];
    let control = Control::good(CompilePhase::Encode);
    let prepared = prepare_constant_namespace_write(
        &pools,
        &bindings,
        &table,
        SOURCE,
        write_limits(),
        namespace_limits(),
        &control,
    )
    .unwrap();
    let facts = *prepared.facts();
    assert_eq!(facts.source_retained_bytes, SOURCE);
    assert_eq!(facts.record_count, 2);
    assert_eq!(
        facts.coexisting_source_and_request_bytes_upper_bound,
        SOURCE + facts.new_allocation_request_bytes_upper_bound
    );
    assert!(facts.binding_work_upper_bound > 0);
    let records = prepared.emit().unwrap();
    assert_eq!(
        records.iter().map(|value| value.id).collect::<Vec<_>>(),
        [0, u32::MAX]
    );
    assert_eq!(records[0].arrow_ipc, records[1].arrow_ipc);
    assert_ne!(records[0].arrow_ipc.as_ptr(), records[1].arrow_ipc.as_ptr());
    let wire_types = table.into_wire();
    let decoded_types = decode_type_table(
        &wire_types,
        type_limits(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    let decoded = decode_constant_namespace(
        &records,
        &decoded_types,
        SOURCE,
        policy(),
        decode_limits(),
        namespace_limits(),
        &verifier(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    assert_ne!(
        decoded.entries()[&ConstantPoolId::new(0)].backing_identity(),
        decoded.entries()[&ConstantPoolId::new(u32::MAX)].backing_identity()
    );
    for result in decoded.entries().values() {
        for ordinal in 0..3 {
            assert!(
                original
                    .value(ordinal)
                    .unwrap()
                    .equals_observed(
                        &result.value(ordinal).unwrap(),
                        CompilePhase::Decode,
                        &Control::good(CompilePhase::Decode)
                    )
                    .unwrap()
            );
        }
    }
}

#[test]
fn write_namespace_mixed_full_fields_and_equal_distinct_arcs_roundtrip() {
    let flat = pool(Arc::new(StringArray::from(vec![Some("exact"), None])));
    let nested = pool(nested_arrays().remove(0));
    let mut pools = novarocks_physical_plan::ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(0), nested.clone())
        .unwrap();
    pools
        .insert(ConstantPoolId::new(u32::MAX), flat.clone())
        .unwrap();
    let roots = [
        (u32::MAX, nested.value_type().clone()),
        (0, flat.value_type().clone()),
    ];
    // An equal distinct Arc is legal. Full facts, rather than pointer identity,
    // bind this authored table source to the original checked pool.
    let fields = [
        (0, Arc::new(nested.field().clone())),
        (u32::MAX, Arc::clone(flat.field_ref())),
    ];
    let table = encode_type_table_sources(
        &roots,
        &fields,
        type_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let bindings = [binding(0, u32::MAX, 0), binding(u32::MAX, 0, u32::MAX)];
    let records = encode_constant_namespace(
        &pools,
        &bindings,
        &table,
        SOURCE,
        write_limits(),
        namespace_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let decoded_types = decode_type_table(
        table.as_wire(),
        type_limits(),
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    for (record, original) in records.iter().zip([&nested, &flat]) {
        let (_, result) = decode(record, &decoded_types);
        for ordinal in 0..original.array().len() as u32 {
            assert!(
                original
                    .value(ordinal)
                    .unwrap()
                    .equals_observed(
                        &result.value(ordinal).unwrap(),
                        CompilePhase::Decode,
                        &Control::good(CompilePhase::Decode)
                    )
                    .unwrap()
            );
        }
    }
}

#[test]
fn write_namespace_rejects_wrong_full_types_fields_ids_count_and_order() {
    let original = pool(Arc::new(Int64Array::from(vec![7])));
    let pools = alias_pools(&original);
    let roots = [
        (0, original.value_type().clone()),
        (1, FunctionValueType::new(DataType::Int64, false)),
        (2, FunctionValueType::new(DataType::Utf8, true)),
    ];
    let fields = [
        (0, Arc::clone(original.field_ref())),
        (
            1,
            Arc::new(
                original
                    .field()
                    .clone()
                    .with_metadata(HashMap::from([("provider".into(), "different".into())])),
            ),
        ),
    ];
    let table = encode_type_table_sources(
        &roots,
        &fields,
        type_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    for bindings in [
        vec![],
        vec![binding(0, 0, 0)],
        vec![binding(u32::MAX, 0, 0), binding(0, 0, 0)],
        vec![binding(0, 99, 0), binding(u32::MAX, 0, 0)],
        vec![binding(0, 0, 99), binding(u32::MAX, 0, 0)],
        vec![binding(0, 1, 0), binding(u32::MAX, 0, 0)],
        vec![binding(0, 2, 0), binding(u32::MAX, 0, 0)],
        vec![binding(0, 0, 1), binding(u32::MAX, 0, 0)],
    ] {
        every_prefix(
            CompilePhase::Encode,
            |control| {
                encode_constant_namespace(
                    &pools,
                    &bindings,
                    &table,
                    SOURCE,
                    write_limits(),
                    namespace_limits(),
                    control,
                )
            },
            false,
            false,
        );
    }
}

#[test]
fn write_namespace_original_invoice_cannot_be_padded_with_prepared_storage() {
    let original = pool(Arc::new(Int64Array::from(vec![7i64; 4096])));
    let mut pools = novarocks_physical_plan::ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(0), original.clone())
        .unwrap();
    let roots = [(0, original.value_type().clone())];
    let fields = [(0, Arc::clone(original.field_ref()))];
    let table = encode_type_table_sources(
        &roots,
        &fields,
        type_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let bindings = [binding(0, 0, 0)];
    let insufficient = original.resource_facts().retained_buffer_capacity_bytes as usize - 1;
    let result = encode_constant_namespace(
        &pools,
        &bindings,
        &table,
        insufficient,
        write_limits(),
        namespace_limits(),
        &Control::good(CompilePhase::Encode),
    );
    assert!(matches!(
        result,
        Err(PhysicalConstantCodecError::InvalidShape(
            "constant namespace writer source invoice excludes original pool backing"
        ))
    ));
    every_prefix(
        CompilePhase::Encode,
        |control| {
            encode_constant_namespace(
                &pools,
                &bindings,
                &table,
                insufficient,
                write_limits(),
                namespace_limits(),
                control,
            )
        },
        false,
        false,
    );
    assert!(
        encode_constant_namespace(
            &pools,
            &bindings,
            &table,
            SOURCE,
            write_limits(),
            namespace_limits(),
            &Control::good(CompilePhase::Encode)
        )
        .is_ok()
    );
}

#[test]
fn write_namespace_near_over_requests_work_and_all_control_prefixes() {
    let original = pool(Arc::new(Int64Array::from(vec![Some(7), None, Some(-9)])));
    let pools = alias_pools(&original);
    let roots = [(0, original.value_type().clone())];
    let fields = [(0, Arc::clone(original.field_ref()))];
    let table = encode_type_table_sources(
        &roots,
        &fields,
        type_limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let bindings = [binding(0, 0, 0), binding(u32::MAX, 0, 0)];
    let control = Control::good(CompilePhase::Encode);
    let facts = *prepare_constant_namespace_write(
        &pools,
        &bindings,
        &table,
        SOURCE,
        write_limits(),
        namespace_limits(),
        &control,
    )
    .unwrap()
    .facts();
    for (index, bound) in [
        facts.prepared_storage_request_bytes,
        facts.new_allocation_request_bytes_upper_bound,
        facts.coexisting_source_and_request_bytes_upper_bound,
        facts.cumulative_library_work_upper_bound,
    ]
    .into_iter()
    .enumerate()
    {
        let set = |limits: &mut ConstantNamespaceProjectionLimits, value| match index {
            0 => limits.max_preparation_request_bytes = value,
            1 => limits.max_new_allocation_request_bytes = value,
            2 => limits.max_coexisting_source_and_request_bytes = value,
            3 => limits.max_cumulative_library_work = value,
            _ => unreachable!(),
        };
        let mut limits = namespace_limits();
        set(&mut limits, bound);
        assert!(
            encode_constant_namespace(
                &pools,
                &bindings,
                &table,
                SOURCE,
                write_limits(),
                limits,
                &control
            )
            .is_ok(),
            "exact {index}"
        );
        set(&mut limits, bound - 1);
        assert!(
            encode_constant_namespace(
                &pools,
                &bindings,
                &table,
                SOURCE,
                write_limits(),
                limits,
                &control
            )
            .is_err(),
            "over {index}"
        );
    }
    every_prefix(
        CompilePhase::Encode,
        |control| {
            prepare_constant_namespace_write(
                &pools,
                &bindings,
                &table,
                SOURCE,
                write_limits(),
                namespace_limits(),
                control,
            )
            .map(|value| *value.facts())
        },
        true,
        false,
    );
    every_prefix(
        CompilePhase::Encode,
        |control| {
            encode_constant_namespace(
                &pools,
                &bindings,
                &table,
                SOURCE,
                write_limits(),
                namespace_limits(),
                control,
            )
        },
        true,
        false,
    );
}
