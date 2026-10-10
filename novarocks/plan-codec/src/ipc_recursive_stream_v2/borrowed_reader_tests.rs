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
use crate::physical_type_v2::{
    DecodedTypeTable, TypeProjectionLimits, decode_type_table, encode_type_table_with_fields,
};
use novarocks_constant_contract::ConstantError;

fn table(field: &Arc<Field>, ty: &FunctionValueType) -> DecodedTypeTable {
    let limits = TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 16384,
        max_string_bytes: schema_limits().max_string_bytes,
    };
    let wire = encode_type_table_with_fields(
        &[(u32::MAX, ty.clone())],
        &[(0, Arc::clone(field))],
        limits,
        &Control::default(),
    )
    .unwrap();
    decode_type_table(&wire, limits, &Control::default()).unwrap()
}

fn invoke(
    borrowed: bool,
    stream: &RecursiveConstantStream<'_, '_>,
    field: &Arc<Field>,
    ty: &FunctionValueType,
    invoice: usize,
    control: &Control,
) -> Result<ConstantPool, RecursiveReaderError> {
    if borrowed {
        stream.materialize_pool_borrowed(
            Arc::clone(field),
            ty,
            invoice,
            policy(),
            reader_limits(),
            control,
        )
    } else {
        stream.materialize_pool(
            Arc::clone(field),
            ty.clone(),
            invoice,
            policy(),
            reader_limits(),
            control,
        )
    }
}

fn check_both_traces(
    stream: &RecursiveConstantStream<'_, '_>,
    field: &Arc<Field>,
    ty: &FunctionValueType,
    invoice: usize,
    succeeds: bool,
) {
    let owned = Control::default();
    let borrowed = Control::default();
    let owned_result = invoke(false, stream, field, ty, invoice, &owned);
    let borrowed_result = invoke(true, stream, field, ty, invoice, &borrowed);
    assert_eq!(owned_result.is_ok(), succeeds);
    assert_eq!(borrowed_result.is_ok(), succeeds);
    if !succeeds {
        let owned_error = owned_result.unwrap_err();
        let borrowed_error = borrowed_result.unwrap_err();
        assert!(matches!(owned_error, RecursiveReaderError::Projection(_)));
        assert!(matches!(
            borrowed_error,
            RecursiveReaderError::Projection(_)
        ));
        assert_eq!(owned_error.to_string(), borrowed_error.to_string());
    }
    let trace = owned.trace.lock().unwrap().clone();
    assert_eq!(*borrowed.trace.lock().unwrap(), trace);
    assert!(!trace.is_empty());
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::Decode && *units <= 256)
    );
    for at in 0..trace.len() {
        for cause in CAUSES {
            for borrowed in [false, true] {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    stop: Some((at, cause)),
                };
                assert!(matches!(
                    invoke(borrowed, stream, field, ty, invoice, &control),
                    Err(RecursiveReaderError::Projection(FlatPoolResourceError::Control(actual)))
                        if actual == cause
                ));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn borrowed_recursive_reader_retains_real_type_table_full_field_and_pool_values() {
    let array = nested();
    let field = Arc::new(
        Field::new("original", array.data_type().clone(), true)
            .with_metadata(HashMap::from([("identity".into(), "retained".into())])),
    );
    let ty = FunctionValueType::new(field.data_type().clone(), true);
    let types = table(&field, &ty);
    let field = types.field(0).unwrap();
    let ty = types.value_type(u32::MAX).unwrap();
    let bytes = fixture(Arc::clone(&array), field);
    let checked = stream(&bytes, field);
    let invoice = source(&bytes);
    let pool = invoke(true, &checked, field, ty, invoice, &Control::default()).unwrap();
    assert!(Arc::ptr_eq(pool.field_ref(), field));
    assert_eq!(pool.field().name(), "original");
    assert_eq!(
        pool.field().metadata().get("identity").map(String::as_str),
        Some("retained")
    );
    assert_eq!(pool.array().to_data(), array.to_data());
    assert!(
        pool.value(0)
            .unwrap()
            .is_null_observed(CompilePhase::Decode, &Control::default())
            .unwrap()
    );
    assert_eq!(pool.value(1).unwrap().ordinal(), 1);
    assert_eq!(pool.resource_facts().array_nodes, 4);
    check_both_traces(&checked, field, ty, invoice, true);
}

#[test]
fn borrowed_recursive_reader_preserves_nested_type_failures_and_original_control_prefixes() {
    let array = nested();
    let field = Arc::new(Field::new("original", array.data_type().clone(), true));
    let bytes = fixture(array, &field);
    let checked = stream(&bytes, &field);
    let invoice = source(&bytes);
    let DataType::List(child) = field.data_type() else {
        panic!("actual nested fixture is a List")
    };
    let changed_child = Arc::new(child.as_ref().clone().with_metadata(HashMap::from([(
        "provider".into(),
        "changed-source".into(),
    )])));
    for wrong in [
        FunctionValueType::new(field.data_type().clone(), false),
        FunctionValueType::new(DataType::List(changed_child), true),
        FunctionValueType::new(
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(field.data_type().clone()),
            ),
            true,
        ),
    ] {
        let error =
            invoke(true, &checked, &field, &wrong, invoice, &Control::default()).unwrap_err();
        assert!(matches!(
            error,
            RecursiveReaderError::Projection(FlatPoolResourceError::Constant(
                ConstantError::Invalid(_)
            ))
        ));
        // The frozen core retains Cow::Borrowed until after the resource gate
        // and reader/to_data; this test does not claim allocator instrumentation.
        check_both_traces(&checked, &field, &wrong, invoice, false);
    }
    let replacement = Arc::new(field.as_ref().clone());
    check_both_traces(
        &checked,
        &replacement,
        &FunctionValueType::new(field.data_type().clone(), true),
        invoice,
        false,
    );
}
