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
use crate::iceberg::spec::NestedField;
use std::cell::Cell;
use std::error::Error as _;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
struct RawCause(Arc<()>);
impl std::fmt::Debug for RawCause {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("raw cause Debug must not be called")
    }
}
impl std::fmt::Display for RawCause {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("raw cause Display must not be called")
    }
}
impl std::error::Error for RawCause {}
fn binding() -> (PartitionSpec, StructType) {
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "s", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(2, "n", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap();
    let spec: PartitionSpec = serde_json::from_str(r#"{"spec-id":2,"fields":[{"source-id":1,"field-id":1000,"name":"s","transform":"identity"},{"source-id":2,"field-id":1001,"name":"n","transform":"identity"}]}"#).unwrap();
    let ty = spec.partition_type(&schema).unwrap();
    (spec, ty)
}
fn success() -> std::result::Result<(), RawCause> {
    Ok(())
}

#[test]
fn ordinary_and_checked_share_actual_long_promotion_and_nulls() {
    let (spec, ty) = binding();
    for value in [None, Some(Literal::Primitive(PrimitiveLiteral::Int(7)))] {
        let tuple: Struct = [None, value].into_iter().collect();
        let projected = Cell::new(0);
        let polls = Cell::new(0);
        let checked = TypedPartition::bind_type_checked(
            &spec,
            &ty,
            &tuple,
            |actual_type, actual_tuple| {
                assert!(std::ptr::eq(actual_type, &ty));
                assert!(std::ptr::eq(actual_tuple, &tuple));
                projected.set(projected.get() + 1);
                Ok::<(), RawCause>(())
            },
            || {
                polls.set(polls.get() + 1);
                success()
            },
        )
        .unwrap();
        assert_eq!(
            checked,
            TypedPartition::bind_type(&spec, &ty, &tuple).unwrap()
        );
        assert!(checked.values()[0].is_none());
        match &tuple.fields()[1] {
            None => assert!(checked.values()[1].is_none()),
            _ => assert_eq!(checked.values()[1], Some(CanonicalScalar::Long(7))),
        }
        assert_eq!(projected.get(), 1);
        assert_eq!(polls.get(), 5);
    }
}
#[test]
fn original_ordered_type_error_precedes_mandatory_prospective() {
    let (spec, _) = binding();
    let wrong = StructType::new(vec![
        NestedField::optional(1001, "n", Type::Primitive(PrimitiveType::Long)).into(),
        NestedField::optional(1000, "s", Type::Primitive(PrimitiveType::String)).into(),
    ]);
    let tuple: Struct = [None].into_iter().collect();
    let calls = Cell::new(0);
    let err = TypedPartition::bind_type_checked(
        &spec,
        &wrong,
        &tuple,
        |_, _| {
            calls.set(1);
            Err(RawCause(Arc::new(())))
        },
        || {
            calls.set(2);
            success()
        },
    )
    .unwrap_err();
    let PartitionBindFailure::Semantic(err) = err else {
        panic!("expected original type diagnostic")
    };
    assert_eq!(err.kind, Kind::InvalidPartition);
    assert_eq!(
        err.message,
        "partition storage type does not match its ordered spec fields"
    );
    assert_eq!(calls.get(), 0);
}
#[test]
fn original_arity_error_precedes_prospective_and_loop() {
    let (spec, ty) = binding();
    let tuple: Struct = [None].into_iter().collect();
    let calls = Cell::new(0);
    let err = TypedPartition::bind_type_checked(
        &spec,
        &ty,
        &tuple,
        |_, _| {
            calls.set(1);
            Err(RawCause(Arc::new(())))
        },
        || {
            calls.set(2);
            success()
        },
    )
    .unwrap_err();
    let PartitionBindFailure::Semantic(err) = err else {
        panic!("expected original arity diagnostic")
    };
    assert_eq!(err.kind, Kind::InvalidPartition);
    assert_eq!(
        err.message,
        "Iceberg partition spec or tuple arity is invalid"
    );
    assert_eq!(calls.get(), 0);
}
#[test]
fn prospective_refusal_preempts_invalid_value_and_retains_exact_cause() {
    let (spec, ty) = binding();
    let tuple: Struct = [Some(Literal::Struct(Struct::empty())), None]
        .into_iter()
        .collect();
    let cause = Arc::new(());
    let calls = Cell::new(0);
    let err = TypedPartition::bind_type_checked(
        &spec,
        &ty,
        &tuple,
        |_, _| Err(RawCause(cause.clone())),
        || {
            calls.set(1);
            success()
        },
    )
    .unwrap_err();
    assert_eq!(calls.get(), 0);
    assert_eq!(
        format!("{err:?}/{err}"),
        "TypedPartition::Original/TypedPartition::Original"
    );
    assert!(Arc::ptr_eq(
        &cause,
        &err.source().unwrap().downcast_ref::<RawCause>().unwrap().0
    ));
    let ordinary = TypedPartition::bind_type(&spec, &ty, &tuple).unwrap_err();
    assert_eq!(ordinary.kind, Kind::InvalidPartition);
    assert_eq!(ordinary.message, "Iceberg partition value is not primitive");
}
#[test]
fn actual_middle_loop_cancel_preserves_raw_object_before_next_semantic_error() {
    let (spec, ty) = binding();
    let tuple: Struct = [
        Some(Literal::string("actual first canonical allocation")),
        Some(Literal::Struct(Struct::empty())),
    ]
    .into_iter()
    .collect();
    let cause = Arc::new(());
    let calls = Cell::new(0);
    let prospective = Cell::new(0);
    let err = TypedPartition::bind_type_checked(
        &spec,
        &ty,
        &tuple,
        |_, _| {
            prospective.set(1);
            success()
        },
        || {
            let n = calls.get() + 1;
            calls.set(n);
            if n == 3 {
                Err(RawCause(cause.clone()))
            } else {
                success()
            }
        },
    )
    .unwrap_err();
    assert_eq!(prospective.get(), 1);
    assert_eq!(calls.get(), 3);
    assert!(matches!(err, PartitionBindFailure::Original(_)));
    assert!(Arc::ptr_eq(
        &cause,
        &err.source().unwrap().downcast_ref::<RawCause>().unwrap().0
    ));
}
#[test]
fn same_stop_token_checked_at_last_cell_then_before_publish() {
    let (spec, ty) = binding();
    let tuple: Struct = [None, None].into_iter().collect();
    let stop = AtomicBool::new(false);
    let cause = Arc::new(());
    let calls = Cell::new(0);
    let err = TypedPartition::bind_type_checked(
        &spec,
        &ty,
        &tuple,
        |_, _| success(),
        || {
            let n = calls.get() + 1;
            calls.set(n);
            if stop.load(Ordering::SeqCst) {
                return Err(RawCause(cause.clone()));
            }
            // The same owner marks stop after the first cell's active observation (following the preallocation check).
            if n == 2 {
                stop.store(true, Ordering::SeqCst)
            }
            success()
        },
    )
    .unwrap_err();
    assert_eq!(calls.get(), 3);
    assert!(Arc::ptr_eq(
        &cause,
        &err.source().unwrap().downcast_ref::<RawCause>().unwrap().0
    ));
}
#[test]
fn failure_after_arc_conversion_still_refuses_publication() {
    let (spec, ty) = binding();
    let tuple: Struct = [Some(Literal::string("last alias")), None]
        .into_iter()
        .collect();
    let calls = Cell::new(0);
    let cause = Arc::new(());
    let err = TypedPartition::bind_type_checked(
        &spec,
        &ty,
        &tuple,
        |_, _| success(),
        || {
            let n = calls.get() + 1;
            calls.set(n);
            if n == 5 {
                Err(RawCause(cause.clone()))
            } else {
                success()
            }
        },
    )
    .unwrap_err();
    assert_eq!(calls.get(), 5);
    assert!(Arc::ptr_eq(
        &cause,
        &err.source().unwrap().downcast_ref::<RawCause>().unwrap().0
    ));
}

#[test]
fn expired_original_absolute_deadline_is_not_renewed_or_entered_into_loop() {
    let (spec, ty) = binding();
    let tuple: Struct = [None, None].into_iter().collect();
    let original_until = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(1))
        .unwrap();
    let cause = Arc::new(());
    let loop_calls = Cell::new(0);
    let err = TypedPartition::bind_type_checked(
        &spec,
        &ty,
        &tuple,
        |_, _| {
            if std::time::Instant::now() >= original_until {
                Err(RawCause(cause.clone()))
            } else {
                success()
            }
        },
        || {
            loop_calls.set(loop_calls.get() + 1);
            success()
        },
    )
    .unwrap_err();
    assert_eq!(loop_calls.get(), 0);
    assert!(Arc::ptr_eq(
        &cause,
        &err.source().unwrap().downcast_ref::<RawCause>().unwrap().0
    ));
}
