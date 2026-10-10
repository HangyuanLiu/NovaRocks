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

#[test]
fn request_bound_uses_original_safe_source_and_real_containers_without_new_admission() {
    let fixture = cow_rewrite_query_fixture(
        vec![7],
        vec![2],
        Arc::new(arrow::array::StringArray::from(vec!["value"])) as ArrayRef,
        DataType::Utf8,
    );
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let context = connector_context_for_test();
    let deadline = context.deadline();
    let selection = collect_admitted_cow_selection(&fixture, &binding, context.clone());
    let source = selection.owned_source_backing_upper().unwrap().unwrap() as u64;
    let containers = selection.retained_container_bytes().unwrap() as u64;
    let held = capacity.snapshot().held_positions;
    let receipt = cow_fe_begin_receipt::before_request(
        &fixture.preparation,
        &selection,
        "db1",
        "t",
        &binding,
        &context,
    )
    .unwrap();
    let actual_total = receipt.through_request_peak_upper().unwrap();
    let source_is_not_visible_bytes = source > selection.byte_count();
    let existing_covers_backing_and_headers = receipt.existing_upper > source + containers;
    let request_has_its_own_bound = actual_total > receipt.existing_upper;
    let held_unchanged = capacity.snapshot().held_positions == held;
    drop(selection);
    drop(fixture);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let exited = capacity.snapshot().held_positions;
    drop(control);
    assert!(source_is_not_visible_bytes);
    assert!(existing_covers_backing_and_headers);
    assert!(request_has_its_own_bound);
    assert!(held_unchanged);
    assert_eq!(context.deadline(), deadline);
    assert_eq!(exited, [0; 4]);
}

#[test]
fn legacy_selection_without_safe_receipts_never_enters_request_construction() {
    let fixture = cow_rewrite_query_fixture(
        vec![7],
        vec![2],
        Arc::new(arrow::array::StringArray::from(vec!["value"])) as ArrayRef,
        DataType::Utf8,
    );
    assert!(!fixture.selection.has_owned_sources());
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let entered = std::cell::Cell::new(false);
    let result = cow_fe_begin_receipt::before_request(
        &fixture.preparation,
        &fixture.selection,
        "db1",
        "t",
        &binding,
        &connector_context_for_test(),
    )
    .map(|_| entered.set(true));
    let rejected = matches!(result, Err(cow_necessary_before_begin::Error::Control(ref error))
        if error.kind() == spi::ConnectorErrorKind::InvalidRequest);
    drop(result);
    drop(fixture);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let exited = capacity.snapshot().held_positions;
    drop(control);
    assert!(rejected);
    assert!(!entered.get());
    assert_eq!(exited, [0; 4]);
}

#[test]
fn original_control_failure_precedes_missing_source_receipts_at_request_boundary() {
    let fixture = cow_rewrite_query_fixture(
        vec![7],
        vec![2],
        Arc::new(arrow::array::StringArray::from(vec!["value"])) as ArrayRef,
        DataType::Utf8,
    );
    let (control, root, binding, capacity) =
        crate::query_execution::internal_result_cpu::admitted_internal_fixture();
    let stop = spi::ConnectorStopOwner::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let context =
        spi::ConnectorRequestContext::try_new(deadline, stop.view(), 64 * 1024, 1024 * 1024)
            .unwrap();
    stop.request_stop();
    let result = cow_fe_begin_receipt::before_request(
        &fixture.preparation,
        &fixture.selection,
        "db1",
        "t",
        &binding,
        &context,
    );
    let first_control = matches!(&result, Err(cow_necessary_before_begin::Error::Control(error))
        if error.kind() == spi::ConnectorErrorKind::Cancelled);
    drop(result);
    drop(fixture);
    drop(binding);
    root.owner.complete();
    root.business.release();
    let exited = capacity.snapshot().held_positions;
    drop(control);
    assert!(first_control);
    assert_eq!(context.deadline(), deadline);
    assert_eq!(exited, [0; 4]);
}
