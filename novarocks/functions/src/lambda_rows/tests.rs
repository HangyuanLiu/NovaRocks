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
use std::{sync::Mutex, time::Duration};

#[derive(Default)]
struct Control {
    fail: Option<ScalarKernelFailure>,
    positive_only: bool,
    work: Mutex<Vec<u32>>,
}
impl ScalarEvaluationControl for Control {
    fn checkpoint(&self, work: u32) -> Result<(), ScalarKernelFailure> {
        self.work.lock().unwrap().push(work);
        if (!self.positive_only || work != 0)
            && let Some(failure) = &self.fail
        {
            return Err(failure.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), ScalarKernelFailure> {
        panic!("row metadata must not wait or invoke a body");
    }
}

#[test]
fn sparse_outer_rows_and_narrowed_elements_keep_original_parent_identity() {
    let outer_rows = [2, 7, 20];
    let outer = Selection::try_sparse(21, &outer_rows).unwrap();
    // Parent 1 contributes no elements. Repeated parent ordinals represent
    // distinct logical elements, not cached evaluations of a physical value.
    let parents = [0, 0, 2, 2, 2];
    let map = LambdaElementRowMap::try_new(outer, &parents, &Control::default()).unwrap();
    assert_eq!(map.outer_selection(), outer);
    assert_eq!(map.element_rows(), 5);
    assert_eq!(map.all_elements(), Selection::all(5));
    assert_eq!(
        (0..5)
            .map(|row| map.parent_row(row).unwrap())
            .collect::<Vec<_>>(),
        [2, 2, 20, 20, 20]
    );
    assert_eq!(map.parent_ordinal(2), Some(2));
    assert_eq!(map.parent_row(5), None);
    let selected_rows = [1, 4];
    let selected = Selection::try_sparse(5, &selected_rows).unwrap();
    assert_eq!(map.selected_parent_row(selected, 0).unwrap(), Some(2));
    assert_eq!(map.selected_parent_row(selected, 1).unwrap(), Some(20));
    assert_eq!(map.selected_parent_row(selected, 2).unwrap(), None);
    assert!(matches!(
        map.validate_selection(Selection::all(21)),
        Err(ScalarKernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn parent_metadata_is_selection_relative_and_never_repaired_or_reordered() {
    let rows = [2, 7];
    let outer = Selection::try_sparse(8, &rows).unwrap();
    for parents in [
        &[2][..],
        &[7][..],
        &[usize::MAX][..],
        &[1, 0][..],
        &[0, 1, 0][..],
    ] {
        assert!(matches!(
            LambdaElementRowMap::try_new(outer, parents, &Control::default()),
            Err(ScalarKernelFailure::InvalidProgram(_))
        ));
    }
    // Original batch row 2 is selected, but the metadata takes ordinal 0.
    let map = LambdaElementRowMap::try_new(outer, &[0, 1], &Control::default()).unwrap();
    assert_eq!(map.parent_row(0), Some(2));
    assert_eq!(map.parent_row(1), Some(7));
}

#[test]
fn empty_collections_and_empty_outer_selection_have_no_element_invocations() {
    for outer in [Selection::all(0), Selection::all(3)] {
        let control = Control::default();
        let map = LambdaElementRowMap::try_new(outer, &[], &control).unwrap();
        assert_eq!(map.element_rows(), 0);
        assert!(map.all_elements().is_empty());
        assert_eq!(
            map.selected_parent_row(map.all_elements(), 0).unwrap(),
            None
        );
        assert_eq!(*control.work.lock().unwrap(), [0]);
    }
    assert!(matches!(
        LambdaElementRowMap::try_new(Selection::all(0), &[0], &Control::default()),
        Err(ScalarKernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn mapping_work_and_outer_failures_are_observed_without_a_row_error_channel() {
    let parents = vec![0; MAX_UNOBSERVED_SCALAR_WORK as usize * 2 + 1];
    let control = Control::default();
    LambdaElementRowMap::try_new(Selection::all(1), &parents, &control).unwrap();
    assert_eq!(
        *control.work.lock().unwrap(),
        [0, MAX_UNOBSERVED_SCALAR_WORK, MAX_UNOBSERVED_SCALAR_WORK, 1]
    );
    for failure in [
        ScalarKernelFailure::Cancelled,
        ScalarKernelFailure::DeadlineExceeded,
        ScalarKernelFailure::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let control = Control {
                fail: Some(failure.clone()),
                positive_only,
                ..Default::default()
            };
            assert_eq!(
                LambdaElementRowMap::try_new(Selection::all(1), &parents, &control).unwrap_err(),
                failure
            );
            assert_eq!(
                *control.work.lock().unwrap(),
                if positive_only {
                    vec![0, MAX_UNOBSERVED_SCALAR_WORK]
                } else {
                    vec![0]
                }
            );
        }
    }
}
