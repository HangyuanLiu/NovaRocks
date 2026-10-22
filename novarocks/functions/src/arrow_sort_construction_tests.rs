// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
use super::*;
use arrow_array::{ArrayRef, Int64Array, StringViewArray};
use arrow_ord::sort::lexsort_to_indices;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::sync::Arc;
#[derive(Debug, Eq, PartialEq)]
struct Refusal(u32);
fn columns(rows: usize, count: usize) -> Vec<SortColumn> {
    (0..count)
        .map(|ordinal| SortColumn {
            values: Arc::new(Int64Array::from(
                (0..rows)
                    .map(|row| i64::try_from(rows - row + ordinal).unwrap())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            options: None,
        })
        .collect()
}
fn observe(columns: &[SortColumn], limit: Option<usize>) -> (LexsortStructureFacts, usize, usize) {
    let (mut temporary_bytes, mut retained_bytes) = (0, 0);
    let facts = {
        let mut temporary_request = |_: Layout| Ok::<_, Refusal>(());
        let mut retained_request = |_: Layout| Ok::<_, Refusal>(());
        let mut temporary_opaque = |bytes| {
            temporary_bytes += bytes;
            Ok::<_, Refusal>(())
        };
        let mut retained_opaque = |bytes| {
            retained_bytes += bytes;
            Ok::<_, Refusal>(())
        };
        // Pure model probes record the two genuine observation channels. They
        // claim no BE funding, owner, capacity or physical allocation receipt.
        let mut temporary = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut temporary_request,
            &mut temporary_opaque,
        );
        let mut retained = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut retained_request,
            &mut retained_opaque,
        );
        preflight_lexsort_structure_construction(
            columns,
            limit,
            &mut temporary,
            &mut retained,
            &mut |_| Ok(()),
        )
        .unwrap()
    };
    (facts, temporary_bytes, retained_bytes)
}
#[test]
fn lexsort_structures_follow_actual_single_fixed_and_dynamic_branches_without_row_fees() {
    let (single, temporary, retained) = observe(&columns(17, 1), None);
    assert_eq!(single.comparator_nodes, 0);
    assert_eq!(
        temporary, 0,
        "all-valid scalar single-key path builds no comparator or NULL scratch"
    );
    assert!(
        retained > 0,
        "the actual UInt32 output/header including its one Arc is observed"
    );
    let (fixed, fixed_bytes, fixed_output) = observe(&columns(17, 2), Some(3));
    assert_eq!(fixed.comparator_nodes, 2);
    assert_eq!(
        fixed.comparator_vectors, 1,
        "FixedComparator still constructs its original Vec first"
    );
    assert!(fixed_bytes > 0);
    let (wide, wide_bytes, wide_output) = observe(&columns(17, 6), None);
    assert_eq!(wide.comparator_nodes, 6);
    assert_eq!(wide.comparator_vectors, 1);
    assert!(wide_bytes > fixed_bytes);
    let (_, other_rows, other_output) = observe(&columns(257, 6), Some(1));
    assert_eq!(
        wide_bytes, other_rows,
        "row indices/tuple/rank stock does not enter structure-only fees"
    );
    assert_eq!(retained, fixed_output);
    assert_eq!(retained, wide_output);
    assert_eq!(retained, other_output);
}
#[test]
fn lexsort_structure_preflight_preserves_original_arrow_indices() {
    let columns = vec![
        SortColumn {
            values: Arc::new(Int64Array::from(vec![Some(2), Some(1), None, Some(1)])) as ArrayRef,
            options: None,
        },
        SortColumn {
            values: Arc::new(StringArray::from(vec!["a", "z", "n", "a"])) as ArrayRef,
            options: None,
        },
    ];
    let (_, temporary, retained) = observe(&columns, None);
    assert!(temporary > 0 && retained > 0);
    let output = lexsort_to_indices(&columns, None).unwrap();
    assert_eq!(output.values().as_ref(), &[2, 3, 1, 0]);
    let limited = lexsort_to_indices(&columns, Some(2)).unwrap();
    assert_eq!(limited.values().as_ref(), &[2, 3]);
}
#[test]
fn lexsort_structure_first_host_or_work_refusal_is_nominal_without_tail() {
    let columns = columns(9, 2);
    let mut temporary_calls = 0;
    let failure = {
        let mut temporary_request = |_: Layout| Ok::<_, Refusal>(());
        let mut retained_request = |_: Layout| -> Result<(), Refusal> {
            panic!("retained request after temporary refusal")
        };
        let mut temporary_opaque = |_: usize| {
            temporary_calls += 1;
            Err(Refusal(37))
        };
        let mut retained_opaque = |_: usize| -> Result<(), Refusal> {
            panic!("retained callback after temporary refusal")
        };
        let mut temporary = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut temporary_request,
            &mut temporary_opaque,
        );
        let mut retained = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut retained_request,
            &mut retained_opaque,
        );
        preflight_lexsort_structure_construction(
            &columns,
            None,
            &mut temporary,
            &mut retained,
            &mut |_| Ok(()),
        )
        .unwrap_err()
    };
    assert!(matches!(
        failure,
        CopyConstructionError::Creator(Refusal(37))
    ));
    assert_eq!(temporary_calls, 1);
    let mut any_request =
        |_: Layout| -> Result<(), Refusal> { panic!("request after original work refusal") };
    let mut any_opaque = |_: usize| -> Result<(), Refusal> {
        panic!("opaque observation after original work refusal")
    };
    let mut work_calls = 0;
    let mut temporary = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut any_request,
        &mut any_opaque,
    );
    let mut retained_request =
        |_: Layout| -> Result<(), Refusal> { panic!("request after original work refusal") };
    let mut retained_opaque = |_: usize| -> Result<(), Refusal> {
        panic!("opaque observation after original work refusal")
    };
    let mut retained = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut retained_request,
        &mut retained_opaque,
    );
    let failure = preflight_lexsort_structure_construction(
        &columns,
        None,
        &mut temporary,
        &mut retained,
        &mut |_| {
            work_calls += 1;
            Err(Refusal(91))
        },
    )
    .unwrap_err();
    assert!(matches!(
        failure,
        CopyConstructionError::Creator(Refusal(91))
    ));
    assert_eq!(work_calls, 1);
}
#[test]
fn lexsort_model_coverage_failure_does_not_claim_arrow_view_is_unsupported() {
    let columns = vec![SortColumn {
        values: Arc::new(StringViewArray::from(vec!["b", "a"])) as ArrayRef,
        options: None,
    }];
    assert_eq!(
        lexsort_to_indices(&columns, None)
            .unwrap()
            .values()
            .as_ref(),
        &[1, 0]
    );
    let mut request =
        |_: Layout| -> Result<(), Refusal> { panic!("request outside reviewed grammar") };
    let mut temporary_opaque =
        |_: usize| -> Result<(), Refusal> { panic!("temporary fee outside reviewed grammar") };
    let mut retained_request =
        |_: Layout| -> Result<(), Refusal> { panic!("retained request outside reviewed grammar") };
    let mut retained_opaque =
        |_: usize| -> Result<(), Refusal> { panic!("retained fee outside reviewed grammar") };
    let mut temporary = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut temporary_opaque,
    );
    let mut retained = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut retained_request,
        &mut retained_opaque,
    );
    let failure = preflight_lexsort_structure_construction::<Refusal>(
        &columns,
        None,
        &mut temporary,
        &mut retained,
        &mut |_| Ok(()),
    )
    .unwrap_err();
    assert!(matches!(
        failure,
        CopyConstructionError::Resource(LayoutResourceError::SourceModel)
    ));
}
