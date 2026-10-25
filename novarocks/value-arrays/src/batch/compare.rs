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

//! Equality over prepared logical byte rows. Result positions are selection
//! ordinals or pair ordinals, never implicit physical bottom coordinates.
use super::{BYTE_BLOCK, PreparedBytes, RowWork, prepared::Mapping, with_mapping};
use crate::{ValueError, ValueFailure, ValueViolation, hash_eq::NullEquality};

/// Compare the same logical rows in two prepared selections. Both selections
/// must contain exactly the same logical positions. Every successful output
/// slot is a SQL truth value: None is UNKNOWN, not an unselected position.
/// On error the caller must discard the entire output of this operation.
pub fn compare_into<E, W>(
    left: &PreparedBytes<'_>,
    right: &PreparedBytes<'_>,
    nulls: NullEquality,
    output: &mut [Option<bool>],
    work: &mut W,
) -> Result<(), ValueFailure<E>>
where
    W: FnMut(usize) -> Result<(), E>,
{
    if left.len() != right.len()
        || left.selection().len() != right.selection().len()
        || output.len() != left.len()
    {
        return Err(ValueError::Invariant(ValueViolation::Bounds).into());
    }
    validate_domains(left, right)?;
    work(0).map_err(ValueFailure::Caller)?;
    with_mapping!(left, |lm| {
        with_mapping!(right, |rm| {
            let mut cursor = left.selection().cursor();
            compare_rows(
                CompareInputs {
                    left,
                    right,
                    positional: true,
                    nulls,
                },
                &lm,
                &rm,
                |control: &mut RowWork<'_, W>| {
                    cursor
                        .next_observed(&mut |n| control.observe_metadata(n))
                        .map(|row| row.map(|row| (row, row)))
                },
                output,
                work,
            )
        })
    })
}

/// Compare ordered logical-row pairs. Repeated rows and pairs remain separate
/// occurrences. Every row must belong to its respective prepared selection.
/// The caller owns output and must discard it if this operation fails.
pub fn compare_pairs_into<E, W>(
    left: &PreparedBytes<'_>,
    left_rows: &[usize],
    right: &PreparedBytes<'_>,
    right_rows: &[usize],
    nulls: NullEquality,
    output: &mut [Option<bool>],
    work: &mut W,
) -> Result<(), ValueFailure<E>>
where
    W: FnMut(usize) -> Result<(), E>,
{
    if left_rows.len() != right_rows.len() || output.len() != left_rows.len() {
        return Err(ValueError::Invariant(ValueViolation::Bounds).into());
    }
    validate_domains(left, right)?;
    work(0).map_err(ValueFailure::Caller)?;
    with_mapping!(left, |lm| {
        with_mapping!(right, |rm| {
            let mut pairs = left_rows.iter().copied().zip(right_rows.iter().copied());
            compare_rows(
                CompareInputs {
                    left,
                    right,
                    positional: false,
                    nulls,
                },
                &lm,
                &rm,
                |_: &mut RowWork<'_, W>| Ok(pairs.next()),
                output,
                work,
            )
        })
    })
}

fn validate_domains(left: &PreparedBytes<'_>, right: &PreparedBytes<'_>) -> Result<(), ValueError> {
    if left.is_utf8() != right.is_utf8() {
        return Err(ValueError::Invariant(ValueViolation::Layout));
    }
    Ok(())
}

struct CompareInputs<'p, 'a> {
    left: &'p PreparedBytes<'a>,
    right: &'p PreparedBytes<'a>,
    positional: bool,
    nulls: NullEquality,
}

fn compare_rows<E, W, L, R, N>(
    inputs: CompareInputs<'_, '_>,
    left_mapping: &L,
    right_mapping: &R,
    mut next: N,
    output: &mut [Option<bool>],
    work: &mut W,
) -> Result<(), ValueFailure<E>>
where
    W: FnMut(usize) -> Result<(), E>,
    L: Mapping,
    R: Mapping,
    N: FnMut(&mut RowWork<'_, W>) -> Result<Option<(usize, usize)>, ValueFailure<E>>,
{
    let CompareInputs {
        left,
        right,
        positional,
        nulls,
    } = inputs;
    let left_view = left.view();
    let right_view = right.view();
    let mut control = RowWork::new(work);
    for slot in output.iter_mut() {
        let (left_row, right_row) =
            next(&mut control)?.ok_or(ValueError::Invariant(ValueViolation::Bounds))?;
        control.row()?;
        if positional && left_row != right_row {
            return Err(ValueError::Invariant(ValueViolation::UndefinedRow).into());
        }
        left.check_row(left_row)?;
        right.check_row(right_row)?;
        let li = left_mapping.index(left_row);
        let ri = right_mapping.index(right_row);
        let a = li.map(|i| left_view.value(i)).transpose()?.flatten();
        let b = ri.map(|i| right_view.value(i)).transpose()?.flatten();
        *slot = match (a, b) {
            (None, None) => match nulls {
                NullEquality::Equal => Some(true),
                NullEquality::Unequal => Some(false),
                NullEquality::Unknown => None,
            },
            (None, Some(_)) | (Some(_), None) => {
                if nulls == NullEquality::Unknown {
                    None
                } else {
                    Some(false)
                }
            }
            (Some(a), Some(b)) => {
                if a.len() != b.len()
                    || (a.len() > 12
                        && left_view.prefix(li.expect("non-NULL mapped row"))?
                            != right_view.prefix(ri.expect("non-NULL mapped row"))?)
                {
                    Some(false)
                } else {
                    let mut equal = true;
                    for (a, b) in a.chunks(BYTE_BLOCK).zip(b.chunks(BYTE_BLOCK)) {
                        control.bytes(a.len())?;
                        if a != b {
                            equal = false;
                            break;
                        }
                    }
                    Some(equal)
                }
            }
        };
    }
    control.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ByteLayout,
        access::ByteSource,
        decode::{DecodedBytes, ValueConversionContext},
        reachability::{ReachableRows, SelectionSet},
    };
    use arrow_array::{
        ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, Int32Array, StringArray,
        StringViewArray, types::Int32Type,
    };
    use std::{convert::Infallible, sync::Arc};

    fn decoded(a: &ArrayRef, layout: ByteLayout) -> DecodedBytes {
        ValueConversionContext::default()
            .decode(
                &ByteSource::parse(a, layout, None).unwrap(),
                &ReachableRows::all(a.len()),
            )
            .unwrap()
    }
    fn rows(len: usize, indices: &[usize]) -> SelectionSet {
        let mut rows = SelectionSet::try_new(len).unwrap();
        for &row in indices {
            rows.insert(row).unwrap();
        }
        rows
    }
    fn all(len: usize) -> SelectionSet {
        rows(len, &(0..len).collect::<Vec<_>>())
    }

    #[test]
    fn null_policies_have_independent_three_value_results() {
        let left: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            None,
            Some("x"),
            Some("x"),
            Some("x"),
        ]));
        let right: ArrayRef = Arc::new(StringViewArray::from(vec![
            None,
            Some("x"),
            None,
            Some("x"),
            Some("y"),
        ]));
        let left = decoded(&left, ByteLayout::Utf8);
        let right = decoded(&right, ByteLayout::Utf8View);
        let selected = all(5);
        let lp = PreparedBytes::new(&left, &selected).unwrap();
        let rp = PreparedBytes::new(&right, &selected).unwrap();
        for (policy, expected) in [
            (
                NullEquality::Unknown,
                [None, None, None, Some(true), Some(false)],
            ),
            (
                NullEquality::Equal,
                [
                    Some(true),
                    Some(false),
                    Some(false),
                    Some(true),
                    Some(false),
                ],
            ),
            (
                NullEquality::Unequal,
                [
                    Some(false),
                    Some(false),
                    Some(false),
                    Some(true),
                    Some(false),
                ],
            ),
        ] {
            let mut result = [None; 5];
            compare_into(&lp, &rp, policy, &mut result, &mut |_| {
                Ok::<_, Infallible>(())
            })
            .unwrap();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn paired_dictionary_rows_keep_repetitions_and_original_order() {
        let bottom: ArrayRef = Arc::new(StringArray::from(vec![Some("alpha"), None, Some("beta")]));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(2), Some(0), None, Some(1)]),
                bottom,
            )
            .unwrap(),
        );
        let right: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("alpha"),
            Some("beta"),
            None,
        ]));
        let left = decoded(&dictionary.slice(1, 3), ByteLayout::Utf8);
        let right = decoded(&right, ByteLayout::Utf8View);
        let selected = all(3);
        let lp = PreparedBytes::new(&left, &selected).unwrap();
        let rp = PreparedBytes::new(&right, &selected).unwrap();
        let mut result = [Some(false); 5];
        compare_pairs_into(
            &lp,
            &[2, 0, 0, 1, 0],
            &rp,
            &[2, 0, 0, 0, 1],
            NullEquality::Equal,
            &mut result,
            &mut |_| Ok::<_, Infallible>(()),
        )
        .unwrap();
        assert_eq!(
            result,
            [Some(true), Some(true), Some(true), Some(false), Some(false)]
        );
    }

    #[test]
    fn sparse_selection_and_pair_domain_are_checked() {
        let input: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("a"),
            Some("unselected"),
            Some("b"),
        ]));
        let input = decoded(&input, ByteLayout::Utf8View);
        let selection = rows(3, &[0, 2]);
        let prepared = PreparedBytes::new(&input, &selection).unwrap();
        let mut result = [None; 2];
        compare_into(
            &prepared,
            &prepared,
            NullEquality::Unknown,
            &mut result,
            &mut |_| Ok::<_, Infallible>(()),
        )
        .unwrap();
        assert_eq!(result, [Some(true), Some(true)]);
        assert!(matches!(
            compare_pairs_into(
                &prepared,
                &[1],
                &prepared,
                &[0],
                NullEquality::Unknown,
                &mut [None],
                &mut |_| Ok::<_, Infallible>(())
            ),
            Err(ValueFailure::Value(ValueError::Invariant(
                ValueViolation::UndefinedRow
            )))
        ));
    }

    #[test]
    fn external_prefix_does_not_replace_full_equality() {
        let left: ArrayRef = Arc::new(StringViewArray::from(vec![
            "same prefix but different a",
            "inline",
        ]));
        let right: ArrayRef = Arc::new(StringArray::from(vec![
            "same prefix but different b",
            "inline",
        ]));
        let left = decoded(&left, ByteLayout::Utf8View);
        let right = decoded(&right, ByteLayout::Utf8);
        let rows = all(2);
        let mut output = [None; 2];
        compare_into(
            &PreparedBytes::new(&left, &rows).unwrap(),
            &PreparedBytes::new(&right, &rows).unwrap(),
            NullEquality::Unknown,
            &mut output,
            &mut |_| Ok::<_, Infallible>(()),
        )
        .unwrap();
        assert_eq!(output, [Some(false), Some(true)]);
    }

    #[test]
    fn long_value_control_preserves_the_original_failure() {
        let text = "x".repeat(BYTE_BLOCK * 3 + 1);
        let input: ArrayRef = Arc::new(StringViewArray::from(vec![text.as_str()]));
        let input = decoded(&input, ByteLayout::Utf8View);
        let rows = all(1);
        let prepared = PreparedBytes::new(&input, &rows).unwrap();
        let mut blocks = 0;
        let error = compare_into(
            &prepared,
            &prepared,
            NullEquality::Equal,
            &mut [None],
            &mut |n| {
                if n >= BYTE_BLOCK {
                    blocks += 1;
                }
                if blocks == 2 {
                    Err("original cancellation")
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ValueFailure::Caller("original cancellation")
        ));
        assert_eq!(blocks, 2);
    }

    #[test]
    fn all_null_rows_are_controlled_by_blocks_not_row_callbacks() {
        let input: ArrayRef = Arc::new(StringViewArray::from(vec![
            None::<&str>;
            super::super::ROW_BLOCK * 2 + 3
        ]));
        let input = decoded(&input, ByteLayout::Utf8View);
        let rows = all(input.len());
        let prepared = PreparedBytes::new(&input, &rows).unwrap();
        let mut output = vec![Some(true); prepared.len()];
        let mut calls = 0;
        compare_into(
            &prepared,
            &prepared,
            NullEquality::Unknown,
            &mut output,
            &mut |_| {
                calls += 1;
                Ok::<_, Infallible>(())
            },
        )
        .unwrap();
        assert!(output.iter().all(Option::is_none));
        // Metadata traversal adds its own bounded observations. Protect the
        // batch property without freezing an exact implementation call count.
        assert!(calls < prepared.len() / 2);
    }

    #[test]
    fn constant_mapping_and_empty_selection_preserve_positions() {
        let value: ArrayRef = Arc::new(StringArray::from(vec![Some("x")]));
        let source = ByteSource::constant(&value, ByteLayout::Utf8, 3).unwrap();
        let constant = ValueConversionContext::default()
            .decode(&source, &ReachableRows::all(3))
            .unwrap();
        let right: ArrayRef = Arc::new(StringViewArray::from(vec![Some("x"), Some("y"), None]));
        let right = decoded(&right, ByteLayout::Utf8View);
        let rows = all(3);
        let mut output = [None; 3];
        compare_into(
            &PreparedBytes::new(&constant, &rows).unwrap(),
            &PreparedBytes::new(&right, &rows).unwrap(),
            NullEquality::Unknown,
            &mut output,
            &mut |_| Ok::<_, Infallible>(()),
        )
        .unwrap();
        assert_eq!(output, [Some(true), Some(false), None]);
        let rows = SelectionSet::try_new(3).unwrap();
        compare_into(
            &PreparedBytes::new(&constant, &rows).unwrap(),
            &PreparedBytes::new(&right, &rows).unwrap(),
            NullEquality::Equal,
            &mut [],
            &mut |_| Ok::<_, Infallible>(()),
        )
        .unwrap();
    }

    #[test]
    fn binary_offsets_and_view_compare_non_utf8_values() {
        let bytes: &[u8] = &[0xff, 0, 0xfe];
        let long: &[u8] = b"same prefix and a long binary value";
        let values = vec![Some(bytes), None, Some(b"".as_slice()), Some(long)];
        let left: ArrayRef = Arc::new(BinaryArray::from(values.clone()));
        let right: ArrayRef = Arc::new(BinaryViewArray::from(values));
        let left = decoded(&left, ByteLayout::Binary);
        let right = decoded(&right, ByteLayout::BinaryView);
        let rows = all(4);
        let mut output = [None; 4];
        compare_into(
            &PreparedBytes::new(&left, &rows).unwrap(),
            &PreparedBytes::new(&right, &rows).unwrap(),
            NullEquality::Equal,
            &mut output,
            &mut |_| Ok::<_, Infallible>(()),
        )
        .unwrap();
        assert_eq!(output, [Some(true); 4]);
    }

    #[test]
    fn sparse_logical_selection_metadata_is_cancellable_before_the_first_pair() {
        let len = 64 * super::super::ROW_BLOCK * 3;
        let value: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
        let source = ByteSource::constant(&value, ByteLayout::Utf8, len).unwrap();
        let decoded = ValueConversionContext::default()
            .decode(&source, &ReachableRows::indices(len, &[len - 1]))
            .unwrap();
        let selection = rows(len, &[len - 1]);
        let prepared = PreparedBytes::new(&decoded, &selection).unwrap();
        let mut output = [Some(false)];
        let failure = compare_into(
            &prepared,
            &prepared,
            NullEquality::Equal,
            &mut output,
            &mut |units| {
                if units > 0 {
                    Err("metadata stop")
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(
            failure,
            Err(ValueFailure::Caller("metadata stop"))
        ));
        assert_eq!(output, [Some(false)]);
        compare_into(
            &prepared,
            &prepared,
            NullEquality::Equal,
            &mut output,
            &mut |_| Ok::<_, Infallible>(()),
        )
        .unwrap();
        assert_eq!(output, [Some(true)]);
    }

    #[test]
    fn empty_large_selection_does_not_scan_metadata_again() {
        let len = 64 * super::super::ROW_BLOCK * 3;
        let value: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
        let source = ByteSource::constant(&value, ByteLayout::Utf8, len).unwrap();
        let decoded = ValueConversionContext::default()
            .decode(&source, &ReachableRows::indices(len, &[]))
            .unwrap();
        let selection = SelectionSet::try_new(len).unwrap();
        let prepared = PreparedBytes::new(&decoded, &selection).unwrap();
        let mut checks = Vec::new();
        compare_into(
            &prepared,
            &prepared,
            NullEquality::Equal,
            &mut [],
            &mut |units| {
                checks.push(units);
                Ok::<_, Infallible>(())
            },
        )
        .unwrap();
        assert_eq!(checks, [0]);
    }

    #[test]
    fn equal_selection_counts_do_not_allow_different_logical_members() {
        let array: ArrayRef = Arc::new(StringViewArray::from(vec!["x", "x", "x"]));
        let decoded = decoded(&array, ByteLayout::Utf8View);
        let left = rows(3, &[0, 2]);
        let right = rows(3, &[0, 1]);
        let failure = compare_into(
            &PreparedBytes::new(&decoded, &left).unwrap(),
            &PreparedBytes::new(&decoded, &right).unwrap(),
            NullEquality::Equal,
            &mut [None; 2],
            &mut |_| Ok::<_, Infallible>(()),
        );
        assert!(matches!(
            failure,
            Err(ValueFailure::Value(ValueError::Invariant(
                ValueViolation::UndefinedRow
            )))
        ));
    }

    #[test]
    fn string_and_binary_domains_do_not_compare_implicitly() {
        let string: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![b"x".as_slice()]));
        let string = decoded(&string, ByteLayout::Utf8);
        let binary = decoded(&binary, ByteLayout::Binary);
        let rows = all(1);
        assert!(matches!(
            compare_into(
                &PreparedBytes::new(&string, &rows).unwrap(),
                &PreparedBytes::new(&binary, &rows).unwrap(),
                NullEquality::Equal,
                &mut [None],
                &mut |_| Ok::<_, Infallible>(())
            ),
            Err(ValueFailure::Value(ValueError::Invariant(
                ValueViolation::Layout
            )))
        ));
    }
}
