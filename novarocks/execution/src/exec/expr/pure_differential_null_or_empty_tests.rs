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

//! Every stable original NULL_OR_EMPTY carrier is checked against raw v1.
use super::*;
use arrow::array::{Int32Builder, ListArray, ListBuilder, NullArray, StringArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Field;
fn list_rows() -> ArrayRef {
    let mut b = ListBuilder::new(Int32Builder::new());
    b.append(false);
    b.append(true);
    b.values().append_null();
    b.append(true);
    b.values().append_value(7);
    b.append(true);
    Arc::new(b.finish())
}
#[test]
fn null_or_empty_differential_null_utf8_and_list_exact_dynamic_profiles() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(NullArray::new(4)),
        Arc::new(StringArray::from(vec![
            None,
            Some(""),
            Some("é\0"),
            Some(" "),
        ])),
        list_rows(),
    ];
    for a in arrays {
        for sliced in [false, true] {
            let a = if sliced { a.slice(1, 3) } else { a.clone() };
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("null_or_empty")
                    .column(a)
                    .sparse_selections(12, 184),
            );
        }
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("null_or_empty")
            .typed_column(
                json,
                Arc::new(StringArray::from(vec![
                    None,
                    Some(""),
                    Some("null"),
                    Some("[]"),
                ])),
            )
            .sparse_selections(12, 185),
    );
}
#[test]
fn null_or_empty_differential_nested_list_and_nominal_child_fields() {
    let mut b = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
    b.append(true);
    b.values().values().append_value(1);
    b.values().append(true);
    b.append(true);
    b.append(false);
    b.values().values().append_null();
    b.values().append(true);
    b.append(true);
    let nested = Arc::new(b.finish()) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("null_or_empty")
            .column(nested)
            .sparse_selections(12, 186),
    );
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let field = Arc::new(json.try_to_field("j").unwrap());
    let a = Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(vec![0i32, 0, 1, 2, 2].into()),
        Arc::new(StringArray::from(vec![None, Some("[]")])),
        Some(NullBuffer::from(vec![true, true, true, false])),
    )) as ArrayRef;
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("null_or_empty")
            .column(a)
            .sparse_selections(12, 187),
    );
}
#[test]
fn null_or_empty_differential_literal_pool_constants_and_empty_batches() {
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for rows in [0, 7] {
            for value in [None, Some(""), Some("中\0")] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("null_or_empty")
                        .constant_array(Arc::new(StringArray::from(vec![value])))
                        .constant_rows(rows)
                        .legacy_constants(form),
                );
            }
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("null_or_empty")
                    .constant_array(Arc::new(NullArray::new(1)))
                    .constant_rows(rows)
                    .legacy_constants(form),
            );
        }
    }
    for rows in [0, 7] {
        for row in 0..4 {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("null_or_empty")
                    .constant_array(list_rows().slice(row, 1))
                    .constant_rows(rows)
                    .legacy_constants(LegacyConstantForm::Pool),
            );
        }
    }
}
