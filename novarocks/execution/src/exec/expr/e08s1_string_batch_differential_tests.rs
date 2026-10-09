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
//! Permanent original-dispatch comparisons for the four existing exact string profiles.
use super::pure_differential::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::{
    array::{ArrayRef, Int64Array, StringArray, new_empty_array, new_null_array},
    datatypes::DataType,
};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;

fn text(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn integers(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn exact(spec: ScalarDiffSpec) {
    let result = assert_scalar_matches_v1(
        spec.expect_result_type(FunctionValueType::new(DataType::Utf8, true))
            .sparse_selections(4, 0x5354_5249),
    );
    assert_eq!(result.legacy_batch_errors, 0);
    assert_eq!(result.attributed_row_errors, 0);
}
fn corpus_columns(name: &str, arguments: Vec<ArrayRef>) {
    for policy in [
        DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::OutputNull,
    ] {
        for layout in 0..4 {
            let mut spec = ScalarDiffSpec::new(name).decimal_overflow(policy);
            for array in &arguments {
                let column = match layout {
                    0 => Arc::clone(array),
                    1 => array.slice(1, array.len() - 2),
                    2 => new_null_array(array.data_type(), 5),
                    3 => new_empty_array(array.data_type()),
                    _ => unreachable!(),
                };
                spec = spec.column(column);
            }
            exact(spec);
        }
    }
}
fn wide(name: &str) {
    let count = 321;
    let strings: ArrayRef = Arc::new(StringArray::from_iter((0..count).map(|i| {
        if i % 17 == 0 {
            None
        } else {
            Some(["", "é中", "e\u{301}", "👩\u{200d}💻", "a\0b"][i % 5])
        }
    })));
    let nums: ArrayRef = Arc::new(Int64Array::from_iter((0..count).map(|i| {
        if i % 19 == 0 {
            None
        } else {
            Some([-1, 0, 1, 4, 7][i % 5])
        }
    })));
    let pads: ArrayRef = Arc::new(StringArray::from_iter((0..count).map(|i| {
        if i % 23 == 0 {
            None
        } else {
            Some(["", "x", "é中", "e\u{301}"][i % 4])
        }
    })));
    let spec = match name {
        "repeat" => ScalarDiffSpec::new(name).column(strings).column(nums),
        "space" => ScalarDiffSpec::new(name).column(nums),
        "lpad" | "rpad" => ScalarDiffSpec::new(name)
            .column(strings)
            .column(nums)
            .column(pads),
        _ => unreachable!("only this fixture's four declared shapes"),
    };
    exact(spec);
}
fn repeat() {
    corpus_columns(
        "repeat",
        vec![
            text(vec![
                Some("guard"),
                Some("é中"),
                Some(""),
                Some("a\0b"),
                Some("e\u{301}"),
                Some("x"),
                None,
                Some("👩\u{200d}💻"),
                Some("x"),
                Some("guard"),
            ]),
            integers(vec![
                Some(1),
                Some(-1),
                Some(i64::MAX),
                Some(0),
                Some(3),
                Some(1_048_577),
                Some(1),
                Some(2),
                Some(i64::MIN),
                None,
            ]),
        ],
    );
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for (s, n) in [
            (Some(""), Some(i64::MAX)),
            (Some("é中"), Some(3)),
            (Some("x"), Some(-1)),
            (None, Some(2)),
            (Some("x"), None),
            (Some("x"), Some(1_048_577)),
        ] {
            exact(
                ScalarDiffSpec::new("repeat")
                    .constant_array(text(vec![s]))
                    .constant_array(integers(vec![n]))
                    .legacy_constants(form),
            );
        }
    }
    exact(
        ScalarDiffSpec::new("repeat")
            .typed_column(
                FunctionValueType::new(DataType::Utf8, false),
                text(vec![Some(""), Some("é中"), Some("x")]),
            )
            .typed_column(
                FunctionValueType::new(DataType::Int64, false),
                integers(vec![Some(i64::MAX), Some(3), Some(-1)]),
            ),
    );
    wide("repeat");
}
fn space() {
    corpus_columns(
        "space",
        vec![integers(vec![
            Some(1),
            Some(i64::MIN),
            Some(-1),
            Some(0),
            Some(1),
            Some(7),
            None,
            Some(1_048_577),
            Some(i64::MAX),
            Some(1),
        ])],
    );
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for n in [
            None,
            Some(-1),
            Some(0),
            Some(4),
            Some(i64::MIN),
            Some(i64::MAX),
        ] {
            exact(
                ScalarDiffSpec::new("space")
                    .constant_array(integers(vec![n]))
                    .legacy_constants(form),
            );
        }
    }
    exact(ScalarDiffSpec::new("space").typed_column(
        FunctionValueType::new(DataType::Int64, false),
        integers(vec![Some(-1), Some(0), Some(4)]),
    ));
    wide("space");
}
fn pad(name: &str) {
    corpus_columns(
        name,
        vec![
            text(vec![
                Some("guard"),
                Some("é中"),
                Some("é中"),
                Some(""),
                Some("a\0b"),
                Some("e\u{301}"),
                None,
                Some("👩\u{200d}💻"),
                Some("x"),
                Some("guard"),
            ]),
            integers(vec![
                Some(1),
                Some(-1),
                Some(0),
                Some(3),
                Some(2),
                Some(7),
                Some(1),
                Some(9),
                Some(i64::MAX),
                Some(1),
            ]),
            text(vec![
                Some("g"),
                Some("x"),
                Some("é"),
                Some(""),
                Some("ab"),
                Some("中"),
                Some("x"),
                Some("é中"),
                Some("x"),
                None,
            ]),
        ],
    );
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        for (s, n, p) in [
            (Some("é中"), Some(7), Some("ab")),
            (Some("é中"), Some(1), Some("")),
            (Some(""), Some(3), Some("")),
            (Some("x"), Some(-1), Some("a")),
            (None, Some(3), Some("a")),
            (Some("x"), None, Some("a")),
            (Some("x"), Some(3), None),
            (Some("x"), Some(i64::MAX), Some("a")),
        ] {
            exact(
                ScalarDiffSpec::new(name)
                    .constant_array(text(vec![s]))
                    .constant_array(integers(vec![n]))
                    .constant_array(text(vec![p]))
                    .legacy_constants(form),
            );
        }
    }
    exact(
        ScalarDiffSpec::new(name)
            .typed_column(
                FunctionValueType::new(DataType::Utf8, false),
                text(vec![Some("é中"), Some(""), Some("x")]),
            )
            .typed_column(
                FunctionValueType::new(DataType::Int64, false),
                integers(vec![Some(1), Some(3), Some(-1)]),
            )
            .typed_column(
                FunctionValueType::new(DataType::Utf8, false),
                text(vec![Some(""), Some(""), Some("中")]),
            ),
    );
    wide(name);
}
#[test]
fn e08s1_string_batch_differential_repeat_complete_declared_shape() {
    repeat();
}
#[test]
fn e08s1_string_batch_differential_space_complete_declared_shape() {
    space();
}
#[test]
fn e08s1_string_batch_differential_lpad_complete_declared_shape() {
    pad("lpad");
}
#[test]
fn e08s1_string_batch_differential_rpad_complete_declared_shape() {
    pad("rpad");
}
