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

//! Immutable differential baselines for the two remaining calendar leaf families.
use super::{LegacyConstantForm, ScalarDiffSpec, assert_scalar_matches_v1};
use arrow::array::{
    ArrayRef, Date32Array, StringArray, TimestampMicrosecondArray, new_empty_array, new_null_array,
};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;

fn sources() -> Vec<ArrayRef> {
    vec![
        Arc::new(Date32Array::from(vec![
            Some(-719_528),
            Some(-1),
            Some(0),
            Some(11_016),
            Some(11_017),
            Some(2_932_896),
            None,
        ])),
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(-62_135_596_800_000_000),
            Some(-1),
            Some(0),
            Some(951_782_400_000_000),
            Some(951_868_799_999_999),
            Some(253_402_300_799_999_999),
            None,
        ])),
        Arc::new(StringArray::from(vec![
            Some("0001-01-01"),
            Some("1969-12-31"),
            Some("1970-01-01"),
            Some("2000-02-29"),
            Some("2000-03-01 23:59:59.999999"),
            Some("9999-12-31"),
            None,
            Some("invalid"),
            Some(""),
        ])),
    ]
}

#[test]
fn pure_differential_calendar_day_number_dedup_all_declared_profiles_before_after() {
    for array in sources() {
        let data_type = array.data_type().clone();
        for input in [
            array.clone(),
            array.slice(1, array.len() - 1),
            new_null_array(&data_type, 3),
            new_empty_array(&data_type),
        ] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("to_days")
                        .column(input.clone())
                        .decimal_overflow(policy)
                        .sparse_selections(9, 917),
                );
            }
        }
        let no_nulls = array.slice(0, 6);
        for nullable in [false, true] {
            assert_scalar_matches_v1(
                ScalarDiffSpec::new("to_days")
                    .typed_column(
                        FunctionValueType::new(data_type.clone(), nullable),
                        no_nulls.clone(),
                    )
                    .sparse_selections(5, 918),
            );
        }
        for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
            for ordinal in [0, 3, 6] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new("to_days")
                        .constant_array(array.slice(ordinal, 1))
                        .legacy_constants(form)
                        .sparse_selections(5, 919),
                );
            }
        }
    }
}

#[test]
fn pure_differential_calendar_diff_dedup_both_aliases_all_declared_profiles_before_after() {
    for name in ["datediff", "days_diff"] {
        for array in sources() {
            let data_type = array.data_type().clone();
            for input in [
                array.clone(),
                array.slice(1, array.len() - 1),
                new_null_array(&data_type, 3),
                new_empty_array(&data_type),
            ] {
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    assert_scalar_matches_v1(
                        ScalarDiffSpec::new(name)
                            .column(input.clone())
                            .column(input.clone())
                            .decimal_overflow(policy)
                            .sparse_selections(9, 923),
                    );
                    assert_scalar_matches_v1(
                        ScalarDiffSpec::new(name)
                            .column(input.clone())
                            .constant_array(array.slice(3, 1))
                            .decimal_overflow(policy)
                            .sparse_selections(7, 924),
                    );
                }
            }
            let no_nulls = array.slice(0, 6);
            for nullable in [false, true] {
                assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .typed_column(
                            FunctionValueType::new(data_type.clone(), nullable),
                            no_nulls.clone(),
                        )
                        .typed_column(
                            FunctionValueType::new(data_type.clone(), nullable),
                            no_nulls.clone(),
                        )
                        .sparse_selections(5, 925),
                );
            }
            for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
                for ordinal in [0, 3, 6] {
                    assert_scalar_matches_v1(
                        ScalarDiffSpec::new(name)
                            .constant_array(array.slice(ordinal, 1))
                            .constant_array(array.slice(3, 1))
                            .legacy_constants(form)
                            .sparse_selections(5, 926),
                    );
                }
            }
        }
    }
}
