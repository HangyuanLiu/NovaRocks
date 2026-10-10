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

//! Complete additional legal carrier families absent from the frozen original matrix.
//! No production profile is narrowed and no original expectation is changed.
use super::*;
use arrow::datatypes::{UnionFields, UnionMode};
#[test]
fn approx_percentile_differential_all_arities_remaining_legal_arrow_carriers() {
    let fields = UnionFields::new(
        [0i8, 1],
        [
            Field::new("authored-i64", DataType::Int64, true),
            Field::new("authored-text", DataType::Utf8, true)
                .with_metadata([("opaque".into(), "source".into())].into()),
        ],
    );
    let mut carriers = vec![
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::ListView(Arc::new(Field::new(
            "view-element",
            DataType::Float64,
            true,
        ))),
        DataType::LargeListView(Arc::new(Field::new(
            "large-view-element",
            DataType::Utf8,
            true,
        ))),
        DataType::Union(fields.clone(), UnionMode::Dense),
        DataType::Union(fields, UnionMode::Sparse),
    ];
    for ends in [DataType::Int16, DataType::Int32, DataType::Int64] {
        carriers.push(DataType::RunEndEncoded(
            Arc::new(Field::new("run-ends", ends, false)),
            Arc::new(Field::new("run-values", DataType::Utf8, true)),
        ));
    }
    for key in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ] {
        carriers.push(DataType::Dictionary(
            Box::new(key),
            Box::new(DataType::Utf8),
        ));
    }
    for (name, weighted, arities) in [
        ("percentile_approx", false, [2, 3]),
        ("percentile_approx_weighted", true, [3, 4]),
    ] {
        for arity in arities {
            for dtype in &carriers {
                for role in 0..arity {
                    for rows in [0usize, 3] {
                        let mut args = base(arity, weighted, rows);
                        args[role] = (
                            FunctionValueType::new(dtype.clone(), true),
                            new_null_array(dtype, rows),
                        );
                        check(name, args, rows, false);
                    }
                }
            }
        }
    }
}
