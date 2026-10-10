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
//! Original interleaved child demand and byte-normalization error witnesses.
use super::legacy_aes_family_original_baseline_tests::{setup, strings};
use super::{eval_aes_decrypt, eval_aes_encrypt};
use crate::exec::expr::function::FunctionKind;
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use std::sync::Arc;
fn call(
    encrypt: bool,
    arena: &mut ExprArena,
    args: &[ExprId],
    chunk: &crate::exec::chunk::Chunk,
) -> String {
    let name = if encrypt {
        "aes_encrypt"
    } else {
        "aes_decrypt"
    };
    let expr = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Encryption(name),
            args: args.to_vec(),
        },
        DataType::Utf8,
    );
    if encrypt {
        eval_aes_encrypt(arena, expr, args, chunk)
    } else {
        eval_aes_decrypt(arena, expr, args, chunk)
    }
    .unwrap_err()
}
#[test]
fn legacy_aes_original_wrong_arity_demands_no_child() {
    for encrypt in [true, false] {
        let (mut arena, _args, chunk) = setup(vec![strings(vec![Some("a")])]);
        for count in [1, 3, 6] {
            let invalid = vec![ExprId(usize::MAX); count];
            let name = if encrypt {
                "aes_encrypt"
            } else {
                "aes_decrypt"
            };
            assert_eq!(
                call(encrypt, &mut arena, &invalid, &chunk),
                format!("{name} expects 2, 4, or 5 arguments")
            );
        }
    }
}
#[test]
fn legacy_aes_original_normalization_failure_suppresses_later_child_error() {
    for encrypt in [true, false] {
        let bad: ArrayRef = Arc::new(Int64Array::from(vec![Some(7)]));
        let (mut arena, args, chunk) = setup(vec![bad]);
        let name = if encrypt {
            "aes_encrypt"
        } else {
            "aes_decrypt"
        };
        assert_eq!(
            call(encrypt, &mut arena, &[args[0], ExprId(usize::MAX)], &chunk),
            format!("{name}: arg0 must be VARCHAR or VARBINARY")
        );
        let bad: ArrayRef = Arc::new(Int64Array::from(vec![Some(7)]));
        let (mut arena, args, chunk) = setup(vec![strings(vec![None]), bad]);
        assert_eq!(
            call(
                encrypt,
                &mut arena,
                &[args[0], args[1], ExprId(usize::MAX), ExprId(usize::MAX)],
                &chunk
            ),
            format!("{name}: arg1 must be VARCHAR or VARBINARY")
        );
    }
}
#[test]
fn legacy_aes_original_unsupported_stage_still_evaluates_its_child() {
    for encrypt in [true, false] {
        let (mut arena, args, chunk) = setup(vec![strings(vec![None])]);
        assert_eq!(
            call(encrypt, &mut arena, &[args[0], ExprId(usize::MAX)], &chunk),
            "invalid ExprId"
        );
        assert_eq!(
            call(encrypt, &mut arena, &[ExprId(usize::MAX), args[0]], &chunk),
            "invalid ExprId"
        );
    }
}
