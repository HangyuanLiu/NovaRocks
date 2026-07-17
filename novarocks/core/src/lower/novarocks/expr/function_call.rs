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

//! Function call expression lowering.

use arrow::datatypes::DataType;

use super::{collection, lower_expr_list};
use crate::exec::expr::function::{FunctionKind, function_metadata, lookup_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode};
use crate::proto::expr;

use super::super::layout::Layout;

pub(crate) fn lower_function_call(
    call: &expr::FunctionCall,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    if call.distinct {
        return Err(format!(
            "native scalar expr lowering does not support DISTINCT FunctionCall '{}'",
            call.function_name
        ));
    }
    if call.function_name == "__array_literal" {
        return collection::lower_array_literal(call, arena, input_layout, data_type);
    }
    if call.function_name.eq_ignore_ascii_case("map") {
        return collection::lower_map_constructor(call, arena, input_layout, data_type);
    }
    let kind = lookup_function(&call.function_name).ok_or_else(|| {
        format!(
            "unsupported native scalar function '{}'",
            call.function_name
        )
    })?;
    let args = lower_expr_list(&call.args, arena, input_layout)?;
    validate_function_arity(&call.function_name, kind, args.len())?;
    Ok(arena.push_typed(ExprNode::FunctionCall { kind, args }, data_type))
}

pub(super) fn validate_function_arity(
    name: &str,
    kind: FunctionKind,
    arg_count: usize,
) -> Result<(), String> {
    let metadata = function_metadata(kind);
    if arg_count < metadata.min_args || arg_count > metadata.max_args {
        return Err(format!(
            "function '{}' expects {} to {} arguments, got {}",
            name, metadata.min_args, metadata.max_args, arg_count
        ));
    }
    Ok(())
}
