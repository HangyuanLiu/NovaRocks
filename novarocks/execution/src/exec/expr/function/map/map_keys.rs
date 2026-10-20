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
use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprId;
use arrow::array::{Array, ArrayRef, MapArray};
use novarocks_functions::{
    Selection,
    builtin::map_projection_core::{MapPart, ProjectionFailure, project_observed},
};
pub fn eval_map_keys<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> Result<ArrayRef, crate::runtime::fragment::ExecutionFailure> {
    let map_arr = arena.eval(args[0], chunk)?;
    let map = map_arr.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
        crate::exec::expr::expression_contract(format_args!(
            "map_keys expects MapArray, got {:?}",
            map_arr.data_type()
        ))
    })?;
    let field = super::common::output_list_field(
        arena.data_type(expr),
        map.keys().data_type(),
        "map_keys",
    )?;
    let out = project_observed(
        map,
        MapPart::Keys,
        field,
        Selection::all(map.len()),
        |_, row| Ok::<_, crate::runtime::fragment::ExecutionFailure>(row),
        None,
        |_, _, _| Ok(()),
        || map.nulls().cloned(),
        |_, _| Ok(()),
        &mut |_| Ok(()),
    )
    .map_err(|e| match e {
        ProjectionFailure::Control(cause) => cause,
        ProjectionFailure::Domain(cause) => {
            crate::runtime::fragment::ExecutionFailure::from_domain(cause)
        }
    })?;
    super::common::cast_output(out, arena.data_type(expr), "map_keys")
}
