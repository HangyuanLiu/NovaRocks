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
use crate::exec::expr::{ExprId, expression_contract};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::{Array, ArrayRef, StringArray, StructArray, UInt32Array};
use arrow::compute::cast;
use arrow::compute::take;

pub fn eval_subfield<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.len() != 2 {
        return Err(expression_contract(
            "subfield requires exactly two arguments",
        ));
    }
    let input = arena.eval(args[0], chunk)?;
    let field_name_arr = arena.eval(args[1], chunk)?;
    let struct_arr = input
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            expression_contract(format_args!(
                "subfield expects StructArray, got {:?}",
                input.data_type()
            ))
        })?;
    let field_name = parse_constant_field_name(field_name_arr.as_ref())?;

    let field_idx = struct_arr
        .fields()
        .iter()
        .position(|f| f.name() == field_name)
        .ok_or_else(|| {
            field_value(FieldValue::Missing(
                novarocks_failure_contract::BoundedDetail::truncating(field_name),
            ))
        })?;
    let field_col = struct_arr.column(field_idx);

    let mut indices = Vec::new();
    indices
        .try_reserve_exact(struct_arr.len())
        .map_err(|source| {
            ExecutionFailure::from(novarocks_functions::KernelFailure::from_domain(
                novarocks_functions::allocation_failure::AllocationRefusal::new(
                    "subfield indices",
                    source,
                ),
            ))
        })?;
    for row in 0..struct_arr.len() {
        if struct_arr.is_null(row) {
            indices.push(None);
        } else {
            let idx = crate::exec::expr::function::array::common::index_u32(row)?;
            indices.push(Some(idx));
        }
    }

    let indices = UInt32Array::from(indices);
    novarocks_functions::selected_copy::preflight_original_take(
        field_col.as_ref(),
        novarocks_functions::selected_copy::CopyIndexView::UInt32(&indices),
        |_| Ok(()),
    )
    .map_err(novarocks_functions::selected_copy::CopyError::into_kernel)?;
    let out = take(field_col.as_ref(), &indices, None).map_err(|source| {
        crate::exec::expr::function::arrow_operation::arrow_contract_failure(
            "subfield take",
            source,
        )
    })?;
    let Some(target_type) = arena.data_type(expr) else {
        return Ok(out);
    };
    if out.data_type() == target_type {
        return Ok(out);
    }
    cast(&out, target_type).map_err(crate::exec::expr::cast::cast_arrow_failure)
}

fn parse_constant_field_name(field_name_arr: &dyn Array) -> ExecutionResult<&str> {
    let arr = field_name_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| expression_contract("subfield field-name argument must be VARCHAR"))?;
    if arr.is_empty() {
        return Err(field_value(FieldValue::Empty));
    }
    let first = if arr.is_null(0) {
        return Err(field_value(FieldValue::Null));
    } else {
        arr.value(0)
    };
    for i in 1..arr.len() {
        if arr.is_null(i) || arr.value(i) != first {
            return Err(field_value(FieldValue::Variable));
        }
    }
    Ok(first)
}

#[derive(Debug)]
enum FieldValue {
    Empty,
    Null,
    Variable,
    Missing(novarocks_failure_contract::BoundedDetail),
}
impl std::fmt::Display for FieldValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("subfield field-name argument is empty"),
            Self::Null => f.write_str("subfield field-name argument must be non-null"),
            Self::Variable => f.write_str("subfield field-name argument must be constant"),
            Self::Missing(name) => write!(f, "subfield field '{}' does not exist", name),
        }
    }
}
impl std::error::Error for FieldValue {}
impl novarocks_failure_contract::FailureProjection for FieldValue {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Data,
            crate::runtime::fragment::ExecutionReason::StructFieldValue.reason(),
        )
    }
}
fn field_value(source: FieldValue) -> ExecutionFailure {
    ExecutionFailure::from_domain(source)
}
#[cfg(test)]
mod field_value_properties {
    use super::*;
    use arrow::array::Int32Array;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn field_name_value_and_physical_contract_have_distinct_facts_and_borrowed_name() {
        let names = StringArray::from(vec!["authored", "authored"]);
        let name = parse_constant_field_name(&names).unwrap();
        assert_eq!(name.as_ptr(), names.value(0).as_ptr());
        for names in [
            StringArray::from(Vec::<Option<&str>>::new()),
            StringArray::from(vec![None::<&str>]),
            StringArray::from(vec![Some("a"), Some("b")]),
        ] {
            assert_eq!(
                parse_constant_field_name(&names)
                    .unwrap_err()
                    .failure_fact()
                    .class(),
                FailureClass::Data
            );
        }
        assert_eq!(
            parse_constant_field_name(&Int32Array::from(vec![1]))
                .unwrap_err()
                .failure_fact()
                .class(),
            FailureClass::Invariant
        );
    }
}
