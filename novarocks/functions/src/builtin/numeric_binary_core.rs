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
//! ONE original binary f64 calculation and NULL loop, independent of arena.
use crate::{KernelFailure, math_numeric::NumericArrayView};
/// Frozen by the original binding owner; evaluation never looks up names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NumericBinaryOp {
    Atan2,
    Fmod,
    Pow,
}
impl NumericBinaryOp {
    fn apply(self, a: f64, b: f64) -> f64 {
        match self {
            Self::Atan2 => a.atan2(b),
            Self::Fmod => a % b,
            Self::Pow => a.powf(b),
        }
    }
}
/// Preserve original raw inputs through the formula, THEN filter the result.
/// NaN^0, atan2(Inf,Inf), finite%Inf and signed zero are not input errors.
/// Exact caller addresses and output emit describe the existing carrier contract.
pub fn evaluate_binary_rows(
    op: NumericBinaryOp,
    left_view: &NumericArrayView<'_>,
    right_view: &NumericArrayView<'_>,
    rows: impl IntoIterator<Item = usize>,
    mut address: impl FnMut(usize, usize) -> Result<(usize, usize), KernelFailure>,
    mut emit: impl FnMut(Option<f64>) -> Result<(), KernelFailure>,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    for (ordinal, row) in rows.into_iter().enumerate() {
        observe()?;
        let (left_row, right_row) = address(ordinal, row)?;
        let l = left_view.value_f64(left_row);
        let r = right_view.value_f64(right_row);
        emit(match (l, r) {
            (Some(a), Some(b)) => {
                let value = op.apply(a, b);
                value.is_finite().then_some(value)
            }
            _ => None,
        })?;
    }
    Ok(())
}
