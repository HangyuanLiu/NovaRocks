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
use std::cmp::Ordering;

use arrow_buffer::i256;

/// Compute 10^exp in i128 with overflow checking.
pub fn pow10_i128(exp: usize) -> Result<i128, DecimalMathFailure> {
    checked_pow10_i128(exp).ok_or(DecimalMathFailure::Power {
        exponent: exp,
        bits: 128,
    })
}

/// Original checked multiplication without an intermediate diagnostic allocation.
/// Legacy Option consumers and Result consumers share this single value author.
pub fn checked_pow10_i128(exp: usize) -> Option<i128> {
    let mut out: i128 = 1;
    for _ in 0..exp {
        out = out.checked_mul(10)?;
    }
    Some(out)
}

/// Compute 10^exp in i256 with overflow checking.
pub fn pow10_i256(exp: usize) -> Result<i256, DecimalMathFailure> {
    let mut out = i256::ONE;
    let ten = i256::from_i128(10);
    for _ in 0..exp {
        out = out.checked_mul(ten).ok_or(DecimalMathFailure::Power {
            exponent: exp,
            bits: 256,
        })?;
    }
    Ok(out)
}

/// Integer division with ROUND_HALF_UP, keeping consistent with StarRocks BE:
/// be/src/runtime/decimalv3.h DecimalV3Arithmetics<..., false>::div_round.
pub fn div_round_i128(dividend: i128, divisor: i128) -> Result<i128, DecimalMathFailure> {
    if divisor == 0 {
        return Err(DecimalMathFailure::DivideByZero);
    }
    let q = dividend
        .checked_div(divisor)
        .ok_or(DecimalMathFailure::Overflow)?;
    let r = dividend
        .checked_rem(divisor)
        .ok_or(DecimalMathFailure::Overflow)?;
    if r == 0 {
        return Ok(q);
    }
    let magnitude = divisor.unsigned_abs();
    let threshold = (magnitude >> 1) + (magnitude & 1);
    if r.unsigned_abs().cmp(&threshold) != Ordering::Less {
        let carry = if (dividend ^ divisor) < 0 { -1 } else { 1 };
        q.checked_add(carry).ok_or(DecimalMathFailure::Overflow)
    } else {
        Ok(q)
    }
}

/// Integer division with ROUND_HALF_UP for i256.
pub fn div_round_i256(dividend: i256, divisor: i256) -> Result<i256, DecimalMathFailure> {
    if divisor == i256::ZERO {
        return Err(DecimalMathFailure::DivideByZero);
    }
    let mut q = dividend
        .checked_div(divisor)
        .ok_or(DecimalMathFailure::Overflow)?;
    let r = dividend
        .checked_rem(divisor)
        .ok_or(DecimalMathFailure::Overflow)?;

    if r == i256::ZERO {
        return Ok(q);
    }

    // Half the divisor fits even when its full positive magnitude does not.
    let half = divisor / i256::from_i128(2);
    let abs_half = if half.is_negative() {
        half.checked_neg().ok_or(DecimalMathFailure::Overflow)?
    } else {
        half
    };
    let abs_r = if r.is_negative() {
        r.checked_neg().ok_or(DecimalMathFailure::Overflow)?
    } else {
        r
    };
    let threshold = abs_half
        .checked_add(divisor & i256::ONE)
        .ok_or(DecimalMathFailure::Overflow)?;

    if abs_r.cmp(&threshold) != Ordering::Less {
        let carry = if dividend.is_negative() ^ divisor.is_negative() {
            i256::MINUS_ONE
        } else {
            i256::ONE
        };
        q = q.checked_add(carry).ok_or(DecimalMathFailure::Overflow)?;
    }

    Ok(q)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// A value failure cannot be promoted through the outer-only kernel constructor.
/// ```compile_fail
/// fn invalid(cause: novarocks_functions::legacy_decimal::DecimalMathFailure) {
///     let _ = novarocks_functions::KernelFailure::from_domain(cause);
/// }
/// ```
pub enum DecimalMathFailure {
    Power { exponent: usize, bits: u16 },
    DivideByZero,
    Overflow,
}
impl DecimalMathFailure {
    pub fn data_reason(self) -> crate::DataFailureReason {
        match self {
            Self::DivideByZero => crate::DataFailureReason::InvalidValue,
            _ => crate::DataFailureReason::NumericOutOfRange,
        }
    }
}
impl std::fmt::Display for DecimalMathFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DivideByZero => f.write_str("decimal division by zero"),
            _ => f.write_str("decimal overflow"),
        }
    }
}
impl std::error::Error for DecimalMathFailure {}
impl novarocks_failure_contract::FailureProjection for DecimalMathFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.data_reason().failure_fact()
    }
}
#[cfg(test)]
mod failure_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    #[test]
    fn decimal_math_signed_extremes_round_without_absolute_value_overflow() {
        assert_eq!(div_round_i128(i128::MIN, i128::MIN).unwrap(), 1);
        assert_eq!(div_round_i128(i128::MAX, i128::MIN).unwrap(), -1);
        assert_eq!(div_round_i128(1, i128::MIN).unwrap(), 0);
        assert_eq!(
            div_round_i128(i128::MIN, -1),
            Err(DecimalMathFailure::Overflow)
        );
        assert_eq!(div_round_i128(1, 0), Err(DecimalMathFailure::DivideByZero));
        assert_eq!(div_round_i256(i256::MIN, i256::MIN).unwrap(), i256::ONE);
        assert_eq!(
            div_round_i256(i256::MAX, i256::MIN).unwrap(),
            i256::MINUS_ONE
        );
        assert_eq!(div_round_i256(i256::ONE, i256::MIN).unwrap(), i256::ZERO);
        assert_eq!(
            div_round_i256(i256::MIN, i256::MINUS_ONE),
            Err(DecimalMathFailure::Overflow)
        );
        assert_eq!(
            div_round_i256(i256::ONE, i256::ZERO),
            Err(DecimalMathFailure::DivideByZero)
        );
        assert_eq!(
            DecimalMathFailure::Overflow.failure_fact(),
            crate::DataFailureReason::NumericOutOfRange.failure_fact()
        );
    }
}
