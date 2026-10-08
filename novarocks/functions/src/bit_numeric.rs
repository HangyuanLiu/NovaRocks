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

//! Shared row arithmetic for legacy shells and exact selected bit owners.
//!
//! Callers own Arrow conversion, NULL propagation, Selection and output casts.
//! A narrow signed source is widened to i64 before shifting; safe narrowing
//! belongs to the result conversion, rather than the wrapping arithmetic.

/// Frozen by the exact owner, independently of a runtime function name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShiftOp {
    Left,
    Right,
    RightLogical,
}
impl ShiftOp {
    pub const fn apply_i64(self, value: i64, count: i64) -> i64 {
        // The original signed-width profiles first widen to BIGINT. The count
        // conversion and wrapping operation mask modulo 64, even for Int8.
        let count = count as u32;
        match self {
            Self::Left => value.wrapping_shl(count),
            Self::Right => value.wrapping_shr(count),
            Self::RightLogical => (value as u64).wrapping_shr(count) as i64,
        }
    }

    pub const fn apply_i128(self, value: i128, count: i64) -> i128 {
        let count = count as u32;
        match self {
            Self::Left => value.wrapping_shl(count),
            Self::Right => value.wrapping_shr(count),
            Self::RightLogical => (value as u128).wrapping_shr(count) as i128,
        }
    }
}

/// Frozen by the exact owner, never selected from runtime names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BitwiseOp {
    And,
    Or,
    Xor,
    Not,
}
impl BitwiseOp {
    pub const fn arity(self) -> usize {
        match self {
            Self::Not => 1,
            _ => 2,
        }
    }
    pub const fn apply_i64(self, left: i64, right: i64) -> i64 {
        match self {
            Self::And => left & right,
            Self::Or => left | right,
            Self::Xor => left ^ right,
            Self::Not => !left,
        }
    }
    pub const fn apply_i128(self, left: i128, right: i128) -> i128 {
        match self {
            Self::And => left & right,
            Self::Or => left | right,
            Self::Xor => left ^ right,
            Self::Not => !left,
        }
    }
}
