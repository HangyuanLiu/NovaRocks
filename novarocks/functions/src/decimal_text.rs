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

//! Original decimal text computation shared by cast, hash and aggregate callers.
//! Decimal128 magnitudes include the full signed range without negation overflow.

use arrow_buffer::i256;

pub fn format_decimal_with_scale(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let scale = scale as usize;
    let abs = unscaled.unsigned_abs().to_string();
    if abs.len() <= scale {
        let frac = format!("{:0>width$}", abs, width = scale);
        if unscaled < 0 {
            format!("-0.{}", frac)
        } else {
            format!("0.{}", frac)
        }
    } else {
        let split = abs.len() - scale;
        let int_part = &abs[..split];
        let frac_part = &abs[split..];
        if unscaled < 0 {
            format!("-{}.{}", int_part, frac_part)
        } else {
            format!("{}.{}", int_part, frac_part)
        }
    }
}

pub fn format_decimal256_with_scale(unscaled: i256, scale: i8) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let scale = scale as usize;
    let negative = unscaled.is_negative();
    let abs = if negative {
        unscaled.checked_neg().unwrap_or(unscaled)
    } else {
        unscaled
    };
    let abs_str = abs.to_string();
    if abs_str.len() <= scale {
        let frac = format!("{:0>width$}", abs_str, width = scale);
        if negative {
            format!("-0.{}", frac)
        } else {
            format!("0.{}", frac)
        }
    } else {
        let split = abs_str.len() - scale;
        let int_part = &abs_str[..split];
        let frac_part = &abs_str[split..];
        if negative {
            format!("-{}.{}", int_part, frac_part)
        } else {
            format!("{}.{}", int_part, frac_part)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::format_decimal_with_scale;
    #[test]
    fn signed_minimum_formats_at_the_real_scale_without_negation_overflow() {
        assert_eq!(
            format_decimal_with_scale(i128::MIN, 2),
            "-1701411834604692317316873037158841057.28"
        );
        assert_eq!(
            format_decimal_with_scale(i128::MIN, 0),
            "-170141183460469231731687303715884105728"
        );
        assert_eq!(
            format_decimal_with_scale(i128::MIN, 38),
            "-1.70141183460469231731687303715884105728"
        );
        assert_eq!(format_decimal_with_scale(-3, 2), "-0.03");
    }
}
