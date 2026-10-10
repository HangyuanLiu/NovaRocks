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
//! Preserve original MIN overflow and Decimal256 sign behavior.

use arrow_buffer::i256;

pub fn format_decimal_with_scale(unscaled: i128, scale: i8) -> String {
    if scale <= 0 {
        return unscaled.to_string();
    }
    let scale = scale as usize;
    let abs = unscaled.abs().to_string();
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
