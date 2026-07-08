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

//! Saturating arithmetic for row-count estimation.
//!
//! Row counts are `f64` but must never reach the magnitudes that the EXPLAIN
//! renderer would saturate into `i64::MAX`. Every product/sum/quotient that
//! feeds a row count goes through these helpers, which clamp to
//! [`MAX_ROW_COUNT`] and report whether the cap was hit (so callers can
//! downgrade confidence to `Fallback`).

/// Upper bound for any estimated row count. Far below `i64::MAX / 2` and any
/// realistic table size, so it both prevents f64 overflow in downstream
/// products and renders cleanly instead of `9223372036854775807`.
pub const MAX_ROW_COUNT: f64 = 1e15;

/// `a * b`, clamped to `[0, MAX_ROW_COUNT]`. Returns `(value, saturated)`.
/// Non-finite results saturate (never propagate NaN/inf).
pub fn sat_mul(a: f64, b: f64) -> (f64, bool) {
    clamp_row_count(a * b)
}

/// `a + b`, clamped to `[0, MAX_ROW_COUNT]`.
pub fn sat_add(a: f64, b: f64) -> (f64, bool) {
    clamp_row_count(a + b)
}

/// `a / b`. Guards `b <= 0` (returns the numerator + saturated=true rather
/// than NaN/inf).
pub fn sat_div(a: f64, b: f64) -> (f64, bool) {
    if b <= 0.0 {
        let (v, _) = clamp_row_count(a);
        return (v, true);
    }
    clamp_row_count(a / b)
}

fn clamp_row_count(v: f64) -> (f64, bool) {
    if v.is_nan() {
        (0.0, true)
    } else if v >= MAX_ROW_COUNT || v.is_infinite() {
        (MAX_ROW_COUNT, true)
    } else if v < 0.0 {
        (0.0, true)
    } else {
        (v, false)
    }
}

/// Combine independent selectivities with exponential backoff so a conjunction
/// never collapses toward zero. Sorts ascending (strongest/smallest first),
/// then `s1 * s2^(1/2) * s3^(1/4) * ...`. Empty slice -> 1.0 (no reduction).
pub fn damped_conjunction(selectivities: &[f64]) -> f64 {
    let mut s: Vec<f64> = selectivities
        .iter()
        .copied()
        .filter(|v| v.is_finite())
        .map(|v| v.clamp(0.0, 1.0))
        .collect();
    if s.is_empty() {
        return 1.0;
    }
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mut combined = 1.0f64;
    let mut exp = 1.0f64;
    for sel in s {
        combined *= sel.powf(exp);
        exp *= 0.5;
    }
    combined
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sat_mul_caps_and_flags() {
        let (v, sat) = sat_mul(1e10, 1e10); // 1e20 > MAX_ROW_COUNT
        assert_eq!(v, MAX_ROW_COUNT);
        assert!(sat);
        let (v2, sat2) = sat_mul(1000.0, 1000.0);
        assert_eq!(v2, 1_000_000.0);
        assert!(!sat2);
        // infinity input saturates, never NaN
        let (v3, sat3) = sat_mul(f64::INFINITY, 2.0);
        assert_eq!(v3, MAX_ROW_COUNT);
        assert!(sat3);
    }

    #[test]
    fn sat_div_guards_zero() {
        let (v, sat) = sat_div(100.0, 0.0);
        assert_eq!(v, 100.0); // numerator returned unchanged
        assert!(sat);
        let (inf, inf_sat) = sat_div(f64::INFINITY, 0.0);
        assert_eq!(inf, MAX_ROW_COUNT);
        assert!(inf_sat);
        let (nan, nan_sat) = sat_div(f64::NAN, 0.0);
        assert_eq!(nan, 0.0);
        assert!(nan_sat);
        let (negative, negative_sat) = sat_div(-1.0, 0.0);
        assert_eq!(negative, 0.0);
        assert!(negative_sat);
        let (v2, sat2) = sat_div(100.0, 4.0);
        assert_eq!(v2, 25.0);
        assert!(!sat2);
    }

    #[test]
    fn damped_conjunction_never_collapses() {
        // 5 predicates at 0.25: naive product = 0.25^5 ~= 0.000977
        let naive = 0.25_f64.powi(5);
        let damped = damped_conjunction(&[0.25, 0.25, 0.25, 0.25, 0.25]);
        assert!(
            damped > naive * 10.0,
            "damped {damped} should be >> naive {naive}"
        );
        assert!(
            damped <= 0.25,
            "damped must not exceed the strongest selectivity"
        );
        assert!((damped_conjunction(&[0.3]) - 0.3).abs() < 1e-9);
        assert_eq!(damped_conjunction(&[]), 1.0);
    }
}
