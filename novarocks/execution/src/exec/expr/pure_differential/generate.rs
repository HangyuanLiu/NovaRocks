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

//! Deterministic random Arrow inputs for pure-vs-legacy differential tests.
//!
//! The generator owns its own SplitMix64 stream, so a seed reproduces the same
//! arrays independently of the `rand` crate version. Every carrier mixes
//! uniformly drawn values with the domain boundaries a kernel is most likely
//! to get wrong (type extremes, signed zero, NaN, decimal precision limits,
//! empty and multibyte text, calendar limits). A carrier without an explicit
//! author is refused instead of being approximated by another type.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryBuilder, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryBuilder, LargeStringBuilder, StringBuilder, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow_buffer::i256;
use chrono::NaiveDate;
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

/// Text shapes for Utf8/LargeUtf8 columns. `Mixed` covers arbitrary text;
/// the other profiles exercise parsers that accept a narrower grammar.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TextProfile {
    /// ASCII, Latin-1 supplements, CJK, emoji, combining marks, NUL and
    /// control whitespace, plus empty and padded strings.
    Mixed,
    /// Printable ASCII only.
    Ascii,
    /// Optionally signed decimal numerals with surrounding blanks and junk.
    Numeric,
    /// `YYYY-MM-DD` texts, mostly valid, with invalid calendar dates.
    DateText,
    /// `YYYY-MM-DD HH:MM:SS[.ffffff]` texts, mostly valid.
    DateTimeText,
}

/// Distribution knobs shared by every carrier.
#[derive(Clone, Debug)]
pub(crate) struct InputProfile {
    /// Probability of a NULL in a nullable column. Non-nullable value types
    /// never receive NULL regardless of this ratio.
    pub null_ratio: f64,
    /// Probability of drawing a domain boundary instead of a uniform value.
    pub boundary_ratio: f64,
    pub text: TextProfile,
    /// Upper bound of characters (text) or bytes (binary) per value.
    pub max_text_len: usize,
}

impl Default for InputProfile {
    fn default() -> Self {
        Self {
            null_ratio: 0.15,
            boundary_ratio: 0.25,
            text: TextProfile::Mixed,
            max_text_len: 24,
        }
    }
}

impl InputProfile {
    pub(crate) fn with_null_ratio(mut self, ratio: f64) -> Self {
        self.null_ratio = ratio;
        self
    }

    pub(crate) fn with_boundary_ratio(mut self, ratio: f64) -> Self {
        self.boundary_ratio = ratio;
        self
    }

    pub(crate) fn with_text(mut self, text: TextProfile) -> Self {
        self.text = text;
        self
    }
}

/// SplitMix64: tiny, fully specified and stable across toolchains.
#[derive(Clone, Debug)]
pub(crate) struct SplitMix64(u64);

impl SplitMix64 {
    pub(crate) const fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..bound`; `bound == 0` yields 0.
    pub(crate) fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        // Rejection sampling keeps the draw exactly uniform.
        let zone = u64::MAX - (u64::MAX % bound);
        loop {
            let value = self.next_u64();
            if value < zone {
                return value % bound;
            }
        }
    }

    /// Uniform in the inclusive range `low..=high`.
    pub(crate) fn range_i64(&mut self, low: i64, high: i64) -> i64 {
        debug_assert!(low <= high);
        let span = (high as i128 - low as i128) as u128 + 1;
        if span > u64::MAX as u128 {
            return self.next_u64() as i64;
        }
        (low as i128 + self.below(span as u64) as i128) as i64
    }

    pub(crate) fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub(crate) fn chance(&mut self, probability: f64) -> bool {
        probability > 0.0 && self.unit() < probability
    }

    pub(crate) fn pick<'a, T>(&mut self, values: &'a [T]) -> &'a T {
        &values[self.below(values.len() as u64) as usize]
    }
}

/// Seeded generator of complete columns and row selections.
#[derive(Clone, Debug)]
pub(crate) struct InputGenerator {
    rng: SplitMix64,
}

impl InputGenerator {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            rng: SplitMix64::new(seed),
        }
    }

    pub(crate) fn rng(&mut self) -> &mut SplitMix64 {
        &mut self.rng
    }

    /// One column of `rows` values of exactly `value_type`. LARGEINT is the
    /// big-endian i128 `FixedSizeBinary(16)` carrier with its logical tag.
    pub(crate) fn column(
        &mut self,
        value_type: &FunctionValueType,
        rows: usize,
        profile: &InputProfile,
    ) -> ArrayRef {
        let null_ratio = if value_type.nullable {
            profile.null_ratio
        } else {
            0.0
        };
        let nulls = (0..rows)
            .map(|_| self.rng.chance(null_ratio))
            .collect::<Vec<_>>();
        let boundary = profile.boundary_ratio;
        macro_rules! primitive {
            ($array:ty, $draw:expr) => {{
                let values = nulls
                    .iter()
                    .map(|null| {
                        if *null {
                            None
                        } else {
                            #[allow(clippy::redundant_closure_call)]
                            Some($draw(&mut self.rng))
                        }
                    })
                    .collect::<Vec<_>>();
                Arc::new(<$array>::from(values)) as ArrayRef
            }};
        }
        match (&value_type.data_type, value_type.logical_type) {
            (DataType::Null, _) => new_null_array(&DataType::Null, rows),
            (DataType::Boolean, ValueLogicalType::Physical) => {
                primitive!(BooleanArray, |rng: &mut SplitMix64| rng.chance(0.5))
            }
            (DataType::Int8, ValueLogicalType::Physical) => {
                primitive!(Int8Array, |rng: &mut SplitMix64| signed(
                    rng,
                    boundary,
                    i8::MIN as i64,
                    i8::MAX as i64
                ) as i8)
            }
            (DataType::Int16, ValueLogicalType::Physical) => {
                primitive!(Int16Array, |rng: &mut SplitMix64| signed(
                    rng,
                    boundary,
                    i16::MIN as i64,
                    i16::MAX as i64
                ) as i16)
            }
            (DataType::Int32, ValueLogicalType::Physical) => {
                primitive!(Int32Array, |rng: &mut SplitMix64| signed(
                    rng,
                    boundary,
                    i32::MIN as i64,
                    i32::MAX as i64
                ) as i32)
            }
            (DataType::Int64, ValueLogicalType::Physical) => {
                primitive!(Int64Array, |rng: &mut SplitMix64| signed(
                    rng,
                    boundary,
                    i64::MIN,
                    i64::MAX
                ))
            }
            (DataType::UInt8, ValueLogicalType::Physical) => {
                primitive!(UInt8Array, |rng: &mut SplitMix64| unsigned(
                    rng,
                    boundary,
                    u8::MAX as u64
                ) as u8)
            }
            (DataType::UInt16, ValueLogicalType::Physical) => {
                primitive!(UInt16Array, |rng: &mut SplitMix64| unsigned(
                    rng,
                    boundary,
                    u16::MAX as u64
                ) as u16)
            }
            (DataType::UInt32, ValueLogicalType::Physical) => {
                primitive!(UInt32Array, |rng: &mut SplitMix64| unsigned(
                    rng,
                    boundary,
                    u32::MAX as u64
                ) as u32)
            }
            (DataType::UInt64, ValueLogicalType::Physical) => {
                primitive!(UInt64Array, |rng: &mut SplitMix64| unsigned(
                    rng,
                    boundary,
                    u64::MAX
                ))
            }
            (DataType::Float32, ValueLogicalType::Physical) => {
                primitive!(Float32Array, |rng: &mut SplitMix64| float32(rng, boundary))
            }
            (DataType::Float64, ValueLogicalType::Physical) => {
                primitive!(Float64Array, |rng: &mut SplitMix64| float64(rng, boundary))
            }
            (DataType::Decimal128(precision, scale), ValueLogicalType::Physical) => {
                let (precision, scale) = (*precision, *scale);
                let values = nulls
                    .iter()
                    .map(|null| {
                        (!*null).then(|| decimal128(&mut self.rng, boundary, precision, scale))
                    })
                    .collect::<Vec<_>>();
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(precision, scale)
                        .expect("generated DECIMAL128 type is valid"),
                )
            }
            (DataType::Decimal256(precision, scale), ValueLogicalType::Physical) => {
                let (precision, scale) = (*precision, *scale);
                let values = nulls
                    .iter()
                    .map(|null| {
                        (!*null).then(|| decimal256(&mut self.rng, boundary, precision, scale))
                    })
                    .collect::<Vec<_>>();
                Arc::new(
                    Decimal256Array::from(values)
                        .with_precision_and_scale(precision, scale)
                        .expect("generated DECIMAL256 type is valid"),
                )
            }
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt) => {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(rows, 16);
                for null in &nulls {
                    if *null {
                        builder.append_null();
                    } else {
                        let value = largeint(&mut self.rng, boundary);
                        builder
                            .append_value(value.to_be_bytes())
                            .expect("LARGEINT carrier width is 16");
                    }
                }
                Arc::new(builder.finish())
            }
            (DataType::Utf8, ValueLogicalType::Physical) => {
                let mut builder = StringBuilder::new();
                for null in &nulls {
                    if *null {
                        builder.append_null();
                    } else {
                        builder.append_value(text(&mut self.rng, boundary, profile));
                    }
                }
                Arc::new(builder.finish())
            }
            (DataType::LargeUtf8, ValueLogicalType::Physical) => {
                let mut builder = LargeStringBuilder::new();
                for null in &nulls {
                    if *null {
                        builder.append_null();
                    } else {
                        builder.append_value(text(&mut self.rng, boundary, profile));
                    }
                }
                Arc::new(builder.finish())
            }
            (DataType::Binary, ValueLogicalType::Physical) => {
                let mut builder = BinaryBuilder::new();
                for null in &nulls {
                    if *null {
                        builder.append_null();
                    } else {
                        builder.append_value(bytes(&mut self.rng, boundary, profile.max_text_len));
                    }
                }
                Arc::new(builder.finish())
            }
            (DataType::LargeBinary, ValueLogicalType::Physical) => {
                let mut builder = LargeBinaryBuilder::new();
                for null in &nulls {
                    if *null {
                        builder.append_null();
                    } else {
                        builder.append_value(bytes(&mut self.rng, boundary, profile.max_text_len));
                    }
                }
                Arc::new(builder.finish())
            }
            (DataType::Date32, ValueLogicalType::Physical) => {
                primitive!(Date32Array, |rng: &mut SplitMix64| date32(rng, boundary))
            }
            (DataType::Timestamp(unit, zone), ValueLogicalType::Physical) => {
                let micros = nulls
                    .iter()
                    .map(|null| (!*null).then(|| timestamp_micros(&mut self.rng, boundary)))
                    .collect::<Vec<_>>();
                let zone = zone.clone();
                match unit {
                    TimeUnit::Second => Arc::new(
                        TimestampSecondArray::from(
                            micros
                                .iter()
                                .map(|v| v.map(|v| v.div_euclid(1_000_000)))
                                .collect::<Vec<_>>(),
                        )
                        .with_timezone_opt(zone),
                    ),
                    TimeUnit::Millisecond => Arc::new(
                        TimestampMillisecondArray::from(
                            micros
                                .iter()
                                .map(|v| v.map(|v| v.div_euclid(1_000)))
                                .collect::<Vec<_>>(),
                        )
                        .with_timezone_opt(zone),
                    ),
                    TimeUnit::Microsecond => {
                        Arc::new(TimestampMicrosecondArray::from(micros).with_timezone_opt(zone))
                    }
                    // Nanoseconds only cover 1677..2262; clamp the draw into
                    // that representable window instead of overflowing.
                    TimeUnit::Nanosecond => Arc::new(
                        TimestampNanosecondArray::from(
                            micros
                                .iter()
                                .map(|v| {
                                    v.map(|v| v.clamp(i64::MIN / 1_000, i64::MAX / 1_000) * 1_000)
                                })
                                .collect::<Vec<_>>(),
                        )
                        .with_timezone_opt(zone),
                    ),
                }
            }
            (other, logical) => panic!(
                "differential input generator has no author for {other:?} ({logical:?}); \
                 build this column explicitly"
            ),
        }
    }

    /// An ordered, non-empty, strict subset of `0..rows` when `rows > 1`.
    /// Each row is kept with probability `keep_ratio`.
    pub(crate) fn selection(&mut self, rows: usize, keep_ratio: f64) -> Vec<usize> {
        let mut selected = (0..rows)
            .filter(|_| self.rng.chance(keep_ratio))
            .collect::<Vec<_>>();
        if rows > 1 && selected.len() == rows {
            let drop = self.rng.below(rows as u64) as usize;
            selected.remove(drop);
        }
        if selected.is_empty() && rows > 0 {
            selected.push(self.rng.below(rows as u64) as usize);
        }
        selected
    }

    /// Group ids in `0..groups` for `rows` rows; every group appears when
    /// `rows >= groups`.
    pub(crate) fn group_ids(&mut self, rows: usize, groups: usize) -> Vec<usize> {
        assert!(groups > 0, "at least one group is required");
        let mut ids = (0..rows)
            .map(|row| {
                if row < groups {
                    row
                } else {
                    self.rng.below(groups as u64) as usize
                }
            })
            .collect::<Vec<_>>();
        // Shuffle so the guaranteed appearances are not a prefix.
        for index in (1..ids.len()).rev() {
            let other = self.rng.below(index as u64 + 1) as usize;
            ids.swap(index, other);
        }
        ids
    }
}

fn signed(rng: &mut SplitMix64, boundary: f64, min: i64, max: i64) -> i64 {
    if rng.chance(boundary) {
        let candidates = [
            min,
            min.saturating_add(1),
            -1,
            0,
            1,
            max.saturating_sub(1),
            max,
        ];
        return *rng.pick(&candidates);
    }
    // Draw a random magnitude class first so small values are common.
    let bits = 1 + rng.below(64) as u32;
    let limit = if bits >= 63 {
        i64::MAX
    } else {
        (1i64 << bits) - 1
    };
    let value = rng.range_i64(-limit.min(max), limit.min(max));
    value.clamp(min, max)
}

fn unsigned(rng: &mut SplitMix64, boundary: f64, max: u64) -> u64 {
    if rng.chance(boundary) {
        return *rng.pick(&[0, 1, max - 1, max]);
    }
    let bits = 1 + rng.below(64) as u32;
    let limit = if bits >= 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    };
    rng.next_u64() & limit.min(max)
}

fn float64(rng: &mut SplitMix64, boundary: f64) -> f64 {
    if rng.chance(boundary) {
        return *rng.pick(&[
            f64::NAN,
            -f64::NAN,
            0.0,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
            f64::from_bits(1),
            f64::MAX,
            f64::MIN,
            f64::EPSILON,
            0.5,
            -0.5,
            1.5,
            2.5,
            -2.5,
            9_007_199_254_740_993.0,
            1e300,
            -1e-300,
        ]);
    }
    match rng.below(3) {
        // Everyday values with a few fractional digits.
        0 => (rng.range_i64(-1_000_000, 1_000_000) as f64) / 100.0,
        // Wide but finite magnitudes.
        1 => {
            let mantissa = rng.unit() * 2.0 - 1.0;
            let exponent = rng.range_i64(-60, 60) as i32;
            mantissa * 2f64.powi(exponent)
        }
        // Arbitrary finite bit patterns, including subnormals.
        _ => loop {
            let value = f64::from_bits(rng.next_u64());
            if value.is_finite() {
                break value;
            }
        },
    }
}

fn float32(rng: &mut SplitMix64, boundary: f64) -> f32 {
    if rng.chance(boundary) {
        return *rng.pick(&[
            f32::NAN,
            -f32::NAN,
            0.0,
            -0.0,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::MIN_POSITIVE,
            f32::from_bits(1),
            f32::MAX,
            f32::MIN,
            f32::EPSILON,
            0.5,
            -0.5,
            2.5,
            16_777_217.0,
        ]);
    }
    match rng.below(3) {
        0 => (rng.range_i64(-1_000_000, 1_000_000) as f32) / 100.0,
        1 => {
            let mantissa = rng.unit() as f32 * 2.0 - 1.0;
            let exponent = rng.range_i64(-30, 30) as i32;
            mantissa * 2f32.powi(exponent)
        }
        _ => loop {
            let value = f32::from_bits(rng.next_u64() as u32);
            if value.is_finite() {
                break value;
            }
        },
    }
}

fn pow10_i128(exponent: u32) -> i128 {
    10i128.pow(exponent)
}

/// A coefficient with at most `digits` decimal digits, drawn digit by digit.
fn random_digits_i128(rng: &mut SplitMix64, digits: u32) -> i128 {
    let mut value = 0i128;
    for _ in 0..digits {
        value = value * 10 + rng.below(10) as i128;
    }
    value
}

fn decimal128(rng: &mut SplitMix64, boundary: f64, precision: u8, scale: i8) -> i128 {
    let precision = u32::from(precision.clamp(1, 38));
    let max = pow10_i128(precision) - 1;
    let one = if scale > 0 && (scale as u32) < precision {
        pow10_i128(scale as u32)
    } else {
        1
    };
    if rng.chance(boundary) {
        let half = if one > 1 { one / 2 } else { 0 };
        return *rng.pick(&[max, -max, 0, 1, -1, one, -one, half, -half, one + half]);
    }
    let digits = 1 + rng.below(u64::from(precision)) as u32;
    let value = random_digits_i128(rng, digits);
    if rng.chance(0.5) { -value } else { value }
}

fn decimal256(rng: &mut SplitMix64, boundary: f64, precision: u8, scale: i8) -> i256 {
    let precision = u32::from(precision.clamp(1, 76));
    let ten = i256::from_i128(10);
    let max = ten.wrapping_pow(precision).wrapping_sub(i256::ONE);
    let one = if scale > 0 && (scale as u32) < precision {
        ten.wrapping_pow(scale as u32)
    } else {
        i256::ONE
    };
    if rng.chance(boundary) {
        let half = one.wrapping_div(i256::from_i128(2));
        return *rng.pick(&[
            max,
            max.wrapping_neg(),
            i256::ZERO,
            i256::ONE,
            i256::MINUS_ONE,
            one,
            one.wrapping_neg(),
            half,
            half.wrapping_neg(),
        ]);
    }
    let digits = 1 + rng.below(u64::from(precision)) as u32;
    let mut value = i256::ZERO;
    for _ in 0..digits {
        value = value
            .wrapping_mul(ten)
            .wrapping_add(i256::from_i128(rng.below(10) as i128));
    }
    if rng.chance(0.5) {
        value.wrapping_neg()
    } else {
        value
    }
}

fn largeint(rng: &mut SplitMix64, boundary: f64) -> i128 {
    if rng.chance(boundary) {
        return *rng.pick(&[
            i128::MIN,
            i128::MIN + 1,
            -1,
            0,
            1,
            i128::MAX - 1,
            i128::MAX,
            i64::MIN as i128 - 1,
            i64::MAX as i128 + 1,
        ]);
    }
    let bits = 1 + rng.below(128) as u32;
    let raw = ((rng.next_u64() as u128) << 64) | rng.next_u64() as u128;
    let magnitude = if bits >= 127 {
        raw >> 1
    } else {
        raw & ((1u128 << bits) - 1)
    } as i128;
    if rng.chance(0.5) {
        -magnitude
    } else {
        magnitude
    }
}

fn epoch() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is a valid date")
}

fn days_since_epoch(year: i32, month: u32, day: u32) -> i32 {
    let date = NaiveDate::from_ymd_opt(year, month, day).expect("boundary date is valid");
    date.signed_duration_since(epoch()).num_days() as i32
}

/// Inclusive Date32 range of the SQL DATE domain, 0000-01-01..9999-12-31.
pub(crate) fn sql_date_range() -> (i32, i32) {
    (days_since_epoch(0, 1, 1), days_since_epoch(9999, 12, 31))
}

const MICROS_PER_DAY: i64 = 86_400_000_000;

fn date32(rng: &mut SplitMix64, boundary: f64) -> i32 {
    let (min, max) = sql_date_range();
    if rng.chance(boundary) {
        return *rng.pick(&[
            min,
            max,
            0,
            -1,
            days_since_epoch(2000, 2, 29),
            days_since_epoch(1900, 2, 28),
            days_since_epoch(1900, 3, 1),
            days_since_epoch(2024, 12, 31),
            days_since_epoch(1, 1, 1),
        ]);
    }
    rng.range_i64(i64::from(min), i64::from(max)) as i32
}

fn timestamp_micros(rng: &mut SplitMix64, boundary: f64) -> i64 {
    let (min_day, max_day) = sql_date_range();
    let min = i64::from(min_day) * MICROS_PER_DAY;
    let max = i64::from(max_day) * MICROS_PER_DAY + MICROS_PER_DAY - 1;
    if rng.chance(boundary) {
        return *rng.pick(&[
            min,
            max,
            0,
            -1,
            1,
            MICROS_PER_DAY - 1,
            i64::from(days_since_epoch(2000, 2, 29)) * MICROS_PER_DAY + 43_200_000_000,
            i64::from(days_since_epoch(2038, 1, 19)) * MICROS_PER_DAY + 11_647_000_000,
        ]);
    }
    let value = rng.range_i64(min, max);
    // Half of the draws land on whole seconds, as most stored values do.
    if rng.chance(0.5) {
        value - value.rem_euclid(1_000_000)
    } else {
        value
    }
}

const MIXED_PIECES: &[&str] = &[
    "a", "b", "Z", "0", "7", " ", "-", "_", ".", ",", "%", "é", "ß", "İ", "Σ", "ς", "中", "文",
    "😀", "\u{0301}", "\u{0000}", "\t", "\n", "abc", "Hello", "NULL",
];

fn text(rng: &mut SplitMix64, boundary: f64, profile: &InputProfile) -> String {
    match profile.text {
        TextProfile::Mixed => {
            if rng.chance(boundary) {
                return (*rng.pick(&[
                    "",
                    " ",
                    "  padded  ",
                    "0",
                    "-1",
                    "abc",
                    "ABC",
                    "😀",
                    "中文",
                ]))
                .to_string();
            }
            let len = rng.below(profile.max_text_len as u64 + 1) as usize;
            let mut value = String::new();
            for _ in 0..len {
                let piece: &str = rng.pick::<&str>(MIXED_PIECES);
                value.push_str(piece);
            }
            value
        }
        TextProfile::Ascii => {
            if rng.chance(boundary) {
                return (*rng.pick(&["", " ", "a", "Z", "0"])).to_string();
            }
            let len = rng.below(profile.max_text_len as u64 + 1) as usize;
            (0..len)
                .map(|_| char::from(b' ' + rng.below(95) as u8))
                .collect()
        }
        TextProfile::Numeric => {
            if rng.chance(boundary) {
                return (*rng.pick(&[
                    "",
                    "0",
                    "-0",
                    "+1",
                    " 42 ",
                    "1e3",
                    "1.5",
                    "-2.5",
                    "9223372036854775807",
                    "-9223372036854775808",
                    "9223372036854775808",
                    "abc",
                    "12abc",
                ]))
                .to_string();
            }
            let sign = *rng.pick(&["", "-", "+"]);
            let integer = rng.range_i64(0, 1_000_000_000);
            if rng.chance(0.5) {
                format!("{sign}{integer}.{:03}", rng.below(1000))
            } else {
                format!("{sign}{integer}")
            }
        }
        TextProfile::DateText => {
            if rng.chance(boundary) {
                return (*rng.pick(&[
                    "",
                    "0000-01-01",
                    "9999-12-31",
                    "2000-02-29",
                    "2023-02-29",
                    "2024-13-01",
                    "0000-00-00",
                    "20240131",
                    "abc",
                ]))
                .to_string();
            }
            let day = date32(rng, 0.0);
            let date = epoch() + chrono::Duration::days(i64::from(day));
            date.format("%Y-%m-%d").to_string()
        }
        TextProfile::DateTimeText => {
            if rng.chance(boundary) {
                return (*rng.pick(&[
                    "",
                    "0000-01-01 00:00:00",
                    "9999-12-31 23:59:59.999999",
                    "2000-02-29 12:00:00",
                    "2024-01-01 24:00:00",
                    "2024-01-01",
                    "abc",
                ]))
                .to_string();
            }
            let micros = timestamp_micros(rng, 0.0);
            let date = chrono::DateTime::from_timestamp_micros(micros)
                .expect("generated timestamp is in chrono range")
                .naive_utc();
            if micros.rem_euclid(1_000_000) == 0 {
                date.format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                date.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
        }
    }
}

fn bytes(rng: &mut SplitMix64, boundary: f64, max_len: usize) -> Vec<u8> {
    if rng.chance(boundary) {
        return rng
            .pick(&[
                &[][..],
                &[0u8][..],
                &[0xFF][..],
                &[0xC3, 0x28][..],
                &[0xE4, 0xB8, 0xAD][..],
            ])
            .to_vec();
    }
    let len = rng.below(max_len as u64 + 1) as usize;
    (0..len).map(|_| rng.next_u64() as u8).collect()
}
