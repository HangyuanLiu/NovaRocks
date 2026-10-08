// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! V1 chained Murmur3 and exact textual integer/boolean inputs.
use crate::{
    KernelFailure,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::*;
use arrow_buffer::i256;
use arrow_schema::{DataType, TimeUnit};
use chrono::DateTime;
pub(super) fn hash_array(
    array: &dyn Array,
    row: usize,
    seed: u32,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<u32, KernelFailure> {
    macro_rules! bytes {
        ($ty:ty, $convert:expr) => {{
            let array = array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| internal("Murmur3 argument downcast failed"))?;
            hash(($convert)(array.value(row)), seed, work)
        }};
    }
    macro_rules! integer {
        ($ty:ty) => {{
            let array = array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| internal("Murmur3 integer downcast failed"))?;
            work.flush()?;
            let text = array.value(row).to_string();
            work.flush()?;
            hash(text.as_bytes(), seed, work)
        }};
    }
    match array.data_type() {
        DataType::Utf8 => bytes!(StringArray, str::as_bytes),
        DataType::LargeUtf8 => bytes!(LargeStringArray, str::as_bytes),
        DataType::Binary => bytes!(BinaryArray, |v| v),
        DataType::LargeBinary => bytes!(LargeBinaryArray, |v| v),
        DataType::Int8 => integer!(Int8Array),
        DataType::Int16 => integer!(Int16Array),
        DataType::Int32 => integer!(Int32Array),
        DataType::Int64 => integer!(Int64Array),
        DataType::UInt8 => integer!(UInt8Array),
        DataType::UInt16 => integer!(UInt16Array),
        DataType::UInt32 => integer!(UInt32Array),
        DataType::UInt64 => integer!(UInt64Array),
        DataType::Float32 => {
            let value = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| internal("Murmur3 Float32 downcast failed"))?
                .value(row);
            work.flush()?;
            let text = format_float32_for_varchar(value);
            work.flush()?;
            hash(text.as_bytes(), seed, work)
        }
        DataType::Float64 => {
            let value = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| internal("Murmur3 Float64 downcast failed"))?
                .value(row);
            work.flush()?;
            let text = format_float64_for_varchar(value);
            work.flush()?;
            hash(text.as_bytes(), seed, work)
        }
        DataType::Decimal128(_, scale) => {
            let value = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| internal("Murmur3 Decimal128 downcast failed"))?
                .value(row);
            work.flush()?;
            let text = format_decimal_with_scale(value, *scale);
            work.flush()?;
            hash(text.as_bytes(), seed, work)
        }
        DataType::Decimal256(_, scale) => {
            let value = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| internal("Murmur3 Decimal256 downcast failed"))?
                .value(row);
            work.flush()?;
            let text = format_decimal256_with_scale(value, *scale);
            work.flush()?;
            hash(text.as_bytes(), seed, work)
        }
        DataType::FixedSizeBinary(16) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| internal("Murmur3 LargeInt downcast failed"))?;
            let value = i128::from_be_bytes(
                array
                    .value(row)
                    .try_into()
                    .map_err(|_| internal("Murmur3 LargeInt width differs"))?,
            );
            work.flush()?;
            let text = value.to_string();
            work.flush()?;
            hash(text.as_bytes(), seed, work)
        }
        DataType::Timestamp(unit, tz) => {
            let value = match unit {
                TimeUnit::Second => array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| internal("Murmur3 timestamp second downcast failed"))?
                    .value(row),
                TimeUnit::Millisecond => array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| internal("Murmur3 timestamp millisecond downcast failed"))?
                    .value(row),
                TimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| internal("Murmur3 timestamp microsecond downcast failed"))?
                    .value(row),
                TimeUnit::Nanosecond => array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| internal("Murmur3 timestamp nanosecond downcast failed"))?
                    .value(row),
            };
            work.flush()?;
            let text = format_timestamp_for_varchar(unit, value, tz.as_deref());
            work.flush()?;
            hash(text.as_bytes(), seed, work)
        }
        DataType::Boolean => {
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| internal("Murmur3 boolean downcast failed"))?
                .value(row);
            hash(if v { b"1" } else { b"0" }, seed, work)
        }
        _ => Err(invalid(
            "Murmur3 has no selected implementation for this exact source profile",
        )),
    }
}
/// Exact v1 residual Arrow carrier projection runs only after preceding
/// arguments and their SQL NULLs have been visited. Pure-owner admission
/// refuses these profiles before computation; this preserves the v1 shell.
pub(super) fn hash_array_selected(
    array: &ArrayRef,
    row: usize,
    seed: u32,
    work: &mut EvaluationCheckpoints<'_>,
    error_boundary: Option<&dyn Fn(&str) -> Result<(), KernelFailure>>,
) -> Result<Option<u32>, KernelFailure> {
    if super::string_extended::murmur_supported_carrier(array.data_type())
        || matches!(array.data_type(), DataType::Timestamp(..))
    {
        return hash_array(array.as_ref(), row, seed, work).map(Some);
    }
    use arrow_cast::{CastOptions, cast_with_options, display::FormatOptions};
    work.flush()?;
    let options = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    let casted = cast_with_options(array.as_ref(), &DataType::Utf8, &options).map_err(|error| {
        let message = format!("cast to Utf8 failed for murmur_hash3_32: {error}");
        project_legacy_error(&message, error_boundary)
    })?;
    work.flush()?;
    let strings = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| project_legacy_error("cast result is not StringArray", error_boundary))?;
    if strings.is_null(row) {
        Ok(None)
    } else {
        hash(strings.value(row).as_bytes(), seed, work).map(Some)
    }
}
fn project_legacy_error(
    message: &str,
    boundary: Option<&dyn Fn(&str) -> Result<(), KernelFailure>>,
) -> KernelFailure {
    if let Some(boundary) = boundary
        && let Err(error) = boundary(message)
    {
        return error;
    }
    KernelFailure::Operational(crate::KernelDiagnostic::new(message))
}

fn hash(
    data: &[u8],
    seed: u32,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<u32, KernelFailure> {
    const C1: u32 = 0xcc9e2d51;
    const C2: u32 = 0x1b873593;

    let mut hash = seed;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        work.step()?;
        work.step()?;
        work.step()?;
        work.step()?;
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(C1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(C2);
        hash ^= k;
        hash = hash.rotate_left(13);
        hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    let rem = chunks.remainder();
    for _ in rem {
        work.step()?;
    }
    let mut k1 = 0u32;
    match rem.len() {
        3 => {
            k1 ^= (rem[2] as u32) << 16;
            k1 ^= (rem[1] as u32) << 8;
            k1 ^= rem[0] as u32;
        }
        2 => {
            k1 ^= (rem[1] as u32) << 8;
            k1 ^= rem[0] as u32;
        }
        1 => {
            k1 ^= rem[0] as u32;
        }
        _ => {}
    }
    if k1 != 0 {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(C2);
        hash ^= k1;
    }

    hash ^= data.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^= hash >> 16;
    work.step()?;
    Ok(hash)
}

// These renderings are the v1 VARCHAR grammar, bounded by scalar widths.
pub fn format_float64_for_varchar(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
    }
    let mut buf = ryu::Buffer::new();
    let formatted = buf.format(value);
    normalize_float_string_for_varchar(formatted)
}

pub fn format_float32_for_varchar(value: f32) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
    }
    let mut buf = ryu::Buffer::new();
    let formatted = buf.format(value);
    normalize_float_string_for_varchar(formatted)
}

fn normalize_float_string_for_varchar(formatted: &str) -> String {
    let stripped = formatted.strip_suffix(".0").unwrap_or(formatted);
    if let Some(exp_pos) = stripped.find('e') {
        let mut out = String::with_capacity(stripped.len() + 1);
        out.push_str(&stripped[..=exp_pos]);
        if let Some(sign_or_digit) = stripped.as_bytes().get(exp_pos + 1) {
            if *sign_or_digit == b'+' || *sign_or_digit == b'-' {
                out.push_str(&stripped[exp_pos + 1..]);
            } else {
                out.push('+');
                out.push_str(&stripped[exp_pos + 1..]);
            }
        }
        out
    } else {
        stripped.to_string()
    }
}

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

pub fn format_timestamp_for_varchar(unit: &TimeUnit, value: i64, tz: Option<&str>) -> String {
    let timestamp_str = match unit {
        TimeUnit::Second => {
            let dt = DateTime::from_timestamp(value, 0)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
        }
        TimeUnit::Millisecond => {
            let seconds = value.div_euclid(1_000);
            let millis = value.rem_euclid(1_000) as u32;
            let dt = DateTime::from_timestamp(seconds, millis * 1_000_000)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if millis == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
            }
        }
        TimeUnit::Microsecond => {
            let seconds = value.div_euclid(1_000_000);
            let micros = value.rem_euclid(1_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, micros * 1_000)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if micros == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
            }
        }
        TimeUnit::Nanosecond => {
            let seconds = value.div_euclid(1_000_000_000);
            let nanos = value.rem_euclid(1_000_000_000) as u32;
            let dt = DateTime::from_timestamp(seconds, nanos)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());
            if nanos == 0 {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string()
            } else {
                dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.9f").to_string()
            }
        }
    };
    if let Some(tz) = tz {
        format!("{timestamp_str} {tz}")
    } else {
        timestamp_str
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::KernelEvaluationControl;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    struct Control {
        calls: AtomicUsize,
        refuse: bool,
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let call = self.calls.fetch_add(1, Ordering::Relaxed);
            if self.refuse && call == 1 {
                Err(KernelFailure::DeadlineExceeded)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("Murmur3 must not wait")
        }
    }
    #[test]
    fn selected_murmur_preserves_frozen_v1_zero_tail_values() {
        let control = Control {
            calls: AtomicUsize::new(0),
            refuse: false,
        };
        let mut work = EvaluationCheckpoints::new(&control);
        assert_eq!(hash(b"", 104_729, &mut work).unwrap(), 3329588566);
        assert_eq!(hash(b"\0", 104_729, &mut work).unwrap(), 500407381);
        assert_eq!(format_float64_for_varchar(-0.0), "0");
        assert_eq!(format_float64_for_varchar(f64::NAN), "nan");
        assert_eq!(format_float32_for_varchar(f32::INFINITY), "inf");
        assert_eq!(format_float64_for_varchar(1e30), "1e+30");
        assert_eq!(format_decimal_with_scale(-5, 3), "-0.005");
        work.finish().unwrap();
    }
    #[test]
    fn selected_murmur_long_byte_input_observes_deadline() {
        let control = Control {
            calls: AtomicUsize::new(0),
            refuse: true,
        };
        let mut work = EvaluationCheckpoints::new(&control);
        assert!(matches!(
            hash(&vec![0; 8192], 104_729, &mut work),
            Err(KernelFailure::DeadlineExceeded)
        ));
        assert_eq!(control.calls.load(Ordering::Relaxed), 2);
    }
}

/// The v1 test adapter enters the same observed byte computation.
pub fn murmur_hash3_32(data: &[u8], seed: u32) -> u32 {
    let mut work = EvaluationCheckpoints::new(&super::string_extended::LegacyStringControl);
    let value = hash(data, seed, &mut work).expect("unrestricted legacy Murmur3 control");
    work.finish().expect("unrestricted legacy Murmur3 control");
    value
}
