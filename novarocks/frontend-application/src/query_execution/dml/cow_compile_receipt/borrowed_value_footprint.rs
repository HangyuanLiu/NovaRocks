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

// The paired builder must allocate exact declared lengths, never call the
// implicit-growth SQL renderers, and preserve the original semantic errors.
use std::fmt::{self, Write};
use std::mem::size_of;

use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_parser::ast::{Expr, Ident};

#[derive(Clone, Copy, Debug)]
pub enum Failure {
    ResourceExhausted,
    Stopped,
    InvalidSource(&'static str),
    OriginalSemantic(&'static str),
}
type Result<T> = std::result::Result<T, Failure>;

fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or(Failure::ResourceExhausted)
}
fn mul(a: u64, b: u64) -> Result<u64> {
    a.checked_mul(b).ok_or(Failure::ResourceExhausted)
}
fn slots<T>(n: usize) -> Result<u64> {
    mul(n as u64, size_of::<T>() as u64)
}

/// Includes the root Expr inline slot. A containing Vec charges
/// the sum of these roots, not another len * size_of::<Expr>() on top.
#[derive(Clone, Copy, Debug)]
pub struct ValueFootprint {
    pub ast_owned: u64,
    pub expr_nodes: u64,
}
impl ValueFootprint {
    fn leaf(ast_payload: u64, negative: bool) -> Result<Self> {
        Ok(Self {
            ast_owned: add(
                size_of::<Expr>() as u64,
                add(
                    ast_payload,
                    if negative {
                        size_of::<Expr>() as u64
                    } else {
                        0
                    },
                )?,
            )?,
            expr_nodes: if negative { 2 } else { 1 },
        })
    }
    fn absorb(&mut self, other: Self) -> Result<()> {
        self.ast_owned = add(self.ast_owned, other.ast_owned)?;
        self.expr_nodes = add(self.expr_nodes, other.expr_nodes)?;
        Ok(())
    }
    fn check(self, original_remaining_upper: u64) -> Result<Self> {
        // This is a necessary early rejection, not final route admission.
        if self.ast_owned > original_remaining_upper {
            Err(Failure::ResourceExhausted)
        } else {
            Ok(self)
        }
    }
}

#[derive(Default)]
struct CountFormat {
    bytes: u64,
    negative: bool,
    first: bool,
}
impl Write for CountFormat {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        if !self.first && !s.is_empty() {
            self.negative = s.starts_with('-');
            self.first = true;
        }
        self.bytes = self.bytes.checked_add(s.len() as u64).ok_or(fmt::Error)?;
        Ok(())
    }
}
fn number(n: impl fmt::Display) -> Result<ValueFootprint> {
    let mut count = CountFormat::default();
    write!(&mut count, "{n}").map_err(|_| Failure::ResourceExhausted)?;
    // Match parsed unary-minus + positive Number spelling, including -0.
    ValueFootprint::leaf(count.bytes - u64::from(count.negative), count.negative)
}
fn downcast<T: 'static>(a: &dyn Array) -> Result<&T> {
    a.as_any()
        .downcast_ref::<T>()
        .ok_or(Failure::InvalidSource("COW literal downcast"))
}

/// Borrow original array offsets; do not slice List/Map into new ArrayRef.
/// Call for the entire selected cell BEFORE any owned Literal construction.
// Diagnostic convenience only. Production admission uses the controlled walk.
#[cfg(test)]
pub fn borrowed_value(
    a: &dyn Array,
    row: usize,
    original_remaining_upper: u64,
) -> Result<ValueFootprint> {
    borrowed_value_with_control(a, row, original_remaining_upper, &mut || Ok(()))
}

pub fn borrowed_value_with_control(
    a: &dyn Array,
    row: usize,
    original_remaining_upper: u64,
    original_scope_and_deadline_check: &mut dyn FnMut() -> Result<()>,
) -> Result<ValueFootprint> {
    original_scope_and_deadline_check()?;
    let mut control = WalkControl {
        visits: 0,
        check: original_scope_and_deadline_check,
    };
    borrowed_value_inner(a, row, original_remaining_upper, &mut control)
}
struct WalkControl<'a> {
    visits: u64,
    check: &'a mut dyn FnMut() -> Result<()>,
}
impl WalkControl<'_> {
    fn enter(&mut self) -> Result<()> {
        self.visits = self
            .visits
            .checked_add(1)
            .ok_or(Failure::ResourceExhausted)?;
        if self.visits % 256 == 0 {
            (self.check)()?;
        }
        Ok(())
    }
}
fn borrowed_value_inner(
    a: &dyn Array,
    row: usize,
    original_remaining_upper: u64,
    control: &mut WalkControl<'_>,
) -> Result<ValueFootprint> {
    control.enter()?;
    if row >= a.len() {
        return Err(Failure::InvalidSource("COW literal row"));
    }
    // Preserve the existing NULL-first rule even for a nonnull-unsupported type.
    if a.is_null(row) {
        return ValueFootprint::leaf(0, false)?.check(original_remaining_upper);
    }
    let value = match a.data_type() {
        DataType::Boolean => {
            let _ = downcast::<BooleanArray>(a)?;
            ValueFootprint::leaf(0, false)?
        }
        DataType::Int8 => number(i64::from(downcast::<Int8Array>(a)?.value(row)))?,
        DataType::Int16 => number(i64::from(downcast::<Int16Array>(a)?.value(row)))?,
        DataType::Int32 => number(i64::from(downcast::<Int32Array>(a)?.value(row)))?,
        DataType::Int64 => number(downcast::<Int64Array>(a)?.value(row))?,
        DataType::Float32 => {
            let n = f64::from(downcast::<Float32Array>(a)?.value(row));
            if !n.is_finite() {
                return Err(Failure::OriginalSemantic("non-finite floating literal"));
            }
            number(n)?
        }
        DataType::Float64 => {
            let n = downcast::<Float64Array>(a)?.value(row);
            if !n.is_finite() {
                return Err(Failure::OriginalSemantic("non-finite floating literal"));
            }
            number(n)?
        }
        DataType::Decimal128(_, scale) => {
            let n = downcast::<Decimal128Array>(a)?.value(row);
            if *scale == 0 {
                number(
                    i64::try_from(n)
                        .map_err(|_| Failure::OriginalSemantic("decimal outside INT64"))?,
                )?
            } else {
                let power = u32::try_from(*scale)
                    .ok()
                    .and_then(|scale| 10_u128.checked_pow(scale))
                    .ok_or(Failure::OriginalSemantic("unsupported decimal scale"))?;
                let mut count = CountFormat::default();
                write!(
                    &mut count,
                    "{}{}.{:0width$}",
                    if n.is_negative() { "-" } else { "" },
                    n.unsigned_abs() / power,
                    n.unsigned_abs() % power,
                    width = *scale as usize
                )
                .map_err(|_| Failure::ResourceExhausted)?;
                ValueFootprint::leaf(count.bytes, false)?
            }
        }
        DataType::Utf8 => {
            let n = downcast::<StringArray>(a)?.value(row).len() as u64;
            // The direct AST stores decoded text, not escaped SQL source.
            ValueFootprint::leaf(n, false)?
        }
        DataType::Binary => binary(downcast::<BinaryArray>(a)?.value(row))?,
        DataType::LargeBinary => binary(downcast::<LargeBinaryArray>(a)?.value(row))?,
        DataType::Date32 => {
            let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let date =
                epoch + chrono::Duration::days(i64::from(downcast::<Date32Array>(a)?.value(row)));
            let mut count = CountFormat::default();
            date.format("%Y-%m-%d")
                .write_to(&mut count)
                .map_err(|_| Failure::ResourceExhausted)?;
            ValueFootprint::leaf(count.bytes, false)?
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let micros = downcast::<TimestampMicrosecondArray>(a)?.value(row);
            // Same existing formatter/panic boundary; no new timestamp semantics.
            let date = chrono::DateTime::from_timestamp_micros(micros)
                .expect("timestamp micros should be valid")
                .naive_utc();
            let mut count = CountFormat::default();
            date.format("%Y-%m-%d %H:%M:%S")
                .write_to(&mut count)
                .map_err(|_| Failure::ResourceExhausted)?;
            ValueFootprint::leaf(count.bytes, false)?
        }
        DataType::List(_) => {
            let list = downcast::<ListArray>(a)?;
            let offsets = list.value_offsets();
            sequence(
                list.values().as_ref(),
                usize::try_from(offsets[row])
                    .map_err(|_| Failure::InvalidSource("COW list offset"))?,
                usize::try_from(offsets[row + 1])
                    .map_err(|_| Failure::InvalidSource("COW list offset"))?,
                false,
                original_remaining_upper,
                control,
            )?
        }
        DataType::Struct(_) => {
            let st = downcast::<StructArray>(a)?;
            let count = st.num_columns();
            reject_slots(count, original_remaining_upper)?;
            let mut out = function_root("row")?;
            for child in st.columns() {
                out.absorb(borrowed_value_inner(
                    child.as_ref(),
                    row,
                    original_remaining_upper,
                    control,
                )?)?;
                out.check(original_remaining_upper)?;
            }
            out
        }
        DataType::Map(_, _) => {
            let map = downcast::<MapArray>(a)?;
            let entries = map.entries();
            if entries.num_columns() != 2 {
                return Err(Failure::OriginalSemantic("map entries width"));
            }
            let offsets = map.value_offsets();
            let begin = usize::try_from(offsets[row])
                .map_err(|_| Failure::InvalidSource("COW map offset"))?;
            let end = usize::try_from(offsets[row + 1])
                .map_err(|_| Failure::InvalidSource("COW map offset"))?;
            if end < begin || end > entries.len() {
                return Err(Failure::InvalidSource("COW map offsets"));
            }
            reject_slots(
                (end - begin)
                    .checked_mul(2)
                    .ok_or(Failure::ResourceExhausted)?,
                original_remaining_upper,
            )?;
            let mut out = function_root("map")?;
            for index in begin..end {
                for child in entries.columns() {
                    out.absorb(borrowed_value_inner(
                        child.as_ref(),
                        index,
                        original_remaining_upper,
                        control,
                    )?)?;
                    out.check(original_remaining_upper)?;
                }
            }
            out
        }
        _ => {
            return Err(Failure::OriginalSemantic(
                "literal_from_batch unsupported nonnull type",
            ));
        }
    };
    value.check(original_remaining_upper)
}

fn reject_slots(count: usize, ceiling: u64) -> Result<()> {
    // Giant Boolean/Null lists refuse immediately, before walking any child.
    if slots::<Expr>(count)? > ceiling {
        Err(Failure::ResourceExhausted)
    } else {
        Ok(())
    }
}
fn sequence(
    a: &dyn Array,
    begin: usize,
    end: usize,
    function: bool,
    ceiling: u64,
    control: &mut WalkControl<'_>,
) -> Result<ValueFootprint> {
    if end < begin || end > a.len() {
        return Err(Failure::InvalidSource("COW list offsets"));
    }
    reject_slots(end - begin, ceiling)?;
    let mut out = if function {
        function_root("row")?
    } else {
        ValueFootprint::leaf(0, false)?
    };
    for row in begin..end {
        out.absorb(borrowed_value_inner(a, row, ceiling, control)?)?;
        out.check(ceiling)?;
    }
    Ok(out)
}
fn function_root(name: &str) -> Result<ValueFootprint> {
    ValueFootprint::leaf(add(size_of::<Ident>() as u64, name.len() as u64)?, false)
}
fn binary(value: &[u8]) -> Result<ValueFootprint> {
    ValueFootprint::leaf(mul(value.len() as u64, 2)?, false)
}
