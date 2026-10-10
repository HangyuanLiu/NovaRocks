// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Selected-value diagnostics, never a value identity or SQL serialization.
//!
//! A first, observed write counts the actual selected representation without
//! retaining output. Its checked length bounds every growth of the second
//! write. This is not admission for Arrow's opaque formatting work or its
//! temporary allocations: formatter construction allocates recursive Box/Vec
//! state, and decimal/chrono formatting may allocate temporary strings. Those
//! library boundaries have checks before and after; they are not cooperative
//! internally, and this module provides neither a MEM grant nor an invoice.

use super::{ConstantError, ConstantValue, Row, logical_null, primitive_bytes, resolve_row};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, PureCompileControl, ValueLogicalType,
};
use std::fmt::{self, Write};

pub(super) fn format(
    value: &ConstantValue,
    phase: CompilePhase,
    control: &dyn PureCompileControl,
) -> Result<String, ConstantError> {
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    let result = format_value(value, &mut work);
    // A primary refusal must not be replaced by a later completion callback.
    if matches!(
        result,
        Err(ConstantError::Control(_) | ConstantError::Limit(_))
    ) {
        return result;
    }
    work.finish()?;
    result
}

fn format_value(
    value: &ConstantValue,
    work: &mut CompileCheckpoints<'_>,
) -> Result<String, ConstantError> {
    format_value_with(value, work, &mut StringRenderer)
}
trait DiagnosticRenderer {
    type Output;
    fn render(
        &mut self,
        work: &mut CompileCheckpoints<'_>,
        write: impl Fn(&mut dyn Write) -> Result<(), ArrowError>,
    ) -> Result<Self::Output, ConstantError>;
}
struct StringRenderer;
impl DiagnosticRenderer for StringRenderer {
    type Output = String;
    fn render(
        &mut self,
        work: &mut CompileCheckpoints<'_>,
        write: impl Fn(&mut dyn Write) -> Result<(), ArrowError>,
    ) -> Result<String, ConstantError> {
        render(work, write)
    }
}
/// Stream the same selected representation to a caller-owned bounded writer.
/// Writer refusal remains separate so its budget journal is not replaced.
pub(super) fn write(
    value: &ConstantValue,
    phase: CompilePhase,
    control: &dyn PureCompileControl,
    output: &mut dyn Write,
) -> Result<fmt::Result, ConstantError> {
    let mut work = CompileCheckpoints::try_new(control, phase)?;
    let result = format_value_with(value, &mut work, &mut BorrowedRenderer { output });
    if matches!(
        result,
        Err(ConstantError::Control(_) | ConstantError::Limit(_)) | Ok(Err(_))
    ) {
        return result;
    }
    work.finish()?;
    result
}
struct BorrowedRenderer<'a> {
    output: &'a mut dyn Write,
}
impl DiagnosticRenderer for BorrowedRenderer<'_> {
    type Output = fmt::Result;
    fn render(
        &mut self,
        work: &mut CompileCheckpoints<'_>,
        write: impl Fn(&mut dyn Write) -> Result<(), ArrowError>,
    ) -> Result<fmt::Result, ConstantError> {
        work.flush()?;
        let mut writer = BorrowedObservedWriter {
            work,
            output: self.output,
            error: None,
            refused: false,
        };
        let result = write(&mut writer);
        if let Some(primary) = writer.error.take() {
            return Err(primary);
        }
        if writer.refused {
            return Ok(Err(fmt::Error));
        }
        result.map_err(|error| ConstantError::Arrow(error.to_string()))?;
        writer.work.flush()?;
        Ok(Ok(()))
    }
}
struct BorrowedObservedWriter<'w, 'c> {
    work: &'w mut CompileCheckpoints<'c>,
    output: &'w mut dyn Write,
    error: Option<ConstantError>,
    refused: bool,
}
impl Write for BorrowedObservedWriter<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.refused || self.error.is_some() {
            return Err(fmt::Error);
        }
        let mut remaining = text;
        while !remaining.is_empty() {
            let mut end = remaining.len().min(256);
            while !remaining.is_char_boundary(end) {
                end -= 1;
            }
            let chunk = &remaining[..end];
            if let Err(error) = self.work.flush() {
                self.error = Some(error.into());
                return Err(fmt::Error);
            }
            if self.output.write_str(chunk).is_err() {
                self.refused = true;
                return Err(fmt::Error);
            }
            for _ in chunk.as_bytes() {
                if let Err(error) = self.work.step() {
                    self.error = Some(error.into());
                    return Err(fmt::Error);
                }
            }
            remaining = &remaining[end..];
        }
        Ok(())
    }
}

fn format_value_with<R: DiagnosticRenderer>(
    value: &ConstantValue,
    work: &mut CompileCheckpoints<'_>,
    renderer: &mut R,
) -> Result<R::Output, ConstantError> {
    let source = Row {
        data: value.pool.data(),
        index: value.ordinal() as usize,
    };
    if logical_null(source.data, source.index, work)? {
        return renderer.render(work, |writer| write_text(writer, format_args!("NULL")));
    }
    if value.value_type().logical_type == ValueLogicalType::LargeInt {
        let selected = value.try_largeint()?.ok_or(ConstantError::Invalid(
            "non-NULL LARGEINT diagnostic lacks a value",
        ))?;
        work.step()?;
        return renderer.render(work, |writer| {
            write_text(writer, format_args!("{selected}"))
        });
    }
    // Expose diagnostic distinctions that Arrow's usual display intentionally
    // omits. This does not change their bits or the source Field/FVT.
    if let Some(selected) = resolve_row(source, work)? {
        match selected.data.data_type() {
            DataType::Float32 => {
                let bytes = primitive_bytes(selected, 4)?
                    .try_into()
                    .map_err(|_| ConstantError::Invalid("Float32 diagnostic width differs"))?;
                let bits = u32::from_ne_bytes(bytes);
                work.step()?;
                if f32::from_bits(bits).is_nan() {
                    return renderer.render(work, |writer| {
                        write_text(writer, format_args!("NaN(Float32,0x{bits:08x})"))
                    });
                }
                if bits & 0x7fff_ffff == 0 {
                    return renderer.render(work, |writer| {
                        write_text(
                            writer,
                            format_args!("{}0.0", if bits == 0 { "" } else { "-" }),
                        )
                    });
                }
            }
            DataType::Float64 => {
                let bytes = primitive_bytes(selected, 8)?
                    .try_into()
                    .map_err(|_| ConstantError::Invalid("Float64 diagnostic width differs"))?;
                let bits = u64::from_ne_bytes(bytes);
                work.step()?;
                if f64::from_bits(bits).is_nan() {
                    return renderer.render(work, |writer| {
                        write_text(writer, format_args!("NaN(Float64,0x{bits:016x})"))
                    });
                }
                if bits & 0x7fff_ffff_ffff_ffff == 0 {
                    return renderer.render(work, |writer| {
                        write_text(
                            writer,
                            format_args!("{}0.0", if bits == 0 { "" } else { "-" }),
                        )
                    });
                }
            }
            _ => {}
        }
    }
    let options = FormatOptions::new()
        .with_null("NULL")
        .with_display_error(false);
    work.flush()?;
    let formatter = ArrayFormatter::try_new(value.pool.array().as_ref(), &options);
    work.flush()?;
    let formatter = formatter.map_err(|error| ConstantError::Arrow(error.to_string()))?;
    renderer.render(work, |writer| {
        formatter.value(value.ordinal() as usize).write(writer)
    })
}

fn write_text(writer: &mut dyn Write, args: fmt::Arguments<'_>) -> Result<(), ArrowError> {
    writer
        .write_fmt(args)
        .map_err(|_| ArrowError::CastError("constant diagnostic write failed".to_owned()))
}

fn render(
    work: &mut CompileCheckpoints<'_>,
    write: impl Fn(&mut dyn Write) -> Result<(), ArrowError>,
) -> Result<String, ConstantError> {
    work.flush()?;
    let bound = {
        let mut counter = ObservedWriter::counting(work);
        let result = write(&mut counter);
        if let Some(primary) = counter.error.take() {
            return Err(primary);
        }
        result.map_err(|error| ConstantError::Arrow(error.to_string()))?;
        counter.length
    };
    work.flush()?;
    // The exact first-pass count avoids repeated String reallocation while
    // retaining a checked pre-growth bound. This remains a requested library
    // allocation with opaque checks, not a host allocation grant.
    let mut reserved = String::new();
    reserved
        .try_reserve_exact(bound)
        .map_err(|_| ConstantError::Limit("constant diagnostic allocation failed"))?;
    work.flush()?;
    let output = {
        let mut writer = ObservedWriter::output(work, bound);
        writer.output = Some(reserved);
        let result = write(&mut writer);
        if let Some(primary) = writer.error.take() {
            return Err(primary);
        }
        result.map_err(|error| ConstantError::Arrow(error.to_string()))?;
        if writer.length != bound {
            return Err(ConstantError::Invalid(
                "constant diagnostic changed between observed writes",
            ));
        }
        writer.output.take().ok_or(ConstantError::Invalid(
            "constant diagnostic output writer lacks output",
        ))?
    };
    work.flush()?;
    Ok(output)
}

struct ObservedWriter<'w, 'c> {
    work: &'w mut CompileCheckpoints<'c>,
    output: Option<String>,
    length: usize,
    bound: usize,
    error: Option<ConstantError>,
}

impl<'w, 'c> ObservedWriter<'w, 'c> {
    fn counting(work: &'w mut CompileCheckpoints<'c>) -> Self {
        Self {
            work,
            output: None,
            length: 0,
            bound: isize::MAX as usize,
            error: None,
        }
    }

    fn output(work: &'w mut CompileCheckpoints<'c>, bound: usize) -> Self {
        Self {
            output: Some(String::new()),
            ..Self::counting(work)
        }
        .with_bound(bound)
    }

    fn with_bound(mut self, bound: usize) -> Self {
        self.bound = bound;
        self
    }

    fn fail(&mut self, error: ConstantError) -> fmt::Result {
        if self.error.is_none() {
            self.error = Some(error);
        }
        Err(fmt::Error)
    }
}

impl Write for ObservedWriter<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.error.is_some() {
            return Err(fmt::Error);
        }
        let Some(total) = self.length.checked_add(text.len()) else {
            return self.fail(ConstantError::Limit("constant diagnostic length overflow"));
        };
        if total > self.bound || total > isize::MAX as usize {
            return self.fail(ConstantError::Limit(
                "constant diagnostic exceeds counted bound",
            ));
        }
        let mut remaining = text;
        while !remaining.is_empty() {
            let mut end = remaining.len().min(256);
            while !remaining.is_char_boundary(end) {
                end -= 1;
            }
            let chunk = &remaining[..end];
            if self.output.is_some() {
                // Observe before the actual growth/copy. This is an original
                // control check, not allocation authorization.
                if let Err(error) = self.work.flush() {
                    return self.fail(error.into());
                }
                let Some(output) = self.output.as_mut() else {
                    return self.fail(ConstantError::Invalid("diagnostic output state changed"));
                };
                if output.try_reserve_exact(chunk.len()).is_err() {
                    return self.fail(ConstantError::Limit(
                        "constant diagnostic allocation failed",
                    ));
                }
                output.push_str(chunk);
            }
            // The counting pass completes length arithmetic; the output pass
            // also completes the copy. Each byte is completed work, with the
            // existing meter's 256-unit quantum and no synthetic reservation.
            for _ in chunk.as_bytes() {
                self.length += 1;
                if let Err(error) = self.work.step() {
                    return self.fail(error.into());
                }
            }
            remaining = &remaining[end..];
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "diagnostic/tests.rs"]
mod tests;
