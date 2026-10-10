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

//! Original DROUND/TRUNCATE arithmetic on evaluated values, shared by both ABIs.
use crate::math_numeric::{
    MathNumericError, MathNumericObservation, NumericArrayView, cast_output_observed, value_at_f64,
    value_at_i64,
};
use crate::{KernelEvaluationControl, Selection, kernel_input::EvaluationCheckpoints};
use arrow_array::{ArrayRef, Float64Array};
use arrow_schema::DataType;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub enum DroundComputation {
    Round,
    Truncate,
    TruncateDigits,
}

/// A callback only projects evaluated rows; this function owns all arithmetic,
/// finite-result filtering and output construction for the two callers.
pub(super) fn compute_dround(
    operation: DroundComputation,
    selection: Selection<'_>,
    target: Option<&DataType>,
    control: Option<&dyn KernelEvaluationControl>,
    mut read: impl FnMut(usize, usize) -> Result<(Option<f64>, Option<i64>), MathNumericError>,
) -> Result<ArrayRef, MathNumericError> {
    let mut work = control.map(EvaluationCheckpoints::new);
    if let Some(w) = &mut work {
        w.flush()?;
    }
    let mut values = Vec::with_capacity(selection.len());
    for (ordinal, row) in selection.iter().enumerate() {
        if let Some(w) = &mut work {
            w.step()?;
        }
        let (x, digits) = read(ordinal, row)?;
        // The original scalar powi has a fixed-width exponent. This row is
        // charged once; Arrow output/cast operations keep opaque boundaries.
        let result = match operation {
            DroundComputation::Round => x.map(f64::round),
            DroundComputation::Truncate => x.map(f64::trunc),
            DroundComputation::TruncateDigits => match (x, digits) {
                (Some(x), Some(dec)) => {
                    if dec >= 0 {
                        let factor = 10_f64.powi(dec as i32);
                        Some((x * factor).trunc() / factor)
                    } else {
                        let factor = 10_f64.powi((-dec) as i32);
                        Some((x / factor).trunc() * factor)
                    }
                }
                _ => None,
            },
        }
        .filter(|value| value.is_finite());
        values.push(result);
    }
    if let Some(w) = &mut work {
        w.flush()?;
    }
    let out = Arc::new(Float64Array::from(values)) as ArrayRef;
    if let Some(w) = &mut work {
        w.flush()?;
    }
    let out = cast_output_observed(out, target, &mut |observation| {
        if let Some(w) = &mut work {
            match observation {
                MathNumericObservation::Step => w.step(),
                MathNumericObservation::OpaqueBoundary => w.flush(),
            }
        } else {
            Ok(())
        }
    })?;
    if let Some(w) = work {
        w.finish()?;
    }
    Ok(out)
}

/// The legacy shell supplies actual evaluated arrays, batch length and its
/// actual optional output type; it performs no numeric decoding itself.
pub fn evaluate_legacy_dround(
    operation: DroundComputation,
    value: &ArrayRef,
    digits: Option<&ArrayRef>,
    batch_rows: usize,
    output_type: Option<&DataType>,
) -> Result<ArrayRef, MathNumericError> {
    let value = NumericArrayView::new(value).map_err(MathNumericError::Legacy)?;
    let digits = digits
        .map(NumericArrayView::new)
        .transpose()
        .map_err(MathNumericError::Legacy)?;
    compute_dround(
        operation,
        Selection::all(batch_rows),
        output_type,
        None,
        |_, row| {
            Ok((
                value_at_f64(&value, row, batch_rows),
                digits
                    .as_ref()
                    .and_then(|digits| value_at_i64(digits, row, batch_rows)),
            ))
        },
    )
}
