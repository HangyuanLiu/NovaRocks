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
//! Original v1 capture extraction shared by full and selected calls.
//! Both leaves keep their original regex and JSON library semantics.
use super::string_extended::StringCoreInput;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::pattern_memo::PatternMemo;
use crate::{KernelEvaluationControl, KernelFailure, RowDataError, SelectedValues};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use regex::Regex;
use std::{alloc::Layout, sync::Arc};

/// The checked owner uses Int32; the legacy shell also preserves raw Int64.
enum IndexValues<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}
impl<'a> IndexValues<'a> {
    fn from_array(array: &'a ArrayRef) -> Result<Self, KernelFailure> {
        if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
            Ok(Self::Int32(values))
        } else if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
            Ok(Self::Int64(values))
        } else {
            Err(internal("regexp extraction index is not Int32 or Int64"))
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Int32(a) => a.len(),
            Self::Int64(a) => a.len(),
        }
    }
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Int32(a) => a.is_null(row),
            Self::Int64(a) => a.is_null(row),
        }
    }
    fn value(&self, row: usize) -> i64 {
        match self {
            Self::Int32(a) => a.value(row) as i64,
            Self::Int64(a) => a.value(row),
        }
    }
}
fn reserve<T>(len: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(len).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(values)
}
fn observe_bytes(text: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure> {
    for _ in text.as_bytes() {
        work.step()?;
    }
    Ok(())
}
enum RowValue {
    Value(String),
    Error(String),
}

/// The original single-capture body: compiling precedes the signed-to-usize cast.
fn extract_one<'a>(
    patterns: &mut PatternMemo<'a, Regex, regex::Error>,
    source: &StringArray,
    patterns_array: &'a StringArray,
    indices: &IndexValues<'_>,
    rows: [usize; 3],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<RowValue, KernelFailure> {
    let pattern = patterns_array.value(rows[1]);
    observe_bytes(pattern, work)?;
    work.flush()?;
    let compiled = patterns.get_or_compile(pattern, Regex::new);
    work.flush()?;
    let re = match compiled {
        Ok(re) => re,
        Err(error) => {
            work.flush()?;
            let message = error.to_string();
            work.flush()?;
            return Ok(RowValue::Error(message));
        }
    };
    let idx = indices.value(rows[2]) as usize;
    let source = source.value(rows[0]);
    observe_bytes(source, work)?;
    work.flush()?;
    let caps = re.captures(source);
    let val = caps
        .and_then(|c| c.get(idx))
        .map(|m| m.as_str().to_string())
        .unwrap_or_default();
    work.flush()?;
    Ok(RowValue::Value(val))
}

/// The original all-captures body: a negative group masks even invalid regex.
fn extract_all<'a>(
    patterns: &mut PatternMemo<'a, Regex, regex::Error>,
    source: &StringArray,
    patterns_array: &'a StringArray,
    group: i64,
    rows: [usize; 3],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<RowValue, KernelFailure> {
    if group < 0 {
        work.flush()?;
        let value = "[]".to_string();
        work.flush()?;
        return Ok(RowValue::Value(value));
    }
    let group_idx = group as usize;
    let pattern = patterns_array.value(rows[1]);
    observe_bytes(pattern, work)?;
    work.flush()?;
    let compiled = patterns.get_or_compile(pattern, Regex::new);
    work.flush()?;
    let re = match compiled {
        Ok(re) => re,
        Err(error) => {
            work.flush()?;
            let message = error.to_string();
            work.flush()?;
            return Ok(RowValue::Error(message));
        }
    };
    let mut matches = Vec::new();
    let source = source.value(rows[0]);
    observe_bytes(source, work)?;
    work.flush()?;
    let mut captures = re.captures_iter(source);
    work.flush()?;
    loop {
        // Searching and capture matching are opaque operations of the original
        // regex library; observation surrounds each such operation.
        work.flush()?;
        let next = captures.next();
        work.flush()?;
        let Some(caps) = next else { break };
        if let Some(matched) = caps.get(group_idx) {
            work.flush()?;
            let value = matched.as_str().to_string();
            work.flush()?;
            observe_bytes(&value, work)?;
            work.flush()?;
            matches
                .try_reserve(1)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            matches.push(value);
            work.flush()?;
        }
        work.step()?;
    }
    work.flush()?;
    let json = serde_json::to_string(&matches);
    work.flush()?;
    match json {
        Ok(json) => Ok(RowValue::Value(json)),
        Err(error) => {
            work.flush()?;
            let message = error.to_string();
            work.flush()?;
            Ok(RowValue::Error(message))
        }
    }
}

pub(super) fn evaluate_selected<'a>(
    all: bool,
    input: StringCoreInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        if input.arguments.len() != 3 {
            return Err(invalid(
                "regexp extraction requires three selected arguments",
            ));
        }
        let source = input.arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("regexp extraction source is not Utf8"))?;
        let pattern = input.arguments[1]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("regexp extraction pattern is not Utf8"))?;
        let indices = IndexValues::from_array(input.arguments[2].array())?;
        let selection = input.selection;
        let mut patterns = PatternMemo::new();
        let mut bytes = reserve::<u8>(0, &mut work)?;
        let mut offsets = reserve::<i32>(
            selection
                .len()
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?,
            &mut work,
        )?;
        let mut errors = reserve::<RowDataError>(0, &mut work)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        offsets.push(0);
        let mut has_null = false;
        for (ordinal, batch) in selection.iter().enumerate() {
            let rows = [
                input.arguments[0].value_row(ordinal, batch),
                input.arguments[1].value_row(ordinal, batch),
                input.arguments[2].value_row(ordinal, batch),
            ];
            work.step()?;
            if rows[0] >= source.len() || rows[1] >= pattern.len() || rows[2] >= indices.len() {
                return Err(internal(
                    "regexp extraction selected argument row out of bounds",
                ));
            }
            let mut valid = false;
            if !source.is_null(rows[0]) && !pattern.is_null(rows[1]) && !indices.is_null(rows[2]) {
                work.step()?;
                let row = if all {
                    let group = indices.value(rows[2]);
                    extract_all(&mut patterns, source, pattern, group, rows, &mut work)?
                } else {
                    extract_one(&mut patterns, source, pattern, &indices, rows, &mut work)?
                };
                match row {
                    RowValue::Value(value) => {
                        let extent = bytes
                            .len()
                            .checked_add(value.len())
                            .ok_or(KernelFailure::ResourceExhausted)?;
                        i32::try_from(extent).map_err(|_| KernelFailure::ResourceExhausted)?;
                        work.flush()?;
                        bytes
                            .try_reserve(value.len())
                            .map_err(|_| KernelFailure::ResourceExhausted)?;
                        work.flush()?;
                        for byte in value.as_bytes() {
                            bytes.push(*byte);
                            work.step()?;
                        }
                        valid = true;
                    }
                    RowValue::Error(message) => {
                        observe_bytes(&message, &mut work)?;
                        if let Some(boundary) = input.error_boundary {
                            work.flush()?;
                            boundary(&message)?;
                            work.flush()?;
                        }
                        work.flush()?;
                        errors
                            .try_reserve(1)
                            .map_err(|_| KernelFailure::ResourceExhausted)?;
                        errors.push(RowDataError::new(ordinal, &message));
                        work.flush()?;
                    }
                }
            }
            validity.append(valid);
            has_null |= !valid;
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        work.flush()?;
        let array = Arc::new(StringArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            has_null.then(|| NullBuffer::new(validity.finish())),
        )) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &DataType::Utf8,
            array,
            errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    work.finish_result(result)
}

#[cfg(test)]
mod tests {
    use super::super::string_extended::{StringOperation, evaluate_legacy};
    use super::*;
    use crate::{EvaluatedArgument, Selection};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    struct Control {
        calls: AtomicUsize,
        reject: Option<usize>,
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let at = self.calls.fetch_add(1, Ordering::Relaxed);
            if self.reject == Some(at) {
                Err(KernelFailure::DeadlineExceeded)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("regexp extraction must not wait")
        }
    }
    fn control(reject: Option<usize>) -> Control {
        Control {
            calls: AtomicUsize::new(0),
            reject,
        }
    }
    #[test]
    fn selected_regexp_extract_sparse_negative_masks_and_optional_capture() {
        let arrays = [
            Arc::new(StringArray::from(vec![
                Some("outside"),
                Some("a1b2"),
                Some("a"),
                None,
                Some("outside"),
                Some("b ab"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("(outside"),
                Some("([0-9])"),
                Some("(bad"),
                Some("[null_masked"),
                Some("[outside"),
                Some("(a)?b"),
            ])) as ArrayRef,
            Arc::new(Int32Array::from(vec![
                Some(0),
                Some(0),
                Some(-1),
                Some(1),
                Some(0),
                Some(1),
            ])) as ArrayRef,
        ];
        let arguments = arrays.each_ref().map(EvaluatedArgument::Column);
        let rows = [1, 2, 3, 5];
        let input = StringCoreInput {
            arguments: &arguments,
            selection: Selection::try_sparse(6, &rows).unwrap(),
            error_boundary: None,
        };
        let one = evaluate_selected(false, input, &control(None)).unwrap();
        assert_eq!(
            one.values().to_data(),
            StringArray::from(vec![Some("1"), None, None, Some("")]).to_data()
        );
        assert_eq!(one.errors().len(), 1);
        assert_eq!(one.errors()[0].selected_ordinal(), 1);
        let all = evaluate_selected(true, input, &control(None)).unwrap();
        assert_eq!(
            all.values().to_data(),
            StringArray::from(vec![
                Some("[\"1\",\"2\"]"),
                Some("[]"),
                None,
                Some("[\"a\"]")
            ])
            .to_data()
        );
        assert!(all.errors().is_empty());
    }
    #[test]
    fn selected_regexp_extract_legacy_preserves_raw_int64_index_projection() {
        let arrays = [
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("a"),
                Some("a"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("(a)"); 4])) as ArrayRef,
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(-1),
                Some(i64::MAX),
                Some(1),
            ])) as ArrayRef,
        ];
        let one = evaluate_legacy(StringOperation::RegexpExtract, &arrays, 4).unwrap();
        assert_eq!(
            one.to_data(),
            StringArray::from(vec![Some(""), Some(""), Some(""), Some("a")]).to_data()
        );
        let all = evaluate_legacy(StringOperation::RegexpExtractAll, &arrays, 4).unwrap();
        assert_eq!(
            all.to_data(),
            StringArray::from(vec![Some("[]"), Some("[]"), Some("[]"), Some("[\"a\"]")]).to_data()
        );
    }
    #[test]
    fn selected_regexp_extract_legacy_full_raw_error_before_bounded_owner_boundary() {
        let pattern = format!("{}(", "a".repeat(900));
        let expected = Regex::new(&pattern).unwrap_err().to_string();
        assert!(expected.len() > 512);
        let arrays = [
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(pattern.as_str()),
                Some("(later"),
            ])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(0), Some(0)])) as ArrayRef,
        ];
        for op in [
            StringOperation::RegexpExtract,
            StringOperation::RegexpExtractAll,
        ] {
            assert_eq!(
                evaluate_legacy(op, &arrays, 2).unwrap_err().as_bytes(),
                expected.as_bytes()
            );
        }
        let arguments = arrays.each_ref().map(EvaluatedArgument::Column);
        for all in [false, true] {
            let output = evaluate_selected(
                all,
                StringCoreInput {
                    arguments: &arguments,
                    selection: Selection::all(2),
                    error_boundary: None,
                },
                &control(None),
            )
            .unwrap();
            assert_eq!(output.errors().len(), 2);
            assert!(expected.starts_with(output.errors()[0].message()));
            assert!(output.errors()[0].message().len() <= crate::MAX_ROW_ERROR_MESSAGE_BYTES);
        }
    }
    #[test]
    fn selected_regexp_extract_first_control_refusal_keeps_deadline_category() {
        let text = "ab".repeat(32);
        let arrays = [
            Arc::new(StringArray::from(vec![Some(text.as_str())])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("(a)")])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(1)])) as ArrayRef,
        ];
        let arguments = arrays.each_ref().map(EvaluatedArgument::Column);
        let input = StringCoreInput {
            arguments: &arguments,
            selection: Selection::all(1),
            error_boundary: None,
        };
        for all in [false, true] {
            let complete = control(None);
            evaluate_selected(all, input, &complete).unwrap();
            for reject in 0..complete.calls.load(Ordering::Relaxed) {
                let refused = control(Some(reject));
                assert!(matches!(
                    evaluate_selected(all, input, &refused),
                    Err(KernelFailure::DeadlineExceeded)
                ));
                assert_eq!(refused.calls.load(Ordering::Relaxed), reject + 1);
            }
        }
    }
}
