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
//! Original v1 regexp_replace computation shared by full and selected calls.
use super::string_extended::StringCoreInput;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::pattern_memo::PatternMemo;
use crate::{KernelEvaluationControl, KernelFailure, RowDataError, SelectedValues};
use arrow_array::{Array, ArrayRef, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use regex::Regex;
use std::alloc::Layout;
use std::sync::Arc;

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

pub(super) fn evaluate_selected<'a>(
    input: StringCoreInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        if input.arguments.len() != 3 {
            return Err(invalid("regexp_replace requires three selected arguments"));
        }
        let strings = input
            .arguments
            .iter()
            .map(|arg| {
                arg.array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("regexp_replace input is not Utf8"))
            })
            .collect::<Result<Vec<_>, _>>()?;
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
            let mut rows = [0usize; 3];
            let mut is_null = false;
            for index in 0..3 {
                rows[index] = input.arguments[index].value_row(ordinal, batch);
                work.step()?;
                if rows[index] >= strings[index].len() {
                    return Err(internal(
                        "regexp_replace selected argument row out of bounds",
                    ));
                }
                is_null |= strings[index].is_null(rows[index]);
            }
            let mut valid = false;
            if !is_null {
                let source = strings[0].value(rows[0]);
                let pattern = strings[1].value(rows[1]);
                let replacement = strings[2].value(rows[2]);
                observe_bytes(source, &mut work)?;
                observe_bytes(pattern, &mut work)?;
                observe_bytes(replacement, &mut work)?;
                work.flush()?;
                // The original library owns compilation and replacement semantics.
                let compiled = patterns.get_or_compile(pattern, Regex::new);
                work.flush()?;
                match compiled {
                    Ok(re) => {
                        work.flush()?;
                        let value = re.replace_all(source, replacement).to_string();
                        work.flush()?;
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
                    Err(error) => {
                        work.flush()?;
                        let message = error.to_string();
                        work.flush()?;
                        observe_bytes(&message, &mut work)?;
                        // Legacy captures the raw first error and fails immediately.
                        // Only the owner boundary truncates row-error diagnostics.
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
                Err(KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("regexp_replace must not wait")
        }
    }
    fn control(reject: Option<usize>) -> Control {
        Control {
            calls: AtomicUsize::new(0),
            reject,
        }
    }
    #[test]
    fn selected_regexp_replace_sparse_masks_and_maps_original_rows() {
        let arrays = [
            Arc::new(StringArray::from(vec![
                Some("outside"),
                Some("中a"),
                Some("outside"),
                Some("ab"),
                Some("ab"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("(masked"),
                Some(""),
                Some("[masked"),
                Some("(bad"),
                Some("(null_masked"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("!"),
                Some("-"),
                Some("!"),
                Some("!"),
                None,
            ])) as ArrayRef,
        ];
        let arguments = arrays.each_ref().map(EvaluatedArgument::Column);
        let rows = [1, 3, 4];
        let selection = Selection::try_sparse(5, &rows).unwrap();
        let result = evaluate_selected(
            StringCoreInput {
                arguments: &arguments,
                selection,
                error_boundary: None,
            },
            &control(None),
        )
        .unwrap();
        assert_eq!(
            result.values().to_data(),
            StringArray::from(vec![Some("-中-a-"), None, None]).to_data()
        );
        assert_eq!(result.errors().len(), 1);
        assert_eq!(result.errors()[0].selected_ordinal(), 1);
        assert_eq!(
            result.errors()[0].message(),
            Regex::new("(bad").unwrap_err().to_string()
        );
    }
    #[test]
    fn selected_regexp_replace_full_raw_error_callback_before_bounded_error() {
        let pattern = format!("{}(", "a".repeat(900));
        let expected = Regex::new(&pattern).unwrap_err().to_string();
        let arrays = [
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(pattern.as_str()),
                Some("(later"),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("-"), Some("-")])) as ArrayRef,
        ];
        let arguments = arrays.each_ref().map(EvaluatedArgument::Column);
        let message = std::cell::RefCell::new(None);
        let boundary = |raw: &str| {
            *message.borrow_mut() = Some(raw.to_string());
            Err(KernelFailure::InstanceFailed)
        };
        let result = evaluate_selected(
            StringCoreInput {
                arguments: &arguments,
                selection: Selection::all(2),
                error_boundary: Some(&boundary),
            },
            &control(None),
        );
        assert!(matches!(result, Err(KernelFailure::InstanceFailed)));
        let message = message.into_inner().unwrap();
        assert!(message.len() > 512);
        assert_eq!(message.as_bytes(), expected.as_bytes());
        let result = evaluate_selected(
            StringCoreInput {
                arguments: &arguments,
                selection: Selection::all(2),
                error_boundary: None,
            },
            &control(None),
        )
        .unwrap();
        assert_eq!(result.errors().len(), 2);
        assert!(expected.starts_with(result.errors()[0].message()));
        assert!(result.errors()[0].message().len() <= crate::MAX_ROW_ERROR_MESSAGE_BYTES);
    }
    #[test]
    fn selected_regexp_replace_first_control_refusal_is_not_row_error() {
        let text = "a".repeat(5000);
        let arrays = [
            Arc::new(StringArray::from(vec![Some(text.as_str())])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("a+")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("<$0>")])) as ArrayRef,
        ];
        let arguments = arrays.each_ref().map(EvaluatedArgument::Column);
        let input = StringCoreInput {
            arguments: &arguments,
            selection: Selection::all(1),
            error_boundary: None,
        };
        let complete = control(None);
        evaluate_selected(input, &complete).unwrap();
        for reject in 0..complete.calls.load(Ordering::Relaxed) {
            let refused = control(Some(reject));
            assert!(matches!(
                evaluate_selected(input, &refused),
                Err(KernelFailure::Cancelled)
            ));
            assert_eq!(refused.calls.load(Ordering::Relaxed), reject + 1);
        }
    }
}
