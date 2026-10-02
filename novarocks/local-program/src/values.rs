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

//! Literal column backing held by a program, before any task memory charge.

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use crate::StaticLayout;

/// The frozen descriptor is capped at 16 MiB by task-codec. Values have an
/// independent cap because Arrow materialization can expand the wire input.
pub const MAX_STATIC_VALUES_BACKING_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StaticValues {
    batch: Arc<RecordBatch>,
    layout: StaticLayout,
    retained_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticValuesError {
    SchemaMismatch,
    TooManyBytes,
}

impl fmt::Display for StaticValuesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid static values backing: {self:?}")
    }
}

impl std::error::Error for StaticValuesError {}

/// Pure construction preserves structural diagnostics and the caller's original
/// interruption separately. Neither branch grants backing memory admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValuesCompileError {
    Values(StaticValuesError),
    Control(CompileControlError),
}
impl From<StaticValuesError> for ValuesCompileError {
    fn from(error: StaticValuesError) -> Self {
        Self::Values(error)
    }
}
impl From<CompileControlError> for ValuesCompileError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for ValuesCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Values(error) => error.fmt(formatter),
            Self::Control(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for ValuesCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Values(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}

impl StaticValues {
    pub fn try_new(batch: RecordBatch, layout: StaticLayout) -> Result<Self, StaticValuesError> {
        match Self::try_new_core(batch, layout, None) {
            Ok(value) => Ok(value),
            Err(ValuesCompileError::Values(error)) => Err(error),
            Err(ValuesCompileError::Control(_)) => {
                unreachable!("legacy values have no compile control")
            }
        }
    }

    pub fn try_new_for_compile(
        batch: RecordBatch,
        layout: StaticLayout,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ValuesCompileError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = Self::try_new_core(batch, layout, Some(&mut work));
        if matches!(&result, Err(ValuesCompileError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn try_new_core(
        batch: RecordBatch,
        layout: StaticLayout,
        mut work: Option<&mut CompileCheckpoints<'_>>,
    ) -> Result<Self, ValuesCompileError> {
        // Arrow's existing schema equality and retained-size traversal remain
        // opaque. These observations neither change their admission semantics
        // nor claim an internal quantum, unique ownership, or a MEM grant.
        if let Some(work) = &mut work {
            work.flush()?;
        }
        let same_schema = batch.schema().as_ref() == layout.schema().as_ref();
        if let Some(work) = &mut work {
            work.flush()?;
        }
        if !same_schema {
            return Err(StaticValuesError::SchemaMismatch.into());
        }
        if let Some(work) = &mut work {
            work.flush()?;
        }
        let retained_bytes = batch.get_array_memory_size();
        if let Some(work) = &mut work {
            work.flush()?;
        }
        let too_many_bytes = retained_bytes > MAX_STATIC_VALUES_BACKING_BYTES;
        if let Some(work) = &mut work {
            work.step()?;
        }
        if too_many_bytes {
            return Err(StaticValuesError::TooManyBytes.into());
        }
        if let Some(work) = &mut work {
            work.flush()?;
        }
        let result = Self {
            batch: Arc::new(batch),
            layout,
            retained_bytes,
        };
        if let Some(work) = &mut work {
            work.flush()?;
        }
        Ok(result)
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub fn layout(&self) -> &StaticLayout {
        &self.layout
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use novarocks_types::SlotId;

    #[test]
    fn static_values_clone_shares_arrow_buffers_without_runtime_charge() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let layout = StaticLayout::try_new(schema, Arc::from([SlotId::new(7)])).unwrap();
        let values = StaticValues::try_new(batch, layout).unwrap();
        let cloned = values.clone();
        assert!(Arc::ptr_eq(&values.batch, &cloned.batch));
        assert_eq!(values.retained_bytes(), cloned.retained_bytes());
    }

    #[test]
    fn static_values_rejects_layout_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let layout = StaticLayout::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            Arc::from([SlotId::new(7)]),
        )
        .unwrap();
        assert!(matches!(
            StaticValues::try_new(batch, layout),
            Err(StaticValuesError::SchemaMismatch)
        ));
    }
    #[derive(Default)]
    struct Control {
        calls: std::sync::Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::LowerProgram);
            let mut calls = self.calls.lock().unwrap();
            calls.push(units);
            if let Some((call, error)) = self.refusal
                && calls.len() == call
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn input() -> (RecordBatch, StaticLayout) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let layout = StaticLayout::try_new(schema, Arc::from([SlotId::new(7)])).unwrap();
        (batch, layout)
    }
    #[test]
    fn compile_values_observe_opaque_boundaries_preserving_backing_and_legacy_charge() {
        let (batch, layout) = input();
        let array = batch.column(0).clone();
        let schema = batch.schema();
        let legacy = StaticValues::try_new(batch.clone(), layout.clone()).unwrap();
        let control = Control::default();
        let actual = StaticValues::try_new_for_compile(batch, layout, &control).unwrap();
        assert!(Arc::ptr_eq(actual.batch().column(0), &array));
        assert!(Arc::ptr_eq(&actual.batch().schema(), &schema));
        assert_eq!(actual.retained_bytes(), legacy.retained_bytes());
        assert_eq!(actual.batch(), legacy.batch());
        assert_eq!(*control.calls.lock().unwrap(), vec![0, 0, 0, 0, 0, 1, 0, 0]);
    }
    #[test]
    fn compile_values_refuse_every_boundary_with_original_cause_and_no_recheck() {
        use std::error::Error;
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for call in 1..=8 {
                let (batch, layout) = input();
                let control = Control {
                    calls: Default::default(),
                    refusal: Some((call, cause)),
                };
                let error = StaticValues::try_new_for_compile(batch, layout, &control).unwrap_err();
                assert_eq!(error, ValuesCompileError::Control(cause));
                assert_eq!(
                    error
                        .source()
                        .unwrap()
                        .downcast_ref::<CompileControlError>(),
                    Some(&cause)
                );
                assert_eq!(control.calls.lock().unwrap().len(), call);
            }
        }
    }
    #[test]
    fn compile_values_schema_error_observes_completion_before_returning_or_interrupting() {
        let (batch, _) = input();
        let layout = StaticLayout::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int64,
                false,
            )])),
            Arc::from([SlotId::new(7)]),
        )
        .unwrap();
        let ordinary = Control::default();
        assert!(matches!(
            StaticValues::try_new_for_compile(batch.clone(), layout.clone(), &ordinary),
            Err(ValuesCompileError::Values(
                StaticValuesError::SchemaMismatch
            ))
        ));
        assert_eq!(*ordinary.calls.lock().unwrap(), vec![0, 0, 0, 0]);
        let control = Control {
            calls: Default::default(),
            refusal: Some((4, CompileControlError::DeadlineExceeded)),
        };
        assert!(matches!(
            StaticValues::try_new_for_compile(batch, layout, &control),
            Err(ValuesCompileError::Control(
                CompileControlError::DeadlineExceeded
            ))
        ));
        assert_eq!(*control.calls.lock().unwrap(), vec![0, 0, 0, 0]);
    }
}
