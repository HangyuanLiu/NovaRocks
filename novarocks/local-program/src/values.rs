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
//!
//! A cell is either a materialized constant or a dynamic cell: a first-class
//! `ValuesCell { row, column }` expression root that the one compiled evaluator
//! evaluates at runtime, once, over an empty input port. A dynamic cell never
//! occupies a placeholder in an Arrow array; its column keeps only its constant
//! cells, in row order and exactly in the column's frozen carrier.

use std::fmt;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use crate::{ProgramExprId, StaticLayout};

/// The frozen descriptor is capped at 16 MiB by task-codec. Values have an
/// independent cap because Arrow materialization can expand the wire input.
pub const MAX_STATIC_VALUES_BACKING_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StaticValues {
    backing: ValuesBacking,
    layout: StaticLayout,
    retained_bytes: usize,
}

#[derive(Clone, Debug)]
enum ValuesBacking {
    Batch(Arc<RecordBatch>),
    Cells {
        rows: usize,
        constants: Arc<[ArrayRef]>,
        dynamic: Arc<[StaticValuesCell]>,
    },
}

/// One dynamic cell position and the main-arena definition its
/// `ValuesCell { row, column }` root evaluates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StaticValuesCell {
    pub row: u32,
    pub column: u32,
    pub definition: ProgramExprId,
}

/// Borrowed view of one Values backing.
#[derive(Clone, Copy, Debug)]
pub enum StaticValuesBacking<'a> {
    /// Every cell is a constant: the complete batch in the layout schema.
    Batch(&'a RecordBatch),
    /// At least one cell is dynamic. Each column holds only its constant
    /// cells, in row order; `dynamic` is strictly increasing in (row, column).
    Cells {
        rows: usize,
        constants: &'a [ArrayRef],
        dynamic: &'a [StaticValuesCell],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticValuesError {
    SchemaMismatch,
    TooManyBytes,
    /// A cell-backed Values has no dynamic cell; an all-constant one is a batch.
    NoDynamicCell,
    /// A dynamic cell names a row or column outside the Values shape.
    CellOutOfRange,
    /// Dynamic cells are not strictly increasing in (row, column), so a
    /// position is repeated or the order is not the evaluation order.
    CellOrder,
    /// A column's constant and dynamic cells do not cover its rows exactly.
    ColumnCoverage,
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
            backing: ValuesBacking::Batch(Arc::new(batch)),
            layout,
            retained_bytes,
        };
        if let Some(work) = &mut work {
            work.flush()?;
        }
        Ok(result)
    }

    /// Values with at least one dynamic cell. `constants` holds, per layout
    /// column, exactly that column's constant cells in row order and in the
    /// column's frozen carrier; `dynamic` names every other position, strictly
    /// increasing in (row, column). The cell definitions are checked against
    /// the program's expression roots, channel types and lexical bindings.
    pub fn try_new_with_cells_for_compile(
        rows: usize,
        constants: Vec<ArrayRef>,
        dynamic: Vec<StaticValuesCell>,
        layout: StaticLayout,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ValuesCompileError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = Self::try_new_cells_core(rows, constants, dynamic, layout, &mut work);
        if matches!(&result, Err(ValuesCompileError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn try_new_cells_core(
        rows: usize,
        constants: Vec<ArrayRef>,
        dynamic: Vec<StaticValuesCell>,
        layout: StaticLayout,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ValuesCompileError> {
        let fields = layout.schema().fields();
        work.step()?;
        if dynamic.is_empty() {
            return Err(StaticValuesError::NoDynamicCell.into());
        }
        if constants.len() != fields.len() {
            return Err(StaticValuesError::ColumnCoverage.into());
        }
        let mut per_column = Vec::new();
        per_column
            .try_reserve_exact(fields.len())
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        for _ in 0..fields.len() {
            per_column.push(0usize);
            work.step()?;
        }
        let mut previous: Option<(u32, u32)> = None;
        for cell in &dynamic {
            work.step()?;
            let in_range = usize::try_from(cell.row).is_ok_and(|row| row < rows)
                && usize::try_from(cell.column).is_ok_and(|column| column < fields.len());
            if !in_range {
                return Err(StaticValuesError::CellOutOfRange.into());
            }
            let position = (cell.row, cell.column);
            if previous.is_some_and(|previous| previous >= position) {
                return Err(StaticValuesError::CellOrder.into());
            }
            previous = Some(position);
            per_column[cell.column as usize] += 1;
        }
        let mut retained_bytes = 0usize;
        for ((array, field), dynamic) in constants.iter().zip(fields.iter()).zip(&per_column) {
            work.step()?;
            if array.len().checked_add(*dynamic) != Some(rows) {
                return Err(StaticValuesError::ColumnCoverage.into());
            }
            // Arrow's type equality, null count and retained-size traversal
            // remain opaque; they claim no internal quantum or MEM grant.
            work.flush()?;
            let same = array.data_type() == field.data_type()
                && (field.is_nullable() || array.null_count() == 0);
            work.flush()?;
            if !same {
                return Err(StaticValuesError::SchemaMismatch.into());
            }
            let bytes = array.get_array_memory_size();
            work.flush()?;
            retained_bytes = retained_bytes
                .checked_add(bytes)
                .filter(|total| *total <= MAX_STATIC_VALUES_BACKING_BYTES)
                .ok_or(StaticValuesError::TooManyBytes)?;
        }
        work.flush()?;
        Ok(Self {
            backing: ValuesBacking::Cells {
                rows,
                constants: Arc::from(constants),
                dynamic: Arc::from(dynamic),
            },
            layout,
            retained_bytes,
        })
    }

    /// The complete constant batch, present exactly when no cell is dynamic.
    pub fn batch(&self) -> Option<&RecordBatch> {
        match &self.backing {
            ValuesBacking::Batch(batch) => Some(batch),
            ValuesBacking::Cells { .. } => None,
        }
    }

    pub fn backing(&self) -> StaticValuesBacking<'_> {
        match &self.backing {
            ValuesBacking::Batch(batch) => StaticValuesBacking::Batch(batch),
            ValuesBacking::Cells {
                rows,
                constants,
                dynamic,
            } => StaticValuesBacking::Cells {
                rows: *rows,
                constants,
                dynamic,
            },
        }
    }

    pub fn num_rows(&self) -> usize {
        match &self.backing {
            ValuesBacking::Batch(batch) => batch.num_rows(),
            ValuesBacking::Cells { rows, .. } => *rows,
        }
    }

    /// Every dynamic cell in row-major order; empty for a constant batch.
    pub fn dynamic_cells(&self) -> &[StaticValuesCell] {
        match &self.backing {
            ValuesBacking::Batch(_) => &[],
            ValuesBacking::Cells { dynamic, .. } => dynamic,
        }
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
        let (ValuesBacking::Batch(left), ValuesBacking::Batch(right)) =
            (&values.backing, &cloned.backing)
        else {
            panic!("constant values keep a batch")
        };
        assert!(Arc::ptr_eq(left, right));
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
        assert!(Arc::ptr_eq(actual.batch().unwrap().column(0), &array));
        assert!(Arc::ptr_eq(&actual.batch().unwrap().schema(), &schema));
        assert_eq!(actual.retained_bytes(), legacy.retained_bytes());
        assert_eq!(actual.batch(), legacy.batch());
        assert!(actual.dynamic_cells().is_empty());
        assert_eq!(actual.num_rows(), 3);
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

    fn cell_layout() -> StaticLayout {
        StaticLayout::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, true),
            ])),
            Arc::from([SlotId::new(1), SlotId::new(2)]),
        )
        .unwrap()
    }
    fn cell(row: u32, column: u32, definition: usize) -> StaticValuesCell {
        StaticValuesCell {
            row,
            column,
            definition: crate::ProgramExprId::new(definition),
        }
    }
    fn int64(values: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(values))
    }
    fn cells(
        constants: Vec<ArrayRef>,
        dynamic: Vec<StaticValuesCell>,
    ) -> Result<StaticValues, ValuesCompileError> {
        StaticValues::try_new_with_cells_for_compile(
            3,
            constants,
            dynamic,
            cell_layout(),
            &Control::default(),
        )
    }

    #[test]
    fn cell_values_keep_only_constant_cells_and_row_major_dynamic_positions() {
        let first = int64(vec![Some(1), Some(2)]);
        let second = int64(vec![None, Some(7)]);
        let dynamic = vec![cell(0, 1, 4), cell(2, 0, 5)];
        let values = cells(vec![first.clone(), second.clone()], dynamic.clone()).unwrap();
        assert!(values.batch().is_none());
        assert_eq!(values.num_rows(), 3);
        assert_eq!(values.dynamic_cells(), dynamic.as_slice());
        let StaticValuesBacking::Cells {
            rows,
            constants,
            dynamic: actual,
        } = values.backing()
        else {
            panic!("dynamic values keep cell sources")
        };
        assert_eq!(rows, 3);
        assert!(Arc::ptr_eq(&constants[0], &first));
        assert!(Arc::ptr_eq(&constants[1], &second));
        assert_eq!(actual, dynamic.as_slice());
        assert_eq!(
            values.retained_bytes(),
            first.get_array_memory_size() + second.get_array_memory_size()
        );
        // A column whose every cell is dynamic keeps an empty constant array.
        let all_dynamic = cells(
            vec![int64(vec![]), int64(vec![Some(1), None, Some(3)])],
            vec![cell(0, 0, 1), cell(1, 0, 2), cell(2, 0, 3)],
        )
        .unwrap();
        assert_eq!(all_dynamic.dynamic_cells().len(), 3);
    }

    #[test]
    fn cell_values_refuse_invalid_positions_coverage_and_carriers() {
        let valid = || vec![int64(vec![Some(1), Some(2)]), int64(vec![None, Some(7)])];
        let refused = |constants, dynamic| match cells(constants, dynamic) {
            Err(ValuesCompileError::Values(error)) => error,
            other => panic!("expected a structural refusal, got {other:?}"),
        };
        let all_constant = vec![
            int64(vec![Some(1), Some(2), Some(3)]),
            int64(vec![None, Some(7), None]),
        ];
        assert_eq!(
            refused(all_constant, vec![]),
            StaticValuesError::NoDynamicCell
        );
        for dynamic in [
            vec![cell(3, 0, 1), cell(3, 1, 2)],
            vec![cell(0, 2, 1), cell(2, 0, 2)],
        ] {
            assert_eq!(refused(valid(), dynamic), StaticValuesError::CellOutOfRange);
        }
        for dynamic in [
            vec![cell(0, 1, 4), cell(0, 1, 5)],
            vec![cell(2, 0, 5), cell(0, 1, 4)],
        ] {
            assert_eq!(refused(valid(), dynamic), StaticValuesError::CellOrder);
        }
        let dynamic = || vec![cell(0, 1, 4), cell(2, 0, 5)];
        assert_eq!(
            refused(
                vec![
                    int64(vec![Some(1), Some(2), Some(3)]),
                    int64(vec![None, Some(7)])
                ],
                dynamic()
            ),
            StaticValuesError::ColumnCoverage
        );
        assert_eq!(
            refused(vec![int64(vec![Some(1), Some(2)])], dynamic()),
            StaticValuesError::ColumnCoverage
        );
        let narrow: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![1, 2]));
        assert_eq!(
            refused(vec![narrow, int64(vec![None, Some(7)])], dynamic()),
            StaticValuesError::SchemaMismatch
        );
        // A NULL constant in a non-null column is not a valid backing value.
        assert_eq!(
            refused(
                vec![int64(vec![Some(1), None]), int64(vec![None, Some(7)])],
                dynamic()
            ),
            StaticValuesError::SchemaMismatch
        );
    }

    #[test]
    fn cell_values_refuse_every_boundary_with_original_cause_and_no_recheck() {
        let constants = || vec![int64(vec![Some(1), Some(2)]), int64(vec![None, Some(7)])];
        let dynamic = || vec![cell(0, 1, 4), cell(2, 0, 5)];
        let baseline = Control::default();
        StaticValues::try_new_with_cells_for_compile(
            3,
            constants(),
            dynamic(),
            cell_layout(),
            &baseline,
        )
        .unwrap();
        let observed = baseline.calls.lock().unwrap().len();
        assert!(observed > 1);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for call in 1..=observed {
                let control = Control {
                    calls: Default::default(),
                    refusal: Some((call, cause)),
                };
                let error = StaticValues::try_new_with_cells_for_compile(
                    3,
                    constants(),
                    dynamic(),
                    cell_layout(),
                    &control,
                )
                .unwrap_err();
                assert_eq!(error, ValuesCompileError::Control(cause));
                assert_eq!(control.calls.lock().unwrap().len(), call);
            }
        }
    }
}
