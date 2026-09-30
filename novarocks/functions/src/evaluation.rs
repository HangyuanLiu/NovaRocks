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

//! Carrier-neutral scalar evaluation inputs and per-row data errors.
//! Control flow and argument evaluation belong to the expression runtime.

use std::fmt;

use arrow_array::ArrayRef;
use arrow_schema::DataType;

/// A checked, ordered set of original batch rows. The dense representation
/// allocates nothing; sparse storage belongs to the evaluation instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Selection<'a> {
    batch_rows: usize,
    rows: Option<&'a [usize]>,
}

impl<'a> Selection<'a> {
    pub const fn all(batch_rows: usize) -> Self {
        Self {
            batch_rows,
            rows: None,
        }
    }

    pub fn try_sparse(
        batch_rows: usize,
        rows: &'a [usize],
    ) -> Result<Self, EvaluationContractError> {
        if rows.last().is_some_and(|row| *row >= batch_rows)
            || rows.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(EvaluationContractError::InvalidSelection);
        }
        if rows.len() == batch_rows {
            return Ok(Self::all(batch_rows));
        }
        Ok(Self {
            batch_rows,
            rows: Some(rows),
        })
    }

    pub const fn batch_rows(self) -> usize {
        self.batch_rows
    }

    pub fn len(self) -> usize {
        self.rows.map_or(self.batch_rows, <[usize]>::len)
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub const fn is_all(self) -> bool {
        self.rows.is_none()
    }

    /// Map a compact result ordinal back to its original input row. An error
    /// ordinal is never interpreted as a row from an unrelated Selection.
    pub fn row(self, selected_ordinal: usize) -> Option<usize> {
        self.rows.map_or_else(
            || (selected_ordinal < self.batch_rows).then_some(selected_ordinal),
            |rows| rows.get(selected_ordinal).copied(),
        )
    }

    pub fn iter(self) -> impl ExactSizeIterator<Item = usize> + 'a {
        (0..self.len()).map(move |ordinal| self.row(ordinal).expect("ordinal is in selection"))
    }
    /// Shared backing compares in O(1); distinct sparse backing observes each row.
    pub fn same_rows_observed<E>(
        self,
        other: Selection<'_>,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        if self.batch_rows != other.batch_rows || self.len() != other.len() {
            return Ok(false);
        }
        match (self.rows, other.rows) {
            (None, None) => Ok(true),
            (Some(left), Some(right)) if std::ptr::eq(left, right) => Ok(true),
            (Some(left), Some(right)) => {
                for (left, right) in left.iter().zip(right) {
                    observe()?;
                    if left != right {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// Broadcasting is explicit. A one-row column is not silently converted to
/// a scalar, and scalar NULL remains distinct from an unevaluated argument.
#[derive(Clone, Copy, Debug)]
pub enum EvaluatedArgument<'a> {
    Scalar(&'a ArrayRef),
    Column(&'a ArrayRef),
    SelectedColumn(&'a SelectedValues<'a>),
}

impl<'a> EvaluatedArgument<'a> {
    pub const fn array(self) -> &'a ArrayRef {
        match self {
            Self::Scalar(array) | Self::Column(array) => array,
            Self::SelectedColumn(output) => output.values(),
        }
    }

    /// After validation, use ordinals and rows from the same Selection walk.
    /// Compact arguments retain original row identity without a full scatter.
    pub const fn value_row(self, selected_ordinal: usize, batch_row: usize) -> usize {
        match self {
            Self::Scalar(_) => 0,
            Self::Column(_) => batch_row,
            Self::SelectedColumn(_) => selected_ordinal,
        }
    }

    pub fn validate(
        self,
        selection: Selection<'_>,
        exact_type: &DataType,
    ) -> Result<(), EvaluationContractError> {
        self.validate_shape(selection)?;
        if !novarocks_type_contract::arrow_data_types_exact(self.array().data_type(), exact_type) {
            return Err(EvaluationContractError::ArgumentType);
        }
        Ok(())
    }

    pub(crate) fn validate_shape(
        self,
        selection: Selection<'_>,
    ) -> Result<(), EvaluationContractError> {
        self.validate_shape_observed(selection, || Ok(()))
    }
    pub(crate) fn validate_shape_observed<E: From<EvaluationContractError>>(
        self,
        selection: Selection<'_>,
        observe: impl FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        let expected_rows = match self {
            Self::Scalar(_) => 1,
            Self::Column(_) => selection.batch_rows(),
            Self::SelectedColumn(output) => {
                if !output.selection.same_rows_observed(selection, observe)? {
                    return Err(EvaluationContractError::ArgumentSelection.into());
                }
                if !output.errors.is_empty() {
                    return Err(EvaluationContractError::ArgumentRowErrors.into());
                }
                selection.len()
            }
        };
        if self.array().len() != expected_rows {
            return Err(EvaluationContractError::ArgumentLength.into());
        }
        Ok(())
    }
}

/// Only errors explicitly classified by the implementation as row data errors
/// enter this channel. Resource, cancellation, deadline, plan and internal
/// failures remain outer failures and cannot be masked by Boolean decisions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowDataError {
    selected_ordinal: usize,
    message: Box<str>,
}

pub const MAX_ROW_ERROR_MESSAGE_BYTES: usize = 512;

impl RowDataError {
    pub fn new(selected_ordinal: usize, message: &str) -> Self {
        let mut end = message.len().min(MAX_ROW_ERROR_MESSAGE_BYTES);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            selected_ordinal,
            message: message[..end].into(),
        }
    }

    pub const fn selected_ordinal(&self) -> usize {
        self.selected_ordinal
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Compact values retain their Selection. An error ordinal has an outer
/// validity NULL (or Null-typed value), so validation needs no dictionary
/// expansion or logical-null bitmap allocation. A logical NULL with no error
/// is a successful SQL NULL. No unselected row appears in values or errors.
#[derive(Clone, Debug)]
pub struct SelectedValues<'a> {
    selection: Selection<'a>,
    values: ArrayRef,
    errors: Box<[RowDataError]>,
}

impl<'a> SelectedValues<'a> {
    pub fn try_new(
        selection: Selection<'a>,
        exact_type: &DataType,
        values: ArrayRef,
        errors: Box<[RowDataError]>,
    ) -> Result<Self, EvaluationContractError> {
        if values.len() != selection.len() {
            return Err(EvaluationContractError::ResultLength);
        }
        if !novarocks_type_contract::arrow_data_types_exact(values.data_type(), exact_type) {
            return Err(EvaluationContractError::ResultType);
        }
        if errors.len() > selection.len()
            || errors
                .windows(2)
                .any(|pair| pair[0].selected_ordinal >= pair[1].selected_ordinal)
            || errors.iter().any(|error| {
                error.selected_ordinal >= selection.len()
                    || (values.data_type() != &DataType::Null
                        && !values.is_null(error.selected_ordinal))
            })
        {
            return Err(EvaluationContractError::InvalidRowErrors);
        }
        Ok(Self {
            selection,
            values,
            errors,
        })
    }

    pub const fn selection(&self) -> Selection<'a> {
        self.selection
    }

    pub const fn values(&self) -> &ArrayRef {
        &self.values
    }

    pub fn errors(&self) -> &[RowDataError] {
        &self.errors
    }

    pub fn into_parts(self) -> (Selection<'a>, ArrayRef, Box<[RowDataError]>) {
        (self.selection, self.values, self.errors)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvaluationContractError {
    InvalidSelection,
    ArgumentLength,
    ArgumentType,
    ArgumentSelection,
    ArgumentRowErrors,
    ResultLength,
    ResultType,
    InvalidRowErrors,
}

impl fmt::Display for EvaluationContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidSelection => "selection rows must be ordered, unique and in the batch",
            Self::ArgumentLength => "evaluated argument length differs from its explicit shape",
            Self::ArgumentType => "evaluated argument type differs from its frozen binding",
            Self::ArgumentSelection => {
                "compact argument selection differs from the evaluation domain"
            }
            Self::ArgumentRowErrors => {
                "row errors must be resolved before calling an ordinary kernel"
            }
            Self::ResultLength => "kernel output length differs from the selected row count",
            Self::ResultType => "kernel output type differs from its frozen binding",
            Self::InvalidRowErrors => {
                "kernel row errors must be ordered, unique NULL result ordinals"
            }
        })
    }
}

impl std::error::Error for EvaluationContractError {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, NullArray};

    use super::*;

    #[test]
    fn selection_preserves_original_rows_without_dense_storage() {
        let dense = Selection::all(usize::MAX);
        assert_eq!(dense.len(), usize::MAX);
        assert_eq!(dense.row(usize::MAX - 1), Some(usize::MAX - 1));
        assert_eq!(dense.row(usize::MAX), None);
        let sparse = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
        assert_eq!(sparse.iter().collect::<Vec<_>>(), [1, 4, 9]);
        for invalid in [&[1, 1][..], &[4, 1], &[10]] {
            assert_eq!(
                Selection::try_sparse(10, invalid),
                Err(EvaluationContractError::InvalidSelection)
            );
        }
        assert!(Selection::try_sparse(0, &[]).unwrap().is_empty());
        assert!(Selection::all(0).is_empty());
    }

    #[test]
    fn scalar_broadcast_is_explicit_and_does_not_rebind_types() {
        let scalar = Arc::new(Int32Array::from(vec![Some(7)])) as ArrayRef;
        let selection = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
        EvaluatedArgument::Scalar(&scalar)
            .validate(selection, &DataType::Int32)
            .unwrap();
        assert_eq!(EvaluatedArgument::Scalar(&scalar).value_row(2, 9), 0);
        assert_eq!(
            EvaluatedArgument::Column(&scalar).validate(selection, &DataType::Int32),
            Err(EvaluationContractError::ArgumentLength)
        );
        assert_eq!(
            EvaluatedArgument::Scalar(&scalar).validate(selection, &DataType::Int64),
            Err(EvaluationContractError::ArgumentType)
        );
    }

    #[test]
    fn compact_children_keep_their_domain_at_the_parent_call() {
        let selection = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
        let other = Selection::try_sparse(10, &[0, 3, 8]).unwrap();
        let child = SelectedValues::try_new(
            selection,
            &DataType::Int32,
            Arc::new(Int32Array::from(vec![7, 8, 9])),
            Box::default(),
        )
        .unwrap();
        let argument = EvaluatedArgument::SelectedColumn(&child);
        argument.validate(selection, &DataType::Int32).unwrap();
        assert_eq!(argument.value_row(2, 9), 2);
        assert_eq!(
            argument.validate(other, &DataType::Int32),
            Err(EvaluationContractError::ArgumentSelection)
        );
        let (preserved, _, _) = child.clone().into_parts();
        assert_eq!(preserved, selection);
        assert_ne!(preserved, other);
        assert_eq!(
            Selection::try_sparse(3, &[0, 1, 2]).unwrap(),
            Selection::all(3)
        );
    }

    #[test]
    fn sparse_row_errors_are_distinct_from_successful_nulls() {
        let selection = Selection::try_sparse(10, &[1, 4, 9]).unwrap();
        let values = Arc::new(Int32Array::from(vec![None, Some(7), None])) as ArrayRef;
        let output = SelectedValues::try_new(
            selection,
            &DataType::Int32,
            values.clone(),
            Box::from([RowDataError::new(2, "invalid input")]),
        )
        .unwrap();
        assert_eq!(
            selection.row(output.errors()[0].selected_ordinal()),
            Some(9)
        );
        assert!(output.values().is_null(0)); // Successful SQL NULL.
        assert_eq!(output.selection(), selection);
        assert_eq!(
            EvaluatedArgument::SelectedColumn(&output).validate(selection, &DataType::Int32),
            Err(EvaluationContractError::ArgumentRowErrors)
        );
        for ordinal in [1, 3] {
            assert!(
                SelectedValues::try_new(
                    selection,
                    &DataType::Int32,
                    values.clone(),
                    Box::from([RowDataError::new(ordinal, "invalid input")])
                )
                .is_err()
            );
        }
        assert!(
            SelectedValues::try_new(
                selection,
                &DataType::Int32,
                values.clone(),
                Box::from([
                    RowDataError::new(2, "first"),
                    RowDataError::new(2, "duplicate")
                ])
            )
            .is_err()
        );
        let nulls = Arc::new(NullArray::new(3)) as ArrayRef;
        assert!(
            SelectedValues::try_new(selection, &DataType::Int32, nulls.clone(), Box::default())
                .is_err()
        );
        SelectedValues::try_new(
            selection,
            &DataType::Null,
            nulls,
            Box::from([RowDataError::new(1, "invalid input")]),
        )
        .unwrap();
    }

    #[test]
    fn row_error_diagnostics_are_bounded_at_utf8_boundaries() {
        let error = RowDataError::new(0, &"界".repeat(512));
        assert!(error.message().len() <= MAX_ROW_ERROR_MESSAGE_BYTES);
        assert_eq!(error.message().len() % 3, 0);
    }
}
