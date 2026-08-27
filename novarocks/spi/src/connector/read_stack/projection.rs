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

//! Projection, filter, and limit pushdown results.

use std::fmt::Debug;
use std::sync::Arc;

use crate::connector::{ConnectorError, ConnectorErrorKind};

use super::predicate::{ConnectorExpression, TupleDomain};
use super::value::ConnectorValueType;

pub const MAX_PROJECTION_ASSIGNMENTS: usize = 4096;

/// The binding between one plan variable and one connector column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Assignment<C> {
    variable: Arc<str>,
    column: C,
    value_type: ConnectorValueType,
}

impl<C> Assignment<C> {
    pub fn try_new(
        variable: impl AsRef<str>,
        column: C,
        value_type: ConnectorValueType,
    ) -> Result<Self, ConnectorError> {
        let variable = variable.as_ref();
        if variable.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector assignment variable must not be empty",
            ));
        }
        Ok(Self {
            variable: Arc::from(variable),
            column,
            value_type,
        })
    }

    pub fn variable(&self) -> &str {
        &self.variable
    }

    pub const fn column(&self) -> &C {
        &self.column
    }

    pub const fn value_type(&self) -> ConnectorValueType {
        self.value_type
    }
}

/// An ordered assignment list.
///
/// Order is the scan's output order and is authoritative; a pushdown fact that
/// records the same columns as a set never overrides it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderedAssignments<C> {
    assignments: Vec<Assignment<C>>,
}

impl<C: Clone + Debug + Eq + Ord> OrderedAssignments<C> {
    pub fn try_new(assignments: Vec<Assignment<C>>) -> Result<Self, ConnectorError> {
        if assignments.len() > MAX_PROJECTION_ASSIGNMENTS {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "connector assignment count exceeds the hard limit",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for assignment in &assignments {
            if !seen.insert(assignment.variable.clone()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector assignment variables must be unique",
                ));
            }
        }
        Ok(Self { assignments })
    }

    pub fn as_slice(&self) -> &[Assignment<C>] {
        &self.assignments
    }

    pub fn len(&self) -> usize {
        self.assignments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.assignments.is_empty()
    }

    /// The projected column set, deliberately unordered.
    pub fn projected_column_set(&self) -> std::collections::BTreeSet<C> {
        self.assignments
            .iter()
            .map(|assignment| assignment.column.clone())
            .collect()
    }
}

/// What a connector accepted from `applyFilter`.
#[derive(Clone, Debug)]
pub struct ConstraintApplicationResult<H, C: Ord + Clone + Debug> {
    handle: H,
    remaining_filter: TupleDomain<C>,
    remaining_expression: Option<ConnectorExpression>,
    precalculate_statistics: bool,
}

impl<H, C: Ord + Clone + Debug> ConstraintApplicationResult<H, C> {
    pub const fn new(
        handle: H,
        remaining_filter: TupleDomain<C>,
        remaining_expression: Option<ConnectorExpression>,
        precalculate_statistics: bool,
    ) -> Self {
        Self {
            handle,
            remaining_filter,
            remaining_expression,
            precalculate_statistics,
        }
    }

    pub const fn handle(&self) -> &H {
        &self.handle
    }

    pub fn into_handle(self) -> H {
        self.handle
    }

    pub const fn remaining_filter(&self) -> &TupleDomain<C> {
        &self.remaining_filter
    }

    pub const fn remaining_expression(&self) -> Option<&ConnectorExpression> {
        self.remaining_expression.as_ref()
    }

    pub const fn precalculate_statistics(&self) -> bool {
        self.precalculate_statistics
    }
}

/// What a connector accepted from `applyProjection`.
#[derive(Clone, Debug)]
pub struct ProjectionApplicationResult<H, C: Clone + Debug + Eq + Ord> {
    handle: H,
    projections: Vec<ConnectorExpression>,
    assignments: OrderedAssignments<C>,
    precalculate_statistics: bool,
}

impl<H, C: Clone + Debug + Eq + Ord> ProjectionApplicationResult<H, C> {
    pub const fn new(
        handle: H,
        projections: Vec<ConnectorExpression>,
        assignments: OrderedAssignments<C>,
        precalculate_statistics: bool,
    ) -> Self {
        Self {
            handle,
            projections,
            assignments,
            precalculate_statistics,
        }
    }

    pub const fn handle(&self) -> &H {
        &self.handle
    }

    pub fn into_handle(self) -> H {
        self.handle
    }

    pub fn projections(&self) -> &[ConnectorExpression] {
        &self.projections
    }

    pub const fn assignments(&self) -> &OrderedAssignments<C> {
        &self.assignments
    }

    pub const fn precalculate_statistics(&self) -> bool {
        self.precalculate_statistics
    }
}

/// What a connector accepted from `applyLimit`.
#[derive(Clone, Debug)]
pub struct LimitApplicationResult<H> {
    handle: H,
    limit_guaranteed: bool,
    precalculate_statistics: bool,
}

impl<H> LimitApplicationResult<H> {
    pub const fn new(handle: H, limit_guaranteed: bool, precalculate_statistics: bool) -> Self {
        Self {
            handle,
            limit_guaranteed,
            precalculate_statistics,
        }
    }

    pub const fn handle(&self) -> &H {
        &self.handle
    }

    pub fn into_handle(self) -> H {
        self.handle
    }

    /// Whether the engine may drop its own limit operator.
    pub const fn limit_guaranteed(&self) -> bool {
        self.limit_guaranteed
    }

    pub const fn precalculate_statistics(&self) -> bool {
        self.precalculate_statistics
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_assignments_reject_duplicate_variables() {
        let first = Assignment::try_new("v0", 1_u32, ConnectorValueType::BigInt).expect("valid");
        let duplicate =
            Assignment::try_new("v0", 2_u32, ConnectorValueType::BigInt).expect("valid");
        assert!(OrderedAssignments::try_new(vec![first, duplicate]).is_err());
    }

    #[test]
    fn ordered_assignments_keep_order_while_the_column_set_is_unordered() {
        let assignments = OrderedAssignments::try_new(vec![
            Assignment::try_new("v1", 9_u32, ConnectorValueType::BigInt).expect("valid"),
            Assignment::try_new("v0", 3_u32, ConnectorValueType::BigInt).expect("valid"),
        ])
        .expect("valid");
        assert_eq!(assignments.as_slice()[0].column(), &9);
        assert_eq!(assignments.as_slice()[1].column(), &3);
        let set = assignments.projected_column_set();
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), vec![3, 9]);
    }

    #[test]
    fn empty_assignment_variables_are_rejected() {
        assert!(Assignment::try_new("", 1_u32, ConnectorValueType::BigInt).is_err());
    }
}
