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

use std::cmp::Ordering;

#[derive(Clone, Debug, PartialEq)]
pub enum MinMaxPredicateValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    ByteArray(Vec<u8>),
    FixedLenByteArray(Vec<u8>),
    Date32(i32),
    DateTimeMicros(i64),
    DateTimeNanos(i64),
    LargeInt(i128),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MinMaxPredicateOp {
    Le,
    Ge,
    Lt,
    Gt,
    Eq,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanPredicateSource {
    Static,
    RuntimeIn,
    RuntimeMembership,
    RuntimeMinMax,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ScanPredicateDomain {
    Range {
        op: MinMaxPredicateOp,
        value: MinMaxPredicateValue,
    },
    DiscreteSet {
        values: Vec<MinMaxPredicateValue>,
        min: MinMaxPredicateValue,
        max: MinMaxPredicateValue,
    },
    Membership {
        values: Vec<MinMaxPredicateValue>,
    },
}

impl ScanPredicateDomain {
    /// Evaluate this domain against one `[min, max]` bound pair.
    ///
    /// The bounds may come from any statistics carrier -- a Parquet footer, an
    /// Iceberg manifest, or another physical source. The evaluation is
    /// deliberately source-agnostic: callers own the decoding that produces the
    /// bounds, this function owns only the comparison. Keeping it here is what
    /// lets file-level and row-group-level pruning share one judgement instead
    /// of drifting apart.
    ///
    /// Returns `true` when a row satisfying the domain may exist inside the
    /// bounds, i.e. the caller must keep the unit.
    ///
    /// Fallback direction: when two values are not comparable (different
    /// variants), `compare` yields `None` and the enclosing `is_some_and` turns
    /// that into "does not match", so the caller prunes the unit.
    pub fn may_match_bounds(
        &self,
        min: &MinMaxPredicateValue,
        max: &MinMaxPredicateValue,
    ) -> bool {
        match self {
            Self::Range { op, value } => match op {
                MinMaxPredicateOp::Le => {
                    compare(min, value).is_some_and(|order| order != Ordering::Greater)
                }
                MinMaxPredicateOp::Lt => {
                    compare(min, value).is_some_and(|order| order == Ordering::Less)
                }
                MinMaxPredicateOp::Ge => {
                    compare(max, value).is_some_and(|order| order != Ordering::Less)
                }
                MinMaxPredicateOp::Gt => {
                    compare(max, value).is_some_and(|order| order == Ordering::Greater)
                }
                MinMaxPredicateOp::Eq => {
                    compare(min, value).is_some_and(|order| order != Ordering::Greater)
                        && compare(max, value).is_some_and(|order| order != Ordering::Less)
                }
            },
            Self::DiscreteSet { values, .. } | Self::Membership { values } => {
                values.iter().any(|value| {
                    compare(min, value).is_some_and(|order| order != Ordering::Greater)
                        && compare(max, value).is_some_and(|order| order != Ordering::Less)
                })
            }
        }
    }
}

fn compare(left: &MinMaxPredicateValue, right: &MinMaxPredicateValue) -> Option<Ordering> {
    match (left, right) {
        (MinMaxPredicateValue::Boolean(a), MinMaxPredicateValue::Boolean(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Int32(a), MinMaxPredicateValue::Int32(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Int64(a), MinMaxPredicateValue::Int64(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Float(a), MinMaxPredicateValue::Float(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::Double(a), MinMaxPredicateValue::Double(b)) => a.partial_cmp(b),
        (MinMaxPredicateValue::ByteArray(a), MinMaxPredicateValue::ByteArray(b))
        | (
            MinMaxPredicateValue::FixedLenByteArray(a),
            MinMaxPredicateValue::FixedLenByteArray(b),
        ) => a.partial_cmp(b),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScanPredicate {
    column: String,
    /// Optional physical field identifier. When present, readers must bind the
    /// predicate by this identifier and must not fall back to a same-named
    /// column. This keeps Iceberg rename/reorder reads conservative.
    physical_field_id: Option<i32>,
    domain: ScanPredicateDomain,
    source: ScanPredicateSource,
}

impl ScanPredicate {
    pub fn new(
        column: impl Into<String>,
        domain: ScanPredicateDomain,
        source: ScanPredicateSource,
    ) -> Self {
        Self {
            column: column.into(),
            physical_field_id: None,
            domain,
            source,
        }
    }

    pub fn with_physical_field_id(mut self, physical_field_id: i32) -> Self {
        self.physical_field_id = Some(physical_field_id);
        self
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn physical_field_id(&self) -> Option<i32> {
        self.physical_field_id
    }

    pub fn domain(&self) -> &ScanPredicateDomain {
        &self.domain
    }

    pub fn source(&self) -> ScanPredicateSource {
        self.source
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PhysicalPruning {
    pub row_groups: Option<Vec<usize>>,
    pub pages: Vec<PhysicalPageSelection>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalPageSelection {
    pub row_group: usize,
    pub page_indices: Vec<usize>,
}
