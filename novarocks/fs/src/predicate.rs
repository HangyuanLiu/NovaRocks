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
