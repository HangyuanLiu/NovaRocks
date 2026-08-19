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

//! Helpers for the `__op` control column used by StarRocks table PK
//! tables to distinguish UPSERT (0) from DELETE (1) rows.
//!
//! StarRocks-aligned: matches the wire-format constant `LOAD_OP_COLUMN`
//! used by stream load and SQL DML alike.
//!
//! The optimizer / analyzer must NOT prune, push-down, or otherwise
//! mangle `__op` projections. These helpers centralize the name and
//! op-code values that previously lived as duplicate private constants
//! in `connector/starrocks/{sink,starrocks,lake}` — re-export from those
//! sites if you need them at a non-FE layer.

pub const LOAD_OP_COLUMN: &str = "__op";

/// Wire-level `__op` values. `Update`/`Insert` variants are reserved
/// to anchor the future MERGE/UPDATE row-delta sink; today the sink
/// only emits `Upsert` (0) and `Delete` (1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadOp {
    Upsert = 0,
    Delete = 1,
}

impl LoadOp {
    pub const fn as_i8(self) -> i8 {
        self as i8
    }
}

#[inline]
pub fn is_load_op_column(name: &str) -> bool {
    name == LOAD_OP_COLUMN
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_matches_starrocks_protocol() {
        assert_eq!(LOAD_OP_COLUMN, "__op");
    }

    #[test]
    fn reserved_variants_have_starrocks_aligned_numeric_values() {
        assert_eq!(LoadOp::Upsert.as_i8(), 0);
        assert_eq!(LoadOp::Delete.as_i8(), 1);
    }

    #[test]
    fn helper_recognizes_load_op_column() {
        assert!(is_load_op_column("__op"));
        assert!(!is_load_op_column("_op"));
        assert!(!is_load_op_column("__op_v2"));
        assert!(!is_load_op_column(""));
    }
}
