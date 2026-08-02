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

use arrow::datatypes::DataType;

use crate::sql::column_id::ColumnId;

/// Ranking semantics selected by SQL for a TopN boundary.
///
/// Native encoding and execution translate this SQL fact explicitly; it is
/// deliberately not an execution-node re-export.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SqlTopNType {
    RowNumber,
    Rank,
    DenseRank,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyKind {
    Scalar,
    Exists { negated: bool },
    In { negated: bool },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScanVariantColumn {
    pub source_column_id: ColumnId,
    pub source_column: String,
    pub synthetic_column_id: ColumnId,
    pub synthetic_column: String,
    pub canonical_path: String,
    pub requested_type: DataType,
    pub strict: bool,
}

#[cfg(test)]
mod tests {
    use super::SqlTopNType;

    #[test]
    fn sqlx2_planner_vocabulary_topn_ranking_is_sql_owned() {
        assert_ne!(SqlTopNType::RowNumber, SqlTopNType::Rank);
        assert_ne!(SqlTopNType::Rank, SqlTopNType::DenseRank);
        assert_ne!(SqlTopNType::DenseRank, SqlTopNType::RowNumber);
    }
}
