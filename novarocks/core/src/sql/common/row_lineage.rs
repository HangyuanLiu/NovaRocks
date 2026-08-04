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

//! SQL-owned hidden-column names for Iceberg row-lineage planning.

pub(crate) const ICEBERG_FILE_PATH_COL: &str = "_file";
pub(crate) const ICEBERG_ROW_POS_COL: &str = "_pos";
pub(crate) const ICEBERG_ROW_ID_COL: &str = "_row_id";
pub(crate) const ICEBERG_LAST_UPDATED_SEQ_COL: &str = "_last_updated_sequence_number";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlx2_planner_vocabulary_row_lineage_columns_are_stable() {
        assert_eq!(ICEBERG_FILE_PATH_COL, "_file");
        assert_eq!(ICEBERG_ROW_POS_COL, "_pos");
        assert_eq!(ICEBERG_ROW_ID_COL, "_row_id");
        assert_eq!(
            ICEBERG_LAST_UPDATED_SEQ_COL,
            "_last_updated_sequence_number"
        );
    }
}
