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

//! SQL-private names for hidden IMV lineage and routing columns.
//!
//! These are part of the SQL planner contract. Persistence and execution
//! materialize them, but must not define an independent vocabulary.

use serde::{Deserialize, Serialize};

pub(crate) const HIDDEN_APPLY_KEY_COLUMN_NAME: &str = "__nova_base_row_id";
pub(crate) const JOIN_APPLY_KEY_COLUMN_NAME: &str = "__nova_join_row_key";
pub(crate) const GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME: &str = "__row_id__";
pub(crate) const BRANCH_ID_COLUMN_NAME: &str = "__branch_id__";

/// SQL lineage source used to derive an IMV target's hidden apply key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApplyKeySource {
    BaseRowId,
    JoinRowKey,
    GroupRowId,
}

impl ApplyKeySource {
    pub(crate) const fn table_property_value(self) -> &'static str {
        match self {
            Self::BaseRowId => "base._row_id",
            Self::JoinRowKey => "JoinRowKey",
            Self::GroupRowId => "GroupRowId",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlx2_planner_vocabulary_owns_hidden_lineage_column_names() {
        assert_eq!(HIDDEN_APPLY_KEY_COLUMN_NAME, "__nova_base_row_id");
        assert_eq!(JOIN_APPLY_KEY_COLUMN_NAME, "__nova_join_row_key");
        assert_eq!(GROUP_ROW_ID_APPLY_KEY_COLUMN_NAME, "__row_id__");
        assert_eq!(BRANCH_ID_COLUMN_NAME, "__branch_id__");
    }

    #[test]
    fn sqlx2_planner_vocabulary_owns_apply_key_source() {
        assert_eq!(
            ApplyKeySource::BaseRowId.table_property_value(),
            "base._row_id"
        );
    }
}
