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

use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StarRocksKeysType {
    Duplicate,
    Unique,
    Aggregate,
    Primary,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct StarRocksScalarType {
    pub(crate) r#type: i32,
    pub(crate) len: Option<i32>,
    pub(crate) precision: Option<i32>,
    pub(crate) scale: Option<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StarRocksStructField {
    pub(crate) name: String,
    pub(crate) comment: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StarRocksTypeNode {
    pub(crate) r#type: i32,
    pub(crate) scalar_type: Option<StarRocksScalarType>,
    pub(crate) struct_fields: Vec<StarRocksStructField>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StarRocksTypeDesc {
    pub(crate) types: Vec<StarRocksTypeNode>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StarRocksAggStateDesc {
    pub(crate) agg_func_name: Option<String>,
    pub(crate) arg_types: Vec<StarRocksTypeDesc>,
    pub(crate) ret_type: Option<StarRocksTypeDesc>,
    pub(crate) is_result_nullable: Option<bool>,
    pub(crate) func_version: Option<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StarRocksColumnSchema {
    pub(crate) unique_id: i32,
    pub(crate) name: Option<String>,
    pub(crate) r#type: String,
    pub(crate) is_key: Option<bool>,
    pub(crate) aggregation: Option<String>,
    pub(crate) is_nullable: Option<bool>,
    pub(crate) default_value: Option<Vec<u8>>,
    pub(crate) precision: Option<i32>,
    pub(crate) frac: Option<i32>,
    pub(crate) length: Option<i32>,
    pub(crate) index_length: Option<i32>,
    pub(crate) is_bf_column: Option<bool>,
    pub(crate) referenced_column_id: Option<i32>,
    pub(crate) referenced_column: Option<String>,
    pub(crate) has_bitmap_index: Option<bool>,
    pub(crate) visible: Option<bool>,
    pub(crate) children_columns: Vec<StarRocksColumnSchema>,
    pub(crate) is_auto_increment: Option<bool>,
    pub(crate) agg_state_desc: Option<StarRocksAggStateDesc>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StarRocksTabletIndex {
    pub(crate) index_id: Option<i64>,
    pub(crate) index_name: Option<String>,
    pub(crate) index_type: Option<i32>,
    pub(crate) col_unique_id: Vec<i32>,
    pub(crate) index_properties: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct StarRocksTabletSchema {
    pub(crate) keys_type: Option<StarRocksKeysType>,
    pub(crate) column: Vec<StarRocksColumnSchema>,
    pub(crate) num_short_key_columns: Option<i32>,
    pub(crate) num_rows_per_row_block: Option<i32>,
    pub(crate) bf_fpp: Option<f64>,
    pub(crate) next_column_unique_id: Option<u32>,
    pub(crate) deprecated_is_in_memory: Option<bool>,
    pub(crate) deprecated_id: Option<i64>,
    pub(crate) compression_type: Option<i32>,
    pub(crate) sort_key_idxes: Vec<u32>,
    pub(crate) schema_version: Option<i32>,
    pub(crate) sort_key_unique_ids: Vec<u32>,
    pub(crate) table_indices: Vec<StarRocksTabletIndex>,
    pub(crate) compression_level: Option<i32>,
    pub(crate) id: Option<i64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct LakeScanColumnHint {
    pub(crate) unique_id: Option<u32>,
    pub(crate) default_value: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct LakeScanTableSchema {
    pub(crate) tablet_schema: StarRocksTabletSchema,
    pub(crate) column_hints: HashMap<String, LakeScanColumnHint>,
}

impl StarRocksTabletSchema {
    pub(crate) fn try_new(
        id: Option<i64>,
        keys_type: Option<StarRocksKeysType>,
        column: Vec<StarRocksColumnSchema>,
    ) -> Result<Self, String> {
        let schema = Self {
            id,
            keys_type,
            column,
            ..Self::default()
        };
        schema.validate()?;
        Ok(schema)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.keys_type.is_none() {
            return Err("StarRocks tablet schema keys type is missing".to_string());
        }
        if self.column.is_empty() {
            return Err("StarRocks tablet schema columns must not be empty".to_string());
        }
        let mut names = HashSet::new();
        let mut unique_ids = HashSet::new();
        for column in &self.column {
            let name = column
                .name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| "StarRocks top-level column name must not be empty".to_string())?;
            if column.unique_id < 0 {
                return Err(format!(
                    "StarRocks column {name} unique id must be non-negative"
                ));
            }
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("duplicate StarRocks column name {name}"));
            }
            if !unique_ids.insert(column.unique_id) {
                return Err(format!(
                    "duplicate StarRocks column unique id {}",
                    column.unique_id
                ));
            }
        }
        if let Some(count) = self.num_short_key_columns
            && (count < 0 || count as usize > self.column.len())
        {
            return Err(format!(
                "StarRocks tablet schema short key count is out of range: {count}"
            ));
        }
        if self
            .sort_key_idxes
            .iter()
            .any(|index| *index as usize >= self.column.len())
        {
            return Err("StarRocks tablet schema sort key index is out of range".to_string());
        }
        if self
            .sort_key_unique_ids
            .iter()
            .any(|unique_id| !unique_ids.contains(&(*unique_id as i32)))
        {
            return Err("StarRocks tablet schema sort key unique id is unknown".to_string());
        }
        Ok(())
    }

    pub(crate) const fn is_primary_keys(&self) -> bool {
        matches!(self.keys_type, Some(StarRocksKeysType::Primary))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tablet_schema_rejects_duplicate_visible_column_identity() {
        let column = StarRocksColumnSchema {
            unique_id: 1,
            name: Some("k".to_string()),
            r#type: "BIGINT".to_string(),
            is_key: Some(true),
            visible: Some(true),
            ..StarRocksColumnSchema::default()
        };
        let error = StarRocksTabletSchema::try_new(
            Some(7),
            Some(StarRocksKeysType::Duplicate),
            vec![column.clone(), column],
        )
        .expect_err("duplicate column identity must fail");
        assert!(error.contains("duplicate"));
    }
}
