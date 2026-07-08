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

//! Generic query result types for standalone SQL execution.
//!
//! These types live here (rather than in `crate::engine`) so that
//! executors and coordinators under `crate::runtime` can reference the
//! result type without creating a dependency on the standalone engine module.

use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;

#[derive(Clone, Debug)]
pub struct QueryResultColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub logical_type: Option<crate::sql::SqlType>,
}

#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<QueryResultColumn>,
    pub chunks: Vec<Chunk>,
}

impl QueryResult {
    pub fn row_count(&self) -> usize {
        self.chunks.iter().map(Chunk::len).sum()
    }

    pub fn into_chunks(self) -> Vec<Chunk> {
        self.chunks
    }

    /// Empty schema, empty chunks. Used as the no-op output when an
    /// IVM branch (insert or delete) has zero input files / rows.
    pub(crate) fn empty() -> Self {
        Self {
            columns: Vec::new(),
            chunks: Vec::new(),
        }
    }
}
