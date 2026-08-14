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

use super::*;
use novarocks_sql::plan_read::{ColumnId, OutputColumn};

fn empty_scan_bindings() -> &'static ScanExecutionBindings {
    Box::leak(Box::new(ScanExecutionBindings::default()))
}

fn output_column(id: u32, name: &str, data_type: DataType) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId(id),
        name: name.to_string(),
        data_type,
        nullable: false,
        is_internal: false,
    }
}

mod output;
mod relational;
mod scan;
mod topology;
mod write;
