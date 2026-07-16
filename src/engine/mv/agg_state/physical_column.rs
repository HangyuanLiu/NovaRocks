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

use crate::catalog::schema::SqlType;
use crate::sql::parser::ast::TableColumnDef;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StarRocksPhysicalColumn {
    pub(crate) column: TableColumnDef,
    pub(crate) visible: bool,
    pub(crate) is_key: bool,
}

pub(crate) fn starrocks_physical_column(
    name: String,
    data_type: SqlType,
    nullable: bool,
    visible: bool,
    is_key: bool,
) -> StarRocksPhysicalColumn {
    StarRocksPhysicalColumn {
        column: TableColumnDef {
            name,
            data_type,
            nullable,
            aggregation: None,
            default: None,
        },
        visible,
        is_key,
    }
}
