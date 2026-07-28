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

//! Raw AST for `ALTER TABLE … (CREATE|DROP) [OR REPLACE] [IF [NOT] EXISTS]
//! (BRANCH|TAG) <name> [AS OF VERSION <id>] [retention …]`.

use crate::sql::parser::ast::ObjectName;

#[derive(Clone, Debug, PartialEq)]
pub enum AlterIcebergRefAction {
    CreateBranch {
        name: String,
        anchor: SnapshotAnchor,
        if_not_exists: bool,
        replace: bool,
        ignored_options: Vec<String>,
    },
    CreateTag {
        name: String,
        anchor: SnapshotAnchor,
        if_not_exists: bool,
        replace: bool,
        ignored_options: Vec<String>,
    },
    DropBranch {
        name: String,
        if_exists: bool,
    },
    DropTag {
        name: String,
        if_exists: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum SnapshotAnchor {
    SnapshotId(i64),
    CurrentMain,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlterIcebergRefStmt {
    pub table: ObjectName,
    pub action: AlterIcebergRefAction,
}
