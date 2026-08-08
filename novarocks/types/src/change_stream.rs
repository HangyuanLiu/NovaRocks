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

/// Execution-neutral branch semantics for change-stream writers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChangeStreamBranchKind {
    DeleteDv,
    ReuseData,
    FreshData,
}

/// Immutable physical route selected for a change-stream branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChangeStreamRouteKey {
    change_op: i32,
    data_route: Option<i32>,
}

impl ChangeStreamRouteKey {
    pub const fn change_op(self) -> i32 {
        self.change_op
    }

    pub const fn data_route(self) -> Option<i32> {
        self.data_route
    }
}

impl ChangeStreamBranchKind {
    pub const fn route_key(self) -> ChangeStreamRouteKey {
        match self {
            Self::DeleteDv => ChangeStreamRouteKey {
                change_op: -1,
                data_route: None,
            },
            Self::ReuseData => ChangeStreamRouteKey {
                change_op: 1,
                data_route: Some(1),
            },
            Self::FreshData => ChangeStreamRouteKey {
                change_op: 1,
                data_route: Some(2),
            },
        }
    }
}
