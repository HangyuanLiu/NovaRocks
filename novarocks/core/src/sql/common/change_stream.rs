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

//! SQL-owned change-stream action and route vocabulary.
//!
//! These facts describe the logical writer branches selected by SQL planning.
//! Execution translates them at its native boundary; SQL must not inherit the
//! execution-layer representation or constants.

pub(crate) const CHANGE_OP_COLUMN: &str = "__change_op";
pub(crate) const CHANGE_OP_INSERT: i8 = 1;
pub(crate) const CHANGE_OP_DELETE: i8 = -1;
pub(crate) const DATA_ROUTE_REUSE: i32 = 1;
pub(crate) const DATA_ROUTE_FRESH: i32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChangeStreamBranchKind {
    DeleteDv,
    ReuseData,
    FreshData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ChangeStreamRouteKey {
    pub(crate) change_op: i32,
    pub(crate) data_route: Option<i32>,
}

impl ChangeStreamBranchKind {
    pub(crate) fn route_key(self) -> ChangeStreamRouteKey {
        match self {
            Self::DeleteDv => ChangeStreamRouteKey {
                change_op: CHANGE_OP_DELETE.into(),
                data_route: None,
            },
            Self::ReuseData => ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT.into(),
                data_route: Some(DATA_ROUTE_REUSE),
            },
            Self::FreshData => ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT.into(),
                data_route: Some(DATA_ROUTE_FRESH),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlx2_planner_vocabulary_change_stream_branch_maps_to_sql_route_key() {
        assert_eq!(
            ChangeStreamBranchKind::DeleteDv.route_key(),
            ChangeStreamRouteKey {
                change_op: -1,
                data_route: None,
            }
        );
        assert_eq!(
            ChangeStreamBranchKind::ReuseData.route_key(),
            ChangeStreamRouteKey {
                change_op: 1,
                data_route: Some(1),
            }
        );
        assert_eq!(
            ChangeStreamBranchKind::FreshData.route_key(),
            ChangeStreamRouteKey {
                change_op: 1,
                data_route: Some(2),
            }
        );
    }
}
