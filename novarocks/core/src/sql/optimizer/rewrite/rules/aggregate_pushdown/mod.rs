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

//! Aggregate pushdown rule (OPT-1).
//!
//! Pushes `LogicalAggregate` past `LogicalJoin` toward leaves when cost-justified.
//! See docs/design/specs/2026-05-20-opt-1-aggregate-pushdown-design.md.

use crate::sql::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) mod collector;
pub(crate) mod context;
pub(crate) mod cost;
pub(crate) mod rewriter;
pub(crate) mod rule;

pub(crate) use rule::AggregatePushdownRule;

#[allow(dead_code)]
pub(crate) fn aggregate_pushdown_rules() -> Vec<Box<dyn LogicalRewriteRule>> {
    vec![Box::new(AggregatePushdownRule)]
}
