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

use crate::mv::aggregate_state::mv_shape::IncrementalMvShape;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MvApplyPolicy {
    Incremental,
    FullRefresh { reason: String },
    Unsupported { reason: String },
}

pub(crate) fn apply_policy_for_change(
    shape: &IncrementalMvShape,
    _has_inserts: bool,
    has_deletes: bool,
    row_identity_available: bool,
) -> MvApplyPolicy {
    match shape {
        IncrementalMvShape::ProjectionFilter(_) => {
            if has_deletes && !row_identity_available {
                MvApplyPolicy::FullRefresh {
                    reason: "projection/filter MV DELETE without base row identity requires full refresh"
                        .to_string(),
                }
            } else {
                MvApplyPolicy::Incremental
            }
        }
        // IVM-P5 Phase 5: MIN/MAX no longer forces a full refresh on DELETE.
        // Phase 4 wired the detail-map state through merge / negate /
        // derive-visible, so DELETE deltas are handled incrementally.
        IncrementalMvShape::Aggregate(_) => MvApplyPolicy::Incremental,
        IncrementalMvShape::UnionAll(_) => MvApplyPolicy::Unsupported {
            reason: "UNION ALL IMV refresh is not supported by the legacy StarRocks MV apply policy"
                .to_string(),
        },
        IncrementalMvShape::JoinProjectionFilter(_) => MvApplyPolicy::Unsupported {
            reason: "join projection/filter IMV refresh is not supported by the legacy StarRocks MV apply policy".to_string(),
        },
        IncrementalMvShape::JoinAggregate(_) => MvApplyPolicy::Unsupported {
            reason:
                "join aggregate IMV refresh is not supported by the legacy StarRocks MV apply policy"
                    .to_string(),
        },
    }
}
