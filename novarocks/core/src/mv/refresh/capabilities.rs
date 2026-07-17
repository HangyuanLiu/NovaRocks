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

use crate::mv::persistence::schema::{ApplyKeySource, MvSchemaContract};
use crate::mv::refresh::apply_key::ApplyKeyValueType;
use crate::mv::refresh::snapshot::BaseSnapshotPolicy;

/// What a NotDerivable partition derivation outcome means for the refresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PartitionPruningPolicy {
    Required,
    BestEffort,
}

/// The compact row-identity discriminant needed by refresh execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefreshIdentity {
    BaseRowId,
    JoinRowKey,
    GroupRowId,
    BranchScoped(Box<RefreshIdentity>),
}

/// Refresh-time capabilities reconstructed from a persisted MV schema contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RefreshCapabilities {
    pub(crate) snapshot_policy: BaseSnapshotPolicy,
    pub(crate) has_agg_state: bool,
    pub(crate) identity: RefreshIdentity,
    pub(crate) apply_key_column: String,
    pub(crate) apply_key_value_type: ApplyKeyValueType,
    pub(crate) partition_pruning: PartitionPruningPolicy,
}

impl RefreshCapabilities {
    pub(crate) fn from_schema_contract(
        contract: &MvSchemaContract,
    ) -> Result<RefreshCapabilities, String> {
        let has_join = contract.join.is_some();
        let has_agg = contract.aggregate.is_some();
        let has_branch = contract.branch.is_some();
        let has_extra_bases = !contract.bases.is_empty();

        let snapshot_policy = if has_branch {
            BaseSnapshotPolicy::AllBasesRequired
        } else if has_join {
            BaseSnapshotPolicy::JoinPairPartialInitialSkip
        } else if has_extra_bases {
            BaseSnapshotPolicy::AllBasesRequired
        } else {
            BaseSnapshotPolicy::SingleBase
        };

        let identity = if let Some(branch) = &contract.branch {
            RefreshIdentity::BranchScoped(Box::new(apply_key_source_to_refresh_identity(
                branch.inner_apply_key_source,
            )))
        } else {
            apply_key_source_to_refresh_identity(contract.target.hidden_apply_key.source)
        };

        let apply_key_value_type = match (contract.target.hidden_apply_key.source, has_branch) {
            (ApplyKeySource::BaseRowId, false) => ApplyKeyValueType::Int64,
            (ApplyKeySource::BaseRowId, true) => ApplyKeyValueType::BranchInt64,
            (ApplyKeySource::JoinRowKey, _) => ApplyKeyValueType::Utf8,
            (ApplyKeySource::GroupRowId, false) => ApplyKeyValueType::Utf8,
            (ApplyKeySource::GroupRowId, true) => ApplyKeyValueType::BranchUtf8,
        };

        match (has_join, has_agg, has_branch) {
            (false, false, false)
            | (true, false, false)
            | (false, false, true)
            | (false, true, false)
            | (true, true, false)
            | (false, true, true)
            | (true, true, true) => {}
            _ => {
                return Err(format!(
                    "unsupported schema contract shape \
                     (join={has_join}, agg={has_agg}, branch={has_branch})"
                ));
            }
        }

        Ok(RefreshCapabilities {
            snapshot_policy,
            has_agg_state: has_agg,
            identity,
            apply_key_column: contract.target.hidden_apply_key.column_name.clone(),
            apply_key_value_type,
            partition_pruning: PartitionPruningPolicy::BestEffort,
        })
    }
}

fn apply_key_source_to_refresh_identity(source: ApplyKeySource) -> RefreshIdentity {
    match source {
        ApplyKeySource::BaseRowId => RefreshIdentity::BaseRowId,
        ApplyKeySource::JoinRowKey => RefreshIdentity::JoinRowKey,
        ApplyKeySource::GroupRowId => RefreshIdentity::GroupRowId,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_key_sources_map_to_runtime_identities() {
        assert_eq!(
            apply_key_source_to_refresh_identity(ApplyKeySource::BaseRowId),
            RefreshIdentity::BaseRowId
        );
        assert_eq!(
            apply_key_source_to_refresh_identity(ApplyKeySource::JoinRowKey),
            RefreshIdentity::JoinRowKey
        );
        assert_eq!(
            apply_key_source_to_refresh_identity(ApplyKeySource::GroupRowId),
            RefreshIdentity::GroupRowId
        );
    }
}
