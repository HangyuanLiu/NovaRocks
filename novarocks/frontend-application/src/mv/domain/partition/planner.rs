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

use crate::mv::domain::model::{AffectedTargetPartitions, MvPartitionKey};
use crate::mv::domain::partition::mapping::map_connector_partition_to_mv_key;
use novarocks_mv_application::persistence::projection::StoredMvProjection;

pub(crate) struct AffectedPartitionPlanInput<'a> {
    pub projection: &'a StoredMvProjection,
    pub partition_impact:
        Option<&'a novarocks_spi::connector::ConnectorChangeWindowPartitionImpact>,
    pub schema_observation:
        Option<&'a crate::mv::domain::storage_observation::MvSchemaValidationObservation>,
}

pub(crate) fn plan_affected_partitions(
    input: &AffectedPartitionPlanInput<'_>,
) -> AffectedTargetPartitions {
    let Some(impact) = input.partition_impact else {
        return AffectedTargetPartitions::not_derived(
            "full refresh affected partition planning is not implemented",
        );
    };
    match impact {
        novarocks_spi::connector::ConnectorChangeWindowPartitionImpact::Unavailable => {
            AffectedTargetPartitions::not_derived(
                "connector change-window partition impact is unavailable",
            )
        }
        novarocks_spi::connector::ConnectorChangeWindowPartitionImpact::Unpartitioned => {
            AffectedTargetPartitions::Unpartitioned
        }
        novarocks_spi::connector::ConnectorChangeWindowPartitionImpact::Exact {
            has_row_deletes,
            added,
            removed,
        } => {
            if *has_row_deletes {
                return AffectedTargetPartitions::not_derived(
                    "row-level delete affected partitions require row-evaluation fallback",
                );
            }
            let Some(observation) = input.schema_observation else {
                return AffectedTargetPartitions::not_derived(
                    "connector partition impact is missing its exact schema observation",
                );
            };
            let mut partitions = Vec::<MvPartitionKey>::with_capacity(added.len() + removed.len());
            for partition in added.iter().chain(removed) {
                match map_connector_partition_to_mv_key(input.projection, observation, partition) {
                    Ok(Some(key)) => partitions.push(key),
                    Ok(None) => return AffectedTargetPartitions::Unpartitioned,
                    Err(reason) => return AffectedTargetPartitions::not_derived(reason),
                }
            }
            AffectedTargetPartitions::known(partitions)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::persistence::test_support::ProjectionFixture;
    use novarocks_mv_application::product::MvTarget;
    use novarocks_spi::connector::ConnectorChangeWindowPartitionImpact;

    fn projection() -> StoredMvProjection {
        StoredMvProjection {
            mv_id: 1,
            facts: ProjectionFixture::new(
                MvTarget::from_parts(Some("ice"), "sales", "mv"),
                Some(11),
            )
            .build()
            .unwrap(),
        }
    }

    #[test]
    fn provider_unpartitioned_fact_is_preserved_without_decoding_spec_identity() {
        let projection = projection();
        assert_eq!(
            plan_affected_partitions(&AffectedPartitionPlanInput {
                projection: &projection,
                partition_impact: Some(&ConnectorChangeWindowPartitionImpact::Unpartitioned),
                schema_observation: None,
            }),
            AffectedTargetPartitions::Unpartitioned
        );
    }

    #[test]
    fn exact_impact_without_exact_schema_fails_closed() {
        let projection = projection();
        let impact = ConnectorChangeWindowPartitionImpact::Exact {
            has_row_deletes: false,
            added: Vec::new(),
            removed: Vec::new(),
        };
        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            projection: &projection,
            partition_impact: Some(&impact),
            schema_observation: None,
        });
        assert_eq!(
            result.not_derived_reason(),
            Some("connector partition impact is missing its exact schema observation")
        );
    }

    #[test]
    fn row_deletes_remain_non_derivable() {
        let projection = projection();
        let impact = ConnectorChangeWindowPartitionImpact::Exact {
            has_row_deletes: true,
            added: Vec::new(),
            removed: Vec::new(),
        };
        let result = plan_affected_partitions(&AffectedPartitionPlanInput {
            projection: &projection,
            partition_impact: Some(&impact),
            schema_observation: None,
        });
        assert_eq!(
            result.not_derived_reason(),
            Some("row-level delete affected partitions require row-evaluation fallback")
        );
    }
}
