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

//! SQL-owned visible-bag quota and one-evaluation fanout payloads.
//!
//! These nodes are independent semantic barriers. They are never joins or
//! ordinary CTE reuse candidates, and their mutable owners remain in Execution.

use crate::analysis::{OutputColumn, TypedExpr};
use crate::column_id::ColumnId;
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PlanQuotaNeed {
    Count(ColumnId),
    NegativeWeight(ColumnId),
}
impl PlanQuotaNeed {
    pub(crate) const fn column(self) -> ColumnId {
        match self {
            Self::Count(column) | Self::NegativeWeight(column) => column,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PlanQuotaPreclaimNode {
    /// Query-local domain label, resolved to the exact native Preclaim node
    /// once during PhysicalPlan construction. This label is never a count.
    pub domain: ColumnId,
    pub demand_entry_id: ColumnId,
    pub demand_key: ColumnId,
    pub demand_need: PlanQuotaNeed,
    pub demand_values: Vec<ColumnId>,
    pub target_values: Vec<ColumnId>,
    pub target_file: ColumnId,
    pub target_position: ColumnId,
    pub output_columns: Vec<OutputColumn>,
    pub max_state_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanQuotaTrimNode {
    pub domain: ColumnId,
    pub seed_entry_id: ColumnId,
    pub seed_need: PlanQuotaNeed,
    pub candidate_entry_id: ColumnId,
    pub candidate_file: ColumnId,
    pub candidate_position: ColumnId,
    pub output_columns: Vec<OutputColumn>,
    pub max_state_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PlanFanoutDistribution {
    RoundRobin,
    Broadcast,
    Hash(Vec<ColumnId>),
}

#[derive(Clone, Debug)]
pub(crate) struct PlanFanoutBranch {
    pub predicate: TypedExpr,
    /// Hash columns refer to the materialized producer's output.
    pub distribution: PlanFanoutDistribution,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanFanoutAnchorNode {
    pub id: ColumnId,
    pub branches: Vec<PlanFanoutBranch>,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanFanoutConsumeNode {
    pub anchor: ColumnId,
    pub branch: usize,
    pub output_columns: Vec<OutputColumn>,
    pub producer_column_ids: Vec<ColumnId>,
    /// The already frozen branch contract, copied for property derivation;
    /// lowering checks it against the anchor instead of trusting this copy.
    pub distribution: PlanFanoutDistribution,
}

impl PlanFanoutConsumeNode {
    /// Validate the complete mapping before optimizer property derivation.
    pub(crate) fn validate_mapping(&self) -> Result<(), String> {
        if self.anchor == ColumnId::UNSET {
            return Err("fanout consumer anchor identity is unset".into());
        }
        if self.output_columns.len() != self.producer_column_ids.len() {
            return Err("fanout consumer output/producers mapping arity differs".into());
        }
        let mut outputs = BTreeSet::new();
        for column in &self.output_columns {
            if column.column_id == ColumnId::UNSET || !outputs.insert(column.column_id) {
                return Err("fanout consumer output identities must be unique and set".into());
            }
        }
        let mut producers = BTreeSet::new();
        for column in &self.producer_column_ids {
            if *column == ColumnId::UNSET || !producers.insert(*column) {
                return Err("fanout consumer producer identities must be unique and set".into());
            }
        }
        if let PlanFanoutDistribution::Hash(keys) = &self.distribution {
            if keys.is_empty() {
                return Err("fanout hash branch requires nonempty keys".into());
            }
            let mut unique = BTreeSet::new();
            for key in keys {
                if !unique.insert(*key) {
                    return Err("fanout hash branch repeats a key identity".into());
                }
                if !producers.contains(key) {
                    return Err(format!(
                        "fanout hash key {} is not materialized in the consumer mapping",
                        key.0
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn consume() -> PlanFanoutConsumeNode {
        PlanFanoutConsumeNode {
            anchor: ColumnId(1),
            branch: 2,
            output_columns: [2, 3]
                .into_iter()
                .map(|id| OutputColumn {
                    column_id: ColumnId(id),
                    name: format!("c{id}"),
                    data_type: DataType::Binary,
                    nullable: false,
                    is_internal: true,
                })
                .collect(),
            producer_column_ids: vec![ColumnId(12), ColumnId(13)],
            distribution: PlanFanoutDistribution::Hash(vec![ColumnId(12)]),
        }
    }

    #[test]
    fn fanout_mapping_accepts_ordered_hash_remapping() {
        consume().validate_mapping().unwrap();
        let mut node = consume();
        node.distribution = PlanFanoutDistribution::Hash(vec![ColumnId(13), ColumnId(12)]);
        node.validate_mapping().unwrap();
    }

    #[test]
    fn fanout_mapping_rejects_missing_or_repeated_hash_keys() {
        for (keys, detail) in [
            (vec![], "nonempty keys"),
            (vec![ColumnId(14)], "not materialized"),
            (vec![ColumnId::UNSET], "not materialized"),
            (vec![ColumnId(12), ColumnId(12)], "repeats a key"),
        ] {
            let mut node = consume();
            node.distribution = PlanFanoutDistribution::Hash(keys);
            assert!(node.validate_mapping().unwrap_err().contains(detail));
        }
    }

    #[test]
    fn fanout_mapping_rejects_ambiguous_or_unset_identities() {
        let mut malformed = Vec::new();
        let mut node = consume();
        node.producer_column_ids.pop();
        malformed.push((node, "arity differs"));
        for invalid in [ColumnId(12), ColumnId::UNSET] {
            let mut node = consume();
            node.producer_column_ids[1] = invalid;
            malformed.push((node, "producer identities"));
        }
        for invalid in [ColumnId(2), ColumnId::UNSET] {
            let mut node = consume();
            node.output_columns[1].column_id = invalid;
            malformed.push((node, "output identities"));
        }
        let mut node = consume();
        node.anchor = ColumnId::UNSET;
        malformed.push((node, "anchor identity"));
        for (node, detail) in malformed {
            assert!(node.validate_mapping().unwrap_err().contains(detail));
        }
    }
}
