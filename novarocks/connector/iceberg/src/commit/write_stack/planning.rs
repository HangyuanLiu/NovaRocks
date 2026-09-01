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

//! Provider-private logical target planning.
//!
//! Every Iceberg write flavor is expressed as a small, dense set of logical
//! write targets. The neutral write stack sees only ordinals and input shapes;
//! the cohort and route vocabulary the provider uses internally never crosses
//! that boundary.
//!
//! The planner's other job is the NCP-6 unique-owner proof. Because a delete
//! branch now re-reads and rewrites old delete artifacts on the backend, two
//! branches that both claimed the same old data file would each write a merged
//! artifact for it, and Iceberg permits only one deletion vector per data file.
//! [`prove_unique_delete_owner`] therefore runs *before* any target is sealed
//! and any writer can stage, so an unprovable routing fails with nothing
//! written.

use std::collections::BTreeMap;

use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::{
    ConnectorError, ConnectorWriteAdmissionPurpose, ConnectorWriteInputShape,
};

use crate::commit::write_stack::domain::{
    IcebergCommitHandle, IcebergDataBranchRecipe, IcebergSealedWriteTarget, IcebergWriteBranch,
    IcebergWriteFlavor, IcebergWriteSessionId, IcebergWriteTableFacts, IcebergWriterHandle,
    IcebergWriterOutput, invalid,
};
use crate::commit::write_stack::old_delete::IcebergOldDeleteMergeTarget;

/// Prove that every old data file needing a delete merge is routed to exactly
/// one logical write target.
///
/// The returned map is the proof: a data file appears once, and its value names
/// the single target allowed to rewrite its deletes. A duplicate is a hard
/// failure, never a "last writer wins".
pub(crate) fn prove_unique_delete_owner(
    targets: &[IcebergSealedWriteTarget],
) -> Result<BTreeMap<String, WriteTargetOrdinal>, ConnectorError> {
    let mut owner: BTreeMap<String, WriteTargetOrdinal> = BTreeMap::new();
    for target in targets {
        if !target.branch().writes_deletes() && !target.owned_data_files().is_empty() {
            return Err(invalid(format!(
                "Iceberg {} branch cannot own an old-delete merge",
                target.branch().as_str()
            )));
        }
        for data_file in target.data_files() {
            if let Some(existing) = owner.get(data_file) {
                return Err(invalid(format!(
                    "Iceberg old data file {data_file} needing a delete merge is claimed by write targets {} and {}; a unique owner cannot be proven",
                    existing.get(),
                    target.ordinal().get()
                )));
            }
            owner.insert(data_file.clone(), target.ordinal());
        }
    }
    Ok(owner)
}

/// One logical target the sealed plan exposes.
#[derive(Clone, Debug)]
pub struct IcebergWriteTargetPlan {
    ordinal: WriteTargetOrdinal,
    branch: IcebergWriteBranch,
    handle: IcebergWriterHandle,
    input: ConnectorWriteInputShape,
}

impl IcebergWriteTargetPlan {
    pub const fn ordinal(&self) -> WriteTargetOrdinal {
        self.ordinal
    }
    pub const fn branch(&self) -> IcebergWriteBranch {
        self.branch
    }
    pub const fn handle(&self) -> &IcebergWriterHandle {
        &self.handle
    }
    pub const fn input(&self) -> &ConnectorWriteInputShape {
        &self.input
    }
    pub fn into_parts(
        self,
    ) -> (
        WriteTargetOrdinal,
        IcebergWriterHandle,
        ConnectorWriteInputShape,
    ) {
        (self.ordinal, self.handle, self.input)
    }
}

/// The data branch every flavor opens.
#[derive(Clone, Debug)]
pub struct IcebergDataBranchPlan {
    pub output: IcebergWriterOutput,
    pub recipe: IcebergDataBranchRecipe,
    pub input: ConnectorWriteInputShape,
}

/// A delete branch, with the old artifacts it claims exclusive ownership of.
#[derive(Clone, Debug)]
pub struct IcebergDeleteBranchPlan {
    pub branch: IcebergWriteBranch,
    pub output: IcebergWriterOutput,
    pub merge_targets: Vec<IcebergOldDeleteMergeTarget>,
    pub input: ConnectorWriteInputShape,
}

/// Everything `begin_write` freezes before it seals a session.
#[derive(Clone, Debug)]
pub struct IcebergWriteSessionPlanInput {
    pub flavor: IcebergWriteFlavor,
    pub purpose: ConnectorWriteAdmissionPurpose,
    pub table: IcebergWriteTableFacts,
    pub base_version_digest: Option<[u8; 32]>,
    pub data: IcebergDataBranchPlan,
    pub deletes: Vec<IcebergDeleteBranchPlan>,
}

/// Seal one write session's logical target map.
///
/// Ordinal assignment follows [`IcebergWriteFlavor::branches`] exactly, so the
/// data branch is always ordinal 0 and a delete branch, when the flavor owns
/// one, is ordinal 1. The unique-owner proof runs before any target is built.
pub fn plan_write_session(
    session_id: IcebergWriteSessionId,
    input: IcebergWriteSessionPlanInput,
) -> Result<(IcebergCommitHandle, Vec<IcebergWriteTargetPlan>), ConnectorError> {
    let allowed = input.flavor.branches();
    for delete in &input.deletes {
        if !delete.branch.writes_deletes() {
            return Err(invalid(
                "Iceberg delete branch plan must name a delete branch",
            ));
        }
        if !allowed.contains(&delete.branch) {
            return Err(invalid(format!(
                "Iceberg {} flavor does not own a {} branch",
                input.flavor.as_str(),
                delete.branch.as_str()
            )));
        }
    }

    // Assemble the ordinal map first, then prove the routing, then build the
    // handles. Proving before building is what makes the failure pre-staging:
    // no writer recipe reaches a driver when the proof fails.
    let mut sealed = Vec::with_capacity(1 + input.deletes.len());
    sealed.push(IcebergSealedWriteTarget::new(
        WriteTargetOrdinal::try_new(0)?,
        IcebergWriteBranch::Data,
        BTreeMap::new(),
    ));
    for (index, delete) in input.deletes.iter().enumerate() {
        let ordinal = u32::try_from(index + 1)
            .map_err(|_| invalid("Iceberg write session exceeds its logical target bound"))?;
        let owned = delete
            .merge_targets
            .iter()
            .map(|target| {
                let mut references = target
                    .references()
                    .iter()
                    .map(|reference| reference.path().to_string())
                    .collect::<Vec<_>>();
                references.sort();
                (target.data_file_path().to_string(), references)
            })
            .collect();
        sealed.push(IcebergSealedWriteTarget::new(
            WriteTargetOrdinal::try_new(ordinal)?,
            delete.branch,
            owned,
        ));
    }
    prove_unique_delete_owner(&sealed)?;

    let mut plans = Vec::with_capacity(sealed.len());
    plans.push(IcebergWriteTargetPlan {
        ordinal: sealed[0].ordinal(),
        branch: IcebergWriteBranch::Data,
        handle: IcebergWriterHandle::try_new_data(
            input.table.clone(),
            input.data.output.clone(),
            input.data.recipe.clone(),
        )?,
        input: input.data.input.clone(),
    });
    for (index, delete) in input.deletes.iter().enumerate() {
        plans.push(IcebergWriteTargetPlan {
            ordinal: sealed[index + 1].ordinal(),
            branch: delete.branch,
            handle: IcebergWriterHandle::try_new_delete(
                delete.branch,
                input.table.clone(),
                delete.output.clone(),
                delete.merge_targets.clone(),
            )?,
            input: delete.input.clone(),
        });
    }

    let handle = IcebergCommitHandle::try_new(
        session_id,
        input.table,
        input.flavor,
        input.purpose,
        input.base_version_digest,
        sealed,
    )?;
    Ok((handle, plans))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::write_stack::test_support::{
        data_branch_plan, delete_branch_plan, merge_target, table_facts,
    };

    fn sealed(
        ordinal: u32,
        branch: IcebergWriteBranch,
        files: &[&str],
    ) -> IcebergSealedWriteTarget {
        IcebergSealedWriteTarget::new(
            WriteTargetOrdinal::try_new(ordinal).expect("ordinal"),
            branch,
            files
                .iter()
                .map(|file| ((*file).to_string(), Vec::new()))
                .collect::<BTreeMap<_, _>>(),
        )
    }

    #[test]
    fn a_unique_owner_is_proven_when_each_data_file_has_one_claimant() {
        let owner = prove_unique_delete_owner(&[
            sealed(0, IcebergWriteBranch::Data, &[]),
            sealed(
                1,
                IcebergWriteBranch::DeletionVector,
                &["a.parquet", "b.parquet"],
            ),
        ])
        .expect("proof");
        assert_eq!(owner.len(), 2);
        assert_eq!(owner["a.parquet"].get(), 1);
    }

    #[test]
    fn two_branches_claiming_one_old_data_file_are_rejected() {
        let error = prove_unique_delete_owner(&[
            sealed(0, IcebergWriteBranch::Data, &[]),
            sealed(1, IcebergWriteBranch::PositionDelete, &["a.parquet"]),
            sealed(2, IcebergWriteBranch::DeletionVector, &["a.parquet"]),
        ])
        .expect_err("non-unique owner");
        assert!(error.message().contains("a unique owner cannot be proven"));
    }

    #[test]
    fn a_data_branch_cannot_own_an_old_delete_merge() {
        assert!(
            prove_unique_delete_owner(&[sealed(0, IcebergWriteBranch::Data, &["a.parquet"])])
                .is_err()
        );
    }

    #[test]
    fn a_flavor_seals_its_branches_as_dense_ordinals() {
        let (handle, plans) = plan_write_session(
            IcebergWriteSessionId::new(),
            IcebergWriteSessionPlanInput {
                flavor: IcebergWriteFlavor::RowMutationDeletionVector,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                table: table_facts(),
                base_version_digest: None,
                data: data_branch_plan(),
                deletes: vec![delete_branch_plan(
                    IcebergWriteBranch::DeletionVector,
                    vec![merge_target("s3://b/a.parquet", 100, Vec::new())],
                )],
            },
        )
        .expect("plan");
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].ordinal().get(), 0);
        assert_eq!(plans[0].branch(), IcebergWriteBranch::Data);
        assert_eq!(plans[1].ordinal().get(), 1);
        assert_eq!(plans[1].branch(), IcebergWriteBranch::DeletionVector);
        assert_eq!(handle.delete_owner().len(), 1);
        assert_eq!(handle.delete_owner()["s3://b/a.parquet"].get(), 1);
    }

    #[test]
    fn an_append_flavor_cannot_seal_a_delete_branch() {
        let error = plan_write_session(
            IcebergWriteSessionId::new(),
            IcebergWriteSessionPlanInput {
                flavor: IcebergWriteFlavor::Append,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                table: table_facts(),
                base_version_digest: None,
                data: data_branch_plan(),
                deletes: vec![delete_branch_plan(
                    IcebergWriteBranch::DeletionVector,
                    vec![merge_target("s3://b/a.parquet", 100, Vec::new())],
                )],
            },
        )
        .expect_err("flavor does not own the branch");
        assert!(error.message().contains("does not own a"));
    }

    #[test]
    fn a_non_unique_old_file_owner_fails_before_any_handle_is_built() {
        // Both delete branches claim the same old data file, so the merge owner
        // is ambiguous. This must be refused while planning — a writer that
        // reached a driver could already have staged a Puffin blob.
        let error = plan_write_session(
            IcebergWriteSessionId::new(),
            IcebergWriteSessionPlanInput {
                flavor: IcebergWriteFlavor::RowMutationDeletionVector,
                purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                table: table_facts(),
                base_version_digest: None,
                data: data_branch_plan(),
                deletes: vec![
                    delete_branch_plan(
                        IcebergWriteBranch::DeletionVector,
                        vec![merge_target("s3://b/a.parquet", 100, Vec::new())],
                    ),
                    delete_branch_plan(
                        IcebergWriteBranch::DeletionVector,
                        vec![merge_target("s3://b/a.parquet", 100, Vec::new())],
                    ),
                ],
            },
        )
        .expect_err("non-unique owner");
        assert!(
            error.message().contains("a unique owner cannot be proven"),
            "unexpected message: {}",
            error.message()
        );
    }
}
