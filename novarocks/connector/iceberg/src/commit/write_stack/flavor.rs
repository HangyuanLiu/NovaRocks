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

//! How each write-session flavor plans its logical branches.
//!
//! `begin_write` performs the external *reads* and freezes the material every
//! branch is cut from; this module is the pure decision that turns that
//! material into a branch list. Keeping it pure is what makes each flavor's
//! branch structure assertable without a catalog.
//!
//! The four flavors differ in exactly one thing — how many branches the write
//! needs and what each one accepts:
//!
//! * `Ordinary` keeps the canonical data-plus-delete shape unchanged.
//! * `ManagedPublication` publishes data only, and carries its technique and
//!   empty-input disposition (never its publication id) into the sealed
//!   session.
//! * `RowMutation` seals one branch per change-event route, each carrying the
//!   routing facts SQL needs. This is the port of the old
//!   `activate_row_mutation` route graph: a route's `cohort_id` becomes its
//!   [`WriteTargetOrdinal`](novarocks_spi::connector::write_stack::WriteTargetOrdinal)
//!   and its `preparation` is subsumed by the provider-private writer recipe,
//!   so neither comes back.
//! * `DistributedRewrite` seals one data branch per frozen rewrite group.

use novarocks_spi::connector::write_stack::session::ConnectorWriteRouteFacts;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationRouteInput, ConnectorRowMutationEffect,
    ConnectorWriteFieldBinding, ConnectorWriteFieldToken, ConnectorWriteInputShape,
    ConnectorWriteRouteId,
};
use sha2::{Digest, Sha256};

use crate::commit::write_stack::domain::{
    IcebergDataBranchRecipe, IcebergFrozenRewriteBranchInput, IcebergManagedPublicationFacts,
    IcebergWriteBranch, IcebergWriteFlavor, IcebergWriteTableFacts, IcebergWriterOutput, invalid,
};
use crate::commit::write_stack::old_delete::IcebergOldDeleteMergeTarget;
use crate::commit::write_stack::planning::{
    IcebergDataBranchPlan, IcebergDeleteBranchPlan, IcebergWriteBranchPlan,
};
use crate::delete_file::IcebergFileFormat;
use crate::distributed_rewrite::IcebergFrozenRewriteGroupV1;
use crate::row_lineage_synth::{ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL};

/// The Iceberg row-identity column naming a physical row's file.
const ICEBERG_FILE_COL: &str = "_file";
/// The Iceberg row-identity column naming a physical row's position.
const ICEBERG_POS_COL: &str = "_pos";

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message.into())
}

/// The frozen material every branch of one session is cut from.
///
/// Everything here was read and validated by `begin_write` before any branch
/// decision runs, so branch planning itself performs no I/O and can fail only
/// on the request's own shape.
#[derive(Clone, Debug)]
pub(crate) struct IcebergSessionMaterial {
    /// The exact table generation the session is frozen against.
    pub table: IcebergWriteTableFacts,
    /// The provider-signed input shape the caller's fields were bound into.
    pub input: ConnectorWriteInputShape,
    /// The physical output every data branch writes.
    pub data_output: IcebergWriterOutput,
    /// The partitioning and schema recipe every data branch writes through.
    pub data_recipe: IcebergDataBranchRecipe,
    /// The exact old delete artifacts a delete branch must supersede. Frozen by
    /// reference only: `begin_write` never opens one.
    pub merge_targets: Vec<IcebergOldDeleteMergeTarget>,
}

impl IcebergSessionMaterial {
    fn data_plan(&self, input: ConnectorWriteInputShape) -> IcebergDataBranchPlan {
        IcebergDataBranchPlan {
            output: self.data_output.clone(),
            recipe: self.data_recipe.clone(),
            input,
        }
    }

    fn delete_plan(
        &self,
        branch: IcebergWriteBranch,
        input: ConnectorWriteInputShape,
    ) -> Result<IcebergDeleteBranchPlan, ConnectorError> {
        Ok(IcebergDeleteBranchPlan {
            branch,
            output: IcebergWriterOutput::try_new(
                delete_branch_format(branch)?,
                parquet::basic::Compression::SNAPPY,
                None,
            )?,
            merge_targets: self.merge_targets.clone(),
            input,
        })
    }
}

fn delete_branch_format(branch: IcebergWriteBranch) -> Result<IcebergFileFormat, ConnectorError> {
    match branch {
        IcebergWriteBranch::DeletionVector => Ok(IcebergFileFormat::Puffin),
        IcebergWriteBranch::PositionDelete => Ok(IcebergFileFormat::Parquet),
        IcebergWriteBranch::Data => Err(invalid(
            "Iceberg data branch cannot be planned as a delete branch",
        )),
    }
}

/// One flavor's resolved branch structure.
#[derive(Clone, Debug)]
pub(crate) struct IcebergSessionFlavorPlan {
    /// The provider-private flavor the session commits as.
    pub flavor: IcebergWriteFlavor,
    /// Present exactly on a managed publication.
    pub publication: Option<IcebergManagedPublicationFacts>,
    /// Present exactly on a distributed rewrite: the exact input files each
    /// branch replaces, in the same order as `branches`.
    pub rewrite_inputs: Vec<IcebergFrozenRewriteBranchInput>,
    /// The branches, in ordinal order.
    pub branches: Vec<IcebergWriteBranchPlan>,
}

/// Plan the canonical data-plus-delete shape.
///
/// This is today's behaviour, unchanged: one data branch, plus one delete
/// branch when the signed input is row-level. Neither branch is routed, because
/// an ordinary write has exactly one thing to do with every row it is given.
pub(crate) fn plan_ordinary_branches(
    flavor: IcebergWriteFlavor,
    material: &IcebergSessionMaterial,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    let mut branches = vec![IcebergWriteBranchPlan::Data {
        plan: material.data_plan(material.input.clone()),
        route: None,
    }];
    if let Some(branch) = ordinary_delete_branch(&material.input) {
        branches.push(IcebergWriteBranchPlan::Delete {
            plan: material.delete_plan(branch, material.input.clone())?,
            route: None,
        });
    }
    Ok(IcebergSessionFlavorPlan {
        flavor,
        publication: None,
        rewrite_inputs: Vec::new(),
        branches,
    })
}

/// The delete branch an ordinary session's signed input calls for, if any.
pub(crate) const fn ordinary_delete_branch(
    input: &ConnectorWriteInputShape,
) -> Option<IcebergWriteBranch> {
    match input {
        ConnectorWriteInputShape::DeletionVector { .. } => Some(IcebergWriteBranch::DeletionVector),
        ConnectorWriteInputShape::PositionDelete { .. } => Some(IcebergWriteBranch::PositionDelete),
        _ => None,
    }
}

/// Plan a managed publication's branches.
///
/// A publication republishes rows, so it seals exactly one data branch. The
/// technique and the empty-input disposition travel with the session rather
/// than with a branch: they decide the single external commit op and what an
/// empty write means, and no writer needs either.
pub(crate) fn plan_managed_publication_branches(
    material: &IcebergSessionMaterial,
    publication: IcebergManagedPublicationFacts,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    if ordinary_delete_branch(&material.input).is_some() {
        return Err(unsupported(
            "Iceberg managed publication publishes data files, not a row-level delete input",
        ));
    }
    Ok(IcebergSessionFlavorPlan {
        flavor: IcebergWriteFlavor::ManagedPublication,
        publication: Some(publication),
        rewrite_inputs: Vec::new(),
        branches: vec![IcebergWriteBranchPlan::Data {
            plan: material.data_plan(material.input.clone()),
            route: None,
        }],
    })
}

/// Plan a distributed rewrite's branches: one data branch per frozen group.
///
/// The ordinal is the group's query-local name. Nothing about the group is
/// copied into a writer recipe — a rewrite writer's job is to write new data
/// files for the rows it is handed, and which old files those rows came from is
/// a planning fact the frontend already holds.
pub(crate) fn plan_distributed_rewrite_branches(
    material: &IcebergSessionMaterial,
    groups: &[IcebergFrozenRewriteGroupV1],
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    if ordinary_delete_branch(&material.input).is_some() {
        return Err(unsupported(
            "Iceberg distributed rewrite republishes data files, not a row-level delete input",
        ));
    }
    // A rewrite that found nothing to rewrite still seals one branch: the
    // session has to exist so it can terminate, and its empty prepared set is
    // what makes it a no-op rather than a failure. Its frozen input is empty
    // for the same reason, and the branch count stays one per input.
    let rewrite_inputs = if groups.is_empty() {
        vec![IcebergFrozenRewriteBranchInput::default()]
    } else {
        groups
            .iter()
            .map(frozen_rewrite_branch_input)
            .collect::<Result<Vec<_>, _>>()?
    };
    let branches = rewrite_inputs
        .iter()
        .map(|_| IcebergWriteBranchPlan::Data {
            plan: material.data_plan(material.input.clone()),
            route: None,
        })
        .collect();
    Ok(IcebergSessionFlavorPlan {
        flavor: IcebergWriteFlavor::DistributedRewrite,
        publication: None,
        rewrite_inputs,
        branches,
    })
}

/// The exact live files one frozen group's branch replaces.
///
/// A data rewrite retires the group's data files together with the delete
/// artifacts the group was proven to own, because the rows those deletions
/// removed are already absent from the files the branch writes. Leaving an
/// owned delete artifact live would re-apply it to rows that no longer exist.
fn frozen_rewrite_branch_input(
    group: &IcebergFrozenRewriteGroupV1,
) -> Result<IcebergFrozenRewriteBranchInput, ConnectorError> {
    IcebergFrozenRewriteBranchInput::try_new(
        group
            .data_files
            .iter()
            .map(|file| file.path.clone())
            .collect(),
        group.owned_data_delete_files.iter().cloned().collect(),
    )
}

/// How many branches a row mutation needs, and what each one accepts.
///
/// The signed input decides it, because the input is what says which halves of
/// a change event SQL can supply:
///
/// * a position-delete or deletion-vector input carries only a row identity, so
///   the mutation is delete-only and needs exactly one delete branch;
/// * a row-lineage input whose identity is `_file`/`_pos` carries both halves,
///   so it is a merge-on-read mutation: the delete branch consumes the
///   before-image identity and the data branch consumes the after-image values.
///   A `Replace` reaches both, which is why both accept it;
/// * a row-lineage input whose identity is `_row_id`/`_last_updated_sequence_number`
///   is a copy-on-write mutation: one data branch rewrites whole files, so it
///   sees every change event that touches a file it rewrites.
pub(crate) fn plan_row_mutation_branches(
    material: &IcebergSessionMaterial,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    let ordinals = InputOrdinals::of(&material.input);
    match &material.input {
        ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields,
        }
        | ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields,
        } => {
            let branch = ordinary_delete_branch(&material.input)
                .ok_or_else(|| invalid("Iceberg row-level input has no delete branch"))?;
            let flavor = match branch {
                IcebergWriteBranch::DeletionVector => IcebergWriteFlavor::RowMutationDeletionVector,
                _ => IcebergWriteFlavor::RowMutationPositionDelete,
            };
            let route = route_facts(
                &material.table,
                RouteKey::new(flavor, branch, 0),
                vec![ConnectorRowMutationEffect::Delete],
                &ordinals,
                identity_fields.iter().chain(partition_source_fields),
                partition_source_fields,
            )?;
            Ok(IcebergSessionFlavorPlan {
                flavor,
                publication: None,
                rewrite_inputs: Vec::new(),
                branches: vec![IcebergWriteBranchPlan::Delete {
                    plan: material.delete_plan(branch, material.input.clone())?,
                    route: Some(route),
                }],
            })
        }
        ConnectorWriteInputShape::RowLineage {
            data_fields,
            row_identity_fields,
        } => {
            if identity_names_are(row_identity_fields, ICEBERG_FILE_COL, ICEBERG_POS_COL) {
                plan_merge_on_read_branches(material, data_fields, row_identity_fields, &ordinals)
            } else if identity_names_are(
                row_identity_fields,
                ICEBERG_ROW_ID_COL,
                ICEBERG_LAST_UPDATED_SEQ_COL,
            ) {
                plan_copy_on_write_branches(material, &ordinals)
            } else {
                Err(unsupported(
                    "Iceberg row mutation requires a `_file`/`_pos` or `_row_id`/`_last_updated_sequence_number` row identity",
                ))
            }
        }
        ConnectorWriteInputShape::Data { .. } | ConnectorWriteInputShape::EqualityDelete { .. } => {
            Err(unsupported(
                "Iceberg row mutation requires a position-delete, deletion-vector, or row-lineage input",
            ))
        }
    }
}

/// Merge-on-read: a deletion-vector branch and a data branch over one input.
///
/// Merge-on-read is admitted only on a row-lineage table, so its delete half is
/// the v3 deletion-vector writer rather than a Parquet position-delete file.
fn plan_merge_on_read_branches(
    material: &IcebergSessionMaterial,
    data_fields: &[ConnectorWriteFieldBinding],
    row_identity_fields: &[ConnectorWriteFieldBinding],
    ordinals: &InputOrdinals,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    if material.table.format_version() < 3 {
        return Err(unsupported(
            "Iceberg merge-on-read row mutation requires a format-version 3 table",
        ));
    }
    let flavor = IcebergWriteFlavor::RowMutationDeletionVector;
    let data_input = ConnectorWriteInputShape::Data {
        fields: data_fields.to_vec(),
    };
    data_input.validate()?;
    let delete_input = ConnectorWriteInputShape::DeletionVector {
        identity_fields: row_identity_fields.to_vec(),
        partition_source_fields: Vec::new(),
    };
    delete_input.validate()?;
    let data_route = route_facts(
        &material.table,
        RouteKey::new(flavor, IcebergWriteBranch::Data, 0),
        vec![
            ConnectorRowMutationEffect::Replace,
            ConnectorRowMutationEffect::Insert,
        ],
        ordinals,
        data_fields.iter(),
        &[],
    )?;
    let delete_route = route_facts(
        &material.table,
        RouteKey::new(flavor, IcebergWriteBranch::DeletionVector, 1),
        vec![
            ConnectorRowMutationEffect::Delete,
            ConnectorRowMutationEffect::Replace,
        ],
        ordinals,
        row_identity_fields.iter(),
        &[],
    )?;
    Ok(IcebergSessionFlavorPlan {
        flavor,
        publication: None,
        rewrite_inputs: Vec::new(),
        branches: vec![
            IcebergWriteBranchPlan::Data {
                plan: material.data_plan(data_input),
                route: Some(data_route),
            },
            IcebergWriteBranchPlan::Delete {
                plan: material.delete_plan(IcebergWriteBranch::DeletionVector, delete_input)?,
                route: Some(delete_route),
            },
        ],
    })
}

/// Copy-on-write: one data branch that rewrites whole files.
fn plan_copy_on_write_branches(
    material: &IcebergSessionMaterial,
    ordinals: &InputOrdinals,
) -> Result<IcebergSessionFlavorPlan, ConnectorError> {
    let flavor = IcebergWriteFlavor::RowMutationCopyOnWrite;
    let route = route_facts(
        &material.table,
        RouteKey::new(flavor, IcebergWriteBranch::Data, 0),
        vec![
            ConnectorRowMutationEffect::Delete,
            ConnectorRowMutationEffect::Replace,
            ConnectorRowMutationEffect::Insert,
        ],
        ordinals,
        material.input.fields().into_iter(),
        &[],
    )?;
    Ok(IcebergSessionFlavorPlan {
        flavor,
        publication: None,
        rewrite_inputs: Vec::new(),
        branches: vec![IcebergWriteBranchPlan::Data {
            plan: material.data_plan(material.input.clone()),
            route: Some(route),
        }],
    })
}

fn identity_names_are(fields: &[ConnectorWriteFieldBinding], first: &str, second: &str) -> bool {
    fields
        .iter()
        .any(|field| field.field().name().eq_ignore_ascii_case(first))
        && fields
            .iter()
            .any(|field| field.field().name().eq_ignore_ascii_case(second))
}

/// Where each signed field sits in the input row SQL builds.
///
/// One row carries every branch's columns; a branch reads its own subset, and
/// this is the only thing that says which positions those are.
struct InputOrdinals {
    tokens: Vec<ConnectorWriteFieldToken>,
}

impl InputOrdinals {
    fn of(input: &ConnectorWriteInputShape) -> Self {
        Self {
            tokens: input
                .fields()
                .into_iter()
                .map(ConnectorWriteFieldBinding::token)
                .collect(),
        }
    }

    fn ordinal_of(&self, token: ConnectorWriteFieldToken) -> Result<u32, ConnectorError> {
        let index = self
            .tokens
            .iter()
            .position(|candidate| *candidate == token)
            .ok_or_else(|| {
                invalid("Iceberg row-mutation route names a field the signed input does not carry")
            })?;
        u32::try_from(index)
            .map_err(|_| invalid("Iceberg row-mutation route input ordinal overflows"))
    }
}

/// Everything that names one branch inside its session.
///
/// The route key is derived from exactly these facts plus the table generation:
/// no operation, cohort, or attempt identity takes part, so two writes of the
/// same shape against the same generation route the same way and what
/// distinguishes them belongs to whoever owns their external effect.
#[derive(Clone, Copy)]
struct RouteKey {
    flavor: IcebergWriteFlavor,
    branch: IcebergWriteBranch,
    ordinal: u32,
}

impl RouteKey {
    const fn new(flavor: IcebergWriteFlavor, branch: IcebergWriteBranch, ordinal: u32) -> Self {
        Self {
            flavor,
            branch,
            ordinal,
        }
    }

    fn route_id(self, table: &IcebergWriteTableFacts) -> ConnectorWriteRouteId {
        let mut hasher = Sha256::new();
        hasher.update(b"novarocks.iceberg.write-stack.route.v1\0");
        hasher.update(table.table_uuid().as_bytes());
        hasher.update([0]);
        hasher.update(table.target_ref().as_bytes());
        hasher.update([0]);
        hasher.update(self.flavor.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(self.branch.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(self.ordinal.to_be_bytes());
        ConnectorWriteRouteId::from_bytes(hasher.finalize().into())
    }
}

/// Derive one branch's routing facts.
fn route_facts<'a>(
    table: &IcebergWriteTableFacts,
    key: RouteKey,
    accepted_effects: Vec<ConnectorRowMutationEffect>,
    ordinals: &InputOrdinals,
    consumed: impl Iterator<Item = &'a ConnectorWriteFieldBinding>,
    partition_source_fields: &[ConnectorWriteFieldBinding],
) -> Result<ConnectorWriteRouteFacts, ConnectorError> {
    let input_ordinals = consumed
        .map(|binding| {
            ordinals
                .ordinal_of(binding.token())
                .map(|ordinal| ConnectorMutationRouteInput::new(binding.token(), ordinal))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let partition_fields = partition_source_fields
        .iter()
        .map(ConnectorWriteFieldBinding::token)
        .collect::<Vec<_>>();
    ConnectorWriteRouteFacts::try_new(
        key.route_id(table),
        accepted_effects,
        input_ordinals,
        partition_fields,
    )
}
