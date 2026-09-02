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

//! What a copy-on-write session freezes before it seals its branches.
//!
//! Every other flavor cuts its branches from metadata alone. Copy-on-write
//! cannot: which files it rewrites, and which rows inside them the statement
//! matched, is the materialized result of running the statement's predicate as
//! a distributed read over the pinned base snapshot. That result arrives as the
//! session flavor's [`ConnectorRowMutationSelection`], and this module is the
//! only place that reads it.
//!
//! Two things come out of one selection, and they stay together because they
//! are two halves of one decision:
//!
//! * the *read contract* of each branch — exactly the one old data file it
//!   re-reads, which is exactly the file its commit replaces; and
//! * the *replacement record* the single external commit needs — that old
//!   file's path and the row ids inside it that were matched.
//!
//! The row ids are load-bearing rather than bookkeeping: their minimum becomes
//! the replacement manifest's `first_row_id`
//! ([`update_cow`](crate::commit::update_cow)), which is what stops the v3
//! manifest-list writer allocating fresh `_row_id`s for rows that already have
//! them. A synthesized value would corrupt row lineage instead of failing a
//! check, so every one of them is read off the selection and validated against
//! the frozen file it claims to belong to.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{Array, Int8Array, Int64Array, StringArray};
use arrow::datatypes::Schema;
use bytes::Bytes;
use novarocks_spi::connector::write_stack::session::ConnectorWriteRewriteSource;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceId, ConnectorPinnedFileSet,
    ConnectorRowMutationScanBinding, ConnectorRowMutationSelection, ConnectorTableHandle,
    ConnectorWriteFieldToken, ConnectorWriteInputShape,
};

use crate::commit::write_stack::domain::{corrupt, invalid};
use crate::iceberg::spec::TableMetadata;
use crate::manifest::{DataFileWithStats, data_file_with_stats_to_iceberg_data_file_info};
use crate::row_lineage_synth::{ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL};

/// The Iceberg row-identity column naming a physical row's file.
const ICEBERG_FILE_COL: &str = "_file";
/// The Iceberg row-identity column naming a physical row's position.
const ICEBERG_POS_COL: &str = "_pos";
/// The provider-signed logical effect column every match selection ends with.
///
/// The name is the one
/// [`row_mutation_preparation`](crate::commit::row_mutation_preparation) signs
/// into the match contract; the two are the same provider's vocabulary and are
/// asserted to agree by `the_effect_column_name_matches_the_signed_contract`.
pub(crate) const ICEBERG_ROW_MUTATION_EFFECT_COL: &str = "__row_mutation_effect";

/// What one sealed copy-on-write branch replaces.
///
/// Retained on the session because the single external commit needs it and
/// nothing on the data plane carries it: a writer's job is to write replacement
/// rows, and which old file those rows supersede is a planning fact the session
/// already holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergCowBranchInput {
    /// Replaces one whole old data file, and names the row ids inside it the
    /// statement matched.
    Rewrite {
        old_file: String,
        matched_row_ids: Vec<i64>,
    },
    /// Publishes rows that belong to no rewritten file — a folded `MERGE`
    /// not-matched insert. It replaces nothing.
    Append,
}

/// One copy-on-write branch, frozen before any target is sealed.
///
/// The read contract and the replacement record travel together because a
/// branch that replaced one file while reading another would silently drop or
/// duplicate rows.
#[derive(Clone, Debug)]
pub struct IcebergCowBranchRecipe {
    input: IcebergCowBranchInput,
    rewrite_source: Option<ConnectorWriteRewriteSource>,
}

impl IcebergCowBranchRecipe {
    pub(crate) const fn input(&self) -> &IcebergCowBranchInput {
        &self.input
    }

    pub(crate) fn into_input(self) -> IcebergCowBranchInput {
        self.input
    }

    /// Present exactly on a rewrite branch: an append reads nothing at all.
    pub(crate) fn rewrite_source(&self) -> Option<&ConnectorWriteRewriteSource> {
        self.rewrite_source.as_ref()
    }

    /// A recipe assembled without a catalog, for tests that assert branch
    /// structure rather than the freeze that produces it.
    #[cfg(test)]
    pub(crate) const fn for_test(
        input: IcebergCowBranchInput,
        rewrite_source: Option<ConnectorWriteRewriteSource>,
    ) -> Self {
        Self {
            input,
            rewrite_source,
        }
    }
}

/// Where each column the provider needs sits in one match selection.
///
/// The selection's schema is laid out by the same match contract this provider
/// signed: identity fields first, at their source ordinals, then the target's
/// before and after images, then the logical effect column last. Resolving the
/// layout from the schema rather than from a second copy of the contract is
/// what lets a session read a selection it did not sign — a session is admitted
/// by table name and never receives the preparation.
#[derive(Clone, Copy, Debug)]
struct IcebergCowSelectionLayout {
    file: usize,
    row_id: usize,
    position: usize,
    last_sequence: usize,
    effect: usize,
}

impl IcebergCowSelectionLayout {
    /// Identity columns are the leading fields, so the first field carrying an
    /// identity name is the identity field even when the target happens to own
    /// a column of the same name. The effect column is the last field by
    /// construction, and is checked rather than searched for the same reason:
    /// a target column named like it would otherwise shadow it.
    fn resolve(schema: &Schema) -> Result<Self, ConnectorError> {
        let ordinal = |name: &str| {
            schema
                .fields()
                .iter()
                .position(|field| field.name().eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    invalid(format!(
                        "Iceberg copy-on-write selection lacks its `{name}` identity"
                    ))
                })
        };
        let effect =
            schema.fields().len().checked_sub(1).ok_or_else(|| {
                invalid("Iceberg copy-on-write selection carries no columns at all")
            })?;
        let effect_field = schema.field(effect);
        if !effect_field
            .name()
            .eq_ignore_ascii_case(ICEBERG_ROW_MUTATION_EFFECT_COL)
            || effect_field.data_type() != &arrow::datatypes::DataType::Int8
            || effect_field.is_nullable()
        {
            return Err(invalid(
                "Iceberg copy-on-write selection does not end with its signed logical effect column",
            ));
        }
        Ok(Self {
            file: ordinal(ICEBERG_FILE_COL)?,
            row_id: ordinal(ICEBERG_ROW_ID_COL)?,
            position: ordinal(ICEBERG_POS_COL)?,
            last_sequence: ordinal(ICEBERG_LAST_UPDATED_SEQ_COL)?,
            effect,
        })
    }
}

/// One matched row, as the selection reports it.
#[derive(Clone, Copy, Debug)]
struct IcebergCowMatchedRow {
    row_id: i64,
    position: i64,
    last_updated_sequence_number: i64,
}

/// The grouping one selection implies: which old file each matched row belongs
/// to, and whether any row is a net-new insert.
#[derive(Debug)]
struct IcebergCowSelectionGroups {
    rewrites: BTreeMap<String, Vec<IcebergCowMatchedRow>>,
    has_appended_rows: bool,
}

/// Read the selection row by row into `old_file -> matched rows`.
///
/// The scan is exact on purpose. A row whose identity or lineage is null, or
/// whose logical effect is not one the change vocabulary defines, would end up
/// either dropped or attributed to the wrong file, and both corrupt the commit
/// silently.
fn group_selection(
    selection: &ConnectorRowMutationSelection,
    layout: IcebergCowSelectionLayout,
) -> Result<IcebergCowSelectionGroups, ConnectorError> {
    let mut rewrites = BTreeMap::<String, Vec<IcebergCowMatchedRow>>::new();
    let mut has_appended_rows = false;
    for batch in selection.batches() {
        let column = |ordinal: usize| batch.column(ordinal);
        let effects = column(layout.effect)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| invalid("Iceberg copy-on-write effect column is not Int8"))?;
        let files = column(layout.file)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("Iceberg copy-on-write `_file` identity is not UTF-8"))?;
        let row_ids = column(layout.row_id)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("Iceberg copy-on-write `_row_id` identity is not INT64"))?;
        let positions = column(layout.position)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("Iceberg copy-on-write `_pos` identity is not INT64"))?;
        let last_sequences = column(layout.last_sequence)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                invalid(
                    "Iceberg copy-on-write `_last_updated_sequence_number` identity is not INT64",
                )
            })?;
        if effects.null_count() != 0 {
            return Err(invalid(
                "Iceberg copy-on-write selection effect column contains nulls",
            ));
        }
        for index in 0..batch.num_rows() {
            // 1 = delete, 2 = replace, 3 = insert. The vocabulary is the
            // neutral change-event one; an insert belongs to no old file.
            match effects.value(index) {
                3 => {
                    has_appended_rows = true;
                    continue;
                }
                1 | 2 => {}
                _ => {
                    return Err(invalid(
                        "Iceberg copy-on-write selection contains an unknown logical effect",
                    ));
                }
            }
            if files.is_null(index)
                || row_ids.is_null(index)
                || positions.is_null(index)
                || last_sequences.is_null(index)
            {
                return Err(invalid(
                    "Iceberg copy-on-write matched row has null physical identity or lineage",
                ));
            }
            rewrites
                .entry(files.value(index).to_string())
                .or_default()
                .push(IcebergCowMatchedRow {
                    row_id: row_ids.value(index),
                    position: positions.value(index),
                    last_updated_sequence_number: last_sequences.value(index),
                });
        }
    }
    let mut mapped = BTreeSet::new();
    for rows in rewrites.values() {
        for row in rows {
            if !mapped.insert(row.row_id) {
                return Err(invalid(
                    "Iceberg copy-on-write selection maps one row identity more than once",
                ));
            }
        }
    }
    if rewrites.is_empty() && !has_appended_rows {
        return Err(invalid(
            "Iceberg copy-on-write selection is known-empty and has no branch to seal",
        ));
    }
    Ok(IcebergCowSelectionGroups {
        rewrites,
        has_appended_rows,
    })
}

/// Prove every matched row is a row of the file it named.
///
/// What is provable here is positional: a matched row must sit at a real
/// position inside the frozen file, and no two matched rows of one file may
/// claim the same position. A synthesized identity fails that, and would
/// otherwise reach the commit as a replacement record for a row nobody read.
///
/// What is deliberately *not* asserted is `_row_id == first_row_id + position`.
/// That equation holds only for rows whose lineage is inherited. A file that
/// materializes `_row_id` -- which is exactly what this provider's own
/// copy-on-write writer produces, because carrying the stored id forward is how
/// a rewrite preserves row identity -- carries ids that are decoupled from both
/// its `first_row_id` and its row order. Concretely, a rewrite stamps
/// `first_row_id = min(matched row ids)` on the replacement manifest
/// ([`update_cow`](crate::commit::update_cow)) while the file also carries the
/// unmatched rows it copied over, whose stored ids may be lower and in any
/// order. Asserting the equation would refuse the second mutation of every
/// file the first one rewrote.
///
/// The file's own lineage metadata is still required: the replacement manifest
/// is written from it, so a source missing `first_row_id`, a record count, or a
/// data sequence cannot be rewritten at all.
fn validate_matched_rows(
    old_file: &str,
    rows: &[IcebergCowMatchedRow],
    data_file: &DataFileWithStats,
) -> Result<(), ConnectorError> {
    if data_file.first_row_id.is_none() {
        return Err(invalid(format!(
            "Iceberg copy-on-write source `{old_file}` is missing first_row_id"
        )));
    }
    let record_count = data_file
        .record_count
        .filter(|count| *count >= 0)
        .ok_or_else(|| {
            invalid(format!(
                "Iceberg copy-on-write source `{old_file}` is missing a valid record count"
            ))
        })?;
    if data_file.data_sequence_number.is_none() {
        return Err(invalid(format!(
            "Iceberg copy-on-write source `{old_file}` is missing its data sequence"
        )));
    }
    let mut positions = BTreeSet::new();
    for row in rows {
        if row.position < 0 || row.position >= record_count || row.row_id < 0 {
            return Err(invalid(format!(
                "Iceberg copy-on-write row {} does not belong to admitted source `{old_file}`",
                row.row_id
            )));
        }
        if row.last_updated_sequence_number < 0 {
            return Err(invalid(format!(
                "Iceberg copy-on-write row {} carries a negative written version in source `{old_file}`",
                row.row_id
            )));
        }
        if !positions.insert(row.position) {
            return Err(invalid(format!(
                "Iceberg copy-on-write source `{old_file}` matched position {} more than once",
                row.position
            )));
        }
    }
    Ok(())
}

/// Everything the frozen base of one copy-on-write session provides.
pub(crate) struct IcebergCowFreezeInput<'a> {
    pub catalog: &'a ConnectorInstanceId,
    pub namespace: &'a str,
    pub table_name: &'a str,
    pub metadata: &'a TableMetadata,
    pub snapshot_id: i64,
    /// Every live data file of the frozen base snapshot. The session already
    /// reads them for its own admission, so the freeze takes them rather than
    /// issuing a second manifest walk.
    pub base_files: Vec<DataFileWithStats>,
    /// The provider-signed writer input every branch consumes.
    pub input: &'a ConnectorWriteInputShape,
    pub base_version_digest: [u8; 32],
    pub max_handle_payload_bytes: usize,
}

/// Freeze one copy-on-write session's branches from its match selection.
///
/// The order is the branch order the session seals, and it is deterministic:
/// one rewrite branch per touched old file in path order, then the append
/// branch when the statement has net-new rows. Deterministic order is what
/// lets the frontend name a branch by its ordinal without a second identity.
pub(crate) fn freeze_copy_on_write_branches(
    selection: &ConnectorRowMutationSelection,
    mut freeze: IcebergCowFreezeInput<'_>,
) -> Result<Vec<IcebergCowBranchRecipe>, ConnectorError> {
    selection.validate()?;
    let layout = IcebergCowSelectionLayout::resolve(selection.schema().as_ref())?;
    let IcebergCowSelectionGroups {
        rewrites,
        has_appended_rows,
    } = group_selection(selection, layout)?;

    let mut by_path = BTreeMap::new();
    for file in std::mem::take(&mut freeze.base_files) {
        if !rewrites.contains_key(&file.path) {
            continue;
        }
        let path = file.path.clone();
        if by_path.insert(path.clone(), file).is_some() {
            return Err(corrupt(format!(
                "Iceberg copy-on-write base contains duplicate data file `{path}`"
            )));
        }
    }
    if by_path.len() != rewrites.len() {
        return Err(invalid(
            "Iceberg copy-on-write selection names a file absent from the frozen base",
        ));
    }

    let mut recipes = Vec::with_capacity(rewrites.len() + usize::from(has_appended_rows));
    for (old_file, rows) in &rewrites {
        let data_file = by_path
            .get(old_file)
            .ok_or_else(|| corrupt("Iceberg copy-on-write base lost a matched data file"))?;
        validate_matched_rows(old_file, rows, data_file)?;
        let rewrite_source = freeze_branch_source(&freeze, data_file.clone())?;
        recipes.push(IcebergCowBranchRecipe {
            input: IcebergCowBranchInput::Rewrite {
                old_file: old_file.clone(),
                matched_row_ids: rows.iter().map(|row| row.row_id).collect(),
            },
            rewrite_source: Some(rewrite_source),
        });
    }
    if has_appended_rows {
        recipes.push(IcebergCowBranchRecipe {
            input: IcebergCowBranchInput::Append,
            rewrite_source: None,
        });
    }
    Ok(recipes)
}

/// Freeze the read contract of one rewrite branch: the single old data file it
/// re-reads, pinned at the base snapshot the session froze.
fn freeze_branch_source(
    freeze: &IcebergCowFreezeInput<'_>,
    data_file: DataFileWithStats,
) -> Result<ConnectorWriteRewriteSource, ConnectorError> {
    let explicit_file = data_file_with_stats_to_iceberg_data_file_info(data_file);
    crate::delete_file::validate_delete_apply_cost(&explicit_file)?;
    let payload = crate::metadata::frozen_copy_on_write_source_payload(
        freeze.catalog,
        freeze.namespace,
        freeze.table_name,
        freeze.metadata,
        freeze.snapshot_id,
        explicit_file,
    )?;
    // The frozen source's admitted schema is what `begin_scan` returns for this
    // very handle, so it is resolved through that one composition instead of
    // being rebuilt here: a frozen read refuses a scan whose output schema
    // differs by so much as one field annotation.
    let scan_schema = crate::metadata::projected_schema(&payload, &[])?;
    let encoded = serde_json::to_vec(&payload).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("encode Iceberg copy-on-write frozen source: {error}"),
        )
    })?;
    if encoded.len() > freeze.max_handle_payload_bytes {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg copy-on-write frozen source exceeds the request handle budget",
        ));
    }
    // The branch's commit replaces exactly this one data file, so the read that
    // produces its replacement rows is defined by the same single name.
    let pinned_source = ConnectorPinnedFileSet::try_new(
        &payload.namespace,
        &payload.table,
        freeze.snapshot_id,
        payload
            .explicit_files
            .iter()
            .flatten()
            .map(|file| file.path.as_str()),
    )?;
    let source = ConnectorTableHandle::try_new(freeze.catalog.clone(), Bytes::from(encoded))?;
    let (scan_bindings, match_tokens, written_version_token) =
        branch_scan_bindings(freeze.input, scan_schema.as_ref())?;
    Ok(ConnectorWriteRewriteSource::new(
        source,
        pinned_source,
        freeze.base_version_digest,
        scan_schema,
        scan_bindings,
        match_tokens,
        written_version_token,
    ))
}

/// Bind every signed writer field to the column of the frozen source that
/// produces it, and name the columns the rewrite joins its matches on.
///
/// Both sides are this provider's own vocabulary — it signed the writer input
/// and it built the frozen source's schema — so a name is an exact key here and
/// the engine never interprets one.
type IcebergCowScanBindings = (
    Vec<ConnectorRowMutationScanBinding>,
    Vec<ConnectorWriteFieldToken>,
    Option<ConnectorWriteFieldToken>,
);

fn branch_scan_bindings(
    input: &ConnectorWriteInputShape,
    scan_schema: &Schema,
) -> Result<IcebergCowScanBindings, ConnectorError> {
    let ConnectorWriteInputShape::RowLineage {
        data_fields,
        row_identity_fields,
    } = input
    else {
        return Err(invalid(
            "Iceberg copy-on-write branch requires a row-lineage writer input",
        ));
    };
    let mut bindings = Vec::with_capacity(data_fields.len() + row_identity_fields.len());
    for binding in data_fields.iter().chain(row_identity_fields) {
        let name = binding.field().name();
        let ordinal = scan_schema
            .fields()
            .iter()
            .position(|candidate| candidate.name().eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                corrupt(format!(
                    "Iceberg copy-on-write source schema omits signed writer field `{name}`"
                ))
            })?;
        let actual = scan_schema.field(ordinal);
        if actual.data_type() != binding.field().data_type() {
            return Err(corrupt(format!(
                "Iceberg copy-on-write source column `{name}` differs from its signed writer type"
            )));
        }
        let ordinal = u32::try_from(ordinal)
            .map_err(|_| corrupt("Iceberg copy-on-write scan ordinal overflowed"))?;
        bindings.push(ConnectorRowMutationScanBinding::new(
            binding.token(),
            ordinal,
        ));
    }
    // `_row_id` is the whole match key. Row lineage makes it unique per row for
    // the life of the table, whether the row inherits it or the file stores it,
    // so a join on it selects precisely the matched rows and nothing else.
    // `_file` and `_pos` are not available here and would be redundant if they
    // were: the branch reads one file, so the file is already fixed.
    let match_tokens = row_identity_fields
        .iter()
        .filter(|binding| {
            binding
                .field()
                .name()
                .eq_ignore_ascii_case(ICEBERG_ROW_ID_COL)
        })
        .map(novarocks_spi::connector::ConnectorWriteFieldBinding::token)
        .collect::<Vec<_>>();
    if match_tokens.is_empty() {
        return Err(invalid(
            "Iceberg copy-on-write writer input lacks its `_row_id` match key",
        ));
    }
    let written_version_token = row_identity_fields
        .iter()
        .find(|binding| {
            binding
                .field()
                .name()
                .eq_ignore_ascii_case(ICEBERG_LAST_UPDATED_SEQ_COL)
        })
        .map(novarocks_spi::connector::ConnectorWriteFieldBinding::token);
    if written_version_token.is_none() {
        return Err(invalid(
            "Iceberg copy-on-write writer input lacks its written-version column",
        ));
    }
    Ok((bindings, match_tokens, written_version_token))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int8Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use super::*;

    /// The selection layout one COW match query produces: identity columns
    /// first, then the target's before and after images, then the effect.
    fn selection_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(ICEBERG_FILE_COL, DataType::Utf8, false),
            Field::new(ICEBERG_POS_COL, DataType::Int64, false),
            Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, false),
            Field::new(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
            Field::new(ICEBERG_ROW_MUTATION_EFFECT_COL, DataType::Int8, false),
        ]))
    }

    fn batch(rows: &[(&str, i64, i64, i8)]) -> RecordBatch {
        RecordBatch::try_new(
            selection_schema(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vec![7_i64; rows.len()])),
                Arc::new(Int64Array::from(vec![1_i64; rows.len()])),
                Arc::new(Int64Array::from(vec![2_i64; rows.len()])),
                Arc::new(Int8Array::from(
                    rows.iter().map(|row| row.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("selection batch")
    }

    fn selection(batches: Vec<RecordBatch>) -> ConnectorRowMutationSelection {
        ConnectorRowMutationSelection::try_new(selection_schema(), batches, 1024, 1 << 20)
            .expect("selection")
    }

    #[test]
    fn the_layout_resolves_every_identity_and_the_trailing_effect_column() {
        let layout =
            IcebergCowSelectionLayout::resolve(selection_schema().as_ref()).expect("layout");
        assert_eq!(layout.file, 0);
        assert_eq!(layout.position, 1);
        assert_eq!(layout.row_id, 2);
        assert_eq!(layout.last_sequence, 3);
        // The effect column is the last one by construction, so a target column
        // sharing its name cannot shadow it.
        assert_eq!(layout.effect, 6);
    }

    #[test]
    fn a_selection_that_does_not_end_with_the_effect_column_is_refused() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(ICEBERG_FILE_COL, DataType::Utf8, false),
            Field::new(ICEBERG_POS_COL, DataType::Int64, false),
            Field::new(ICEBERG_ROW_ID_COL, DataType::Int64, false),
            Field::new(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
            Field::new(ICEBERG_ROW_MUTATION_EFFECT_COL, DataType::Int8, false),
            Field::new("v", DataType::Int64, true),
        ]));
        let error =
            IcebergCowSelectionLayout::resolve(schema.as_ref()).expect_err("misplaced effect");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    /// One selection spanning several old files groups into one branch each,
    /// in path order, with every matched row id kept exactly as it was read.
    #[test]
    fn a_selection_groups_into_one_branch_per_touched_file_in_path_order() {
        let layout =
            IcebergCowSelectionLayout::resolve(selection_schema().as_ref()).expect("layout");
        let groups = group_selection(
            &selection(vec![
                batch(&[
                    ("s3://b/c.parquet", 0, 200, 2),
                    ("s3://b/a.parquet", 1, 101, 2),
                ]),
                batch(&[
                    ("s3://b/a.parquet", 0, 100, 1),
                    ("s3://b/b.parquet", 0, 300, 2),
                ]),
            ]),
            layout,
        )
        .expect("groups");
        assert_eq!(
            groups.rewrites.keys().cloned().collect::<Vec<_>>(),
            vec![
                "s3://b/a.parquet".to_string(),
                "s3://b/b.parquet".to_string(),
                "s3://b/c.parquet".to_string(),
            ]
        );
        // Each file keeps its own rows; nothing is merged across files.
        assert_eq!(
            groups.rewrites["s3://b/a.parquet"]
                .iter()
                .map(|row| row.row_id)
                .collect::<Vec<_>>(),
            vec![101, 100]
        );
        assert_eq!(
            groups.rewrites["s3://b/b.parquet"]
                .iter()
                .map(|row| row.row_id)
                .collect::<Vec<_>>(),
            vec![300]
        );
        assert!(!groups.has_appended_rows);
    }

    #[test]
    fn inserted_rows_belong_to_no_rewritten_file() {
        let layout =
            IcebergCowSelectionLayout::resolve(selection_schema().as_ref()).expect("layout");
        let groups = group_selection(
            &selection(vec![batch(&[
                ("", 0, 0, 3),
                ("s3://b/a.parquet", 0, 100, 2),
            ])]),
            layout,
        )
        .expect("groups");
        assert_eq!(groups.rewrites.len(), 1);
        assert!(groups.has_appended_rows);
    }

    #[test]
    fn one_row_identity_cannot_be_mapped_twice() {
        let layout =
            IcebergCowSelectionLayout::resolve(selection_schema().as_ref()).expect("layout");
        let error = group_selection(
            &selection(vec![batch(&[
                ("s3://b/a.parquet", 0, 100, 2),
                ("s3://b/b.parquet", 0, 100, 2),
            ])]),
            layout,
        )
        .expect_err("one identity mapped twice");
        assert!(error.message().contains("more than once"), "{error}");
    }

    fn frozen_file(path: &str, first_row_id: i64, record_count: i64) -> DataFileWithStats {
        DataFileWithStats {
            path: path.to_string(),
            size: 128,
            record_count: Some(record_count),
            column_stats: None,
            partition_spec_id: Some(0),
            partition_key: None,
            partition_values: None,
            manifest_path: None,
            partition_field_values: Vec::new(),
            first_row_id: Some(first_row_id),
            data_sequence_number: Some(3),
            delete_files: Vec::new(),
        }
    }

    #[test]
    fn a_matched_row_outside_its_file_is_refused() {
        let file = frozen_file("s3://b/a.parquet", 100, 4);
        assert!(
            validate_matched_rows(
                "s3://b/a.parquet",
                &[IcebergCowMatchedRow {
                    row_id: 102,
                    position: 2,
                    last_updated_sequence_number: 1,
                }],
                &file,
            )
            .is_ok()
        );
        // A position past the file's own record count names a row the frozen
        // source does not hold.
        let error = validate_matched_rows(
            "s3://b/a.parquet",
            &[IcebergCowMatchedRow {
                row_id: 102,
                position: 4,
                last_updated_sequence_number: 1,
            }],
            &file,
        )
        .expect_err("position past the end of the file");
        assert!(error.message().contains("does not belong"), "{error}");
    }

    /// A second copy-on-write mutation of a file the first one rewrote is
    /// admitted.
    ///
    /// A rewrite stamps `first_row_id = min(matched row ids)` on the
    /// replacement manifest while the file also materializes the stored
    /// `_row_id` of every unmatched row it copied over, so the file's rows are
    /// routinely below its own `first_row_id` and out of positional order.
    /// This is the exact shape `UPDATE ... ; UPDATE ...` produces on a v3
    /// row-lineage table: refusing it made the second statement fail.
    #[test]
    fn a_rewritten_file_whose_stored_row_ids_precede_its_first_row_id_is_admitted() {
        // Row id 2 sits at position 0 and row id 1 at position 1, under a
        // `first_row_id` of 2 -- the file a rewrite of the matched row id 2
        // leaves behind.
        let file = frozen_file("s3://b/a.parquet", 2, 2);
        validate_matched_rows(
            "s3://b/a.parquet",
            &[IcebergCowMatchedRow {
                row_id: 1,
                position: 1,
                last_updated_sequence_number: 1,
            }],
            &file,
        )
        .expect("a stored row id below the file's first_row_id is still its own row");
    }

    #[test]
    fn two_matched_rows_of_one_file_cannot_claim_the_same_position() {
        let file = frozen_file("s3://b/a.parquet", 100, 4);
        let error = validate_matched_rows(
            "s3://b/a.parquet",
            &[
                IcebergCowMatchedRow {
                    row_id: 100,
                    position: 1,
                    last_updated_sequence_number: 1,
                },
                IcebergCowMatchedRow {
                    row_id: 101,
                    position: 1,
                    last_updated_sequence_number: 1,
                },
            ],
            &file,
        )
        .expect_err("two rows cannot share one physical position");
        assert!(error.message().contains("more than once"), "{error}");
    }
}
