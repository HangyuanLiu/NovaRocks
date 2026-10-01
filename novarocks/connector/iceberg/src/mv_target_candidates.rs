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

//! One conservative candidate selector shared by target reads and delete freezing.
use crate::iceberg::spec::{Literal, PrimitiveLiteral, TableMetadata, Transform};
use crate::read_model::IcebergReadSnapshot;
use crate::typed_read::column_handle::{corrupt, invalid};
use crate::typed_read::table_handle::IcebergPinnedDataFileSet;
use novarocks_spi::connector::read_stack::{
    ConnectorMvPartitionValue, ConnectorMvTargetPartitionSelection,
};
use novarocks_spi::connector::{ConnectorError, MvExactPartitionTransform};
use std::sync::Arc;
fn not_found(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(
        novarocks_spi::connector::ConnectorErrorKind::NotFound,
        message.into(),
    )
}

/// Check the exact snapshot before any candidate enumeration or empty-set shortcut.
pub(crate) fn validate_mv_target_candidate_snapshot(
    metadata: &TableMetadata,
    exact_snapshot_id: i64,
    candidates: &novarocks_spi::connector::write_stack::ConnectorMvTargetCandidateSelection,
) -> Result<(), ConnectorError> {
    if metadata.snapshot_by_id(exact_snapshot_id).is_none() {
        return Err(not_found(format!(
            "MV target candidate snapshot {exact_snapshot_id} no longer exists"
        )));
    }
    if let novarocks_spi::connector::write_stack::ConnectorMvTargetCandidateSelection::Partitions(
        selection,
    ) = candidates
    {
        if selection.snapshot_id() != exact_snapshot_id {
            return Err(invalid(
                "MV candidate selection and read base snapshots disagree",
            ));
        }
        validate_mv_target_partition_selection(metadata, selection)?;
    }
    Ok(())
}

/// Keep the read baseline identical to delete freezing, including format admission.
/// An unprovable partition exclusion remains an unrestricted pinned snapshot.
pub(crate) fn freeze_mv_target_candidate_files(
    metadata: &TableMetadata,
    exact_snapshot_id: i64,
    files: Vec<crate::manifest::DataFileWithStats>,
    candidates: &novarocks_spi::connector::write_stack::ConnectorMvTargetCandidateSelection,
) -> Result<Option<IcebergPinnedDataFileSet>, ConnectorError> {
    validate_mv_target_candidate_snapshot(metadata, exact_snapshot_id, candidates)?;
    let selected = match candidates {
        novarocks_spi::connector::write_stack::ConnectorMvTargetCandidateSelection::All => None,
        novarocks_spi::connector::write_stack::ConnectorMvTargetCandidateSelection::Partitions(
            selection,
        ) => select_mv_target_file_paths(
            metadata,
            Some(exact_snapshot_id),
            files.iter().map(|file| {
                (
                    file.path.as_str(),
                    file.partition_spec_id,
                    file.partition_values.as_ref(),
                )
            }),
            selection,
        ),
    };
    crate::commit::write_stack::control::freeze_target_delete_references(
        files,
        metadata,
        exact_snapshot_id,
        Some(candidates),
    )?;
    Ok(selected)
}

/// Validate the persisted target identity and ordered partition contract
/// against one provider metadata generation before any file can be excluded.
pub(crate) fn validate_mv_target_partition_selection(
    metadata: &TableMetadata,
    selection: &ConnectorMvTargetPartitionSelection,
) -> Result<(), ConnectorError> {
    if selection.object_id().as_bytes().as_ref() != metadata.uuid().to_string().as_bytes() {
        return Err(invalid(
            "MV target selection names a different Iceberg table object",
        ));
    }
    if metadata.snapshot_by_id(selection.snapshot_id()).is_none() {
        return Err(not_found(format!(
            "MV target selection snapshot {} no longer exists",
            selection.snapshot_id()
        )));
    }
    let spec_id = metadata.default_partition_spec_id();
    if selection.partition_spec_version().as_ref()
        != crate::storage_inspector::exact_partition_spec_version(spec_id).as_ref()
    {
        return Err(invalid(
            "MV target selection disagrees with the current partition spec",
        ));
    }
    let spec = metadata.partition_spec_by_id(spec_id).ok_or_else(|| {
        corrupt(format!(
            "Iceberg table metadata does not carry default partition spec {spec_id}"
        ))
    })?;
    if selection.partition_fields().len() != spec.fields().len() {
        return Err(invalid(
            "MV target partition field count disagrees with Iceberg metadata",
        ));
    }
    for (provided, actual) in selection.partition_fields().iter().zip(spec.fields()) {
        if provided.partition_field_id().as_ref() != actual.field_id.to_be_bytes()
            || provided.source_target_field_id().as_ref() != actual.source_id.to_be_bytes()
            || !mv_target_transform_matches(provided.transform(), &actual.transform)
        {
            return Err(invalid(
                "MV target partition field disagrees with Iceberg metadata",
            ));
        }
    }
    Ok(())
}

fn mv_target_transform_matches(expected: &MvExactPartitionTransform, actual: &Transform) -> bool {
    matches!(
        (expected, actual),
        (MvExactPartitionTransform::Identity, Transform::Identity)
            | (MvExactPartitionTransform::Year, Transform::Year)
            | (MvExactPartitionTransform::Month, Transform::Month)
            | (MvExactPartitionTransform::Day, Transform::Day)
            | (MvExactPartitionTransform::Hour, Transform::Hour)
            | (MvExactPartitionTransform::Void, Transform::Void)
    ) || matches!(
        (expected, actual),
        (
            MvExactPartitionTransform::Bucket { num_buckets: left },
            Transform::Bucket(right)
        ) if left == right
    ) || matches!(
        (expected, actual),
        (MvExactPartitionTransform::Truncate { width: left }, Transform::Truncate(right))
            if left == right
    )
}

/// A `None` result is an unrestricted scan of the same pinned snapshot. A
/// historical/unknown file spec or an uncomparable value invalidates the
/// entire selection, including candidates already found in earlier files.
pub(crate) fn select_mv_target_files(
    metadata: &TableMetadata,
    snapshot: &IcebergReadSnapshot,
    selection: &ConnectorMvTargetPartitionSelection,
) -> Option<IcebergPinnedDataFileSet> {
    select_mv_target_file_paths(
        metadata,
        snapshot.snapshot_id,
        snapshot.files.iter().map(|file| {
            (
                file.path.as_str(),
                file.partition_spec_id,
                file.partition_values.as_ref(),
            )
        }),
        selection,
    )
}

pub(crate) fn select_mv_target_file_paths<'a>(
    metadata: &TableMetadata,
    snapshot_id: Option<i64>,
    files: impl Iterator<
        Item = (
            &'a str,
            Option<i32>,
            Option<&'a crate::iceberg::spec::Struct>,
        ),
    >,
    selection: &ConnectorMvTargetPartitionSelection,
) -> Option<IcebergPinnedDataFileSet> {
    if snapshot_id != Some(selection.snapshot_id()) {
        return None;
    }
    if selection.keys().is_empty() {
        return IcebergPinnedDataFileSet::try_new(std::iter::empty::<&str>()).ok();
    }
    let current_spec_id = metadata.default_partition_spec_id();
    let mut selected = Vec::new();
    for (path, spec_id, partition) in files {
        if spec_id != Some(current_spec_id) {
            return None;
        }
        let values = partition?.fields();
        if values.len() != selection.partition_fields().len() {
            return None;
        }
        let comparable = values
            .iter()
            .map(|value| mv_target_partition_value(value.as_ref()))
            .collect::<Option<Vec<_>>>()?;
        if selection.keys().iter().any(|key| key == &comparable) {
            selected.push(path);
            if selected.len() > crate::typed_read::table_handle::MAX_PINNED_DATA_FILES {
                return None;
            }
        }
    }
    IcebergPinnedDataFileSet::try_new(selected).ok()
}

/// The same primitive spelling as Iceberg's change-window partition impact.
/// Values outside that vocabulary cannot justify excluding a target file.
fn mv_target_partition_value(value: Option<&Literal>) -> Option<ConnectorMvPartitionValue> {
    let Some(value) = value else {
        return Some(ConnectorMvPartitionValue::Null);
    };
    let Literal::Primitive(value) = value else {
        return None;
    };
    let string = match value {
        PrimitiveLiteral::Boolean(value) => value.to_string(),
        PrimitiveLiteral::Int(value) => value.to_string(),
        PrimitiveLiteral::Long(value) => value.to_string(),
        PrimitiveLiteral::Float(_) | PrimitiveLiteral::Double(_) => return None,
        PrimitiveLiteral::String(value) => value.clone(),
        PrimitiveLiteral::Binary(_)
        | PrimitiveLiteral::Int128(_)
        | PrimitiveLiteral::UInt128(_)
        | PrimitiveLiteral::AboveMax
        | PrimitiveLiteral::BelowMin => return None,
    };
    Some(ConnectorMvPartitionValue::String(Arc::from(string)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floating_partition_values_never_exclude_a_candidate() {
        for value in [f64::NAN, 0.0, -0.0, 42.0] {
            assert!(mv_target_partition_value(Some(&Literal::double(value))).is_none());
        }
        for value in [f32::NAN, 0.0, -0.0, 42.0] {
            assert!(mv_target_partition_value(Some(&Literal::float(value))).is_none());
        }
    }
}
