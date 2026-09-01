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

//! Structural validation of the commit fragment carrier.

use novarocks_proto_models::connector_write as dto;
use prost::Message;

use super::shared::{
    validate_artifact_metrics, validate_artifact_partition, validate_content_range,
    validate_file_format,
};
use super::{
    MAX_COMMIT_FRAGMENT_ENCODED_BYTES, MAX_MERGED_OLD_REFERENCES, MAX_PATH_BYTES, bounded_count,
    bounded_text, inconsistent, missing, nonnegative_i64, out_of_range,
};
use crate::{FieldPath, ProtocolError};

/// A commit fragment whose carrier is canonical, in bounds, and structurally
/// one Iceberg artifact.
///
/// The generic root aggregation validates fragments with this and never decodes
/// them further: knowing a carrier is well formed is enough to count it, bound
/// it, and forward it, and interpreting the artifact is the frontend control
/// binding's job.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedCommitFragment {
    raw: dto::ConnectorCommitFragment,
    encoded_len: usize,
}

impl ValidatedCommitFragment {
    pub fn parse(
        raw: dto::ConnectorCommitFragment,
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        let encoded_len = raw.encoded_len();
        if encoded_len > MAX_COMMIT_FRAGMENT_ENCODED_BYTES {
            return Err(out_of_range(
                path,
                format!(
                    "commit fragment encodes to {encoded_len} bytes, over the hard limit {MAX_COMMIT_FRAGMENT_ENCODED_BYTES}"
                ),
            ));
        }
        let fragment = raw.fragment.as_ref().ok_or_else(|| {
            missing(
                path.clone(),
                "a commit fragment requires one provider variant",
            )
        })?;
        match fragment {
            dto::connector_commit_fragment::Fragment::Iceberg(iceberg) => {
                validate_iceberg_commit_fragment(iceberg, path.field("iceberg"))?;
            }
        }
        Ok(Self { raw, encoded_len })
    }

    /// The canonical encoded size, so a caller charges its budget with the same
    /// number this validation bounded.
    pub const fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    pub const fn as_proto(&self) -> &dto::ConnectorCommitFragment {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorCommitFragment {
        self.raw
    }

    pub fn iceberg(&self) -> &dto::IcebergCommitFragment {
        match self.raw.fragment.as_ref() {
            Some(dto::connector_commit_fragment::Fragment::Iceberg(iceberg)) => iceberg,
            None => unreachable!("a validated commit fragment always carries a variant"),
        }
    }
}

fn validate_merged_old_references(
    references: &[String],
    path: FieldPath,
) -> Result<(), ProtocolError> {
    bounded_count(
        references.len(),
        MAX_MERGED_OLD_REFERENCES,
        path.clone(),
        "merged old reference",
    )?;
    let mut previous: Option<&str> = None;
    for (index, reference) in references.iter().enumerate() {
        let reference_path = path.clone().index(index);
        bounded_text(reference, MAX_PATH_BYTES, reference_path.clone(), false)?;
        // These paths tell the commit which artifacts to retire. A duplicate
        // would retire one twice, and an unstable order would make the same
        // write look different on replay.
        if let Some(previous) = previous
            && previous >= reference.as_str()
        {
            return Err(inconsistent(
                reference_path,
                "merged old references must be sorted and unique by path",
            ));
        }
        previous = Some(reference.as_str());
    }
    Ok(())
}

fn validate_iceberg_commit_fragment(
    fragment: &dto::IcebergCommitFragment,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let artifact = fragment.artifact.as_ref().ok_or_else(|| {
        missing(
            path.clone(),
            "an Iceberg commit fragment describes exactly one artifact",
        )
    })?;
    match artifact {
        dto::iceberg_commit_fragment::Artifact::DataFile(data) => {
            let path = path.field("data_file");
            bounded_text(
                &data.path,
                MAX_PATH_BYTES,
                path.clone().field("path"),
                false,
            )?;
            validate_file_format(data.file_format, path.clone().field("file_format"))?;
            validate_artifact_partition(data.partition.as_ref(), path.clone().field("partition"))?;
            validate_artifact_metrics(data.metrics.as_ref(), path.clone().field("metrics"))?;
            if let Some(first_row_id) = data.first_row_id {
                nonnegative_i64(first_row_id, path.field("first_row_id"), "first row id")?;
            }
        }
        dto::iceberg_commit_fragment::Artifact::PositionDeleteFile(delete) => {
            let path = path.field("position_delete_file");
            bounded_text(
                &delete.path,
                MAX_PATH_BYTES,
                path.clone().field("path"),
                false,
            )?;
            validate_artifact_partition(
                delete.partition.as_ref(),
                path.clone().field("partition"),
            )?;
            validate_artifact_metrics(delete.metrics.as_ref(), path.clone().field("metrics"))?;
            bounded_text(
                &delete.referenced_data_file,
                MAX_PATH_BYTES,
                path.clone().field("referenced_data_file"),
                false,
            )?;
            validate_merged_old_references(
                &delete.merged_old_references,
                path.field("merged_old_references"),
            )?;
        }
        dto::iceberg_commit_fragment::Artifact::DeletionVector(vector) => {
            let path = path.field("deletion_vector");
            bounded_text(
                &vector.path,
                MAX_PATH_BYTES,
                path.clone().field("path"),
                false,
            )?;
            validate_artifact_partition(
                vector.partition.as_ref(),
                path.clone().field("partition"),
            )?;
            validate_artifact_metrics(vector.metrics.as_ref(), path.clone().field("metrics"))?;
            bounded_text(
                &vector.referenced_data_file,
                MAX_PATH_BYTES,
                path.clone().field("referenced_data_file"),
                false,
            )?;
            let range = vector.content_range.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("content_range"),
                    "a deletion vector requires its blob range",
                )
            })?;
            validate_content_range(range, path.clone().field("content_range"))?;
            // A deletion vector that deletes nothing is not an artifact worth
            // committing, and would make an empty blob look like a real merge.
            if vector.cardinality == 0 {
                return Err(out_of_range(
                    path.clone().field("cardinality"),
                    "a deletion vector must delete at least one position",
                ));
            }
            validate_merged_old_references(
                &vector.merged_old_references,
                path.field("merged_old_references"),
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProtocolErrorKind;

    fn partition() -> dto::IcebergArtifactPartition {
        dto::IcebergArtifactPartition {
            partition_path: "d_day=2026-09-01".to_string(),
            null_fingerprint: String::new(),
            partition_spec_id: 0,
            descriptor: Some(dto::IcebergPartitionDescriptor {
                values: vec![dto::IcebergPartitionValueDescriptor {
                    is_null: false,
                    datum_bytes: Some(vec![1, 2, 3, 4]),
                }],
            }),
        }
    }

    fn metrics() -> dto::IcebergArtifactMetrics {
        dto::IcebergArtifactMetrics {
            record_count: 10,
            file_size_in_bytes: 4096,
            split_offsets: vec![0, 2048],
            column_stats: Some(dto::IcebergColumnStats {
                column_sizes: [(1_i32, 128_i64)].into_iter().collect(),
                value_counts: [(1_i32, 10_i64)].into_iter().collect(),
                null_value_counts: [(1_i32, 0_i64)].into_iter().collect(),
                nan_value_counts: std::collections::BTreeMap::new(),
                lower_bounds: [(1_i32, vec![0_u8])].into_iter().collect(),
                upper_bounds: [(1_i32, vec![9_u8])].into_iter().collect(),
            }),
        }
    }

    fn wrap(artifact: dto::iceberg_commit_fragment::Artifact) -> dto::ConnectorCommitFragment {
        dto::ConnectorCommitFragment {
            fragment: Some(dto::connector_commit_fragment::Fragment::Iceberg(
                dto::IcebergCommitFragment {
                    artifact: Some(artifact),
                },
            )),
        }
    }

    fn data_file() -> dto::ConnectorCommitFragment {
        wrap(dto::iceberg_commit_fragment::Artifact::DataFile(
            dto::IcebergDataFileArtifact {
                path: "s3://bucket/db/t/data/new.parquet".to_string(),
                file_format: dto::IcebergFileFormat::Parquet as i32,
                partition: Some(partition()),
                metrics: Some(metrics()),
                first_row_id: Some(100),
            },
        ))
    }

    fn deletion_vector(cardinality: u64, merged: Vec<&str>) -> dto::ConnectorCommitFragment {
        wrap(dto::iceberg_commit_fragment::Artifact::DeletionVector(
            dto::IcebergDeletionVectorArtifact {
                path: "s3://bucket/db/t/data/new.puffin".to_string(),
                partition: Some(partition()),
                metrics: Some(metrics()),
                referenced_data_file: "s3://bucket/db/t/data/a.parquet".to_string(),
                content_range: Some(dto::IcebergContentRange {
                    offset: 4,
                    size_in_bytes: 64,
                }),
                cardinality,
                merged_old_references: merged.into_iter().map(str::to_string).collect(),
            },
        ))
    }

    fn parse(raw: dto::ConnectorCommitFragment) -> Result<ValidatedCommitFragment, ProtocolError> {
        ValidatedCommitFragment::parse(raw, FieldPath::root("commit_fragment"))
    }

    #[test]
    fn each_artifact_kind_round_trips_and_reports_the_bytes_it_was_bounded_by() {
        for raw in [
            data_file(),
            deletion_vector(3, vec!["s3://bucket/db/t/data/a-dv-1.puffin"]),
            wrap(dto::iceberg_commit_fragment::Artifact::PositionDeleteFile(
                dto::IcebergPositionDeleteFileArtifact {
                    path: "s3://bucket/db/t/data/new-pos.parquet".to_string(),
                    partition: Some(partition()),
                    metrics: Some(metrics()),
                    referenced_data_file: "s3://bucket/db/t/data/a.parquet".to_string(),
                    merged_old_references: Vec::new(),
                },
            )),
        ] {
            let expected_len = raw.encoded_len();
            let validated = parse(raw.clone()).expect("valid fragment");
            assert_eq!(validated.encoded_len(), expected_len);
            assert_eq!(validated.as_proto(), &raw);
            assert_eq!(validated.into_proto(), raw);
        }
    }

    #[test]
    fn a_fragment_without_a_variant_or_an_artifact_is_a_missing_field() {
        let error = parse(dto::ConnectorCommitFragment { fragment: None }).expect_err("no variant");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(error.path().to_string(), "commit_fragment");

        let error = parse(dto::ConnectorCommitFragment {
            fragment: Some(dto::connector_commit_fragment::Fragment::Iceberg(
                dto::IcebergCommitFragment { artifact: None },
            )),
        })
        .expect_err("no artifact");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(error.path().to_string(), "commit_fragment.iceberg");
    }

    #[test]
    fn a_deletion_vector_that_deletes_nothing_is_out_of_range() {
        let error = parse(deletion_vector(0, Vec::new())).expect_err("empty deletion vector");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "commit_fragment.iceberg.deletion_vector.cardinality"
        );
    }

    #[test]
    fn merged_old_references_must_be_sorted_and_unique() {
        let duplicated = deletion_vector(
            1,
            vec![
                "s3://bucket/db/t/data/a-dv-1.puffin",
                "s3://bucket/db/t/data/a-dv-1.puffin",
            ],
        );
        let error = parse(duplicated).expect_err("duplicate merged reference");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "commit_fragment.iceberg.deletion_vector.merged_old_references[1]"
        );

        let unsorted = deletion_vector(
            1,
            vec![
                "s3://bucket/db/t/data/a-dv-2.puffin",
                "s3://bucket/db/t/data/a-dv-1.puffin",
            ],
        );
        assert_eq!(
            parse(unsorted).expect_err("unsorted").kind(),
            ProtocolErrorKind::InconsistentFields
        );
    }

    #[test]
    fn a_negative_statistic_is_rejected_at_its_exact_field_path() {
        let mut raw = data_file();
        let dto::connector_commit_fragment::Fragment::Iceberg(iceberg) =
            raw.fragment.as_mut().expect("variant");
        let Some(dto::iceberg_commit_fragment::Artifact::DataFile(data)) =
            iceberg.artifact.as_mut()
        else {
            unreachable!("data file fixture")
        };
        data.metrics
            .as_mut()
            .expect("metrics")
            .column_stats
            .as_mut()
            .expect("stats")
            .null_value_counts
            .insert(1, -1);
        let error = parse(raw).expect_err("negative null count");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "commit_fragment.iceberg.data_file.metrics.column_stats.null_value_counts[\"1\"]"
        );
    }

    #[test]
    fn an_oversized_fragment_is_rejected_before_it_is_walked() {
        let mut raw = data_file();
        let dto::connector_commit_fragment::Fragment::Iceberg(iceberg) =
            raw.fragment.as_mut().expect("variant");
        let Some(dto::iceberg_commit_fragment::Artifact::DataFile(data)) =
            iceberg.artifact.as_mut()
        else {
            unreachable!("data file fixture")
        };
        // An oversized path would also fail its own bound; the size gate must
        // fire first, so the error names the fragment rather than the field.
        data.path = "x".repeat(MAX_COMMIT_FRAGMENT_ENCODED_BYTES + 1);
        let error = parse(raw).expect_err("oversized fragment");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(error.path().to_string(), "commit_fragment");
    }

    #[test]
    fn a_content_range_must_cover_at_least_one_byte() {
        let mut raw = deletion_vector(1, Vec::new());
        let dto::connector_commit_fragment::Fragment::Iceberg(iceberg) =
            raw.fragment.as_mut().expect("variant");
        let Some(dto::iceberg_commit_fragment::Artifact::DeletionVector(vector)) =
            iceberg.artifact.as_mut()
        else {
            unreachable!("deletion vector fixture")
        };
        vector.content_range = Some(dto::IcebergContentRange {
            offset: 4,
            size_in_bytes: 0,
        });
        let error = parse(raw).expect_err("empty blob range");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "commit_fragment.iceberg.deletion_vector.content_range.size_in_bytes"
        );
    }
}
