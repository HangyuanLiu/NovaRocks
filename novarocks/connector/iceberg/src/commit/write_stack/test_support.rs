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

//! Shared constructors for the write-stack unit tests.
//!
//! They build *valid* values so each test can perturb exactly one fact and
//! assert that the perturbation is what fails.

use arrow::datatypes::{DataType, Field};
use novarocks_spi::connector::{
    ConnectorError, ConnectorWriteFieldBinding, ConnectorWriteFieldToken, ConnectorWriteInputShape,
};

use crate::commit::write_stack::domain::{
    IcebergArtifactMetrics, IcebergArtifactPartition, IcebergContentRange, IcebergDataBranchRecipe,
    IcebergDeletionVectorArtifact, IcebergWriteBranch, IcebergWriteTableFacts, IcebergWriterOutput,
};
use crate::commit::write_stack::flavor::IcebergSessionMaterial;
use crate::commit::write_stack::old_delete::{
    IcebergOldDeleteArtifactRef, IcebergOldDeleteMergeTarget, IcebergStorageRoute,
};
use crate::commit::write_stack::planning::{IcebergDataBranchPlan, IcebergDeleteBranchPlan};
use crate::delete_file::{IcebergFileContent, IcebergFileFormat};
use crate::write_descriptor::IcebergPartitionDescriptor;

pub(crate) fn table_facts() -> IcebergWriteTableFacts {
    IcebergWriteTableFacts::try_new(
        "6f7c3a0e-0000-4000-8000-000000000001".to_string(),
        "db".to_string(),
        "t".to_string(),
        "s3://b/wh/db/t".to_string(),
        "s3://b/wh/db/t/data".to_string(),
        "main".to_string(),
        Some(77),
        4,
        1,
        0,
        3,
    )
    .expect("table facts")
}

/// A frozen staged target, as the staged-create capability would hand one to a
/// session. It carries no snapshot, because nothing has ever been written to a
/// staged table.
pub(crate) fn staged_table_metadata() -> std::sync::Arc<crate::iceberg::spec::TableMetadata> {
    let schema = crate::iceberg::spec::Schema::builder()
        .with_fields(vec![std::sync::Arc::new(
            crate::iceberg::spec::NestedField::required(
                1,
                "id",
                crate::iceberg::spec::Type::Primitive(crate::iceberg::spec::PrimitiveType::Long),
            ),
        )])
        .build()
        .expect("staged schema");
    let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
        schema,
        crate::iceberg::spec::PartitionSpec::unpartition_spec(),
        crate::iceberg::spec::SortOrder::unsorted_order(),
        "s3://b/wh/_novarocks/ctas-staging/v1/staged/table".to_string(),
        crate::iceberg::spec::FormatVersion::V2,
        std::collections::HashMap::new(),
    )
    .expect("staged metadata builder")
    .build()
    .expect("staged metadata")
    .metadata;
    std::sync::Arc::new(metadata)
}

pub(crate) fn sample_partition() -> IcebergArtifactPartition {
    IcebergArtifactPartition::try_new(
        String::new(),
        String::new(),
        0,
        IcebergPartitionDescriptor { values: Vec::new() },
    )
    .expect("partition")
}

pub(crate) fn sample_metrics(record_count: u64, file_size: u64) -> IcebergArtifactMetrics {
    IcebergArtifactMetrics::try_new(record_count, file_size, Vec::new(), None).expect("metrics")
}

pub(crate) fn dv_artifact(
    path: &str,
    referenced: &str,
    cardinality: u64,
    file_size: u64,
    offset: i64,
    size: i64,
) -> Result<IcebergDeletionVectorArtifact, ConnectorError> {
    IcebergDeletionVectorArtifact::try_new(
        path.to_string(),
        sample_partition(),
        sample_metrics(cardinality, file_size),
        referenced.to_string(),
        IcebergContentRange::try_new(offset, size)?,
        cardinality,
        Vec::new(),
    )
}

pub(crate) fn parquet_ref(
    path: &str,
    referenced: Option<&str>,
    file_size: u64,
    record_count: u64,
) -> Result<IcebergOldDeleteArtifactRef, ConnectorError> {
    IcebergOldDeleteArtifactRef::try_new(
        path.to_string(),
        IcebergFileContent::PositionDeletes,
        IcebergFileFormat::Parquet,
        file_size,
        (record_count > 0).then_some(record_count),
        None,
        referenced.map(str::to_string),
        Some(3),
        None,
        0,
        IcebergStorageRoute::try_for_location(path)?,
    )
}

pub(crate) fn puffin_ref(
    path: &str,
    referenced: Option<&str>,
    file_size: u64,
    record_count: u64,
    offset: i64,
    size: i64,
) -> Result<IcebergOldDeleteArtifactRef, ConnectorError> {
    IcebergOldDeleteArtifactRef::try_new(
        path.to_string(),
        IcebergFileContent::PositionDeletes,
        IcebergFileFormat::Puffin,
        file_size,
        (record_count > 0).then_some(record_count),
        Some(IcebergContentRange::try_new(offset, size)?),
        referenced.map(str::to_string),
        Some(3),
        None,
        0,
        IcebergStorageRoute::try_for_location(path)?,
    )
}

pub(crate) fn merge_target(
    data_file: &str,
    record_count: u64,
    references: Vec<IcebergOldDeleteArtifactRef>,
) -> IcebergOldDeleteMergeTarget {
    IcebergOldDeleteMergeTarget::try_new(
        data_file.to_string(),
        record_count,
        Some(4),
        sample_partition(),
        77,
        references,
    )
    .expect("merge target")
}

pub(crate) fn binding(name: &str, seed: u8, data_type: DataType) -> ConnectorWriteFieldBinding {
    ConnectorWriteFieldBinding::new(
        ConnectorWriteFieldToken::from_bytes([seed; 32]),
        Field::new(name, data_type, false),
    )
}

/// A row-lineage input whose identity is `_file`/`_pos`: the merge-on-read
/// shape, carrying both halves of a change event in one row.
pub(crate) fn merge_on_read_input_shape() -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::RowLineage {
        data_fields: vec![
            binding("k1", 1, DataType::Int64),
            binding("v1", 4, DataType::Utf8),
        ],
        row_identity_fields: vec![
            binding("_file", 2, DataType::Utf8),
            binding("_pos", 3, DataType::Int64),
        ],
    }
}

/// A row-lineage input whose identity is `_row_id`/`_last_updated_sequence_number`:
/// the copy-on-write shape.
pub(crate) fn copy_on_write_input_shape() -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::RowLineage {
        data_fields: vec![binding("k1", 1, DataType::Int64)],
        row_identity_fields: vec![
            binding("_row_id", 5, DataType::Int64),
            binding("_last_updated_sequence_number", 6, DataType::Int64),
        ],
    }
}

/// The frozen material a flavor's branch planning is cut from.
///
/// It is deliberately built the same way `begin_write` builds it, so a test
/// perturbs only the input shape or the merge targets and asserts that the
/// branch structure is what changes.
pub(crate) fn session_material(input: ConnectorWriteInputShape) -> IcebergSessionMaterial {
    let data = data_branch_plan();
    IcebergSessionMaterial {
        table: table_facts(),
        input,
        data_output: data.output,
        data_recipe: data.recipe,
        merge_targets: Vec::new(),
    }
}

pub(crate) fn data_input_shape() -> ConnectorWriteInputShape {
    ConnectorWriteInputShape::Data {
        fields: vec![binding("k1", 1, DataType::Int64)],
    }
}

pub(crate) fn delete_input_shape(branch: IcebergWriteBranch) -> ConnectorWriteInputShape {
    let identity_fields = vec![
        binding("_file", 2, DataType::Utf8),
        binding("_pos", 3, DataType::Int64),
    ];
    match branch {
        IcebergWriteBranch::DeletionVector => ConnectorWriteInputShape::DeletionVector {
            identity_fields,
            partition_source_fields: Vec::new(),
        },
        _ => ConnectorWriteInputShape::PositionDelete {
            identity_fields,
            partition_source_fields: Vec::new(),
        },
    }
}

pub(crate) fn data_branch_plan() -> IcebergDataBranchPlan {
    IcebergDataBranchPlan {
        output: IcebergWriterOutput::try_new(
            IcebergFileFormat::Parquet,
            parquet::basic::Compression::SNAPPY,
            None,
        )
        .expect("output"),
        recipe: IcebergDataBranchRecipe::try_new(None, Vec::new(), Vec::new(), Vec::new(), false)
            .expect("recipe"),
        input: data_input_shape(),
    }
}

pub(crate) fn delete_branch_plan(
    branch: IcebergWriteBranch,
    merge_targets: Vec<IcebergOldDeleteMergeTarget>,
) -> IcebergDeleteBranchPlan {
    let file_format = match branch {
        IcebergWriteBranch::DeletionVector => IcebergFileFormat::Puffin,
        _ => IcebergFileFormat::Parquet,
    };
    IcebergDeleteBranchPlan {
        branch,
        output: IcebergWriterOutput::try_new(
            file_format,
            parquet::basic::Compression::SNAPPY,
            None,
        )
        .expect("output"),
        merge_targets,
        input: delete_input_shape(branch),
    }
}
