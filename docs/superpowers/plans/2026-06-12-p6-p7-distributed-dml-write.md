# P6/P7 Distributed DML Write Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Iceberg writer reports descriptor-authoritative and cut DELETE/UPDATE/MERGE/MV refresh over to distributed writer sinks with no coordinator-local file writer fallback.

**Architecture:** P6 lands first: every `TIcebergDataFile` carries an internal partition descriptor and `IcebergCommitCollector` decodes `WrittenFile.partition_values` from that descriptor, never from `partition_path`. P7 then moves DML/MV file production into distributed pipeline sinks and leaves the coordinator responsible only for `WriteCommitInput` aggregation, typed commit, operation state transitions, cleanup, and finalization.

**Tech Stack:** Rust, Apache Arrow, iceberg-rust 0.9, NovaRocks thrift IDL, standalone distributed execution, `IcebergWriteTransactionRunner`, `WriteCoordinator`, SQL test runner.

---

## Scope Check

This plan intentionally keeps P6 and P7 together. They touch different modules, but P7's distributed DML correctness depends on P6's descriptor-authoritative writer report. Splitting them would either preserve the lossy `partition_path` carrier in new distributed DML, or leave coordinator-local DML writers in place after descriptor cutover. The tasks below are still independently reviewable and commit-sized.

## File Structure

- Modify `idl/thrift/Types.thrift`
  Add NovaRocks-internal partition descriptor structs and a field on `TIcebergDataFile`.
- Modify `src/connector/iceberg/mod.rs`
  Export the new descriptor module.
- Create `src/connector/iceberg/write_descriptor.rs`
  Own encode/decode of Iceberg partition `Struct` to/from thrift descriptor payloads.
- Modify `src/connector/iceberg/data_writer.rs`
  Populate partition descriptors in every writer report construction path.
- Modify `src/connector/iceberg/commit/collector.rs`
  Decode partition values from descriptors, delete the internal `partition_path` dependency, and keep path parsing only if needed as a named compat helper.
- Modify `src/connector/iceberg/sink.rs`
  Make position-delete sink use planning-provided output schema and emit descriptor-complete commit infos.
- Modify `src/sql/codegen/iceberg_write_sink.rs`
  Extend sink specs so data and delete writers can be built from planning descriptors.
- Modify `src/sql/codegen/fragment_builder.rs`
  Build position-delete writer fragments for DELETE/UPDATE/MERGE.
- Modify `src/engine/delete_flow.rs`
  Replace coordinator-local position-delete file writing with distributed `ICEBERG_DELETE_SINK`.
- Modify `src/engine/mutation_flow.rs`
  Replace coordinator-local MOR/COW/MERGE data/delete file writing with distributed writer outputs or fail-fast unsupported shapes.
- Modify `src/engine/equality_delete_flow.rs`
  Either cut ADD EQUALITY DELETE to a distributed sink or fail fast without local writer fallback.
- Modify `src/engine/mv/iceberg_refresh.rs`, `src/engine/mv/iceberg_merge_sink.rs`, `src/engine/mv/iceberg_join_coalesce.rs`
  Route MV refresh file output through `IcebergWriteTransactionRunner`.
- Modify `src/engine/write_transaction.rs`
  Delete `local_writer_commit_input` and `new_local_writer_write_id`.
- Add/modify SQL tests under `sql-tests/iceberg-rest/` and `sql-tests/iceberg-ivm/`.

## Task 1: Add Partition Descriptor IDL and Codec Test Harness

**Files:**
- Modify: `idl/thrift/Types.thrift:576-608`
- Modify: `src/connector/iceberg/mod.rs:18-42`
- Create: `src/connector/iceberg/write_descriptor.rs`
- Test: `src/connector/iceberg/write_descriptor.rs`

- [ ] **Step 1: Add failing codec tests**

Create `src/connector/iceberg/write_descriptor.rs` with tests first:

```rust
use iceberg::spec::{Literal, PrimitiveLiteral, Struct, TableMetadata};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum IcebergWriteDescriptorError {
    MissingDescriptor,
    UnknownPartitionSpec { spec_id: i32 },
    FieldCountMismatch { expected: usize, actual: usize },
    MissingPayload { index: usize },
    DecodeFailed { index: usize, message: String },
}

impl IcebergWriteDescriptorError {
    pub(crate) fn code(&self) -> &'static str {
        "IcebergWriteDescriptorMismatch"
    }
}

impl std::fmt::Display for IcebergWriteDescriptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingDescriptor => write!(f, "IcebergWriteDescriptorMismatch: missing partition descriptor"),
            Self::UnknownPartitionSpec { spec_id } => write!(
                f,
                "IcebergWriteDescriptorMismatch: unknown partition spec id {spec_id}"
            ),
            Self::FieldCountMismatch { expected, actual } => write!(
                f,
                "IcebergWriteDescriptorMismatch: partition descriptor field count mismatch: expected {expected}, got {actual}"
            ),
            Self::MissingPayload { index } => write!(
                f,
                "IcebergWriteDescriptorMismatch: partition descriptor value {index} is non-null but has no payload"
            ),
            Self::DecodeFailed { index, message } => write!(
                f,
                "IcebergWriteDescriptorMismatch: decode partition descriptor value {index} failed: {message}"
            ),
        }
    }
}

impl std::error::Error for IcebergWriteDescriptorError {}

pub(crate) fn encode_partition_descriptor(
    _values: &Struct,
    _partition_spec_id: i32,
    _metadata: &TableMetadata,
) -> Result<crate::types::TIcebergPartitionDescriptor, IcebergWriteDescriptorError> {
    panic!("encode_partition_descriptor is implemented in Task 2")
}

pub(crate) fn decode_partition_descriptor(
    _desc: Option<crate::types::TIcebergPartitionDescriptor>,
    _partition_spec_id: i32,
    _metadata: &TableMetadata,
) -> Result<Struct, IcebergWriteDescriptorError> {
    panic!("decode_partition_descriptor is implemented in Task 2")
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{
        DataFileFormat, FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema,
        TableMetadataBuilder, Transform, Type,
    };
    use std::sync::Arc;

    fn metadata_with_identity_partition() -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "region",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .expect("schema");
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(7)
            .add_partition_field("region", "region", Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec");
        TableMetadataBuilder::from_table_creation(
            schema,
            spec,
            "file:///warehouse/db/t".to_string(),
            Default::default(),
        )
        .expect("table metadata builder")
        .format_version(FormatVersion::V2)
        .build()
        .expect("table metadata")
    }

    #[test]
    fn descriptor_round_trips_identity_partition() {
        let metadata = metadata_with_identity_partition();
        let values = Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::String(
            "us west".to_string(),
        )))]);

        let desc = encode_partition_descriptor(&values, 7, &metadata).expect("encode descriptor");
        let decoded = decode_partition_descriptor(Some(desc), 7, &metadata).expect("decode descriptor");

        assert_eq!(decoded, values);
    }

    #[test]
    fn descriptor_round_trips_null_partition_value() {
        let metadata = metadata_with_identity_partition();
        let values = Struct::from_iter([None]);

        let desc = encode_partition_descriptor(&values, 7, &metadata).expect("encode descriptor");
        let decoded = decode_partition_descriptor(Some(desc), 7, &metadata).expect("decode descriptor");

        assert_eq!(decoded, values);
    }

    #[test]
    fn descriptor_rejects_missing_payload_for_non_null_value() {
        let metadata = metadata_with_identity_partition();
        let desc = crate::types::TIcebergPartitionDescriptor {
            values: Some(vec![crate::types::TIcebergPartitionValue {
                is_null: Some(false),
                datum_bytes: None,
            }]),
        };

        let err = decode_partition_descriptor(Some(desc), 7, &metadata).expect_err("expected error");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(err.to_string().contains("has no payload"));
    }

    #[test]
    fn descriptor_rejects_unknown_partition_spec_id() {
        let metadata = metadata_with_identity_partition();
        let desc = crate::types::TIcebergPartitionDescriptor { values: Some(vec![]) };

        let err = decode_partition_descriptor(Some(desc), 99, &metadata).expect_err("expected error");

        assert_eq!(
            err,
            IcebergWriteDescriptorError::UnknownPartitionSpec { spec_id: 99 }
        );
    }

    #[test]
    fn descriptor_rejects_field_count_mismatch() {
        let metadata = metadata_with_identity_partition();
        let desc = crate::types::TIcebergPartitionDescriptor { values: Some(vec![]) };

        let err = decode_partition_descriptor(Some(desc), 7, &metadata).expect_err("expected error");

        assert_eq!(
            err,
            IcebergWriteDescriptorError::FieldCountMismatch {
                expected: 1,
                actual: 0,
            }
        );
    }
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cargo test -q connector::iceberg::write_descriptor::tests::descriptor_round_trips_identity_partition
```

Expected: FAIL because `TIcebergPartitionDescriptor` and `TIcebergPartitionValue` do not exist.

- [ ] **Step 3: Add thrift structs and field**

In `idl/thrift/Types.thrift`, insert this block immediately above `struct TIcebergDataFile`:

```thrift
struct TIcebergPartitionValue {
    1: optional bool is_null;
    2: optional binary datum_bytes;
}

struct TIcebergPartitionDescriptor {
    1: optional list<TIcebergPartitionValue> values;
}
```

Then add tag 15 to `TIcebergDataFile`:

```thrift
    15: optional TIcebergPartitionDescriptor partition_values_descriptor;
```

- [ ] **Step 4: Register the new module**

In `src/connector/iceberg/mod.rs`, add:

```rust
pub(crate) mod write_descriptor;
```

Place it near `data_writer` and `partition_spec`.

- [ ] **Step 5: Run the failing test again**

Run:

```bash
cargo test -q connector::iceberg::write_descriptor::tests::descriptor_round_trips_identity_partition
```

Expected: FAIL at runtime with `encode_partition_descriptor is implemented in Task 2`.

- [ ] **Step 6: Commit Task 1**

```bash
git add idl/thrift/Types.thrift src/connector/iceberg/mod.rs src/connector/iceberg/write_descriptor.rs
git commit -m "Add Iceberg write partition descriptor scaffold"
```

## Task 2: Implement Descriptor Encode/Decode

**Files:**
- Modify: `src/connector/iceberg/write_descriptor.rs`
- Test: `src/connector/iceberg/write_descriptor.rs`

- [ ] **Step 1: Replace scaffold functions with implementation**

Replace the two scaffold functions in `src/connector/iceberg/write_descriptor.rs` with:

```rust
pub(crate) fn encode_partition_descriptor(
    values: &Struct,
    partition_spec_id: i32,
    metadata: &TableMetadata,
) -> Result<crate::types::TIcebergPartitionDescriptor, IcebergWriteDescriptorError> {
    let spec = metadata
        .partition_spec_by_id(partition_spec_id)
        .ok_or(IcebergWriteDescriptorError::UnknownPartitionSpec {
            spec_id: partition_spec_id,
        })?;
    let partition_type = spec
        .partition_type(metadata.current_schema().as_ref())
        .map_err(|e| IcebergWriteDescriptorError::DecodeFailed {
            index: 0,
            message: e.to_string(),
        })?;
    if values.fields().len() != partition_type.fields().len() {
        return Err(IcebergWriteDescriptorError::FieldCountMismatch {
            expected: partition_type.fields().len(),
            actual: values.fields().len(),
        });
    }
    let mut encoded = Vec::with_capacity(values.fields().len());
    for (idx, value) in values.fields().iter().enumerate() {
        match value {
            None => encoded.push(crate::types::TIcebergPartitionValue {
                is_null: Some(true),
                datum_bytes: None,
            }),
            Some(Literal::Primitive(primitive)) => {
                let field_type = partition_type.fields()[idx].field_type.as_ref();
                let iceberg::spec::Type::Primitive(primitive_type) = field_type else {
                    return Err(IcebergWriteDescriptorError::DecodeFailed {
                        index: idx,
                        message: format!("partition field type is not primitive: {field_type:?}"),
                    });
                };
                encoded.push(crate::types::TIcebergPartitionValue {
                    is_null: Some(false),
                    datum_bytes: Some(primitive_literal_to_iceberg_bytes(primitive, primitive_type)
                        .map_err(|message| IcebergWriteDescriptorError::DecodeFailed {
                            index: idx,
                            message,
                        })?),
                });
            }
            Some(other) => {
                return Err(IcebergWriteDescriptorError::DecodeFailed {
                    index: idx,
                    message: format!("partition descriptor only supports primitive literals, got {other:?}"),
                });
            }
        }
    }
    Ok(crate::types::TIcebergPartitionDescriptor {
        values: Some(encoded),
    })
}

pub(crate) fn decode_partition_descriptor(
    desc: Option<crate::types::TIcebergPartitionDescriptor>,
    partition_spec_id: i32,
    metadata: &TableMetadata,
) -> Result<Struct, IcebergWriteDescriptorError> {
    let desc = desc.ok_or(IcebergWriteDescriptorError::MissingDescriptor)?;
    let values = desc.values.unwrap_or_default();
    let spec = metadata
        .partition_spec_by_id(partition_spec_id)
        .ok_or(IcebergWriteDescriptorError::UnknownPartitionSpec {
            spec_id: partition_spec_id,
        })?;
    let partition_type = spec
        .partition_type(metadata.current_schema().as_ref())
        .map_err(|e| IcebergWriteDescriptorError::DecodeFailed {
            index: 0,
            message: e.to_string(),
        })?;
    if values.len() != partition_type.fields().len() {
        return Err(IcebergWriteDescriptorError::FieldCountMismatch {
            expected: partition_type.fields().len(),
            actual: values.len(),
        });
    }

    let mut decoded = Vec::with_capacity(values.len());
    for (idx, value) in values.into_iter().enumerate() {
        if value.is_null.unwrap_or(false) {
            decoded.push(None);
            continue;
        }
        let bytes = value
            .datum_bytes
            .ok_or(IcebergWriteDescriptorError::MissingPayload { index: idx })?;
        let field_type = partition_type.fields()[idx].field_type.as_ref();
        let iceberg::spec::Type::Primitive(primitive_type) = field_type else {
            return Err(IcebergWriteDescriptorError::DecodeFailed {
                index: idx,
                message: format!("partition field type is not primitive: {field_type:?}"),
            });
        };
        let datum = iceberg::spec::Datum::try_from_bytes(&bytes, primitive_type.clone()).map_err(|e| {
            IcebergWriteDescriptorError::DecodeFailed {
                index: idx,
                message: e.to_string(),
            }
        })?;
        decoded.push(Some(Literal::Primitive(datum.literal().clone())));
    }

    Ok(Struct::from_iter(decoded))
}
```

Add this local helper below the public functions. It mirrors iceberg-rust's `Datum::to_bytes` rules while using the partition field type supplied by table metadata:

```rust
fn primitive_literal_to_iceberg_bytes(
    literal: &PrimitiveLiteral,
    primitive_type: &iceberg::spec::PrimitiveType,
) -> Result<Vec<u8>, String> {
    use iceberg::spec::PrimitiveType;
    let bytes = match (literal, primitive_type) {
        (PrimitiveLiteral::Boolean(v), PrimitiveType::Boolean) => vec![u8::from(*v)],
        (PrimitiveLiteral::Int(v), PrimitiveType::Int | PrimitiveType::Date) => {
            v.to_le_bytes().to_vec()
        }
        (PrimitiveLiteral::Long(v), PrimitiveType::Long
            | PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs) => v.to_le_bytes().to_vec(),
        (PrimitiveLiteral::Float(v), PrimitiveType::Float) => v.to_le_bytes().to_vec(),
        (PrimitiveLiteral::Double(v), PrimitiveType::Double) => v.to_le_bytes().to_vec(),
        (PrimitiveLiteral::String(v), PrimitiveType::String) => v.as_bytes().to_vec(),
        (PrimitiveLiteral::Binary(v), PrimitiveType::Binary | PrimitiveType::Fixed(_)) => v.clone(),
        (PrimitiveLiteral::UInt128(v), PrimitiveType::Uuid) => v.to_be_bytes().to_vec(),
        (PrimitiveLiteral::Int128(v), PrimitiveType::Decimal { precision, .. }) => {
            let required = iceberg::spec::Type::decimal_required_bytes(*precision)
                .map_err(|e| e.to_string())? as usize;
            let mut all = v.to_be_bytes().to_vec();
            all.split_off(all.len() - required)
        }
        (other, ty) => {
            return Err(format!(
                "partition literal {other:?} is not compatible with partition type {ty:?}"
            ));
        }
    };
    Ok(bytes)
}
```

- [ ] **Step 2: Run descriptor unit tests**

Run:

```bash
cargo test -q connector::iceberg::write_descriptor::tests
```

Expected: PASS.

- [ ] **Step 3: Add primitive coverage tests**

Add one test in the same `tests` module:

```rust
#[test]
fn descriptor_round_trips_common_primitive_literals() {
    let metadata = metadata_with_multi_partition_fields();
    let values = Struct::from_iter([
        Some(Literal::Primitive(PrimitiveLiteral::Boolean(true))),
        Some(Literal::Primitive(PrimitiveLiteral::Int(7))),
        Some(Literal::Primitive(PrimitiveLiteral::Long(9))),
        Some(Literal::Primitive(PrimitiveLiteral::String("west".to_string()))),
        Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![1, 2, 3]))),
    ]);

        let desc = encode_partition_descriptor(&values, 8, &metadata).expect("encode descriptor");
    let decoded = decode_partition_descriptor(Some(desc), 8, &metadata).expect("decode descriptor");

    assert_eq!(decoded, values);
}
```

Add the helper directly below `metadata_with_identity_partition()`:

```rust
fn metadata_with_multi_partition_fields() -> TableMetadata {
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            Arc::new(NestedField::required(1, "flag", Type::Primitive(PrimitiveType::Boolean))),
            Arc::new(NestedField::required(2, "i", Type::Primitive(PrimitiveType::Int))),
            Arc::new(NestedField::required(3, "l", Type::Primitive(PrimitiveType::Long))),
            Arc::new(NestedField::required(4, "s", Type::Primitive(PrimitiveType::String))),
            Arc::new(NestedField::required(5, "b", Type::Primitive(PrimitiveType::Binary))),
        ])
        .build()
        .expect("schema");
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(8)
        .add_partition_field("flag", "flag", Transform::Identity)
        .expect("flag partition")
        .add_partition_field("i", "i", Transform::Identity)
        .expect("i partition")
        .add_partition_field("l", "l", Transform::Identity)
        .expect("l partition")
        .add_partition_field("s", "s", Transform::Identity)
        .expect("s partition")
        .add_partition_field("b", "b", Transform::Identity)
        .expect("b partition")
        .build()
        .expect("partition spec");
    TableMetadataBuilder::from_table_creation(
        schema,
        spec,
        "file:///warehouse/db/t".to_string(),
        Default::default(),
    )
    .expect("table metadata builder")
    .format_version(FormatVersion::V2)
    .build()
    .expect("table metadata")
}
```

- [ ] **Step 4: Run descriptor unit tests again**

Run:

```bash
cargo test -q connector::iceberg::write_descriptor::tests
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

```bash
git add src/connector/iceberg/write_descriptor.rs
git commit -m "Implement Iceberg write partition descriptor codec"
```

## Task 3: Populate Descriptors in All Writer Report Paths

**Files:**
- Modify: `src/connector/iceberg/data_writer.rs:373-583`
- Test: `src/connector/iceberg/data_writer.rs`

- [ ] **Step 1: Add failing tests for descriptor presence**

In the existing `#[cfg(test)]` module in `src/connector/iceberg/data_writer.rs`, add:

```rust
#[test]
fn data_file_to_iceberg_thrift_carries_partition_descriptor() {
    let metadata = test_string_partition_metadata(7);
    let partition = Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::String(
        "west".to_string(),
    )))]);
    let mut b = DataFileBuilder::default();
    b.content(DataContentType::Data)
        .file_path("file:///warehouse/t/data/a.parquet".to_string())
        .file_format(DataFileFormat::Parquet)
        .partition(partition)
        .record_count(1)
        .file_size_in_bytes(12)
        .partition_spec_id(7);
    let df = b.build().expect("data file");

    let thrift = data_file_to_iceberg_thrift(
        &df,
        "wrong=path".to_string(),
        "0".to_string(),
        "PARQUET".to_string(),
        crate::types::TIcebergFileContent::DATA,
        Some(7),
        &metadata,
    )
    .expect("thrift");

    let desc = thrift
        .partition_values_descriptor
        .expect("partition descriptor must be present");
    assert_eq!(desc.values.expect("values").len(), 1);
}

#[test]
fn written_file_to_sink_commit_info_carries_partition_descriptor() {
    let metadata = test_string_partition_metadata(7);
    let file = super::commit::WrittenFile {
        path: "file:///warehouse/t/data/a.parquet".to_string(),
        format: DataFileFormat::Parquet,
        content: DataContentType::Data,
        partition_values: Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::String(
            "west".to_string(),
        )))]),
        partition_spec_id: 7,
        record_count: 1,
        file_size_in_bytes: 12,
        split_offsets: Vec::new(),
        column_sizes: Default::default(),
        value_counts: Default::default(),
        null_value_counts: Default::default(),
        lower_bounds: Default::default(),
        upper_bounds: Default::default(),
        key_metadata: None,
        referenced_data_file: None,
        equality_ids: None,
        first_row_id: None,
    };

    let info = written_file_to_sink_commit_info(&file, &metadata).expect("commit info");
    let desc = info
        .iceberg_data_file
        .expect("data file")
        .partition_values_descriptor
        .expect("partition descriptor must be present");

    assert_eq!(desc.values.expect("values").len(), 1);
}
```

Add this helper in the same tests module:

```rust
fn test_string_partition_metadata(spec_id: i32) -> TableMetadata {
    let schema = Schema::builder()
        .with_schema_id(1)
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "region",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()
        .expect("schema");
    let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(spec_id)
            .add_partition_field("region", "region", Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec");
    TableMetadataBuilder::from_table_creation(
        schema,
        spec,
        "file:///warehouse/db/t".to_string(),
        Default::default(),
    )
    .expect("table metadata builder")
    .format_version(FormatVersion::V2)
    .build()
    .expect("table metadata")
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test -q data_file_to_iceberg_thrift_carries_partition_descriptor written_file_to_sink_commit_info_carries_partition_descriptor
```

Expected: FAIL because `partition_values_descriptor` is `None`.

- [ ] **Step 3: Extend and populate `data_file_to_iceberg_thrift`**

In `src/connector/iceberg/data_writer.rs`, add:

```rust
use crate::connector::iceberg::write_descriptor::encode_partition_descriptor;
```

Change `data_file_to_iceberg_thrift` to accept table metadata:

```rust
pub(crate) fn data_file_to_iceberg_thrift(
    df: &DataFile,
    partition_path: String,
    null_fingerprint: String,
    format: String,
    content: crate::types::TIcebergFileContent,
    partition_spec_id: Option<i32>,
    metadata: &TableMetadata,
) -> Result<crate::types::TIcebergDataFile, String> {
```

Update callers in `to_sink_commit_info` to pass `staged.metadata.as_ref()` by first adding
`metadata: Arc<TableMetadata>` to `StagedDataFile` and filling it from `StagedWriteContext`.
Then inside `data_file_to_iceberg_thrift`, before the `Ok(crate::types::TIcebergDataFile { ... })`, add:

```rust
    let partition_spec_id = partition_spec_id
        .ok_or_else(|| "IcebergWriteDescriptorMismatch: missing partition_spec_id".to_string())?;
    let partition_values_descriptor =
        encode_partition_descriptor(df.partition(), partition_spec_id, metadata)
        .map_err(|e| e.to_string())?;
```

And add this field to the `TIcebergDataFile` literal:

```rust
        partition_values_descriptor: Some(partition_values_descriptor),
```

- [ ] **Step 4: Populate descriptor in `written_file_to_sink_commit_info`**

Change `written_file_to_sink_commit_info` to accept table metadata instead of only a partition spec:

```rust
pub(crate) fn written_file_to_sink_commit_info(
    file: &super::commit::WrittenFile,
    metadata: &TableMetadata,
) -> Result<crate::types::TSinkCommitInfo, String> {
```

Inside the function, derive the partition spec from `file.partition_spec_id`, then build both
the compatibility path fields and the descriptor:

```rust
    let partition_spec = metadata
        .partition_spec_by_id(file.partition_spec_id)
        .ok_or_else(|| {
            format!(
                "iceberg written file `{}` references unknown partition spec id {}",
                file.path, file.partition_spec_id
            )
        })?;
    let (partition_path, null_fingerprint) =
        partition_path_from_struct(&file.partition_values, partition_spec)?;
    let partition_values_descriptor =
        encode_partition_descriptor(&file.partition_values, file.partition_spec_id, metadata)
            .map_err(|e| e.to_string())?;
```

Then add this field to the literal:

```rust
        partition_values_descriptor: Some(partition_values_descriptor),
```

- [ ] **Step 5: Ensure explicit literals compile**

Search:

```bash
rg -n "TIcebergDataFile \\{" src
```

For every explicit `TIcebergDataFile` literal outside `data_writer.rs`, add either a real descriptor or a test-only empty descriptor. Production literals in `src/connector/iceberg/sink.rs` must call the same descriptor encoder. Test fixtures can use:

```rust
partition_values_descriptor: Some(crate::types::TIcebergPartitionDescriptor {
    values: Some(Vec::new()),
}),
```

- [ ] **Step 6: Run tests**

Run:

```bash
cargo test -q data_file_to_iceberg_thrift_carries_partition_descriptor written_file_to_sink_commit_info_carries_partition_descriptor
```

Expected: PASS.

- [ ] **Step 7: Commit Task 3**

```bash
git add src/connector/iceberg/data_writer.rs src/connector/iceberg/sink.rs src/runtime/write_coordinator.rs src/service/grpc_server.rs
git commit -m "Carry Iceberg partition descriptors in writer reports"
```

## Task 4: Flip Collector to Descriptor Decode

**Files:**
- Modify: `src/connector/iceberg/commit/collector.rs:322-350`
- Test: `src/connector/iceberg/commit/collector.rs`

- [ ] **Step 1: Add failing collector tests**

In `src/connector/iceberg/commit/collector.rs` tests, add:

```rust
#[test]
fn convert_uses_partition_descriptor_not_partition_path() {
    let (collector, metadata) = test_collector_with_string_partition_spec(7);
    let values = iceberg::spec::Struct::from_iter([Some(iceberg::spec::Literal::string("west"))]);
    let descriptor =
        crate::connector::iceberg::write_descriptor::encode_partition_descriptor(
            &values,
            7,
            &metadata,
        )
            .expect("descriptor");
    let thrift = crate::types::TIcebergDataFile {
        path: Some("file:///warehouse/t/data/a.parquet".to_string()),
        format: Some("PARQUET".to_string()),
        record_count: Some(1),
        file_size_in_bytes: Some(12),
        partition_path: Some("region=east".to_string()),
        split_offsets: None,
        column_stats: None,
        partition_null_fingerprint: Some("0".to_string()),
        file_content: Some(crate::types::TIcebergFileContent::DATA),
        referenced_data_file: None,
        first_row_id: None,
        equality_ids: None,
        key_metadata: None,
        partition_spec_id: Some(7),
        partition_values_descriptor: Some(descriptor),
    };

    let written = collector.convert(thrift).expect("convert");

    assert_eq!(written.partition_values, values);
    assert_eq!(written.partition_spec_id, 7);
    drop(metadata);
}

#[test]
fn convert_rejects_missing_partition_descriptor() {
    let (collector, _metadata) = test_collector_with_string_partition_spec(7);
    let thrift = crate::types::TIcebergDataFile {
        path: Some("file:///warehouse/t/data/a.parquet".to_string()),
        format: Some("PARQUET".to_string()),
        record_count: Some(1),
        file_size_in_bytes: Some(12),
        partition_path: Some("region=west".to_string()),
        split_offsets: None,
        column_stats: None,
        partition_null_fingerprint: Some("0".to_string()),
        file_content: Some(crate::types::TIcebergFileContent::DATA),
        referenced_data_file: None,
        first_row_id: None,
        equality_ids: None,
        key_metadata: None,
        partition_spec_id: Some(7),
        partition_values_descriptor: None,
    };

    let err = collector.convert(thrift).expect_err("expected descriptor error");

    assert!(err.contains("IcebergWriteDescriptorMismatch"));
}
```

Add helper:

```rust
fn test_collector_with_string_partition_spec(
    spec_id: i32,
) -> (IcebergCommitCollector, iceberg::spec::TableMetadata) {
    let metadata = test_table_metadata_with_string_partition_spec(spec_id);
    let collector = IcebergCommitCollector::new(
        CommitOpKind::FastAppend,
        iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new("ns".to_string()),
            "t".to_string(),
        ),
        None,
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata
            .partition_spec_by_id(spec_id)
            .expect("partition spec")
            .clone(),
        "file:///warehouse/t/data/_staging/test".to_string(),
        crate::common::types::UniqueId { hi: 1, lo: 2 },
    );
    (collector, metadata)
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test -q convert_uses_partition_descriptor_not_partition_path convert_rejects_missing_partition_descriptor
```

Expected: first test FAILS because current code uses `partition_path`; second test may PASS or FAIL with the wrong error. Continue either way.

- [ ] **Step 3: Add metadata to collector**

`IcebergCommitCollector` currently stores `schema` and `partition_spec`. Add a field:

```rust
metadata: Option<iceberg::spec::TableMetadata>,
```

Add a constructor overload and update every production writer collector construction to use it:

```rust
pub(crate) fn with_table_metadata(mut self, metadata: iceberg::spec::TableMetadata) -> Self {
    self.metadata = Some(metadata);
    self
}
```

Then update every production collector creation site that already has `table.metadata()` in scope to call:

```rust
.with_table_metadata(table.metadata().clone())
```

Use `rg -n "IcebergCommitCollector::new" src` to update all writer paths.

- [ ] **Step 4: Replace partition decode in `convert`**

Replace lines that compute `partition_spec_id` and `partition_values` with:

```rust
        let partition_spec_id = df
            .partition_spec_id
            .ok_or_else(|| "IcebergWriteDescriptorMismatch: TIcebergDataFile missing partition_spec_id".to_string())?;
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            "IcebergWriteDescriptorMismatch: IcebergCommitCollector missing table metadata".to_string()
        })?;
        let partition_values =
            crate::connector::iceberg::write_descriptor::decode_partition_descriptor(
                df.partition_values_descriptor,
                partition_spec_id,
                metadata,
            )
            .map_err(|e| e.to_string())?;
```

- [ ] **Step 5: Keep `parse_partition_path` out of internal convert path**

Search:

```bash
rg -n "parse_partition_path\\(|partition_path.as_deref" src/connector/iceberg/commit/collector.rs
```

Expected after edits: no match inside `convert`. Move `parse_partition_path` below a `compat_partition_path` comment, keep it private, and use it only from tests that explicitly validate legacy compatibility parsing.

- [ ] **Step 6: Run collector tests**

Run:

```bash
cargo test -q connector::iceberg::commit::collector::tests
```

Expected: PASS.

- [ ] **Step 7: Commit Task 4**

```bash
git add src/connector/iceberg/commit/collector.rs src/engine src/connector/iceberg
git commit -m "Decode Iceberg commit partitions from descriptors"
```

## Task 5: Make Position-Delete Sink Descriptor-Authoritative

**Files:**
- Modify: `src/connector/iceberg/sink.rs:122-190,470-520,760-777`
- Modify: `src/sql/codegen/iceberg_write_sink.rs:13-62`
- Modify: `src/sql/codegen/fragment_builder.rs:651-701`
- Test: `src/connector/iceberg/sink.rs`

- [ ] **Step 1: Add failing sink test for descriptor on position-delete file**

In `src/connector/iceberg/sink.rs` tests, add this test and the two named helpers below it:

```rust
#[test]
fn position_delete_sink_commit_info_carries_partition_descriptor() {
    let schema = build_position_delete_output_schema();
    let batch = position_delete_batch_for_single_file("file:///warehouse/t/data/a.parquet", 7);
    let commit_info = write_position_delete_test_batch(schema, batch).expect("commit info");

    let data_file = commit_info.iceberg_data_file.expect("iceberg data file");
    assert_eq!(
        data_file.file_content,
        Some(crate::types::TIcebergFileContent::POSITION_DELETES)
    );
    assert!(
        data_file.partition_values_descriptor.is_some(),
        "position-delete files must carry partition descriptor"
    );
}
```

Add helper `position_delete_batch_for_single_file`:

```rust
fn position_delete_batch_for_single_file(path: &str, pos: i64) -> RecordBatch {
    let schema = build_position_delete_output_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![path])) as ArrayRef,
            Arc::new(Int64Array::from(vec![pos])) as ArrayRef,
        ],
    )
    .expect("position delete batch")
}
```

Add helper `write_position_delete_test_batch` by copying the construction pattern from the existing
`IcebergSinkMode::PositionDeletes` test in this module. The helper signature must be:

```rust
fn write_position_delete_test_batch(
    schema: SchemaRef,
    batch: RecordBatch,
) -> Result<crate::types::TSinkCommitInfo, String>
```

The helper must instantiate an `IcebergTableSinkFactory` with `IcebergSinkMode::PositionDeletes`,
push `batch`, finish the sink, and return the single `TSinkCommitInfo` recorded in the test
`RuntimeState`. The helper must assert there is exactly one commit info:

```rust
let infos = crate::runtime::sink_commit::list(finst_id);
assert_eq!(infos.len(), 1, "expected one position-delete commit info");
Ok(infos.into_iter().next().expect("commit info"))
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cargo test -q position_delete_sink_commit_info_carries_partition_descriptor
```

Expected: FAIL because the explicit `TIcebergDataFile` literal in `push_chunk_position_delete` does not set descriptor from a partition `Struct`.

- [ ] **Step 3: Add `partition_values` to `PartitionKey`**

In `src/connector/iceberg/sink.rs`, extend the private `PartitionKey` struct with:

```rust
partition_values: iceberg::spec::Struct,
partition_spec_id: i32,
```

When constructing a key from evaluated partition expressions, build the `Struct` at the same time the path/null fingerprint are built. Use the existing evaluated literal values as the single source of truth.

- [ ] **Step 4: Set descriptor in position-delete `TIcebergDataFile`**

Inside `push_chunk_position_delete`, before the `TIcebergDataFile` literal, add:

```rust
            let partition_values_descriptor =
                crate::connector::iceberg::write_descriptor::encode_partition_descriptor(
                    &key.partition_values,
                    key.partition_spec_id,
                    self.plan.table_metadata.as_ref(),
                )
                .map_err(|e| e.to_string())?;
```

Then set:

```rust
                partition_values_descriptor: Some(partition_values_descriptor),
                partition_spec_id: Some(key.partition_spec_id),
```

Replace the existing `partition_spec_id: None`.

- [ ] **Step 5: Thread planning descriptor through sink plan**

In `src/sql/codegen/iceberg_write_sink.rs`, extend `IcebergWriteSinkSpec`:

```rust
pub(crate) enum IcebergWriteSinkMode {
    Data,
    PositionDeletes,
}
```

Add:

```rust
pub mode: IcebergWriteSinkMode,
```

In `build_sink`, use:

```rust
let sink_type = match self.mode {
    IcebergWriteSinkMode::Data => data_sinks::TDataSinkType::ICEBERG_TABLE_SINK,
    IcebergWriteSinkMode::PositionDeletes => data_sinks::TDataSinkType::ICEBERG_DELETE_SINK,
};
```

And pass `sink_type` to `TDataSink::new`.

- [ ] **Step 6: Update insert callers to set data mode**

Every construction of `IcebergWriteSinkSpec` for INSERT must set:

```rust
mode: IcebergWriteSinkMode::Data,
```

Search:

```bash
rg -n "IcebergWriteSinkSpec \\{" src
```

Expected: all literals set `mode`.

- [ ] **Step 7: Run sink tests**

Run:

```bash
cargo test -q connector::iceberg::sink::tests
```

Expected: PASS.

- [ ] **Step 8: Commit Task 5**

```bash
git add src/connector/iceberg/sink.rs src/sql/codegen/iceberg_write_sink.rs src/sql/codegen/fragment_builder.rs
git commit -m "Make Iceberg delete sink report partition descriptors"
```

## Task 6: Cut DELETE to Distributed Position-Delete Sink

**Files:**
- Modify: `src/engine/delete_flow.rs:139-250,253-315`
- Modify: `src/engine/mod.rs:3103-3158`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Test: `sql-tests/iceberg-rest/sql/iceberg_rest_distributed_delete.sql`
- Test: `sql-tests/iceberg-rest/result/iceberg_rest_distributed_delete.result`

- [ ] **Step 1: Add SQL regression case**

Create `sql-tests/iceberg-rest/sql/iceberg_rest_distributed_delete.sql`:

```sql
-- @suite iceberg-rest
-- @require iceberg_rest

CREATE EXTERNAL CATALOG ice_del_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "${NOVAROCKS_ICEBERG_REST_URI}",
  "iceberg.catalog.warehouse" = "${NOVAROCKS_ICEBERG_REST_WAREHOUSE}",
  "aws.s3.endpoint" = "${AWS_S3_ENDPOINT}",
  "aws.s3.access_key" = "${AWS_S3_ACCESS_KEY_ID}",
  "aws.s3.secret_key" = "${AWS_S3_SECRET_ACCESS_KEY}",
  "aws.s3.enable_path_style_access" = "true"
);

CREATE DATABASE ice_del_${uuid0}.ns_${uuid0};

CREATE TABLE ice_del_${uuid0}.ns_${uuid0}.orders (
  id INT,
  region STRING,
  amount INT
)
USING ICEBERG
PARTITIONED BY (region);

INSERT INTO ice_del_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'east', 10),
  (2, 'east', 20),
  (3, 'west', 30);

DELETE FROM ice_del_${uuid0}.ns_${uuid0}.orders WHERE region = 'east' AND amount = 10;

SELECT id, region, amount FROM ice_del_${uuid0}.ns_${uuid0}.orders ORDER BY id;
```

Create expected result file:

```text
2	east	20
3	west	30
```

- [ ] **Step 2: Run case to record current behavior**

Run with an active runtime:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_distributed_delete --mode verify
```

Expected before cutover: PASS or FAIL depending on existing local DELETE behavior. This establishes the behavior contract.

- [ ] **Step 3: Introduce distributed DELETE executor**

In `src/engine/delete_flow.rs`, add:

```rust
struct DistributedDeleteWriteExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    delete_query: sqlparser::ast::Query,
    sink_spec: crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec,
    commit_executor: IcebergWriteCommitExecutor,
}

impl IcebergWriteTransactionExecutor for DistributedDeleteWriteExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.delete_query,
            self.sink_spec.clone(),
            None,
        )
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}
```

- [ ] **Step 4: Build DELETE row-identity SELECT**

Replace the coordinator-local call to `scan_for_position_deletes_at` for `IcebergSqlDeleteStrategy::PositionDeleteFiles` with a helper:

```rust
fn build_delete_position_sink_query(
    target: &TargetBackend,
    where_clause: &sqlparser::ast::Expr,
) -> Result<sqlparser::ast::Query, String> {
    let qualified = format!(
        "{}.{}.{}",
        target.catalog, target.namespace, target.table
    );
    let sql = format!(
        "SELECT _file, _pos FROM {qualified} WHERE {where_clause}"
    );
    match crate::sql::parser::parse_sql_raw(&sql).map_err(|e| {
        format!("UnsupportedDistributedDmlShape: build distributed DELETE query failed: {e}")
    })? {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        other => Err(format!(
            "UnsupportedDistributedDmlShape: distributed DELETE helper built non-query statement {other:?}"
        )),
    }
}
```

The generated SELECT projects the row identity columns used by the Iceberg scan layer. Add constants beside the scan metadata-column definitions:

```rust
pub(crate) const ICEBERG_ROW_IDENTITY_FILE_COLUMN: &str = "_file";
pub(crate) const ICEBERG_ROW_IDENTITY_POS_COLUMN: &str = "_pos";
```

Use those constants in the generated SELECT instead of string literals after the constants are added.

- [ ] **Step 5: Build position-delete sink spec**

In `src/engine/iceberg_writer.rs`, replace the private `build_insert_write_sink_spec` implementation
with a mode-parameterized helper and keep a data-mode wrapper for INSERT:

```rust
pub(crate) fn build_iceberg_write_sink_spec(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    table: &iceberg::table::Table,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    mode: crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkMode,
) -> Result<crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec, String> {
    let mut spec = build_insert_write_sink_spec(target, resolved, table, entry)?;
    spec.mode = mode;
    Ok(spec)
}

pub(crate) fn build_position_delete_sink_spec(
    target: &TargetBackend,
    resolved: &ResolvedTable,
    table: &iceberg::table::Table,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
) -> Result<crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec, String> {
    build_iceberg_write_sink_spec(
        target,
        resolved,
        table,
        entry,
        crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkMode::PositionDeletes,
    )
}
```

At DELETE call sites, reuse the `ResolvedTable` already produced by backend resolution for the target.
Do not reconstruct `TableDef` from scratch in `delete_flow.rs`.

- [ ] **Step 6: Wire DELETE transaction to distributed executor**

Inside `run_delete_write_transaction`, construct `DistributedDeleteWriteExecutor` instead of `DeleteWriteExecutor` for `PositionDeleteFiles`. Remove the branch that calls `write_position_delete_files`.

For `DeletionVectors`, do not keep the coordinator-local collector injection in this PR. Return:

```rust
return Err(
    "UnsupportedDistributedDmlShape: deletion-vector DELETE is not yet representable as a distributed writer output".to_string(),
);
```

- [ ] **Step 7: Run DELETE test**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_distributed_delete --mode verify
```

Expected: PASS.

- [ ] **Step 8: Run targeted unit tests**

Run:

```bash
cargo test -q delete_flow
cargo test -q connector::iceberg::sink::tests
```

Expected: PASS.

- [ ] **Step 9: Commit Task 6**

```bash
git add src/engine/delete_flow.rs src/engine/mod.rs src/engine/iceberg_writer.rs src/sql/codegen/fragment_builder.rs sql-tests/iceberg-rest/sql/iceberg_rest_distributed_delete.sql sql-tests/iceberg-rest/result/iceberg_rest_distributed_delete.result
git commit -m "Cut Iceberg DELETE to distributed delete sink"
```

## Task 7: Cut UPDATE and MERGE Writer Outputs to Distributed Sinks

**Files:**
- Modify: `src/engine/mutation_flow.rs:438-675`
- Modify: `src/engine/iceberg_writer.rs`
- Test: `sql-tests/iceberg-rest/sql/iceberg_rest_distributed_update_merge.sql`
- Test: `sql-tests/iceberg-rest/result/iceberg_rest_distributed_update_merge.result`

- [ ] **Step 1: Add SQL regression for MOR UPDATE and MERGE**

Create `sql-tests/iceberg-rest/sql/iceberg_rest_distributed_update_merge.sql`:

```sql
-- @suite iceberg-rest
-- @require iceberg_rest

CREATE EXTERNAL CATALOG ice_dml_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "${NOVAROCKS_ICEBERG_REST_URI}",
  "iceberg.catalog.warehouse" = "${NOVAROCKS_ICEBERG_REST_WAREHOUSE}",
  "aws.s3.endpoint" = "${AWS_S3_ENDPOINT}",
  "aws.s3.access_key" = "${AWS_S3_ACCESS_KEY_ID}",
  "aws.s3.secret_key" = "${AWS_S3_SECRET_ACCESS_KEY}",
  "aws.s3.enable_path_style_access" = "true"
);

CREATE DATABASE ice_dml_${uuid0}.ns_${uuid0};

CREATE TABLE ice_dml_${uuid0}.ns_${uuid0}.orders (
  id INT,
  region STRING,
  amount INT
)
USING ICEBERG;

INSERT INTO ice_dml_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'east', 10),
  (2, 'west', 20);

UPDATE ice_dml_${uuid0}.ns_${uuid0}.orders SET amount = 99 WHERE id = 1;

CREATE TABLE ice_dml_${uuid0}.ns_${uuid0}.updates (
  id INT,
  region STRING,
  amount INT
)
USING ICEBERG;

INSERT INTO ice_dml_${uuid0}.ns_${uuid0}.updates VALUES
  (2, 'west', 200),
  (3, 'north', 300);

MERGE INTO ice_dml_${uuid0}.ns_${uuid0}.orders AS t
USING ice_dml_${uuid0}.ns_${uuid0}.updates AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET amount = s.amount
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.region, s.amount);

SELECT id, region, amount FROM ice_dml_${uuid0}.ns_${uuid0}.orders ORDER BY id;
```

Create expected result:

```text
1	east	99
2	west	200
3	north	300
```

- [ ] **Step 2: Run the regression before code changes**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_distributed_update_merge --mode verify
```

Expected: existing behavior may pass via local writers. Keep the golden as the semantic contract.

- [ ] **Step 3: Add output validator**

In `src/engine/mutation_flow.rs`, add:

```rust
fn ensure_distributed_write_output_has_files(
    write: &CoordinatedQueryResult,
    context: &str,
) -> Result<(), String> {
    let Some(commit) = &write.write_commit else {
        return Err(format!(
            "DistributedWriteOutputMismatch: {context} produced no write commit"
        ));
    };
    if !crate::engine::write_transaction::write_commit_has_files(commit) {
        return Err(format!(
            "DistributedWriteOutputMismatch: {context} produced no sink commit infos"
        ));
    }
    Ok(())
}
```

- [ ] **Step 4: Add batch-backed distributed write helper**

In `src/engine/mod.rs`, add a sibling of `execute_query_as_iceberg_write`:

```rust
pub(crate) fn execute_record_batches_as_iceberg_write(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    batches: Vec<RecordBatch>,
    sink_spec: crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<crate::runtime::coordinator::CoordinatedQueryResult, String> {
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(crate::runtime::coordinator::CoordinatedQueryResult {
            query_result: QueryResult::empty(),
            write_commit: None,
            write_abort: None,
        });
    }
    let schema = batches[0].schema();
    for batch in &batches {
        if batch.schema() != schema {
            return Err("DistributedWriteOutputMismatch: batch-backed write received mismatched schemas".to_string());
        }
    }
    let values_plan = crate::sql::planner::logical_values_from_record_batches(batches)
        .map_err(|e| format!("DistributedWriteOutputMismatch: build batch-backed write values failed: {e}"))?;
    let table_stats = build_table_stats_from_plan(&values_plan);
    let physical = crate::sql::optimizer::optimize(
        values_plan,
        &table_stats,
        crate::sql::analyzer::ColumnRefFactory::default(),
        None,
        Vec::new(),
    )?;
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let build_result =
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_with_iceberg_sink(
            &physical,
            &catalog_snapshot,
            &connectors_snapshot,
            current_database,
            None,
            &sink_spec,
        )?;
    let exchange_port = if state.exchange_port == 0 {
        ensure_standalone_exchange_server()?
    } else {
        state.exchange_port
    };
    let (dispatcher, scheduler) = coordinated_execution_services(exchange_port)?;
    crate::runtime::coordinator::ExecutionCoordinator::new(
        build_result,
        dispatcher,
        scheduler,
        query_opts,
    )
    .execute_with_write_outcome()
}
```

Create `logical_values_from_record_batches` in `src/sql/planner/mod.rs` with this signature:

```rust
pub(crate) fn logical_values_from_record_batches(
    batches: Vec<RecordBatch>,
) -> Result<LogicalPlan, String>
```

It must wrap the batches in the existing logical/physical Values node path used by SQL `VALUES`, preserving Arrow schema and row order.

- [ ] **Step 5: Replace MOR UPDATE data writer**

Replace the loop that calls `write_row_lineage_batches_as_data_files` with a call to a distributed data sink query executor. Use the same helper shape as INSERT:

```rust
let write = crate::engine::execute_record_batches_as_iceberg_write(
    &self.commit_executor.state,
    Some(&self.commit_executor.target.catalog),
    &self.commit_executor.target.namespace,
    runs.iter().map(|run| run.batch.clone()).collect(),
    self.build_data_sink_spec()?,
    None,
)?;
ensure_distributed_write_output_has_files(&write, "MOR UPDATE replacement writer")?;
sink_commit_infos.extend(flatten_sink_commit_infos(write.write_commit));
```

- [ ] **Step 6: Replace MOR UPDATE delete writer**

Replace `self.collector.inject_delete_group(group)` in `run_mor_update_write` with distributed `ICEBERG_DELETE_SINK` output. Build a `RecordBatch` with file path and pos columns from `delete_groups`, then run it through `execute_record_batches_as_iceberg_write` using a `PositionDeletes` sink spec.

The batch schema must be:

```rust
Schema::new(vec![
    Field::new(ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, DataType::Utf8, false),
    Field::new(ICEBERG_POSITION_DELETE_POS_COLUMN, DataType::Int64, false),
])
```

After execution:

```rust
ensure_distributed_write_output_has_files(&write, "MOR UPDATE position-delete writer")?;
sink_commit_infos.extend(flatten_sink_commit_infos(write.write_commit));
```

- [ ] **Step 7: Replace MERGE matched DELETE**

In `MutationWritePlan::MergeMatchedDelete`, replace direct `collector.inject_delete_group` with the same position-delete batch writer used by MOR UPDATE.

- [ ] **Step 8: Replace MERGE unmatched INSERT**

Replace `write_record_batches_as_data_files` in `MutationWritePlan::MergeUnmatchedInsert` with distributed data sink batch execution. Use the same data sink spec as INSERT and append the returned sink commit infos.

- [ ] **Step 9: Make COW UPDATE fail fast without local writer fallback**

At the point where COW UPDATE would call `write_cow_update_files`, return:

```rust
return Err(
    "UnsupportedDistributedDmlShape: COW UPDATE requires distributed rewrite metadata before it can run without coordinator-local writers".to_string(),
);
```

Do not keep `write_cow_update_files` as fallback.

- [ ] **Step 10: Run regression**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_distributed_update_merge --mode verify
```

Expected: PASS for MOR paths; COW-specific tests either pass through distributed metadata or fail with stable `UnsupportedDistributedDmlShape` golden if the test exercises unsupported COW.

- [ ] **Step 11: Commit Task 7**

```bash
git add src/engine/mutation_flow.rs src/engine/mod.rs src/engine/iceberg_writer.rs sql-tests/iceberg-rest/sql/iceberg_rest_distributed_update_merge.sql sql-tests/iceberg-rest/result/iceberg_rest_distributed_update_merge.result
git commit -m "Cut Iceberg UPDATE and MERGE writers to distributed sinks"
```

## Task 8: Remove Equality-Delete Local Writer Fallback

**Files:**
- Modify: `src/engine/equality_delete_flow.rs:177-225`
- Test: `src/engine/equality_delete_flow.rs`

- [ ] **Step 1: Add fail-fast test**

In `src/engine/equality_delete_flow.rs` tests, add a unit test for the executor entrypoint that asserts:

```rust
assert!(
    err.contains("UnsupportedDistributedDmlShape: ADD EQUALITY DELETE requires distributed equality-delete sink support"),
    "{err}"
);
```

- [ ] **Step 2: Replace local writer call**

In `EqualityDeleteWriteExecutor::run_coordinated_write`, remove the call to `write_equality_delete_file` and return:

```rust
Err(
    "UnsupportedDistributedDmlShape: ADD EQUALITY DELETE requires distributed equality-delete sink support".to_string(),
)
```

This satisfies the strong terminal rule by removing the coordinator-local writer. This PR chooses fail-fast behavior for ADD EQUALITY DELETE unless an earlier task adds a complete distributed equality-delete sink.

- [ ] **Step 3: Run targeted test**

Run:

```bash
cargo test -q equality_delete_flow
```

Expected: PASS with updated fail-fast behavior.

- [ ] **Step 4: Commit Task 8**

```bash
git add src/engine/equality_delete_flow.rs
git commit -m "Fail fast unsupported distributed equality deletes"
```

## Task 9: Route MV Refresh Through Transaction Runner

**Files:**
- Modify: `src/engine/mv/iceberg_refresh.rs:7539-7592,7617-7671,7774-7785`
- Modify: `src/engine/mv/iceberg_merge_sink.rs`
- Modify: `src/engine/mv/iceberg_join_coalesce.rs`
- Test: existing `sql-tests/iceberg-ivm/*`

- [ ] **Step 1: Add runner adapter**

In `src/engine/mv/iceberg_refresh.rs`, add:

```rust
struct MvRefreshWriteExecutor {
    commit_executor: crate::engine::write_transaction::IcebergWriteCommitExecutor,
    write_output: std::sync::Mutex<Option<CoordinatedQueryResult>>,
}

impl IcebergWriteTransactionExecutor for MvRefreshWriteExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        self.write_output
            .lock()
            .expect("MV refresh write output lock poisoned")
            .take()
            .ok_or_else(|| "DistributedWriteOutputMismatch: MV refresh write output was already consumed".to_string())
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}
```

- [ ] **Step 2: Convert populated collector paths to `WriteCommitInput`**

Where MV refresh currently calls `collector.inject_sink_commit_infos(...)` or `run_iceberg_commit(RunInput { ... })`, build a `CoordinatedQueryResult`:

```rust
let write_commit = WriteCommitInput {
    write_id: crate::common::types::UniqueId { hi: 0, lo: 0 }.into(),
    writers: vec![WriterCommitInput {
        writer_id: 0,
        writer_key: WriterKey {
            query_id: write_id.clone(),
            fragment_instance_id: write_id.clone(),
            backend_num: 0,
        },
        sink_commit_infos,
        tablet_commit_infos: Vec::new(),
        tablet_fail_infos: Vec::new(),
        load_counters: Default::default(),
        loaded_rows: 0,
        loaded_bytes: 0,
        filtered_rows: 0,
    }],
};
```

This adapter is allowed only inside the MV refresh runner cutover task. Task 10 must delete it or replace it with a shared `WriteCommitInput` builder that is not named or exported as a local writer shim.

- [ ] **Step 3: Run MV through `IcebergWriteTransactionRunner`**

Construct `IcebergWriteTransactionSpec` from the current MV commit metadata and call:

```rust
let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
let outcome = runner.run(spec)?;
```

Return `outcome.committed_snapshot_id` as the refresh snapshot.

- [ ] **Step 4: Remove direct DML/MV `run_iceberg_commit` calls**

Search:

```bash
rg -n "run_iceberg_commit\\(" src/engine/mv src/engine/delete_flow.rs src/engine/mutation_flow.rs src/engine/equality_delete_flow.rs
```

Expected after edits: no match in DML/MV refresh paths. Matches in non-DML maintenance modules are allowed.

- [ ] **Step 5: Run targeted MV suites**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_aggregate_target,iceberg_ivm_partitioned_aggregate_target --mode verify
```

Expected: PASS or updated fail-fast golden only for unsupported COW/equality-delete paths.

- [ ] **Step 6: Commit Task 9**

```bash
git add src/engine/mv/iceberg_refresh.rs src/engine/mv/iceberg_merge_sink.rs src/engine/mv/iceberg_join_coalesce.rs
git commit -m "Route Iceberg MV refresh through write transaction runner"
```

## Task 10: Delete Coordinator-Local Writer Shim

**Files:**
- Modify: `src/engine/write_transaction.rs:204-242`
- Modify: all references found by `rg`

- [ ] **Step 1: Delete shim functions**

Remove from `src/engine/write_transaction.rs`:

```rust
pub(crate) fn new_local_writer_write_id() -> crate::types::TUniqueId { ... }

pub(crate) fn local_writer_commit_input(
    write_id: crate::types::TUniqueId,
    sink_commit_infos: Vec<crate::types::TSinkCommitInfo>,
) -> WriteCommitInput { ... }
```

If `synthetic_unique_id` is only used by these functions, delete it too.

- [ ] **Step 2: Delete shim tests**

Remove the `local_writer_commit_input_carries_sink_commit_infos` test and any fixture that directly calls the deleted helpers.

- [ ] **Step 3: Verify there are no references**

Run:

```bash
rg -n "local_writer_commit_input|new_local_writer_write_id" src
```

Expected: no output.

- [ ] **Step 4: Verify no direct DML/MV commit legacy calls**

Run:

```bash
rg -n "run_iceberg_commit\\(" src/engine/delete_flow.rs src/engine/mutation_flow.rs src/engine/equality_delete_flow.rs src/engine/mv
```

Expected: no direct call in DELETE/UPDATE/MERGE/equality-delete/MV refresh paths. Matches under non-DML maintenance modules are outside this command's file list and should not appear here.

- [ ] **Step 5: Run write transaction tests**

Run:

```bash
cargo test -q write_transaction
```

Expected: PASS.

- [ ] **Step 6: Commit Task 10**

```bash
git add src/engine/write_transaction.rs src/engine src/connector/iceberg
git commit -m "Remove coordinator-local Iceberg writer shim"
```

## Task 11: End-to-End Verification and Golden Updates

**Files:**
- Modify only result files needed by intentional fail-fast behavior.

- [ ] **Step 1: Format**

Run:

```bash
cargo fmt
```

Expected: no errors.

- [ ] **Step 2: Build**

Run:

```bash
cargo build --profile dev-opt
```

Expected: PASS.

- [ ] **Step 3: Unit tests**

Run:

```bash
cargo test -q connector::iceberg::write_descriptor::tests
cargo test -q connector::iceberg::commit::collector::tests
cargo test -q connector::iceberg::sink::tests
cargo test -q write_transaction
```

Expected: PASS.

- [ ] **Step 4: SQL tests**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_distributed_insert_append,iceberg_rest_distributed_delete,iceberg_rest_distributed_update_merge --mode verify
```

Expected: PASS.

- [ ] **Step 5: MV tests**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-ivm --only iceberg_ivm_aggregate_target,iceberg_ivm_partitioned_aggregate_target,iceberg_ivm_base_delete_row_lineage --mode verify
```

Expected: PASS or stable fail-fast output for explicitly unsupported COW/equality-delete shapes.

- [ ] **Step 6: Compatibility test**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-compatibility --mode verify
```

Expected: PASS for Spark reading NovaRocks-written data/delete files.

- [ ] **Step 7: Deletion checks**

Run:

```bash
rg -n "local_writer_commit_input|new_local_writer_write_id" src
rg -n "partition_path.as_deref|parse_partition_path\\(" src/connector/iceberg/commit/collector.rs
```

Expected: both commands produce no output.

- [ ] **Step 8: Commit verification-only golden changes**

If any SQL result files changed because unsupported coordinator-local COW/equality-delete paths now fail fast, commit them:

```bash
git add sql-tests
git commit -m "Update Iceberg distributed write regression results"
```

If no result files changed, do not create an empty commit.

## Task 12: Final Review Checklist

**Files:**
- No code edits unless checklist finds a concrete issue.

- [ ] **Step 1: Confirm spec coverage**

Run:

```bash
rg -n "partition_values_descriptor|IcebergWriteDescriptorMismatch|UnsupportedDistributedDmlShape|DistributedWriteOutputMismatch" src idl
```

Expected:

- `partition_values_descriptor` appears in thrift, data writer, sink writer, collector decode, and tests.
- `IcebergWriteDescriptorMismatch` appears in descriptor decode errors and collector tests.
- `UnsupportedDistributedDmlShape` appears at unsupported DML entrypoints.
- `DistributedWriteOutputMismatch` appears where multi-sink writer output is validated.

- [ ] **Step 2: Confirm no path-based partition authority**

Run:

```bash
rg -n "partition_path|partition_null_fingerprint|parse_partition_path" src/connector/iceberg/commit src/connector/iceberg/data_writer.rs src/connector/iceberg/sink.rs
```

Expected:

- `partition_path` may still appear in writer report construction for compatibility fields.
- `partition_path` must not appear in `IcebergCommitCollector::convert`.
- `parse_partition_path` must not be called by production commit conversion.

- [ ] **Step 3: Confirm no coordinator-local DML file writer**

Run:

```bash
rg -n "write_position_delete_files|write_equality_delete_file|write_cow_update_files|write_record_batches_as_data_files|write_row_lineage_batches_as_data_files" src/engine/delete_flow.rs src/engine/mutation_flow.rs src/engine/equality_delete_flow.rs src/engine/mv
```

Expected:

- No match in DELETE/UPDATE/MERGE/equality-delete/MV refresh executor paths.
- A match in a helper module is acceptable only if it is called by a distributed sink operator, not directly by coordinator DML flow.

- [ ] **Step 4: Prepare PR summary**

Use this PR summary skeleton:

```markdown
## Summary
- add descriptor-authoritative Iceberg partition metadata to writer reports
- decode commit `WrittenFile` partition values from descriptor + partition spec id
- cut Iceberg DML/MV write file production to distributed sinks and remove coordinator-local writer shim

## Tests
- cargo test -q connector::iceberg::write_descriptor::tests
- cargo test -q connector::iceberg::commit::collector::tests
- cargo test -q connector::iceberg::sink::tests
- cargo test -q write_transaction
- sql-tests iceberg-rest targeted distributed write cases
- sql-tests iceberg-ivm targeted MV refresh cases
- sql-tests iceberg-compatibility verify
```

- [ ] **Step 5: Commit final cleanup if needed**

If checklist fixes were necessary:

```bash
git add <fixed-files>
git commit -m "Finalize Iceberg distributed DML write cutover"
```

If no fixes were necessary, do not create a commit.
