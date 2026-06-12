use iceberg::spec::{Struct, TableMetadata};

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
            Self::MissingDescriptor => write!(
                f,
                "IcebergWriteDescriptorMismatch: missing partition descriptor"
            ),
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
    use iceberg::TableCreation;
    use iceberg::spec::{
        FormatVersion, Literal, NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType,
        Schema, TableMetadataBuilder, Transform, Type,
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
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(spec)
            .format_version(FormatVersion::V2)
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .expect("table metadata builder")
            .build()
            .expect("table metadata")
            .metadata
    }

    #[test]
    fn descriptor_round_trips_identity_partition() {
        let metadata = metadata_with_identity_partition();
        let values = Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::String(
            "us west".to_string(),
        )))]);

        let desc = encode_partition_descriptor(&values, 7, &metadata).expect("encode descriptor");
        let decoded =
            decode_partition_descriptor(Some(desc), 7, &metadata).expect("decode descriptor");

        assert_eq!(decoded, values);
    }

    #[test]
    fn descriptor_round_trips_null_partition_value() {
        let metadata = metadata_with_identity_partition();
        let values = Struct::from_iter([None]);

        let desc = encode_partition_descriptor(&values, 7, &metadata).expect("encode descriptor");
        let decoded =
            decode_partition_descriptor(Some(desc), 7, &metadata).expect("decode descriptor");

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

        let err =
            decode_partition_descriptor(Some(desc), 7, &metadata).expect_err("expected error");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(err.to_string().contains("has no payload"));
    }

    #[test]
    fn descriptor_rejects_unknown_partition_spec_id() {
        let metadata = metadata_with_identity_partition();
        let desc = crate::types::TIcebergPartitionDescriptor {
            values: Some(vec![]),
        };

        let err =
            decode_partition_descriptor(Some(desc), 99, &metadata).expect_err("expected error");

        assert_eq!(
            err,
            IcebergWriteDescriptorError::UnknownPartitionSpec { spec_id: 99 }
        );
    }

    #[test]
    fn descriptor_rejects_field_count_mismatch() {
        let metadata = metadata_with_identity_partition();
        let desc = crate::types::TIcebergPartitionDescriptor {
            values: Some(vec![]),
        };

        let err =
            decode_partition_descriptor(Some(desc), 7, &metadata).expect_err("expected error");

        assert_eq!(
            err,
            IcebergWriteDescriptorError::FieldCountMismatch {
                expected: 1,
                actual: 0,
            }
        );
    }
}
