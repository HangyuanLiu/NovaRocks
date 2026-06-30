use crate::connector::iceberg::write_descriptor::{
    IcebergPartitionDescriptor, IcebergPartitionValueDescriptor, IcebergWriteDescriptorError,
};
use crate::thrift::types;

pub(crate) fn partition_descriptor_to_thrift(
    desc: IcebergPartitionDescriptor,
) -> types::TIcebergPartitionDescriptor {
    types::TIcebergPartitionDescriptor {
        values: Some(
            desc.values
                .into_iter()
                .map(|value| types::TIcebergPartitionValue {
                    is_null: Some(value.is_null),
                    datum_bytes: value.datum_bytes,
                })
                .collect(),
        ),
    }
}

pub(crate) fn partition_descriptor_from_thrift(
    desc: Option<types::TIcebergPartitionDescriptor>,
) -> Result<Option<IcebergPartitionDescriptor>, IcebergWriteDescriptorError> {
    let Some(desc) = desc else {
        return Ok(None);
    };
    let values = desc
        .values
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .map(|(idx, value)| {
            let is_null =
                value
                    .is_null
                    .ok_or_else(|| IcebergWriteDescriptorError::DecodeFailed {
                        index: idx,
                        message: "partition descriptor value is missing null marker".to_string(),
                    })?;
            Ok(IcebergPartitionValueDescriptor {
                is_null,
                datum_bytes: value.datum_bytes,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(IcebergPartitionDescriptor { values }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_descriptor_from_thrift_rejects_missing_null_marker() {
        let desc = types::TIcebergPartitionDescriptor {
            values: Some(vec![types::TIcebergPartitionValue {
                is_null: None,
                datum_bytes: Some(b"west".to_vec()),
            }]),
        };

        let err =
            partition_descriptor_from_thrift(Some(desc)).expect_err("expected missing null marker");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(
            err.to_string().contains("null marker"),
            "missing null marker should be rejected, got: {err}"
        );
    }
}
