use crate::connector::iceberg::write_descriptor::{
    IcebergPartitionDescriptor, IcebergPartitionValueDescriptor,
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
) -> Option<IcebergPartitionDescriptor> {
    desc.map(|desc| IcebergPartitionDescriptor {
        values: desc
            .values
            .unwrap_or_default()
            .into_iter()
            .map(|value| IcebergPartitionValueDescriptor {
                is_null: value.is_null.unwrap_or(false),
                datum_bytes: value.datum_bytes,
            })
            .collect(),
    })
}
