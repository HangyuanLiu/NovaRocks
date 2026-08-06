// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Core compatibility exports for provider-owned partition descriptors.

pub(crate) use novarocks_connector_iceberg::write_descriptor::{
    IcebergPartitionDescriptor, IcebergPartitionValueDescriptor, IcebergWriteDescriptorError,
    decode_partition_descriptor, encode_partition_descriptor,
};

impl From<IcebergWriteDescriptorError> for crate::common::engine_error::EngineError {
    fn from(value: IcebergWriteDescriptorError) -> Self {
        crate::common::engine_error::EngineError::iceberg_write_descriptor_mismatch(
            value.detail_message(),
        )
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn descriptor_error_converts_to_engine_error_without_double_prefix() {
        let err = crate::common::engine_error::EngineError::from(
            super::IcebergWriteDescriptorError::MissingDescriptor,
        );

        assert_eq!(
            err.to_bracketed_user_message(),
            "[IcebergWriteDescriptorMismatch] missing partition descriptor"
        );
        let message = err.to_bracketed_user_message();
        let payload = message.split_once("] ").expect("bracketed payload").1;
        assert!(
            !payload.contains("IcebergWriteDescriptorMismatch:"),
            "got: {message}"
        );
    }
}
