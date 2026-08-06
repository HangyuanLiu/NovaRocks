// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Core adapters for the provider-owned position-delete descriptor contract.

use arrow::datatypes::SchemaRef;

pub(crate) use novarocks_connector_iceberg::position_delete_descriptor::{
    ICEBERG_POSITION_DELETE_FILE_PATH_COLUMN, ICEBERG_POSITION_DELETE_FILE_PATH_FIELD_ID,
    ICEBERG_POSITION_DELETE_POS_COLUMN, ICEBERG_POSITION_DELETE_POS_FIELD_ID,
    PositionDeleteDescriptorBinding, PositionDeleteDescriptorInput, PositionDeleteExpectedBinding,
    PositionDeleteOutputField, PositionDeletePartitionSourceField,
};

pub(crate) fn validate_required_fields(
    desc: &PositionDeleteDescriptorInput,
) -> Result<(), crate::common::engine_error::EngineError> {
    novarocks_connector_iceberg::position_delete_descriptor::validate_required_fields(desc).map_err(
        |error| {
            crate::common::engine_error::EngineError::unsupported_position_delete_descriptor(
                error.detail_message(),
            )
        },
    )
}

pub(crate) fn output_schema_from_descriptor(
    desc: &PositionDeleteDescriptorInput,
) -> Result<SchemaRef, crate::common::engine_error::EngineError> {
    novarocks_connector_iceberg::position_delete_descriptor::output_schema_from_descriptor(desc)
        .map_err(|error| {
            crate::common::engine_error::EngineError::unsupported_position_delete_descriptor(
                error.detail_message(),
            )
        })
}

pub(crate) fn canonical_output_schema() -> SchemaRef {
    novarocks_connector_iceberg::position_delete_descriptor::canonical_output_schema()
}

pub(crate) fn bind_position_delete_descriptor(
    desc: &PositionDeleteDescriptorInput,
    expected: &PositionDeleteExpectedBinding,
) -> Result<PositionDeleteDescriptorBinding, crate::common::engine_error::EngineError> {
    novarocks_connector_iceberg::position_delete_descriptor::bind_position_delete_descriptor(
        desc, expected,
    )
    .map_err(|error| {
        crate::common::engine_error::EngineError::unsupported_position_delete_descriptor(
            error.detail_message(),
        )
    })
}
