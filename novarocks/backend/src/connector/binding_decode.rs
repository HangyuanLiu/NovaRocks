//! BE request framing for the shared connector execution codecs.

use novarocks_proto_codec::connector::{
    AdmittedConnectorExecutionDeclaration, decode_connector_execution_binding_key,
    decode_connector_execution_declaration,
};
use novarocks_proto_codec::lifecycle::decode_query_execution_id;
use novarocks_proto_codec::provider::{
    EnsureConnectorExecutionBindingRejection, EnsureConnectorExecutionBindingRejectionReason,
    EnsureConnectorExecutionBindingResult, RetireConnectorExecutionBindingOutcome,
    RetireConnectorExecutionBindingResult,
};
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::novarocks::{
    EnsureConnectorExecutionBindingRequest, RetireConnectorExecutionBindingRequest,
};
use novarocks_spi::connector::ConnectorExecutionBindingKey;
use novarocks_types::QueryExecutionId;

/// Parses the BE-owned Ensure request framing, then delegates every shared
/// identity/declaration conversion to Protocol.
pub(crate) fn decode_ensure_request(
    request: EnsureConnectorExecutionBindingRequest,
) -> Result<
    (QueryExecutionId, AdmittedConnectorExecutionDeclaration),
    EnsureConnectorExecutionBindingResult,
> {
    let execution_id = request.execution_id.as_ref().ok_or_else(|| {
        invalid_declaration(ProtocolError::new(
            FieldPath::root("ensure_connector_execution_binding_request").field("execution_id"),
            ProtocolErrorKind::MissingField,
            "connector execution binding request requires execution_id",
        ))
    })?;
    let execution_id = decode_query_execution_id(execution_id).map_err(invalid_declaration)?;
    let declaration = request.declaration.ok_or_else(|| {
        invalid_declaration(ProtocolError::new(
            FieldPath::root("ensure_connector_execution_binding_request").field("declaration"),
            ProtocolErrorKind::MissingField,
            "connector execution binding request requires declaration",
        ))
    })?;
    let declaration =
        decode_connector_execution_declaration(declaration).map_err(invalid_declaration)?;
    Ok((execution_id, declaration))
}

/// Parses the BE-owned Retire request framing through Protocol's shared key
/// codec and maps malformed input to the existing typed result.
pub(crate) fn decode_retire_request(
    request: RetireConnectorExecutionBindingRequest,
) -> Result<ConnectorExecutionBindingKey, RetireConnectorExecutionBindingResult> {
    decode_connector_execution_binding_key(
        &request.instance_id,
        &request.incarnation,
        FieldPath::root("retire_connector_execution_binding_request"),
    )
    .map_err(|_| {
        RetireConnectorExecutionBindingResult::new(
            RetireConnectorExecutionBindingOutcome::InvalidKey,
        )
    })
}

fn invalid_declaration(error: ProtocolError) -> EnsureConnectorExecutionBindingResult {
    let mut end = error.detail().len().min(512);
    while end > 0 && !error.detail().is_char_boundary(end) {
        end -= 1;
    }
    let rejection = EnsureConnectorExecutionBindingRejection::try_new(
        EnsureConnectorExecutionBindingRejectionReason::InvalidDeclaration,
        false,
        error.detail()[..end].to_owned(),
        Some(error.path().to_string()),
    )
    .expect("fixed invalid declaration outcome is Protocol-valid");
    EnsureConnectorExecutionBindingResult::rejected(rejection)
}
