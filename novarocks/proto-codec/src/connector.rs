//! Shared codecs for native connector execution bindings.
//!
//! The SPI declaration remains transport-neutral. This module is the sole
//! owner of its conversion to and from the generated native wire DTOs.

use crate::provider::connector_execution_binding_declaration_digest;
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::novarocks;
use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration,
    ConnectorExecutionDeclarationProvider, ConnectorInstanceId, ConnectorInstanceIncarnation,
};

/// A connector declaration admitted from its original wire carrier.
///
/// The digest is calculated from the generated DTO before domain conversion;
/// callers must pass the declaration directly to the execution host without
/// re-encoding or recomputing it.
#[derive(Clone, Debug)]
pub struct AdmittedConnectorExecutionDeclaration {
    digest: [u8; 32],
    declaration: ConnectorExecutionDeclaration,
}

impl AdmittedConnectorExecutionDeclaration {
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn declaration(&self) -> &ConnectorExecutionDeclaration {
        &self.declaration
    }

    pub fn into_declaration(self) -> ConnectorExecutionDeclaration {
        self.declaration
    }
}

/// Encodes a validated SPI declaration as its unique generated representation.
pub fn encode_connector_execution_declaration(
    declaration: &ConnectorExecutionDeclaration,
) -> novarocks::ConnectorExecutionBindingDeclaration {
    let binding_key = declaration.binding_key();
    let provider = match declaration.provider() {
        ConnectorExecutionDeclarationProvider::Iceberg { access_binding } => {
            novarocks::connector_execution_binding_declaration::Provider::Iceberg(
                novarocks::IcebergExecutionBindingDeclaration {
                    access_binding: access_binding.to_owned(),
                },
            )
        }
        ConnectorExecutionDeclarationProvider::StarRocks { local_binding } => {
            novarocks::connector_execution_binding_declaration::Provider::Starrocks(
                novarocks::StarRocksExecutionBindingDeclaration {
                    local_binding: local_binding.to_owned(),
                },
            )
        }
    };
    novarocks::ConnectorExecutionBindingDeclaration {
        instance_id: binding_key.instance_id.as_str().to_owned(),
        incarnation: binding_key.incarnation.to_bytes().to_vec(),
        provider: Some(provider),
    }
}

/// Decodes a generated declaration and retains its raw-DTO canonical digest.
pub fn decode_connector_execution_declaration(
    raw: novarocks::ConnectorExecutionBindingDeclaration,
) -> Result<AdmittedConnectorExecutionDeclaration, ProtocolError> {
    let digest = connector_execution_binding_declaration_digest(&raw)?;
    let incarnation = decode_incarnation(
        &raw.incarnation,
        FieldPath::root("connector_execution_binding_declaration").field("incarnation"),
    )?;
    let root = FieldPath::root("connector_execution_binding_declaration");
    let declaration = match raw.provider.as_ref() {
        Some(novarocks::connector_execution_binding_declaration::Provider::Iceberg(provider)) => {
            ConnectorExecutionDeclaration::iceberg(
                &raw.instance_id,
                incarnation,
                &provider.access_binding,
            )
            .map_err(|error| {
                ProtocolError::new(
                    root.clone().field("instance_id"),
                    ProtocolErrorKind::InvalidValue,
                    error.to_string(),
                )
            })?
        }
        Some(novarocks::connector_execution_binding_declaration::Provider::Starrocks(provider)) => {
            ConnectorExecutionDeclaration::starrocks(
                &raw.instance_id,
                incarnation,
                &provider.local_binding,
            )
            .map_err(|error| {
                ProtocolError::new(
                    root.clone().field("instance_id"),
                    ProtocolErrorKind::InvalidValue,
                    error.to_string(),
                )
            })?
        }
        None => {
            return Err(ProtocolError::new(
                root.field("provider"),
                ProtocolErrorKind::MissingField,
                "connector execution declaration provider is required",
            ));
        }
    };
    Ok(AdmittedConnectorExecutionDeclaration {
        digest,
        declaration,
    })
}

/// Encodes a binding key as the fields used by native Retire requests.
pub fn encode_connector_execution_binding_key(
    key: &ConnectorExecutionBindingKey,
) -> (String, Vec<u8>) {
    (
        key.instance_id.as_str().to_owned(),
        key.incarnation.to_bytes().to_vec(),
    )
}

/// Decodes a native binding key from its canonical identifier and incarnation.
pub fn decode_connector_execution_binding_key(
    instance_id: &str,
    incarnation: &[u8],
    root: FieldPath,
) -> Result<ConnectorExecutionBindingKey, ProtocolError> {
    let incarnation = decode_incarnation(incarnation, root.clone().field("incarnation"))?;
    let instance_id = ConnectorInstanceId::try_from_canonical(instance_id).map_err(|error| {
        ProtocolError::new(
            root.field("instance_id"),
            ProtocolErrorKind::InvalidValue,
            error.to_string(),
        )
    })?;
    Ok(ConnectorExecutionBindingKey {
        instance_id,
        incarnation: ConnectorInstanceIncarnation::from_bytes(incarnation),
    })
}

fn decode_incarnation(incarnation: &[u8], path: FieldPath) -> Result<[u8; 16], ProtocolError> {
    incarnation.try_into().map_err(|_| {
        ProtocolError::new(
            path,
            ProtocolErrorKind::InvalidValue,
            "connector execution incarnation must contain exactly 16 bytes",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iceberg_declaration(
        instance_id: impl Into<String>,
        incarnation: Vec<u8>,
        access_binding: impl Into<String>,
    ) -> novarocks::ConnectorExecutionBindingDeclaration {
        novarocks::ConnectorExecutionBindingDeclaration {
            instance_id: instance_id.into(),
            incarnation,
            provider: Some(
                novarocks::connector_execution_binding_declaration::Provider::Iceberg(
                    novarocks::IcebergExecutionBindingDeclaration {
                        access_binding: access_binding.into(),
                    },
                ),
            ),
        }
    }

    #[test]
    fn declaration_round_trip_uses_exhaustive_provider_mapping() {
        let iceberg = ConnectorExecutionDeclaration::iceberg("catalog", [7; 16], "access")
            .expect("valid iceberg declaration");
        let starrocks = ConnectorExecutionDeclaration::starrocks("catalog", [7; 16], "local")
            .expect("valid starrocks declaration");

        for declaration in [&iceberg, &starrocks] {
            let raw = encode_connector_execution_declaration(declaration);
            let admitted = decode_connector_execution_declaration(raw)
                .expect("encoded declaration must decode");
            assert_eq!(admitted.declaration(), declaration);
        }
    }

    #[test]
    fn declaration_digest_is_calculated_from_the_original_dto() {
        let raw = iceberg_declaration("catalog", vec![7; 16], "access");
        let expected = connector_execution_binding_declaration_digest(&raw)
            .expect("test declaration canonicalizes");
        let admitted = decode_connector_execution_declaration(raw).expect("valid declaration");
        assert_eq!(admitted.digest(), expected);
    }

    #[test]
    fn provider_kind_changes_the_digest() {
        let iceberg = iceberg_declaration("catalog", vec![7; 16], "binding");
        let starrocks = novarocks::ConnectorExecutionBindingDeclaration {
            instance_id: "catalog".to_owned(),
            incarnation: vec![7; 16],
            provider: Some(
                novarocks::connector_execution_binding_declaration::Provider::Starrocks(
                    novarocks::StarRocksExecutionBindingDeclaration {
                        local_binding: "binding".to_owned(),
                    },
                ),
            ),
        };
        assert_ne!(
            decode_connector_execution_declaration(iceberg)
                .expect("valid iceberg declaration")
                .digest(),
            decode_connector_execution_declaration(starrocks)
                .expect("valid starrocks declaration")
                .digest(),
        );
    }

    #[test]
    fn declaration_failures_keep_their_field_path() {
        let missing = novarocks::ConnectorExecutionBindingDeclaration {
            instance_id: "catalog".to_owned(),
            incarnation: vec![7; 16],
            provider: None,
        };
        let missing_error =
            decode_connector_execution_declaration(missing).expect_err("provider is required");
        assert_eq!(
            missing_error.path().to_string(),
            "connector_execution_binding_declaration.provider"
        );

        let invalid_incarnation = iceberg_declaration("catalog", vec![7; 15], "access");
        let incarnation_error = decode_connector_execution_declaration(invalid_incarnation)
            .expect_err("incarnation length is fixed");
        assert_eq!(
            incarnation_error.path().to_string(),
            "connector_execution_binding_declaration.incarnation"
        );

        let invalid_instance = iceberg_declaration("MyCatalog", vec![7; 16], "access");
        let instance_error = decode_connector_execution_declaration(invalid_instance)
            .expect_err("wire identifier is canonical");
        assert_eq!(
            instance_error.path().to_string(),
            "connector_execution_binding_declaration.instance_id"
        );
    }

    #[test]
    fn binding_key_round_trip_rejects_noncanonical_values() {
        let key = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::try_from_canonical("catalog")
                .expect("canonical identifier"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([9; 16]),
        };
        let (instance_id, incarnation) = encode_connector_execution_binding_key(&key);
        assert_eq!(
            decode_connector_execution_binding_key(
                &instance_id,
                &incarnation,
                FieldPath::root("retire_connector_execution_binding_request"),
            )
            .expect("encoded key must decode"),
            key,
        );
        let error = decode_connector_execution_binding_key(
            "MyCatalog",
            &[9; 16],
            FieldPath::root("retire_connector_execution_binding_request"),
        )
        .expect_err("noncanonical identifier must fail");
        assert_eq!(
            error.path().to_string(),
            "retire_connector_execution_binding_request.instance_id"
        );
    }
}
