//! Central execution-binding declaration and typed control-plane outcomes.

use std::fmt;

use crate::{FieldPath, ProtocolError, ProtocolErrorKind, canonical, novarocks};

const DECLARATION_DIGEST_DOMAIN: &[u8] = b"novarocks.connector.execution-binding-declaration.v1\0";
const DECLARATION_MESSAGE_NAME: &str = "novarocks.ConnectorExecutionBindingDeclaration";
const MAX_INSTANCE_ID_BYTES: usize = 128;
const MAX_LOCAL_BINDING_BYTES: usize = 256;
const MAX_SAFE_DETAIL_BYTES: usize = 512;
const MAX_SAFE_FIELD_PATH_BYTES: usize = 256;

/// Exact process-local generation key carried by an execution declaration.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectorExecutionBindingKey {
    instance_id: String,
    incarnation: [u8; 16],
}

impl ConnectorExecutionBindingKey {
    pub fn try_new(
        instance_id: impl Into<String>,
        incarnation: impl AsRef<[u8]>,
    ) -> Result<Self, ProtocolError> {
        let instance_id = instance_id.into();
        validate_instance_id(
            &instance_id,
            FieldPath::root("connector_execution_binding").field("instance_id"),
        )?;
        let incarnation = parse_incarnation(
            incarnation.as_ref(),
            FieldPath::root("connector_execution_binding").field("incarnation"),
        )?;
        Ok(Self {
            instance_id,
            incarnation,
        })
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub const fn incarnation(&self) -> [u8; 16] {
        self.incarnation
    }
}

/// Closed provider kind derived only from the declaration oneof.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorExecutionProviderKind {
    Iceberg,
    StarRocks,
}

/// Borrowed provider-specific declaration facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorExecutionBindingProvider<'a> {
    Iceberg { access_binding: &'a str },
    StarRocks { local_binding: &'a str },
}

impl ConnectorExecutionBindingProvider<'_> {
    pub const fn kind(self) -> ConnectorExecutionProviderKind {
        match self {
            Self::Iceberg { .. } => ConnectorExecutionProviderKind::Iceberg,
            Self::StarRocks { .. } => ConnectorExecutionProviderKind::StarRocks,
        }
    }
}

// Design: ADR-0104 (docs/adr/ADR-0104-typed-connector-execution-binding-declaration.md)
/// Validated generated declaration root. It intentionally keeps no duplicate
/// domain model and computes its digest from the Protocol-owned DTO.
#[derive(Clone, PartialEq)]
pub struct ConnectorExecutionBindingDeclaration {
    raw: novarocks::ConnectorExecutionBindingDeclaration,
}

impl Eq for ConnectorExecutionBindingDeclaration {}

impl ConnectorExecutionBindingDeclaration {
    pub fn try_from_proto(
        raw: novarocks::ConnectorExecutionBindingDeclaration,
    ) -> Result<Self, ProtocolError> {
        let root = FieldPath::root("connector_execution_binding");
        validate_instance_id(&raw.instance_id, root.clone().field("instance_id"))?;
        parse_incarnation(&raw.incarnation, root.clone().field("incarnation"))?;
        validate_provider(raw.provider.as_ref(), root.field("provider"))?;
        Ok(Self { raw })
    }

    pub fn iceberg(
        instance_id: impl Into<String>,
        incarnation: [u8; 16],
        access_binding: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        Self::try_from_proto(novarocks::ConnectorExecutionBindingDeclaration {
            instance_id: instance_id.into(),
            incarnation: incarnation.to_vec(),
            provider: Some(
                novarocks::connector_execution_binding_declaration::Provider::Iceberg(
                    novarocks::IcebergExecutionBindingDeclaration {
                        access_binding: access_binding.into(),
                    },
                ),
            ),
        })
    }

    pub fn starrocks(
        instance_id: impl Into<String>,
        incarnation: [u8; 16],
        local_binding: impl Into<String>,
    ) -> Result<Self, ProtocolError> {
        Self::try_from_proto(novarocks::ConnectorExecutionBindingDeclaration {
            instance_id: instance_id.into(),
            incarnation: incarnation.to_vec(),
            provider: Some(
                novarocks::connector_execution_binding_declaration::Provider::Starrocks(
                    novarocks::StarRocksExecutionBindingDeclaration {
                        local_binding: local_binding.into(),
                    },
                ),
            ),
        })
    }

    pub fn as_proto(&self) -> &novarocks::ConnectorExecutionBindingDeclaration {
        &self.raw
    }

    pub fn into_proto(self) -> novarocks::ConnectorExecutionBindingDeclaration {
        self.raw
    }

    pub fn binding_key(&self) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: self.raw.instance_id.clone(),
            incarnation: self
                .raw
                .incarnation
                .as_slice()
                .try_into()
                .expect("validated declaration has a 16-byte incarnation"),
        }
    }

    pub fn provider_kind(&self) -> ConnectorExecutionProviderKind {
        self.provider().kind()
    }

    pub fn provider(&self) -> ConnectorExecutionBindingProvider<'_> {
        match self
            .raw
            .provider
            .as_ref()
            .expect("validated declaration has a provider variant")
        {
            novarocks::connector_execution_binding_declaration::Provider::Iceberg(value) => {
                ConnectorExecutionBindingProvider::Iceberg {
                    access_binding: &value.access_binding,
                }
            }
            novarocks::connector_execution_binding_declaration::Provider::Starrocks(value) => {
                ConnectorExecutionBindingProvider::StarRocks {
                    local_binding: &value.local_binding,
                }
            }
        }
    }

    pub fn digest(&self) -> Result<[u8; 32], ProtocolError> {
        canonical::digest_message(
            DECLARATION_DIGEST_DOMAIN,
            DECLARATION_MESSAGE_NAME,
            &self.raw,
        )
        .map_err(|error| {
            ProtocolError::new(
                FieldPath::root("connector_execution_binding"),
                ProtocolErrorKind::InvalidValue,
                format!("cannot canonicalize execution binding declaration: {error}"),
            )
        })
    }
}

impl fmt::Debug for ConnectorExecutionBindingDeclaration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectorExecutionBindingDeclaration")
            .field("instance_id", &self.raw.instance_id)
            .field("incarnation", &"<16-byte-id>")
            .field("provider_kind", &self.provider_kind())
            .finish()
    }
}

/// Closed Ensure rejection reason set validated at the Protocol boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EnsureConnectorExecutionBindingRejectionReason {
    InvalidDeclaration,
    ConflictingDeclaration,
    QueryIncarnationConflict,
    Retiring,
    HostUnavailable,
    ActivationUnavailable,
    DeadlineExceeded,
    ResourceExhausted,
    InternalFailure,
}

impl EnsureConnectorExecutionBindingRejectionReason {
    fn try_from_proto(value: i32) -> Result<Self, ProtocolError> {
        use novarocks::EnsureConnectorExecutionBindingRejectionReason as ProtoReason;

        match ProtoReason::try_from(value) {
            Ok(ProtoReason::InvalidDeclaration) => Ok(Self::InvalidDeclaration),
            Ok(ProtoReason::ConflictingDeclaration) => Ok(Self::ConflictingDeclaration),
            Ok(ProtoReason::QueryIncarnationConflict) => Ok(Self::QueryIncarnationConflict),
            Ok(ProtoReason::Retiring) => Ok(Self::Retiring),
            Ok(ProtoReason::HostUnavailable) => Ok(Self::HostUnavailable),
            Ok(ProtoReason::ActivationUnavailable) => Ok(Self::ActivationUnavailable),
            Ok(ProtoReason::DeadlineExceeded) => Ok(Self::DeadlineExceeded),
            Ok(ProtoReason::ResourceExhausted) => Ok(Self::ResourceExhausted),
            Ok(ProtoReason::InternalFailure) => Ok(Self::InternalFailure),
            Ok(ProtoReason::Unspecified) | Err(_) => Err(ProtocolError::new(
                FieldPath::root("ensure_connector_execution_binding_response")
                    .field("rejection")
                    .field("reason"),
                ProtocolErrorKind::InvalidEnum,
                "unknown or unspecified execution binding rejection reason",
            )),
        }
    }

    fn to_proto(self) -> i32 {
        use novarocks::EnsureConnectorExecutionBindingRejectionReason as ProtoReason;

        (match self {
            Self::InvalidDeclaration => ProtoReason::InvalidDeclaration,
            Self::ConflictingDeclaration => ProtoReason::ConflictingDeclaration,
            Self::QueryIncarnationConflict => ProtoReason::QueryIncarnationConflict,
            Self::Retiring => ProtoReason::Retiring,
            Self::HostUnavailable => ProtoReason::HostUnavailable,
            Self::ActivationUnavailable => ProtoReason::ActivationUnavailable,
            Self::DeadlineExceeded => ProtoReason::DeadlineExceeded,
            Self::ResourceExhausted => ProtoReason::ResourceExhausted,
            Self::InternalFailure => ProtoReason::InternalFailure,
        }) as i32
    }

    fn allows_retryable_before_progress(self, value: bool) -> bool {
        match self {
            Self::InvalidDeclaration
            | Self::ConflictingDeclaration
            | Self::QueryIncarnationConflict
            | Self::Retiring
            | Self::HostUnavailable => !value,
            Self::DeadlineExceeded => value,
            Self::ActivationUnavailable | Self::ResourceExhausted | Self::InternalFailure => true,
        }
    }
}

/// A safe, application-produced Ensure rejection preserved across the wire.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsureConnectorExecutionBindingRejection {
    reason: EnsureConnectorExecutionBindingRejectionReason,
    retryable_before_progress: bool,
    safe_detail: String,
    safe_field_path: Option<String>,
}

impl EnsureConnectorExecutionBindingRejection {
    pub fn try_new(
        reason: EnsureConnectorExecutionBindingRejectionReason,
        retryable_before_progress: bool,
        safe_detail: impl Into<String>,
        safe_field_path: Option<String>,
    ) -> Result<Self, ProtocolError> {
        let safe_detail = safe_detail.into();
        validate_bounded_text(
            &safe_detail,
            MAX_SAFE_DETAIL_BYTES,
            FieldPath::root("ensure_connector_execution_binding_response")
                .field("rejection")
                .field("safe_detail"),
            "safe detail",
            true,
        )?;
        if let Some(path) = safe_field_path.as_deref() {
            validate_bounded_text(
                path,
                MAX_SAFE_FIELD_PATH_BYTES,
                FieldPath::root("ensure_connector_execution_binding_response")
                    .field("rejection")
                    .field("safe_field_path"),
                "safe field path",
                false,
            )?;
        }
        if !reason.allows_retryable_before_progress(retryable_before_progress) {
            return Err(ProtocolError::new(
                FieldPath::root("ensure_connector_execution_binding_response")
                    .field("rejection")
                    .field("retryable_before_progress"),
                ProtocolErrorKind::InconsistentFields,
                "execution binding rejection reason does not allow this retryability",
            ));
        }
        Ok(Self {
            reason,
            retryable_before_progress,
            safe_detail,
            safe_field_path,
        })
    }

    pub fn reason(&self) -> EnsureConnectorExecutionBindingRejectionReason {
        self.reason
    }

    pub const fn retryable_before_progress(&self) -> bool {
        self.retryable_before_progress
    }

    pub fn safe_detail(&self) -> &str {
        &self.safe_detail
    }

    pub fn safe_field_path(&self) -> Option<&str> {
        self.safe_field_path.as_deref()
    }

    fn try_from_proto(
        raw: novarocks::EnsureConnectorExecutionBindingRejection,
    ) -> Result<Self, ProtocolError> {
        Self::try_new(
            EnsureConnectorExecutionBindingRejectionReason::try_from_proto(raw.reason)?,
            raw.retryable_before_progress,
            raw.safe_detail,
            raw.safe_field_path,
        )
    }

    fn to_proto(&self) -> novarocks::EnsureConnectorExecutionBindingRejection {
        novarocks::EnsureConnectorExecutionBindingRejection {
            reason: self.reason.to_proto(),
            retryable_before_progress: self.retryable_before_progress,
            safe_detail: self.safe_detail.clone(),
            safe_field_path: self.safe_field_path.clone(),
        }
    }
}

/// Validated Ensure outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnsureConnectorExecutionBindingOutcome {
    Ensured,
    Rejected(EnsureConnectorExecutionBindingRejection),
}

/// Validated Protocol result wrapper for Ensure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnsureConnectorExecutionBindingResult {
    outcome: EnsureConnectorExecutionBindingOutcome,
}

impl EnsureConnectorExecutionBindingResult {
    pub const fn ensured() -> Self {
        Self {
            outcome: EnsureConnectorExecutionBindingOutcome::Ensured,
        }
    }

    pub const fn rejected(rejection: EnsureConnectorExecutionBindingRejection) -> Self {
        Self {
            outcome: EnsureConnectorExecutionBindingOutcome::Rejected(rejection),
        }
    }

    pub fn outcome(&self) -> &EnsureConnectorExecutionBindingOutcome {
        &self.outcome
    }

    pub fn try_from_proto(
        raw: novarocks::EnsureConnectorExecutionBindingResponse,
    ) -> Result<Self, ProtocolError> {
        use novarocks::ensure_connector_execution_binding_response::Outcome;

        let outcome = match raw.outcome {
            Some(Outcome::Ensured(_)) => EnsureConnectorExecutionBindingOutcome::Ensured,
            Some(Outcome::Rejection(rejection)) => {
                EnsureConnectorExecutionBindingOutcome::Rejected(
                    EnsureConnectorExecutionBindingRejection::try_from_proto(rejection)?,
                )
            }
            None => {
                return Err(ProtocolError::new(
                    FieldPath::root("ensure_connector_execution_binding_response").field("outcome"),
                    ProtocolErrorKind::MissingField,
                    "ensure execution binding outcome is required",
                ));
            }
        };
        Ok(Self { outcome })
    }

    pub fn to_proto(&self) -> novarocks::EnsureConnectorExecutionBindingResponse {
        use novarocks::ensure_connector_execution_binding_response::Outcome;

        let outcome = match &self.outcome {
            EnsureConnectorExecutionBindingOutcome::Ensured => {
                Outcome::Ensured(novarocks::EnsureConnectorExecutionBindingEnsured {})
            }
            EnsureConnectorExecutionBindingOutcome::Rejected(rejection) => {
                Outcome::Rejection(rejection.to_proto())
            }
        };
        novarocks::EnsureConnectorExecutionBindingResponse {
            outcome: Some(outcome),
        }
    }
}

/// Closed Retire result set.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RetireConnectorExecutionBindingOutcome {
    Accepted,
    NotFound,
    Unavailable,
    InvalidKey,
    Internal,
}

/// Validated Protocol result wrapper for Retire.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RetireConnectorExecutionBindingResult {
    outcome: RetireConnectorExecutionBindingOutcome,
}

impl RetireConnectorExecutionBindingResult {
    pub const fn new(outcome: RetireConnectorExecutionBindingOutcome) -> Self {
        Self { outcome }
    }

    pub const fn outcome(self) -> RetireConnectorExecutionBindingOutcome {
        self.outcome
    }

    pub fn try_from_proto(
        raw: novarocks::RetireConnectorExecutionBindingResponse,
    ) -> Result<Self, ProtocolError> {
        use novarocks::retire_connector_execution_binding_response::Outcome;

        let outcome = match raw.outcome {
            Some(Outcome::Accepted(_)) => RetireConnectorExecutionBindingOutcome::Accepted,
            Some(Outcome::NotFound(_)) => RetireConnectorExecutionBindingOutcome::NotFound,
            Some(Outcome::Unavailable(_)) => RetireConnectorExecutionBindingOutcome::Unavailable,
            Some(Outcome::InvalidKey(_)) => RetireConnectorExecutionBindingOutcome::InvalidKey,
            Some(Outcome::Internal(_)) => RetireConnectorExecutionBindingOutcome::Internal,
            None => {
                return Err(ProtocolError::new(
                    FieldPath::root("retire_connector_execution_binding_response").field("outcome"),
                    ProtocolErrorKind::MissingField,
                    "retire execution binding outcome is required",
                ));
            }
        };
        Ok(Self { outcome })
    }

    pub fn to_proto(self) -> novarocks::RetireConnectorExecutionBindingResponse {
        use novarocks::retire_connector_execution_binding_response::Outcome;

        let outcome = match self.outcome {
            RetireConnectorExecutionBindingOutcome::Accepted => {
                Outcome::Accepted(novarocks::RetireConnectorExecutionBindingAccepted {})
            }
            RetireConnectorExecutionBindingOutcome::NotFound => {
                Outcome::NotFound(novarocks::RetireConnectorExecutionBindingNotFound {})
            }
            RetireConnectorExecutionBindingOutcome::Unavailable => {
                Outcome::Unavailable(novarocks::RetireConnectorExecutionBindingUnavailable {})
            }
            RetireConnectorExecutionBindingOutcome::InvalidKey => {
                Outcome::InvalidKey(novarocks::RetireConnectorExecutionBindingInvalidKey {})
            }
            RetireConnectorExecutionBindingOutcome::Internal => {
                Outcome::Internal(novarocks::RetireConnectorExecutionBindingInternal {})
            }
        };
        novarocks::RetireConnectorExecutionBindingResponse {
            outcome: Some(outcome),
        }
    }
}

fn validate_instance_id(value: &str, path: FieldPath) -> Result<(), ProtocolError> {
    let bytes = value.as_bytes();
    let valid = !bytes.is_empty()
        && bytes.len() <= MAX_INSTANCE_ID_BYTES
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(ProtocolError::new(
            path,
            ProtocolErrorKind::InvalidValue,
            "instance ID must already be canonical lowercase ASCII",
        ))
    }
}

fn parse_incarnation(value: &[u8], path: FieldPath) -> Result<[u8; 16], ProtocolError> {
    value.try_into().map_err(|_| {
        ProtocolError::new(
            path,
            ProtocolErrorKind::OutOfRange,
            "connector execution binding incarnation must be exactly 16 bytes",
        )
    })
}

fn validate_provider(
    provider: Option<&novarocks::connector_execution_binding_declaration::Provider>,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    match provider {
        Some(novarocks::connector_execution_binding_declaration::Provider::Iceberg(value)) => {
            validate_local_binding(
                &value.access_binding,
                path.field("iceberg").field("access_binding"),
                "Iceberg access binding",
            )
        }
        Some(novarocks::connector_execution_binding_declaration::Provider::Starrocks(value)) => {
            validate_local_binding(
                &value.local_binding,
                path.field("starrocks").field("local_binding"),
                "StarRocks local binding",
            )
        }
        None => Err(ProtocolError::new(
            path,
            ProtocolErrorKind::MissingField,
            "connector execution binding provider is required",
        )),
    }
}

fn validate_local_binding(value: &str, path: FieldPath, label: &str) -> Result<(), ProtocolError> {
    if value.is_empty() || value.len() > MAX_LOCAL_BINDING_BYTES || !value.is_ascii() {
        return Err(ProtocolError::new(
            path,
            ProtocolErrorKind::InvalidValue,
            format!("{label} must be non-empty ASCII and at most {MAX_LOCAL_BINDING_BYTES} bytes"),
        ));
    }
    Ok(())
}

fn validate_bounded_text(
    value: &str,
    max_bytes: usize,
    path: FieldPath,
    label: &str,
    allow_empty: bool,
) -> Result<(), ProtocolError> {
    if (!allow_empty && value.is_empty()) || value.len() > max_bytes {
        return Err(ProtocolError::new(
            path,
            ProtocolErrorKind::OutOfRange,
            format!(
                "{label} must {} and be at most {max_bytes} bytes",
                if allow_empty { "be" } else { "be non-empty" }
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use prost_reflect::DescriptorPool;

    use super::*;

    fn declaration() -> ConnectorExecutionBindingDeclaration {
        ConnectorExecutionBindingDeclaration::iceberg("catalog.analytics", [7; 16], "local-iceberg")
            .expect("valid declaration")
    }

    #[test]
    fn validates_a_typed_declaration_without_normalizing_the_wire_identity() {
        let declaration = declaration();
        assert_eq!(declaration.binding_key().instance_id(), "catalog.analytics");
        assert_eq!(declaration.binding_key().incarnation(), [7; 16]);
        assert_eq!(
            declaration.provider_kind(),
            ConnectorExecutionProviderKind::Iceberg
        );
        assert_eq!(
            declaration.provider(),
            ConnectorExecutionBindingProvider::Iceberg {
                access_binding: "local-iceberg"
            }
        );

        let error = ConnectorExecutionBindingDeclaration::iceberg("MyCatalog", [7; 16], "local")
            .expect_err("wire identity must not be normalized");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(
            error.path().to_string(),
            "connector_execution_binding.instance_id"
        );
    }

    #[test]
    fn rejects_missing_variant_bad_incarnation_and_invalid_binding_before_host_admission() {
        let missing_provider = ConnectorExecutionBindingDeclaration::try_from_proto(
            novarocks::ConnectorExecutionBindingDeclaration {
                instance_id: "catalog".into(),
                incarnation: vec![1; 16],
                provider: None,
            },
        )
        .expect_err("provider is required");
        assert_eq!(missing_provider.kind(), ProtocolErrorKind::MissingField);

        let short_incarnation = ConnectorExecutionBindingDeclaration::try_from_proto(
            novarocks::ConnectorExecutionBindingDeclaration {
                instance_id: "catalog".into(),
                incarnation: vec![1; 15],
                provider: Some(
                    novarocks::connector_execution_binding_declaration::Provider::Iceberg(
                        novarocks::IcebergExecutionBindingDeclaration {
                            access_binding: "local".into(),
                        },
                    ),
                ),
            },
        )
        .expect_err("incarnation length is structural");
        assert_eq!(short_incarnation.kind(), ProtocolErrorKind::OutOfRange);

        let empty_binding = ConnectorExecutionBindingDeclaration::iceberg("catalog", [1; 16], "")
            .expect_err("empty binding is invalid");
        assert_eq!(empty_binding.kind(), ProtocolErrorKind::InvalidValue);

        let oversized_binding = ConnectorExecutionBindingDeclaration::starrocks(
            "catalog",
            [1; 16],
            "x".repeat(MAX_LOCAL_BINDING_BYTES + 1),
        )
        .expect_err("binding bound is structural");
        assert_eq!(oversized_binding.kind(), ProtocolErrorKind::InvalidValue);
    }

    #[test]
    fn declaration_digest_is_domain_separated_and_covers_presence_and_variant() {
        const ICEBERG_DECLARATION_DIGEST_GOLDEN_HEX: &str =
            "c443163cfd63a77ca3c489f407fee2e4ad08e97cd5d9d6ffdc7bd3ca596c5ee3";

        let iceberg = declaration();
        let same = declaration();
        let starrocks = ConnectorExecutionBindingDeclaration::starrocks(
            "catalog.analytics",
            [7; 16],
            "local-starrocks",
        )
        .expect("valid starrocks declaration");
        let changed_binding = ConnectorExecutionBindingDeclaration::iceberg(
            "catalog.analytics",
            [7; 16],
            "different-binding",
        )
        .expect("valid declaration");
        let changed_instance = ConnectorExecutionBindingDeclaration::iceberg(
            "catalog.replacement",
            [7; 16],
            "local-iceberg",
        )
        .expect("valid declaration");
        let changed_incarnation = ConnectorExecutionBindingDeclaration::iceberg(
            "catalog.analytics",
            [8; 16],
            "local-iceberg",
        )
        .expect("valid declaration");

        assert_eq!(
            iceberg.digest().expect("digest"),
            same.digest().expect("digest")
        );
        assert_ne!(
            iceberg.digest().expect("digest"),
            starrocks.digest().expect("digest")
        );
        assert_ne!(
            iceberg.digest().expect("digest"),
            changed_binding.digest().expect("digest")
        );
        assert_ne!(
            iceberg.digest().expect("digest"),
            changed_instance.digest().expect("digest")
        );
        assert_ne!(
            iceberg.digest().expect("digest"),
            changed_incarnation.digest().expect("digest")
        );
        assert_eq!(
            iceberg
                .digest()
                .expect("digest")
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            ICEBERG_DECLARATION_DIGEST_GOLDEN_HEX,
        );
    }

    #[test]
    fn validates_the_closed_ensure_reason_retry_matrix() {
        for reason in [
            EnsureConnectorExecutionBindingRejectionReason::InvalidDeclaration,
            EnsureConnectorExecutionBindingRejectionReason::ConflictingDeclaration,
            EnsureConnectorExecutionBindingRejectionReason::QueryIncarnationConflict,
            EnsureConnectorExecutionBindingRejectionReason::Retiring,
            EnsureConnectorExecutionBindingRejectionReason::HostUnavailable,
        ] {
            assert!(
                EnsureConnectorExecutionBindingRejection::try_new(reason, false, "safe", None)
                    .is_ok()
            );
            assert!(
                EnsureConnectorExecutionBindingRejection::try_new(reason, true, "safe", None)
                    .is_err()
            );
        }
        assert!(
            EnsureConnectorExecutionBindingRejection::try_new(
                EnsureConnectorExecutionBindingRejectionReason::DeadlineExceeded,
                true,
                "safe",
                None,
            )
            .is_ok()
        );
        assert!(
            EnsureConnectorExecutionBindingRejection::try_new(
                EnsureConnectorExecutionBindingRejectionReason::DeadlineExceeded,
                false,
                "safe",
                None,
            )
            .is_err()
        );
        for reason in [
            EnsureConnectorExecutionBindingRejectionReason::ActivationUnavailable,
            EnsureConnectorExecutionBindingRejectionReason::ResourceExhausted,
            EnsureConnectorExecutionBindingRejectionReason::InternalFailure,
        ] {
            for retryable in [false, true] {
                assert!(
                    EnsureConnectorExecutionBindingRejection::try_new(
                        reason,
                        retryable,
                        "safe",
                        Some("binding.access".into())
                    )
                    .is_ok()
                );
            }
        }
    }

    #[test]
    fn ensure_and_retire_results_round_trip_and_reject_missing_or_unknown_outcomes() {
        let rejection = EnsureConnectorExecutionBindingRejection::try_new(
            EnsureConnectorExecutionBindingRejectionReason::Retiring,
            false,
            "generation is retiring",
            Some("declaration.incarnation".into()),
        )
        .expect("valid rejection");
        let result = EnsureConnectorExecutionBindingResult::rejected(rejection.clone());
        assert_eq!(
            EnsureConnectorExecutionBindingResult::try_from_proto(result.to_proto())
                .expect("round trip")
                .outcome(),
            result.outcome()
        );
        assert!(EnsureConnectorExecutionBindingResult::try_from_proto(Default::default()).is_err());

        let retire = RetireConnectorExecutionBindingResult::new(
            RetireConnectorExecutionBindingOutcome::Accepted,
        );
        assert_eq!(
            RetireConnectorExecutionBindingResult::try_from_proto(retire.to_proto())
                .expect("round trip"),
            retire
        );
        assert!(RetireConnectorExecutionBindingResult::try_from_proto(Default::default()).is_err());

        let unknown_reason = novarocks::EnsureConnectorExecutionBindingResponse {
            outcome: Some(
                novarocks::ensure_connector_execution_binding_response::Outcome::Rejection(
                    novarocks::EnsureConnectorExecutionBindingRejection {
                        reason: 99,
                        retryable_before_progress: false,
                        safe_detail: "safe".into(),
                        safe_field_path: None,
                    },
                ),
            ),
        };
        assert!(EnsureConnectorExecutionBindingResult::try_from_proto(unknown_reason).is_err());

        for outcome in [
            RetireConnectorExecutionBindingOutcome::Accepted,
            RetireConnectorExecutionBindingOutcome::NotFound,
            RetireConnectorExecutionBindingOutcome::Unavailable,
            RetireConnectorExecutionBindingOutcome::InvalidKey,
            RetireConnectorExecutionBindingOutcome::Internal,
        ] {
            let result = RetireConnectorExecutionBindingResult::new(outcome);
            assert_eq!(
                RetireConnectorExecutionBindingResult::try_from_proto(result.to_proto())
                    .expect("closed retire outcome round trip"),
                result
            );
        }
    }

    #[test]
    fn generated_descriptor_exposes_only_the_closed_provider_and_result_sets() {
        let pool = DescriptorPool::decode(crate::FILE_DESCRIPTOR_SET).expect("descriptor set");
        let declaration = pool
            .get_message_by_name("novarocks.ConnectorExecutionBindingDeclaration")
            .expect("declaration descriptor");
        let provider = declaration
            .oneofs()
            .find(|oneof| oneof.name() == "provider")
            .expect("provider oneof");
        let names = provider
            .fields()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, ["iceberg", "starrocks"]);

        let ensure_reason = pool
            .get_enum_by_name("novarocks.EnsureConnectorExecutionBindingRejectionReason")
            .expect("ensure reason descriptor");
        assert!(ensure_reason.get_value_by_name("CANCELLED").is_none());

        let ensure_request = pool
            .get_message_by_name("novarocks.EnsureConnectorExecutionBindingRequest")
            .expect("ensure request descriptor");
        assert_eq!(
            ensure_request
                .fields()
                .map(|field| (field.number(), field.name().to_string()))
                .collect::<Vec<_>>(),
            [
                (1, "execution_id".to_string()),
                (6, "declaration".to_string())
            ]
        );
        assert_eq!(
            ensure_request.reserved_ranges().collect::<Vec<_>>(),
            vec![2..3, 3..4, 4..5, 5..6]
        );
        for name in [
            "provider_id",
            "instance_id",
            "incarnation",
            "declaration_payload",
        ] {
            assert!(
                ensure_request
                    .reserved_names()
                    .any(|reserved| reserved == name)
            );
        }

        for response_name in [
            "novarocks.EnsureConnectorExecutionBindingResponse",
            "novarocks.RetireConnectorExecutionBindingResponse",
        ] {
            let response = pool
                .get_message_by_name(response_name)
                .expect("response descriptor");
            assert_eq!(
                response.reserved_ranges().collect::<Vec<_>>(),
                vec![1..2, 2..3]
            );
            assert!(response.reserved_names().any(|name| name == "status_code"));
            assert!(response.reserved_names().any(|name| name == "message"));
        }
    }
}
