//! Validated generated values for the native query participant manifest.
//!
//! Every wrapper in this module retains exactly one generated protobuf message.
//! Validation is performed at ingress; accessors re-parse or copy generated
//! leaves rather than keeping a Core-style parallel representation in sync.

use std::collections::BTreeSet;
use std::time::Duration;

use super::identity::{QueryExecutionId, decode_query_execution_id, encode_query_execution_id};
use super::query_options::QueryOptions;
use crate::canonical;
use crate::catalog::CatalogSet;
use crate::membership::{BackendProcessId, required_native_compatibility_id};
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{common, novarocks};
use novarocks_types::{BackendProcessId as DomainBackendProcessId, NativeCompatibilityId};

const PARTICIPANT_MANIFEST_V1_DOMAIN: &[u8] =
    b"novarocks.query-lifecycle.participant-manifest.v1\0";

/// Validated generated query-control endpoint.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryControlEndpoint {
    raw: novarocks::QueryControlEndpoint,
}

impl QueryControlEndpoint {
    /// Constructs a generated endpoint before applying the canonical
    /// lifecycle validation. This is a convenience for role-local assembly
    /// and tests; the generated message remains the stored representation.
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::QueryControlEndpoint {
            host: host.into(),
            port: u32::from(port),
        })
    }

    pub fn parse(raw: novarocks::QueryControlEndpoint) -> Result<Self, ProtocolError> {
        if raw.host.trim().is_empty() {
            return Err(invalid(
                FieldPath::root("query_control_endpoint").field("host"),
                "query control endpoint host must not be empty",
            ));
        }
        if raw.port == 0 {
            return Err(invalid(
                FieldPath::root("query_control_endpoint").field("port"),
                "query control endpoint port must be nonzero",
            ));
        }
        if raw.port > u32::from(u16::MAX) {
            return Err(out_of_range(
                FieldPath::root("query_control_endpoint").field("port"),
                "query control endpoint port exceeds u16 range",
            ));
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::QueryControlEndpoint {
        &self.raw
    }

    pub fn host(&self) -> &str {
        &self.raw.host
    }

    pub const fn port(&self) -> u16 {
        self.raw.port as u16
    }
}

/// Validated generated backend identity.
#[derive(Clone, Debug, PartialEq)]
pub struct ParticipantBackendIdentity {
    raw: novarocks::ParticipantBackendIdentity,
}

impl ParticipantBackendIdentity {
    /// Constructs a validated generated backend identity without a Core
    /// mirror value.
    pub fn new(
        process_id: DomainBackendProcessId,
        endpoint: QueryControlEndpoint,
    ) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::ParticipantBackendIdentity {
            endpoint: Some(endpoint.as_proto().clone()),
            process_id: Some(BackendProcessId::from_domain(process_id).as_proto().clone()),
        })
    }

    pub fn parse(raw: novarocks::ParticipantBackendIdentity) -> Result<Self, ProtocolError> {
        let endpoint = raw.endpoint.clone().ok_or_else(|| {
            missing(
                FieldPath::root("participant_backend_identity").field("endpoint"),
                "participant backend endpoint is required",
            )
        })?;
        QueryControlEndpoint::parse(endpoint).map_err(|error| {
            prefix_path(
                FieldPath::root("participant_backend_identity").field("endpoint"),
                error,
            )
        })?;
        let process_id = raw.process_id.clone().ok_or_else(|| {
            missing(
                FieldPath::root("participant_backend_identity").field("process_id"),
                "backend process id is required",
            )
        })?;
        BackendProcessId::parse(process_id).map_err(|error| {
            prefix_path(
                FieldPath::root("participant_backend_identity").field("process_id"),
                error,
            )
        })?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::ParticipantBackendIdentity {
        &self.raw
    }

    pub fn process_id(&self) -> Result<DomainBackendProcessId, ProtocolError> {
        let raw = self.raw.process_id.clone().ok_or_else(|| {
            missing(
                FieldPath::root("participant_backend_identity").field("process_id"),
                "backend process id is required",
            )
        })?;
        BackendProcessId::parse(raw)?.domain()
    }

    pub fn endpoint(&self) -> Result<QueryControlEndpoint, ProtocolError> {
        required_endpoint(
            &self.raw.endpoint,
            "participant backend endpoint is required",
        )
    }
}

/// Validated allocated identity of one admitted query participant.
///
/// This value intentionally excludes endpoint and backend ordinal: both are
/// routing facts and cannot identify a BE process incarnation.
// Design: ADR-0126 (docs/adr/ADR-0126-terminal-delivery-participant-attempt-ref.md)
#[derive(Clone, Debug, PartialEq)]
pub struct ParticipantAttemptRef {
    raw: novarocks::ParticipantAttemptRef,
}

impl ParticipantAttemptRef {
    pub fn new(
        execution_id: QueryExecutionId,
        backend_process_id: DomainBackendProcessId,
    ) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::ParticipantAttemptRef {
            execution_id: Some(encode_query_execution_id(execution_id)),
            backend_process_id: Some(
                BackendProcessId::from_domain(backend_process_id)
                    .as_proto()
                    .clone(),
            ),
        })
    }

    pub fn parse(raw: novarocks::ParticipantAttemptRef) -> Result<Self, ProtocolError> {
        required_execution_id(&raw.execution_id).map_err(|error| {
            prefix_path(
                FieldPath::root("participant_attempt_ref").field("execution_id"),
                error,
            )
        })?;
        let process_id = raw.backend_process_id.clone().ok_or_else(|| {
            missing(
                FieldPath::root("participant_attempt_ref").field("backend_process_id"),
                "participant backend process id is required",
            )
        })?;
        BackendProcessId::parse(process_id).map_err(|error| {
            prefix_path(
                FieldPath::root("participant_attempt_ref").field("backend_process_id"),
                error,
            )
        })?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::ParticipantAttemptRef {
        &self.raw
    }

    pub fn execution_id(&self) -> Result<QueryExecutionId, ProtocolError> {
        required_execution_id(&self.raw.execution_id)
    }

    pub fn backend_process_id(&self) -> Result<DomainBackendProcessId, ProtocolError> {
        let raw = self.raw.backend_process_id.clone().ok_or_else(|| {
            missing(
                FieldPath::root("participant_attempt_ref").field("backend_process_id"),
                "participant backend process id is required",
            )
        })?;
        BackendProcessId::parse(raw)?.domain()
    }
}

/// Validated generated exchange route.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExchangeRouteManifest {
    raw: novarocks::ExchangeRouteManifest,
}

impl ExchangeRouteManifest {
    /// Constructs a validated generated exchange route without an execution
    /// layer DTO.
    pub fn new(
        source_fragment_instance_id: common::UniqueId,
        destination_fragment_instance_id: common::UniqueId,
        destination_node_id: i32,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> Result<Self, ProtocolError> {
        Self::parse(novarocks::ExchangeRouteManifest {
            source_fragment_instance_id: Some(source_fragment_instance_id),
            destination_fragment_instance_id: Some(destination_fragment_instance_id),
            destination_node_id,
            sender_ordinal,
            sender_count,
        })
    }

    pub fn parse(raw: novarocks::ExchangeRouteManifest) -> Result<Self, ProtocolError> {
        let source = required_unique_id(
            &raw.source_fragment_instance_id,
            "exchange route source fragment instance id is required",
        )?;
        let destination = required_unique_id(
            &raw.destination_fragment_instance_id,
            "exchange route destination fragment instance id is required",
        )?;
        if is_missing_unique_id(source) || is_missing_unique_id(destination) {
            return Err(invalid(
                FieldPath::root("exchange_route_manifest").field("source_fragment_instance_id"),
                "exchange route fragment instance ids must be nonzero",
            ));
        }
        if raw.destination_node_id < 0 {
            return Err(invalid(
                FieldPath::root("exchange_route_manifest").field("destination_node_id"),
                "exchange route destination node id must be nonnegative",
            ));
        }
        if raw.sender_count == 0 || raw.sender_ordinal >= raw.sender_count {
            return Err(invalid(
                FieldPath::root("exchange_route_manifest").field("sender_ordinal"),
                "exchange route sender ordinal must be less than nonzero sender count",
            ));
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::ExchangeRouteManifest {
        &self.raw
    }

    pub fn source_fragment_instance_id(&self) -> Result<common::UniqueId, ProtocolError> {
        required_unique_id(
            &self.raw.source_fragment_instance_id,
            "exchange route source fragment instance id is required",
        )
    }

    pub fn destination_fragment_instance_id(&self) -> Result<common::UniqueId, ProtocolError> {
        required_unique_id(
            &self.raw.destination_fragment_instance_id,
            "exchange route destination fragment instance id is required",
        )
    }

    pub const fn destination_node_id(&self) -> i32 {
        self.raw.destination_node_id
    }

    pub const fn sender_ordinal(&self) -> u32 {
        self.raw.sender_ordinal
    }

    pub const fn sender_count(&self) -> u32 {
        self.raw.sender_count
    }
}

/// Opaque, validated generated runtime-filter contribution.
///
/// The lifecycle and install payloads deliberately remain generated values:
/// Backend owns their semantic decoding, while this contract owns only the
/// participant-manifest carrier shape.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeFilterContribution {
    raw: novarocks::RuntimeFilterContribution,
}

impl RuntimeFilterContribution {
    pub fn parse(raw: novarocks::RuntimeFilterContribution) -> Result<Self, ProtocolError> {
        if raw.participant_id == 0 {
            return Err(invalid(
                FieldPath::root("runtime_filter_contribution").field("participant_id"),
                "runtime filter participant id must be nonzero",
            ));
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::RuntimeFilterContribution {
        &self.raw
    }

    pub const fn participant_id(&self) -> u32 {
        self.raw.participant_id
    }
}

/// A fixed-width digest for one validated manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParticipantManifestDigest([u8; 32]);

impl ParticipantManifestDigest {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn try_from_slice(bytes: &[u8]) -> Result<Self, ProtocolError> {
        let bytes = bytes.try_into().map_err(|_| {
            invalid(
                FieldPath::root("participant_manifest").field("digest"),
                "participant manifest digest must be 32 bytes",
            )
        })?;
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Validated generated manifest, retaining the exact wire representation.
#[derive(Clone, Debug, PartialEq)]
pub struct ParticipantManifest {
    raw: novarocks::ParticipantManifest,
}

impl ParticipantManifest {
    /// Assembles a participant manifest from validated Protocol leaves.
    ///
    /// The returned wrapper retains only the generated protobuf message; this
    /// is intentionally a role-local construction convenience rather than a
    /// second lifecycle data model.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        native_compatibility_id: NativeCompatibilityId,
        expected_fragment_instance_ids: impl IntoIterator<Item = common::UniqueId>,
        query_options: QueryOptions,
        query_deadline_unix_ms: u64,
        exchange_routes: impl IntoIterator<Item = ExchangeRouteManifest>,
        runtime_filter: Option<RuntimeFilterContribution>,
        pre_start_timeout: Duration,
        report_endpoint: QueryControlEndpoint,
    ) -> Result<Self, ProtocolError> {
        Self::new_with_catalog_set(
            execution_id,
            backend,
            native_compatibility_id,
            expected_fragment_instance_ids,
            query_options,
            query_deadline_unix_ms,
            exchange_routes,
            runtime_filter,
            pre_start_timeout,
            report_endpoint,
            CatalogSet::new([])?,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_catalog_set(
        execution_id: QueryExecutionId,
        backend: ParticipantBackendIdentity,
        native_compatibility_id: NativeCompatibilityId,
        expected_fragment_instance_ids: impl IntoIterator<Item = common::UniqueId>,
        query_options: QueryOptions,
        query_deadline_unix_ms: u64,
        exchange_routes: impl IntoIterator<Item = ExchangeRouteManifest>,
        runtime_filter: Option<RuntimeFilterContribution>,
        pre_start_timeout: Duration,
        report_endpoint: QueryControlEndpoint,
        catalog_set: CatalogSet,
    ) -> Result<Self, ProtocolError> {
        let pre_start_timeout_ms = u64::try_from(pre_start_timeout.as_millis()).map_err(|_| {
            out_of_range(
                FieldPath::root("participant_manifest").field("pre_start_timeout_ms"),
                "pre-start timeout exceeds u64 milliseconds",
            )
        })?;
        Self::parse(novarocks::ParticipantManifest {
            execution_id: Some(encode_query_execution_id(execution_id)),
            backend: Some(backend.as_proto().clone()),
            native_compatibility_id: Some(novarocks::NativeCompatibilityId {
                value: native_compatibility_id.as_bytes().to_vec(),
            }),
            expected_fragment_instance_ids: expected_fragment_instance_ids.into_iter().collect(),
            query_options: Some(*query_options.as_proto()),
            query_deadline_unix_ms,
            exchange_routes: exchange_routes
                .into_iter()
                .map(|route| *route.as_proto())
                .collect(),
            runtime_filter: runtime_filter.map(|contribution| contribution.as_proto().clone()),
            pre_start_timeout_ms,
            report_endpoint: Some(report_endpoint.as_proto().clone()),
            catalog_set: Some(catalog_set.as_proto().clone()),
        })
    }

    /// Validates all manifest and leaf invariants without normalizing or
    /// rebuilding the generated message.
    pub fn parse(raw: novarocks::ParticipantManifest) -> Result<Self, ProtocolError> {
        required_execution_id(&raw.execution_id).map_err(|error| {
            prefix_path(
                FieldPath::root("participant_manifest").field("execution_id"),
                error,
            )
        })?;
        required_backend(&raw.backend).map_err(|error| {
            prefix_path(
                FieldPath::root("participant_manifest").field("backend"),
                error,
            )
        })?;
        required_native_compatibility_id(
            &raw.native_compatibility_id,
            FieldPath::root("participant_manifest").field("native_compatibility_id"),
        )?;
        let catalog_set = raw.catalog_set.clone().ok_or_else(|| {
            missing(
                FieldPath::root("participant_manifest").field("catalog_set"),
                "catalog set is required",
            )
        })?;
        CatalogSet::parse(catalog_set).map_err(|error| {
            prefix_path(
                FieldPath::root("participant_manifest").field("catalog_set"),
                error,
            )
        })?;

        let mut fragment_ids = BTreeSet::new();
        for (index, fragment_id) in raw
            .expected_fragment_instance_ids
            .iter()
            .copied()
            .enumerate()
        {
            if is_missing_unique_id(fragment_id) {
                return Err(invalid(
                    FieldPath::root("participant_manifest")
                        .field("expected_fragment_instance_ids")
                        .index(index),
                    "expected fragment instance ids must be nonzero",
                ));
            }
            if !fragment_ids.insert((fragment_id.hi, fragment_id.lo)) {
                return Err(ProtocolError::new(
                    FieldPath::root("participant_manifest")
                        .field("expected_fragment_instance_ids")
                        .index(index),
                    ProtocolErrorKind::InvalidValue,
                    "duplicate fragment instance id",
                ));
            }
        }
        let options = raw.query_options.ok_or_else(|| {
            missing(
                FieldPath::root("participant_manifest").field("query_options"),
                "query options are required",
            )
        })?;
        QueryOptions::parse(options).map_err(|error| {
            prefix_path(
                FieldPath::root("participant_manifest").field("query_options"),
                error,
            )
        })?;

        let mut exchange_routes = BTreeSet::new();
        for (index, route) in raw.exchange_routes.iter().copied().enumerate() {
            let route = ExchangeRouteManifest::parse(route).map_err(|error| {
                prefix_path(
                    FieldPath::root("participant_manifest")
                        .field("exchange_routes")
                        .index(index),
                    error,
                )
            })?;
            let source = route.source_fragment_instance_id()?;
            let destination = route.destination_fragment_instance_id()?;
            let route_key = (
                source.hi,
                source.lo,
                destination.hi,
                destination.lo,
                route.destination_node_id(),
                route.sender_ordinal(),
                route.sender_count(),
            );
            if !exchange_routes.insert(route_key) {
                return Err(ProtocolError::new(
                    FieldPath::root("participant_manifest")
                        .field("exchange_routes")
                        .index(index),
                    ProtocolErrorKind::InvalidValue,
                    "duplicate exchange route",
                ));
            }
        }

        let runtime_filter = raw
            .runtime_filter
            .clone()
            .map(RuntimeFilterContribution::parse)
            .transpose()?;
        // A participant must carry work. Fragment execution and the runtime
        // filter service are the only two forms, and each is read directly
        // from the payload that carries it.
        if fragment_ids.is_empty() && runtime_filter.is_none() {
            return Err(ProtocolError::new(
                FieldPath::root("participant_manifest"),
                ProtocolErrorKind::InvalidValue,
                "participant manifest must declare fragment instances or a runtime filter contribution",
            ));
        }
        if raw.query_deadline_unix_ms == 0 {
            return Err(invalid(
                FieldPath::root("participant_manifest").field("query_deadline_unix_ms"),
                "query deadline must be nonzero",
            ));
        }
        if raw.pre_start_timeout_ms == 0 {
            return Err(invalid(
                FieldPath::root("participant_manifest").field("pre_start_timeout_ms"),
                "pre-start timeout must be nonzero",
            ));
        }
        required_endpoint(&raw.report_endpoint, "report endpoint is required").map_err(
            |error| {
                prefix_path(
                    FieldPath::root("participant_manifest").field("report_endpoint"),
                    error,
                )
            },
        )?;

        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &novarocks::ParticipantManifest {
        &self.raw
    }

    pub fn execution_id(&self) -> Result<QueryExecutionId, ProtocolError> {
        required_execution_id(&self.raw.execution_id)
    }

    pub fn backend(&self) -> Result<ParticipantBackendIdentity, ProtocolError> {
        required_backend(&self.raw.backend)
    }

    pub fn native_compatibility_id(&self) -> Result<NativeCompatibilityId, ProtocolError> {
        required_native_compatibility_id(
            &self.raw.native_compatibility_id,
            FieldPath::root("participant_manifest").field("native_compatibility_id"),
        )
    }

    pub fn catalog_set(&self) -> Result<CatalogSet, ProtocolError> {
        let raw = self.raw.catalog_set.clone().ok_or_else(|| {
            missing(
                FieldPath::root("participant_manifest").field("catalog_set"),
                "catalog set is required",
            )
        })?;
        CatalogSet::parse(raw).map_err(|error| {
            prefix_path(
                FieldPath::root("participant_manifest").field("catalog_set"),
                error,
            )
        })
    }

    pub fn expected_fragment_instance_ids(&self) -> Vec<common::UniqueId> {
        self.raw.expected_fragment_instance_ids.clone()
    }

    pub fn query_options(&self) -> Result<QueryOptions, ProtocolError> {
        let raw = self.raw.query_options.ok_or_else(|| {
            missing(
                FieldPath::root("participant_manifest").field("query_options"),
                "query options are required",
            )
        })?;
        QueryOptions::parse(raw).map_err(|error| {
            prefix_path(
                FieldPath::root("participant_manifest").field("query_options"),
                error,
            )
        })
    }

    pub const fn query_deadline_unix_ms(&self) -> u64 {
        self.raw.query_deadline_unix_ms
    }

    pub fn exchange_routes(&self) -> Result<Vec<ExchangeRouteManifest>, ProtocolError> {
        self.raw
            .exchange_routes
            .iter()
            .copied()
            .map(ExchangeRouteManifest::parse)
            .collect()
    }

    pub fn runtime_filter(&self) -> Result<Option<RuntimeFilterContribution>, ProtocolError> {
        self.raw
            .runtime_filter
            .clone()
            .map(RuntimeFilterContribution::parse)
            .transpose()
    }

    pub const fn pre_start_timeout_ms(&self) -> u64 {
        self.raw.pre_start_timeout_ms
    }

    pub fn report_endpoint(&self) -> Result<QueryControlEndpoint, ProtocolError> {
        required_endpoint(&self.raw.report_endpoint, "report endpoint is required")
    }

    /// Computes the descriptor-driven digest of the complete generated
    /// manifest, so new schema fields enter the fence without a hand-written
    /// projection update.
    pub fn digest(&self) -> Result<ParticipantManifestDigest, ProtocolError> {
        canonical::digest_message(
            PARTICIPANT_MANIFEST_V1_DOMAIN,
            "novarocks.ParticipantManifest",
            &self.raw,
        )
        .map(ParticipantManifestDigest::new)
        .map_err(|error| {
            invalid(
                FieldPath::root("participant_manifest"),
                format!("cannot compute participant manifest digest: {error}"),
            )
        })
    }
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn out_of_range(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn prefix_path(prefix: FieldPath, error: ProtocolError) -> ProtocolError {
    ProtocolError::new(
        prefix.append_segments(error.path().segments().iter().skip(1).cloned()),
        error.kind(),
        error.detail(),
    )
}

fn required_execution_id(
    raw: &Option<novarocks::QueryExecutionId>,
) -> Result<QueryExecutionId, ProtocolError> {
    let raw = raw.as_ref().ok_or_else(|| {
        missing(
            FieldPath::root("participant_manifest").field("execution_id"),
            "query execution id is required",
        )
    })?;
    decode_query_execution_id(raw)
}

fn required_backend(
    raw: &Option<novarocks::ParticipantBackendIdentity>,
) -> Result<ParticipantBackendIdentity, ProtocolError> {
    let raw = raw.clone().ok_or_else(|| {
        missing(
            FieldPath::root("participant_manifest").field("backend"),
            "participant backend identity is required",
        )
    })?;
    ParticipantBackendIdentity::parse(raw)
}

fn required_endpoint(
    raw: &Option<novarocks::QueryControlEndpoint>,
    missing_detail: &'static str,
) -> Result<QueryControlEndpoint, ProtocolError> {
    let raw = raw.clone().ok_or_else(|| {
        missing(
            FieldPath::root("participant_manifest").field("report_endpoint"),
            missing_detail,
        )
    })?;
    QueryControlEndpoint::parse(raw)
}

fn required_unique_id(
    raw: &Option<common::UniqueId>,
    missing_detail: &'static str,
) -> Result<common::UniqueId, ProtocolError> {
    (*raw).ok_or_else(|| {
        missing(
            FieldPath::root("exchange_route_manifest").field("fragment_instance_id"),
            missing_detail,
        )
    })
}

const fn is_missing_unique_id(id: common::UniqueId) -> bool {
    id.hi == 0 && id.lo == 0
}

#[cfg(test)]
mod tests {
    use super::{
        ExchangeRouteManifest, ParticipantManifest, ParticipantManifestDigest,
        QueryControlEndpoint, RuntimeFilterContribution,
    };
    use crate::ProtocolErrorKind;
    use novarocks_proto_models::{catalog, common, novarocks};
    use novarocks_types::{NativeCompatibilityId, QueryId};

    fn id(hi: i64, lo: i64) -> common::UniqueId {
        common::UniqueId { hi, lo }
    }

    fn endpoint(port: u32) -> novarocks::QueryControlEndpoint {
        novarocks::QueryControlEndpoint {
            host: "127.0.0.1".into(),
            port,
        }
    }

    fn backend() -> novarocks::ParticipantBackendIdentity {
        novarocks::ParticipantBackendIdentity {
            endpoint: Some(endpoint(9030)),
            process_id: Some(novarocks::BackendProcessId {
                value: vec![
                    0x01, 0x9c, 0x98, 0xa9, 0x33, 0x90, 0x75, 0x76, 0x97, 0x7b, 0x33, 0xd1, 0x88,
                    0xad, 0x1f, 0x06,
                ],
            }),
        }
    }

    fn execution_id() -> novarocks::QueryExecutionId {
        novarocks::QueryExecutionId {
            query_id: Some(id(5, 6)),
            attempt_id: 1,
        }
    }

    fn route() -> novarocks::ExchangeRouteManifest {
        novarocks::ExchangeRouteManifest {
            source_fragment_instance_id: Some(id(7, 8)),
            destination_fragment_instance_id: Some(id(9, 10)),
            destination_node_id: 4,
            sender_ordinal: 0,
            sender_count: 1,
        }
    }

    fn contribution() -> novarocks::RuntimeFilterContribution {
        novarocks::RuntimeFilterContribution {
            participant_id: 7,
            ..Default::default()
        }
    }

    fn manifest() -> novarocks::ParticipantManifest {
        novarocks::ParticipantManifest {
            execution_id: Some(execution_id()),
            backend: Some(backend()),
            native_compatibility_id: Some(novarocks::NativeCompatibilityId { value: vec![7; 32] }),
            expected_fragment_instance_ids: vec![id(11, 12)],
            query_options: Some(novarocks::QueryOptions::default()),
            query_deadline_unix_ms: 1_000,
            exchange_routes: vec![route()],
            pre_start_timeout_ms: 30_000,
            report_endpoint: Some(endpoint(9031)),
            catalog_set: Some(catalog::CatalogSet { catalogs: vec![] }),
            ..Default::default()
        }
    }

    fn assert_invalid(raw: novarocks::ParticipantManifest, detail: &str) {
        let error = ParticipantManifest::parse(raw).expect_err("fixture must be invalid");
        assert_eq!(error.detail(), detail);
    }

    #[test]
    fn accepts_a_service_only_participant_carrying_no_fragment_instances() {
        // A participant that runs no fragment is admissible as long as it
        // carries a runtime filter contribution. The empty instance list is
        // the authoritative representation of that shape; nothing declares it.
        let mut raw = manifest();
        raw.expected_fragment_instance_ids.clear();
        raw.runtime_filter = Some(contribution());
        let parsed = ParticipantManifest::parse(raw).expect("service-only manifest is valid");
        assert!(parsed.expected_fragment_instance_ids().is_empty());
        assert!(parsed.runtime_filter().expect("contribution").is_some());
    }

    #[test]
    fn retains_the_exact_generated_manifest_and_parses_leaves_on_access() {
        let raw = manifest();
        let parsed = ParticipantManifest::parse(raw.clone()).expect("valid manifest");

        assert_eq!(parsed.as_proto(), &raw);
        assert_eq!(
            parsed.execution_id().expect("execution id").query_id(),
            QueryId::new(5, 6)
        );
        assert!(parsed.backend().expect("backend").process_id().is_ok());
        assert_eq!(
            parsed.native_compatibility_id().expect("compatibility id"),
            NativeCompatibilityId::new([7; 32])
        );
        assert_eq!(parsed.expected_fragment_instance_ids(), vec![id(11, 12)]);
        assert_eq!(
            parsed.query_options().expect("options").as_proto(),
            raw.query_options.as_ref().expect("options")
        );
        assert_eq!(parsed.exchange_routes().expect("routes").len(), 1);
        assert!(parsed.runtime_filter().expect("filter").is_none());
        assert_eq!(parsed.report_endpoint().expect("endpoint").port(), 9031);
    }

    #[test]
    fn validates_required_messages_and_leaf_shapes() {
        let mut raw = manifest();
        raw.execution_id = None;
        assert_invalid(raw, "query execution id is required");

        let mut raw = manifest();
        raw.backend = None;
        assert_invalid(raw, "participant backend identity is required");

        let mut raw = manifest();
        raw.native_compatibility_id = None;
        assert_invalid(raw, "native compatibility id is required");

        for width in [31, 33] {
            let mut raw = manifest();
            raw.native_compatibility_id = Some(novarocks::NativeCompatibilityId {
                value: vec![7; width],
            });
            let error = ParticipantManifest::parse(raw).expect_err("bad id rejects");
            assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
            assert_eq!(
                error.path().to_string(),
                "participant_manifest.native_compatibility_id.value"
            );
            assert_eq!(
                error.detail(),
                format!(
                    "native compatibility id must contain exactly 32 bytes: native compatibility id must be 32 bytes, got {width}"
                )
            );
        }

        let mut raw = manifest();
        raw.query_options = None;
        assert_invalid(raw, "query options are required");

        let mut raw = manifest();
        raw.report_endpoint = None;
        assert_invalid(raw, "report endpoint is required");

        let mut raw = manifest();
        raw.backend.as_mut().expect("backend").endpoint = None;
        assert_invalid(raw, "participant backend endpoint is required");

        let mut raw = manifest();
        raw.exchange_routes[0].source_fragment_instance_id = None;
        assert_invalid(
            raw,
            "exchange route source fragment instance id is required",
        );

        let endpoint_error = QueryControlEndpoint::parse(novarocks::QueryControlEndpoint {
            host: " ".into(),
            port: 1,
        })
        .expect_err("empty endpoint host");
        assert_eq!(
            endpoint_error.detail(),
            "query control endpoint host must not be empty"
        );

        let route_error = ExchangeRouteManifest::parse(novarocks::ExchangeRouteManifest {
            sender_count: 0,
            ..route()
        })
        .expect_err("zero sender count");
        assert_eq!(
            route_error.detail(),
            "exchange route sender ordinal must be less than nonzero sender count"
        );

        let filter_error = RuntimeFilterContribution::parse(novarocks::RuntimeFilterContribution {
            participant_id: 0,
            ..contribution()
        })
        .expect_err("unaddressed runtime-filter contribution");
        assert_eq!(
            filter_error.detail(),
            "runtime filter participant id must be nonzero"
        );
    }

    #[test]
    fn rejects_each_manifest_set_and_cross_field_violation() {
        let mut raw = manifest();
        raw.expected_fragment_instance_ids = vec![id(11, 12), id(11, 12)];
        assert_invalid(raw, "duplicate fragment instance id");

        let mut raw = manifest();
        raw.expected_fragment_instance_ids = vec![id(0, 0)];
        assert_invalid(raw, "expected fragment instance ids must be nonzero");

        // A participant that carries neither fragment instances nor a runtime
        // filter contribution has no work, and no longer has a role set that
        // could have declared any.
        let mut raw = manifest();
        raw.expected_fragment_instance_ids.clear();
        assert_invalid(
            raw,
            "participant manifest must declare fragment instances or a runtime filter contribution",
        );

        let mut raw = manifest();
        raw.query_deadline_unix_ms = 0;
        assert_invalid(raw, "query deadline must be nonzero");

        let mut raw = manifest();
        raw.pre_start_timeout_ms = 0;
        assert_invalid(raw, "pre-start timeout must be nonzero");

        let mut raw = manifest();
        raw.exchange_routes.push(route());
        assert_invalid(raw, "duplicate exchange route");
    }

    #[test]
    fn reports_nested_manifest_rejection_paths_with_repeated_indexes() {
        let mut raw = manifest();
        raw.exchange_routes[0].sender_count = 0;
        let route_error = ParticipantManifest::parse(raw).expect_err("invalid route");
        assert_eq!(route_error.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(
            route_error.path().to_string(),
            "participant_manifest.exchange_routes[0].sender_ordinal"
        );
    }

    #[test]
    fn descriptor_digest_includes_generated_fields_without_a_hand_written_projection() {
        let first = ParticipantManifest::parse(manifest()).expect("valid manifest");
        let mut changed_raw = manifest();
        changed_raw.runtime_filter = Some(novarocks::RuntimeFilterContribution {
            lifecycle: Some(
                novarocks_proto_models::filter::RuntimeFilterQueryLifecycleOptions {
                    delivery_expire_ms: 1,
                    ..Default::default()
                },
            ),
            ..contribution()
        });
        let changed = ParticipantManifest::parse(changed_raw.clone()).expect("valid manifest");
        let mut changed_again_raw = changed_raw;
        changed_again_raw
            .runtime_filter
            .as_mut()
            .expect("filter")
            .lifecycle
            .as_mut()
            .expect("lifecycle")
            .delivery_expire_ms = 2;
        let changed_again =
            ParticipantManifest::parse(changed_again_raw.clone()).expect("valid manifest");
        let mut changed_install_raw = changed_again_raw;
        changed_install_raw
            .runtime_filter
            .as_mut()
            .expect("filter")
            .install = Some(
            novarocks_proto_models::filter::RuntimeFilterParticipantInstall {
                core_channels: vec![
                    novarocks_proto_models::filter::RuntimeFilterChannelDeployment {
                        channel_id: 9,
                        ..Default::default()
                    },
                ],
                routing_channels: Vec::new(),
            },
        );
        let changed_install =
            ParticipantManifest::parse(changed_install_raw).expect("valid manifest");

        assert_ne!(
            first.digest().expect("digest"),
            changed.digest().expect("digest")
        );
        // A hand-written projection had to enumerate every nested generated
        // field such as these. Descriptor traversal covers the contribution's
        // lifecycle and install subtrees directly, so the contribution needs no
        // self-attestation of its own.
        assert_ne!(
            changed.digest().expect("digest"),
            changed_again.digest().expect("digest")
        );
        assert_ne!(
            changed_again.digest().expect("digest"),
            changed_install.digest().expect("digest")
        );
    }

    #[test]
    fn manifest_digest_requires_exactly_thirty_two_bytes() {
        assert_eq!(
            ParticipantManifestDigest::try_from_slice(&[7; 32])
                .expect("digest")
                .as_bytes(),
            &[7; 32]
        );
        let error = ParticipantManifestDigest::try_from_slice(&[7; 31])
            .expect_err("short digest must fail");
        assert_eq!(
            error.detail(),
            "participant manifest digest must be 32 bytes"
        );
    }
}
