// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Catalog-wide collection for CTAS roots that have no published table anchor.
//!
//! Every candidate is discovered beneath one fixed warehouse prefix, then is
//! independently re-read immediately before its exact prefix is removed. A
//! malformed sidecar, a target that now exists, or any uncertain observation
//! is retained: crash-only recovery must never infer authority from a name.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use novarocks_fs::{FsLocation, FsScheme};
use novarocks_spi::connector::{
    ConnectorCtasUnanchoredCleanupOutcome, ConnectorCtasUnanchoredCleanupRequest,
    ConnectorCtasUnanchoredDiscoveryRequest, ConnectorCtasUnanchoredProvenance, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorInstanceIncarnation, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorRequestContext, ConnectorUnanchoredCtasCleanup,
};

use super::staged_create::{
    ctas_staging_location, decode_unanchored_ctas_provenance, unanchored_ctas_provenance_location,
};
use crate::metadata_context::IcebergMetadataContext;

const CTAS_STAGING_NAMESPACE: &str = "_novarocks/ctas-staging/v1";
const CTAS_UNANCHORED_PROVENANCE_FILE: &str = "_novarocks.ctas.provenance.v1.json";

#[derive(Clone)]
pub(crate) struct IcebergUnanchoredCtasCleanupAdapter {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    runtime: Arc<IcebergMetadataContext>,
}

impl IcebergUnanchoredCtasCleanupAdapter {
    /// Attach a sweeper for this generation's unanchored CTAS staging root.
    ///
    /// This deliberately does not ask whether the catalog can run a CTAS. A
    /// catalog that cannot has never staged anything unanchored, so the sweep
    /// finds nothing and deletes nothing -- and gating here would replace the
    /// catalog's own explanation of why CTAS is impossible with a generic
    /// "no cleanup capability" from the lease derivation. The refusal belongs
    /// where the reason is known.
    pub(crate) fn try_new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Result<Self, ConnectorError> {
        let warehouse = runtime.control_state().configuration().warehouse_uri.trim();
        if warehouse.is_empty() {
            return Err(unsupported(
                "unanchored CTAS cleanup requires an explicit warehouse URI",
            ));
        }
        let parsed =
            FsLocation::parse(warehouse).map_err(|error| unavailable(error.to_string()))?;
        if !matches!(
            parsed.scheme(),
            FsScheme::Local | FsScheme::ObjectStore | FsScheme::Hdfs
        ) {
            return Err(unsupported(
                "unanchored CTAS cleanup warehouse has no list/stat/delete prefix support",
            ));
        }
        if !matches!(parsed.scheme(), FsScheme::Local) {
            let access = crate::fs_io::resolve_access_for_location(
                warehouse,
                runtime.control_state().object_store_config(),
            )
            .map_err(unavailable)?;
            let capability = access.operator().info().full_capability();
            if !capability.list
                || !capability.list_with_recursive
                || !capability.stat
                || !capability.read
                || !capability.delete
            {
                return Err(unsupported(
                    "unanchored CTAS cleanup warehouse lacks recursive list, stat, read, or exact-delete support",
                ));
            }
        }
        Ok(Self {
            descriptor,
            incarnation,
            runtime,
        })
    }

    fn owner(&self) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        }
    }

    fn warehouse_root(&self) -> &str {
        self.runtime
            .control_state()
            .configuration()
            .warehouse_uri
            .trim_end_matches('/')
    }

    fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
        if context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "unanchored CTAS cleanup request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "unanchored CTAS cleanup request deadline elapsed",
            ));
        }
        Ok(())
    }

    fn validate_warehouse(&self, requested: &str) -> Result<(), ConnectorError> {
        if requested.trim_end_matches('/') != self.warehouse_root() {
            return Err(invalid(
                "unanchored CTAS cleanup request warehouse differs from its control generation",
            ));
        }
        Ok(())
    }

    fn file_io(&self) -> crate::iceberg::io::FileIO {
        crate::fs_io::build_file_io_for_location(
            self.warehouse_root(),
            self.runtime.control_state().object_store_config(),
        )
    }

    fn read_optional(&self, location: &str) -> Result<Option<Bytes>, ConnectorError> {
        let file_io = self.file_io();
        let input = file_io
            .new_input(location)
            .map_err(|error| unavailable(error.to_string()))?;
        let exists = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { input.exists().await })
            .map_err(unavailable)?
            .map_err(|error| unavailable(error.to_string()))?;
        if !exists {
            return Ok(None);
        }
        let input = file_io
            .new_input(location)
            .map_err(|error| unavailable(error.to_string()))?;
        let bytes = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { input.read().await })
            .map_err(unavailable)?
            .map_err(|error| unavailable(error.to_string()))?;
        Ok(Some(bytes))
    }

    fn root_for(
        &self,
        publication: novarocks_spi::connector::LakePublicationId,
    ) -> Result<String, ConnectorError> {
        // `try_new` already proved this generation has an explicit warehouse,
        // so the only arm this can take is unreachable here. Report it as the
        // refusal it is rather than as a transient failure a caller might retry.
        let table = ctas_staging_location(self.warehouse_root(), publication)
            .map_err(|_| unsupported("derive unanchored CTAS staging location"))?;
        table
            .strip_suffix("/table")
            .map(ToOwned::to_owned)
            .ok_or_else(|| invalid("CTAS staging location does not end in /table"))
    }

    fn sidecar_for(
        &self,
        publication: novarocks_spi::connector::LakePublicationId,
    ) -> Result<String, ConnectorError> {
        // See `root_for`: unreachable, and a refusal rather than a retryable
        // failure if it ever became reachable.
        let table = ctas_staging_location(self.warehouse_root(), publication)
            .map_err(|_| unsupported("derive unanchored CTAS staging location"))?;
        unanchored_ctas_provenance_location(&table)
    }

    fn sidecar_locations(&self) -> Result<Vec<String>, ConnectorError> {
        let warehouse = self.warehouse_root();
        let root = format!("{warehouse}/{CTAS_STAGING_NAMESPACE}");
        let parsed = FsLocation::parse(&root).map_err(|error| unavailable(error.to_string()))?;
        match parsed.scheme() {
            FsScheme::Local => {
                let mut locations = Vec::new();
                let root_path = std::path::Path::new(parsed.path());
                let entries = match std::fs::read_dir(root_path) {
                    Ok(entries) => entries,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(locations);
                    }
                    Err(error) => {
                        return Err(unavailable(format!("list unanchored CTAS roots: {error}")));
                    }
                };
                for entry in entries {
                    let entry = entry.map_err(|error| {
                        unavailable(format!("read unanchored CTAS root: {error}"))
                    })?;
                    let path = entry.path().join(CTAS_UNANCHORED_PROVENANCE_FILE);
                    if path.is_file() {
                        locations.push(format!("file://{}", path.display()));
                    }
                }
                locations.sort();
                Ok(locations)
            }
            FsScheme::ObjectStore | FsScheme::Hdfs => {
                let access = crate::fs_io::resolve_access_for_location(
                    &root,
                    self.runtime.control_state().object_store_config(),
                )
                .map_err(unavailable)?;
                let path = access
                    .single_relative_path()
                    .map_err(unavailable)?
                    .trim_matches('/')
                    .to_string();
                let prefix = format!("{path}/");
                let operator = access.operator();
                let entries = self
                    .runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(async move { operator.list_with(&prefix).recursive(true).await })
                    .map_err(unavailable)?
                    .map_err(|error| unavailable(error.to_string()))?;
                let mut locations = entries
                    .into_iter()
                    .filter(|entry| entry.path().ends_with(CTAS_UNANCHORED_PROVENANCE_FILE))
                    .filter_map(|entry| {
                        crate::fs_io::format_resolved_location(access.handle(), entry.path()).ok()
                    })
                    .collect::<Vec<_>>();
                locations.sort();
                locations.dedup();
                Ok(locations)
            }
        }
    }
}

impl ConnectorUnanchoredCtasCleanup for IcebergUnanchoredCtasCleanupAdapter {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }

    fn warehouse_root(&self) -> Result<Arc<str>, ConnectorError> {
        Ok(Arc::from(self.warehouse_root()))
    }

    fn discover_unanchored_ctas(
        &self,
        request: ConnectorCtasUnanchoredDiscoveryRequest,
        context: ConnectorRequestContext,
    ) -> Result<Vec<ConnectorCtasUnanchoredProvenance>, ConnectorError> {
        Self::validate_context(&context)?;
        if request.owner != self.owner() {
            return Err(invalid("unanchored CTAS discovery has a foreign owner"));
        }
        self.validate_warehouse(&request.warehouse_root)?;
        let mut candidates = Vec::new();
        for sidecar in self.sidecar_locations()? {
            let Some(bytes) = self.read_optional(&sidecar)? else {
                continue;
            };
            let Ok(provenance) = decode_unanchored_ctas_provenance(&bytes) else {
                continue;
            };
            if provenance.target.instance_id != self.descriptor.instance_id
                || provenance.created_at_ms >= request.cutoff_ms
            {
                continue;
            }
            let expected_sidecar = self.sidecar_for(provenance.publication_id)?;
            if sidecar != expected_sidecar {
                continue;
            }
            candidates.push(provenance);
        }
        candidates.sort_by_key(|candidate| candidate.publication_id);
        candidates.dedup_by_key(|candidate| candidate.publication_id);
        Ok(candidates)
    }

    fn inspect_then_delete_unanchored_ctas(
        &self,
        request: ConnectorCtasUnanchoredCleanupRequest,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorCtasUnanchoredCleanupOutcome, ConnectorError> {
        Self::validate_context(&context)?;
        if request.owner != self.owner() {
            return Err(invalid("unanchored CTAS cleanup has a foreign owner"));
        }
        self.validate_warehouse(&request.warehouse_root)?;
        if request.provenance.created_at_ms >= request.cutoff_ms {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        }
        let sidecar = self.sidecar_for(request.provenance.publication_id)?;
        let Some(bytes) = self.read_optional(&sidecar)? else {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        };
        let Ok(observed) = decode_unanchored_ctas_provenance(&bytes) else {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        };
        if observed != request.provenance {
            return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
        }
        // The current target must be a fresh exact lookup. A matching UUID is
        // the successful publication and pins this root. A different UUID is
        // an ambiguous drop/recreate observation: the old staging root may be
        // shared or otherwise still operator-relevant, so it also leaks. Only
        // a definite NotFound target permits unanchored-root deletion.
        self.runtime
            .control_state()
            .invalidate_table(&observed.target.namespace, &observed.target.table);
        match self
            .runtime
            .load_table_classified(&observed.target.namespace, &observed.target.table)
        {
            Ok(_physical) => {
                let _expected_uuid = observed.staged_table_uuid.ok_or_else(|| {
                    invalid("unanchored CTAS cleanup provenance lacks staged table UUID")
                })?;
                return Ok(ConnectorCtasUnanchoredCleanupOutcome::Retained);
            }
            Err((ConnectorErrorKind::NotFound, _)) => {}
            Err((kind, message)) => return Err(ConnectorError::new(kind, message)),
        }
        let root = self.root_for(observed.publication_id)?;
        let file_io = self.file_io();
        let delete = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move { file_io.delete_prefix(root).await });
        match delete {
            Ok(Ok(())) => Ok(ConnectorCtasUnanchoredCleanupOutcome::Deleted),
            Ok(Err(error)) => Ok(ConnectorCtasUnanchoredCleanupOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    format!("delete unanchored CTAS root: {error}"),
                ),
            }),
            Err(error) => Ok(ConnectorCtasUnanchoredCleanupOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    format!("run unanchored CTAS root delete: {error}"),
                ),
            }),
        }
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn unsupported(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, message.into())
}
