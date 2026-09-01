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

//! Native write assembly for the Frontend-owned MV refresh lifecycle.
//!
//! The MV application module owns refresh domain facts; this module owns the
//! assembly vocabulary those facts are dispatched through.  Keeping the two
//! apart lets the MV application port stay with the MV domain while the
//! sealed encoding carrier and its provider activation port travel with the
//! rest of query assembly.

use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorRequestContext, ConnectorTableIdentity,
    ConnectorWriteCohortId, ConnectorWriteLease, ConnectorWriteOperationId, ConnectorWriteReceipt,
    MvLakePackageObservation,
};

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::contract::ConnectorWriteOperationRegistration;
use crate::query_execution::mv_assembly::refresh_artifact::{
    MvRefreshCommittedFacts, MvRefreshPublicationIntent,
};
use crate::query_execution::mv_assembly::refresh_handoff::PreparedMvRefreshWrite;
use crate::query_execution::native_fragment::NativeFragmentAttachment;
use crate::query_execution::post_compile::NativeFragmentEncodingInput;
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;

/// Exact Core-retained inputs for one Frontend-owned MV native assembly.
///
/// The frontend may read the immutable input only to encode the native
/// fragment bundle.  Finishing consumes the same retained pair, so neither a
/// newer binding nor a replacement prepared fragment set can reach dispatch.
pub struct PreparedMvNativeWriteAssembly {
    encoding: NativeFragmentEncodingInput,
    query_options: Option<QueryOptions>,
    terminal: MvNativeWriteTerminal,
}

/// The one commit authority a prepared MV write carries.
///
/// A first refresh writes through the NCP-6 write session; an incremental
/// refresh still writes through the staged-report operation. Those are two data
/// planes with two different commit authorities, so the carrier names which one
/// it is instead of letting the terminal guess. Each individual write still has
/// exactly one.
enum MvNativeWriteTerminal {
    /// The staged-report operation carrier of an incremental refresh.
    Operation {
        registration: ConnectorWriteOperationRegistration,
        cohort_id: ConnectorWriteCohortId,
        lease: ConnectorWriteLease,
    },
    /// The write-session carrier of a first refresh. The session sealed the
    /// recipes this plan's writer node carries, so the two travel together.
    Session(std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>),
}

impl PreparedMvNativeWriteAssembly {
    pub(crate) fn operation(
        encoding: NativeFragmentEncodingInput,
        query_options: Option<QueryOptions>,
        registration: ConnectorWriteOperationRegistration,
        cohort_id: ConnectorWriteCohortId,
        lease: ConnectorWriteLease,
    ) -> Self {
        Self {
            encoding,
            query_options,
            terminal: MvNativeWriteTerminal::Operation {
                registration,
                cohort_id,
                lease,
            },
        }
    }

    pub(crate) fn session(
        encoding: NativeFragmentEncodingInput,
        query_options: Option<QueryOptions>,
        write_session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ) -> Self {
        Self {
            encoding,
            query_options,
            terminal: MvNativeWriteTerminal::Session(write_session),
        }
    }

    pub fn native_encoding(&self) -> &NativeFragmentEncodingInput {
        &self.encoding
    }

    /// The staged-report operation identity, present exactly on a write that
    /// still binds one.
    ///
    /// A session-driven write deliberately has none: its branch identity is the
    /// sealed write target ordinal, and no operation or cohort id reaches its
    /// writer data plane.
    pub fn operation_identity(
        &self,
    ) -> Option<(ConnectorWriteOperationId, ConnectorWriteCohortId)> {
        match &self.terminal {
            MvNativeWriteTerminal::Operation {
                registration,
                cohort_id,
                ..
            } => Some((registration.operation_id(), *cohort_id)),
            MvNativeWriteTerminal::Session(_) => None,
        }
    }

    /// The commit authority of a session-driven write, so a caller that fails
    /// between assembly and dispatch can release it rather than leaving the
    /// provider holding a session for a plan that will never run.
    pub(crate) fn write_session(
        &self,
    ) -> Option<&std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>> {
        match &self.terminal {
            MvNativeWriteTerminal::Session(session) => Some(session),
            MvNativeWriteTerminal::Operation { .. } => None,
        }
    }

    pub fn finish(
        self,
        native_bundle: NativeFragmentAttachment,
    ) -> Result<PreparedMvNativeWrite, String> {
        if !self.encoding.matches_native_attachment(&native_bundle) {
            return Err(
                "native fragment bundle does not match the sealed MV encoding input".into(),
            );
        }
        let (_, prepared) = self.encoding.into_parts();
        match self.terminal {
            MvNativeWriteTerminal::Operation {
                registration,
                cohort_id,
                lease,
            } => PreparedDistributedWriteRequest::new(
                prepared,
                native_bundle,
                self.query_options,
                registration,
                cohort_id,
                lease,
            )
            .map(PreparedMvNativeWrite::Operation)
            .map_err(|error| error.to_string()),
            MvNativeWriteTerminal::Session(session) => {
                Ok(PreparedMvNativeWrite::Session(PreparedMvSessionWrite {
                    prepared,
                    native_bundle,
                    query_options: self.query_options,
                    session,
                }))
            }
        }
    }
}

/// One MV write sealed against its native bundle, still naming its own commit
/// authority.
pub enum PreparedMvNativeWrite {
    /// Awaits the staged-report operation session the terminal begins for it.
    Operation(PreparedDistributedWriteRequest),
    /// Already bound to the write session that admitted it.
    Session(PreparedMvSessionWrite),
}

/// A session-driven MV write, one step away from dispatch.
///
/// The session rides along as the request's single commit authority, so no
/// operation, cohort, or attempt identity reaches the writer data plane.
pub struct PreparedMvSessionWrite {
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: NativeFragmentAttachment,
    query_options: Option<QueryOptions>,
    session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl PreparedMvSessionWrite {
    pub(crate) fn into_request(
        self,
        execution: &QueryExecutionContext,
    ) -> Result<crate::query_execution::contract::DistributedQueryRequest, String> {
        let request =
            crate::query_execution::contract::build_distributed_query_request_with_execution(
                self.prepared,
                self.native_bundle,
                self.query_options,
                crate::query_execution::contract::DistributedQueryIntent::Write,
                execution,
            )
            .map_err(|error| error.to_string())?;
        crate::query_execution::contract::with_connector_write_session(request, self.session)
            .map_err(|error| error.to_string())
    }
}

/// Provider activation and native fragment preparation for a SQL-shaped
/// refresh artifact. The frontend owns intent persistence, write-session
/// admission, native assembly, execution, commit, publication, and cleanup;
/// the port returns only an exact sealed encoding carrier after the lease is
/// retained.
pub trait MvRefreshProviderActivation: Send + Sync {
    fn activate_write(
        &self,
        prepared: PreparedMvRefreshWrite,
        planning_lease: &ConnectorControlPlanningLease,
        exact_lease: &ConnectorWriteLease,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedMvNativeWriteAssembly, String>;

    fn interpret_write_commit(
        &self,
        intent: MvRefreshPublicationIntent,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, String>;

    /// Reobserve the complete lake-owned package after a known publication.
    /// The caller supplies the retained exact lease and snapshot identity it
    /// already proved; implementations reject a missing, stale, or advanced
    /// head before returning the package for Accelerator convergence.
    fn observe_published_package(
        &self,
        planning_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableIdentity,
        expected_snapshot_id: i64,
        connector_context: &ConnectorRequestContext,
    ) -> Result<MvLakePackageObservation, String>;
}

/// Composition sink installed before the activation adapter exists. The
/// adapter is bound only after connector control and the engine state are
/// available, avoiding a direct all-in-one call path.
pub trait MvRefreshProviderActivationSink: Send + Sync {
    fn bind_mv_refresh_provider_activation(
        &self,
        activation: std::sync::Arc<dyn MvRefreshProviderActivation>,
    ) -> Result<(), String>;
}
