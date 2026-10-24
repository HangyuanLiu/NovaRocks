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

//! Prepared publication mechanisms have no independently writable verdict.
//! The journal consumes one mechanism, records its evidence synchronously, and
//! only then runs independently bounded finalization.

use crate::catalog::publication::EvidenceContext;
use crate::commit::attempt::observation::DispatchEvidence;
use crate::commit::publication_descriptor::RecoveryBinding;
use async_trait::async_trait;
use novarocks_spi::connector::ExternalMutationFinalization;

/// Preparation has never entered a mutation interval. Preserve its typed
/// refusal independently of an SDK error kind or a later transport fact.
#[derive(Debug)]
pub(crate) struct PreparationFailure(pub novarocks_spi::connector::ConnectorMutationFailure);
pub(crate) type PreparationResult<T> = std::result::Result<T, PreparationFailure>;
impl From<crate::iceberg::Error> for PreparationFailure {
    fn from(error: crate::iceberg::Error) -> Self {
        Self(crate::commit::attempt::before_dispatch_failure(&error))
    }
}
impl From<novarocks_spi::connector::ConnectorError> for PreparationFailure {
    fn from(error: novarocks_spi::connector::ConnectorError) -> Self {
        crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::Unexpected,
            "Publication preparation stopped",
        )
        .with_source(error)
        .into()
    }
}

pub(crate) enum PreparedPublication {
    Mutation {
        binding: Option<RecoveryBinding>,
        dispatch: Box<dyn PreparedCatalogDispatch>,
    },
    NoOp,
}
impl std::fmt::Debug for PreparedPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mutation { binding, .. } => formatter
                .debug_struct("PreparedPublication::Mutation")
                .field("bound", &binding.is_some())
                .finish_non_exhaustive(),
            Self::NoOp => formatter.write_str("PreparedPublication::NoOp"),
        }
    }
}

/// The evidence is complete when the adapter returns. Finalization may not be
/// awaited while deriving it, including cache acquisition or table construction.
pub(crate) struct DispatchCompletion {
    pub evidence: DispatchEvidence,
    pub finalization: Option<Box<dyn FinalizationWork>>,
}
impl DispatchCompletion {
    pub(crate) fn evidence(evidence: DispatchEvidence) -> Self {
        Self {
            evidence,
            finalization: None,
        }
    }
}

#[async_trait]
pub(crate) trait PreparedCatalogDispatch: Send {
    /// Consumes the exact prepared request once. REST session/authentication
    /// and request encoding are complete; the retained filesystem protocol
    /// still loads, validates and encodes its metadata in this interval.
    async fn dispatch_once(self: Box<Self>, evidence: EvidenceContext) -> DispatchCompletion;
}

#[async_trait]
pub(crate) trait FinalizationWork: Send {
    /// The work owns any in-flight I/O until its actual exit. The context bounds
    /// starting/polling work; callers must not time out a join and detach it.
    async fn run(self: Box<Self>, context: EvidenceContext) -> ExternalMutationFinalization;
}

impl EvidenceContext {
    /// Project an explicit phase grant to already admitted storage capability.
    /// This grants no new credential collection, source operation, or admission;
    /// those belong to the statement/attempt that prepared the publication.
    pub(crate) fn for_io(
        self,
        original: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> std::result::Result<
        novarocks_spi::connector::ConnectorRequestContext,
        novarocks_spi::connector::ConnectorError,
    > {
        use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
        if original.io_admission().is_some() {
            return Ok(original
                .for_existing_obligation(self.deadline(), ConnectorStopOwner::new().view())
                .after_external_effect());
        }
        let mut request = ConnectorRequestContext::try_new(
            self.deadline(),
            ConnectorStopOwner::new().view(),
            original.max_handle_payload_bytes(),
            original.max_total_payload_bytes(),
        )?
        .with_initiation(original.initiation())
        .after_external_effect();
        if let Some(resolver) = original.storage_resolver() {
            request = request.with_storage_resolver(std::sync::Arc::clone(resolver));
        }
        Ok(request)
    }
}
