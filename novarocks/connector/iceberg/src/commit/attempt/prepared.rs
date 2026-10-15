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

//! Publication preparation has no dispatch state. The journal consumes the
//! resulting mechanism and is the sole production owner of its verdict.

use super::*;
use crate::catalog::publication::CatalogEvidencePolicy;
use crate::catalog::publication_dispatch::{
    PreparationFailure, PreparationResult, PreparedPublication,
};
use crate::commit::publication_descriptor::PublicationDescriptor;
use novarocks_spi::connector::{
    ConnectorOperationControl, ConnectorRequestContext, ProviderBindingEpoch,
};
use std::sync::Arc;

#[async_trait]
pub(crate) trait PublicationPreparer: Send + Sync + 'static {
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase>;
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()>;
    fn evidence_policy(&self) -> CatalogEvidencePolicy;
    async fn prepare_dispatch(
        self: Arc<Self>,
        request: FrozenRequest,
        descriptor: PublicationDescriptor,
        generation: ProviderBindingEpoch,
        preparation: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication>;
}

pub(crate) struct CatalogPublisher {
    pub catalog_operation: crate::catalog::admission::CatalogOperation,
    pub catalog: Arc<dyn crate::catalog::NovaRocksCatalog>,
    pub ident: crate::iceberg::TableIdent,
    pub target_ref: String,
    pub operation: super::super::model::OperationToken,
    pub recovery_preflight: RecoveryPreflight,
}
impl CatalogPublisher {
    fn admit(&self) -> PreparationResult<()> {
        self.catalog
            .admit_operation(
                &self.catalog_operation,
                &crate::catalog::CatalogTableName::from_identifier(&self.ident).into(),
            )
            .map_err(|error| {
                PreparationFailure(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unsupported,
                    error.message(),
                ))
            })
    }
}
#[async_trait]
impl PublicationPreparer for CatalogPublisher {
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
        attempt.check_active()?;
        self.admit()
            .map_err(|failure| Error::new(ErrorKind::FeatureUnsupported, failure.0.message()))?;
        let loaded = self
            .catalog
            .load_commit_base(
                crate::catalog::CatalogTableName::from_identifier(&self.ident),
                attempt.file_io().clone(),
            )
            .await;
        attempt.check_active()?;
        let base = loaded?;
        Ok(StagingBase::Existing {
            metadata: base.metadata,
            metadata_location: base.metadata_location,
        })
    }
    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()> {
        if operation.token() != self.operation
            || request.artifacts().attempt().operation() != self.operation
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Frozen publication differs from its admitted operation",
            ));
        }
        (self.recovery_preflight)(request, operation)
    }
    fn evidence_policy(&self) -> CatalogEvidencePolicy {
        self.catalog.evidence_policy()
    }
    async fn prepare_dispatch(
        self: Arc<Self>,
        request: FrozenRequest,
        descriptor: PublicationDescriptor,
        generation: ProviderBindingEpoch,
        preparation: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        preparation.check_active()?;
        self.admit()?;
        if request.identifier() != &self.ident
            || request.target_ref() != self.target_ref
            || request.artifacts().attempt().operation() != self.operation
            || descriptor.operation_id != self.operation.to_bytes()
        {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Frozen publication differs from its exact admitted operation or target",
            )
            .into());
        }
        descriptor.validate_frozen(&request)?;
        self.catalog
            .prepare_frozen_publication(
                request,
                self.catalog_operation,
                None,
                descriptor,
                generation,
                preparation,
            )
            .await
    }
}
