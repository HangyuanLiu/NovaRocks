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

//! The backend-side StarRocks execution binding.
//!
//! A declared StarRocks binding still installs, because installation is what
//! turns a malformed or foreign declaration into a clear error. What it
//! installs can read nothing: both read entry points refuse. See the crate
//! documentation for what the read cut removed.

use std::sync::Arc;
use std::time::Instant;

use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, CatalogProviderKind, CatalogRuntime,
    CatalogRuntimeMaterializer, ConnectorBatchReader, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBinding, ConnectorExecutionBindingKey, ConnectorExecutionDeclaration,
    ConnectorExecutionInstaller, ConnectorExecutionProviderKind, ConnectorOpenReaderRequest,
    ConnectorPrepareSplitRequest, ConnectorPreparedScanUnit, ConnectorPreparedScanUnitSet,
    ConnectorProviderId, ConnectorReadExecution, ConnectorRequestContext, ConnectorSplit,
};

use crate::domain::StarRocksLocalBindingRef;
use crate::{STARROCKS_PROVIDER_ID, starrocks_read_unsupported};

pub struct StarRocksExecutionInstaller {
    provider_id: ConnectorProviderId,
}

/// Startup-composed materializer for the closed StarRocks catalog family.
/// StarRocks read execution remains unsupported, but its catalog lifecycle is
/// still explicit and keyed by the immutable catalog handle.
#[derive(Default)]
pub struct StarRocksCatalogRuntimeMaterializer;

struct StarRocksCatalogRuntime {
    handle: CatalogHandle,
}

impl CatalogRuntime for StarRocksCatalogRuntime {
    fn handle(&self) -> &CatalogHandle {
        &self.handle
    }

    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::StarRocks
    }
}

impl CatalogRuntimeMaterializer for StarRocksCatalogRuntimeMaterializer {
    fn provider_kind(&self) -> CatalogProviderKind {
        CatalogProviderKind::StarRocks
    }

    fn materialize(
        &self,
        properties: &CatalogProperties,
    ) -> Result<Arc<dyn CatalogRuntime>, ConnectorError> {
        if properties.provider_kind() != CatalogProviderKind::StarRocks {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "StarRocks catalog materializer received another provider kind",
            ));
        }
        Ok(Arc::new(StarRocksCatalogRuntime {
            handle: properties.handle().clone(),
        }))
    }
}

impl StarRocksExecutionInstaller {
    pub fn new() -> Self {
        Self {
            provider_id: ConnectorProviderId::parse(STARROCKS_PROVIDER_ID)
                .expect("valid StarRocks provider ID"),
        }
    }

    /// Validates a declaration before anything is installed from it.
    ///
    /// The declared local binding is still parsed even though no reader
    /// consumes it: an ill-formed declaration must fail here rather than
    /// survive as far as the read refusal and look like a supported binding.
    fn prepare(
        declaration: &ConnectorExecutionDeclaration,
    ) -> Result<ConnectorExecutionBindingKey, ConnectorError> {
        let local_binding = declaration.starrocks_local_binding().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "StarRocks installer received a declaration for another provider kind",
            )
        })?;
        StarRocksLocalBindingRef::parse(local_binding)?;
        Ok(declaration.binding_key().clone())
    }
}

impl Default for StarRocksExecutionInstaller {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectorExecutionInstaller for StarRocksExecutionInstaller {
    fn provider_kind(&self) -> ConnectorExecutionProviderKind {
        ConnectorExecutionProviderKind::StarRocks
    }

    fn install(
        &self,
        declaration: &ConnectorExecutionDeclaration,
        context: &ConnectorRequestContext,
    ) -> Result<ConnectorExecutionBinding, ConnectorError> {
        let key = Self::prepare(declaration)?;
        ensure_active(context)?;
        ConnectorExecutionBinding::try_new(
            self.provider_id.clone(),
            key.clone(),
            Arc::new(UnsupportedReadExecution { key }),
        )
    }
}

/// The read capability of an installed StarRocks binding.
///
/// It exists because an execution binding must carry at least one capability
/// and StarRocks has no write capability. Every entry point refuses before it
/// looks at its argument, so no split payload is decoded and no reader is
/// opened on the way to a refusal that is certain from the start.
struct UnsupportedReadExecution {
    key: ConnectorExecutionBindingKey,
}

impl ConnectorReadExecution for UnsupportedReadExecution {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn prepare_split(
        &self,
        _split: &ConnectorSplit,
        _request: ConnectorPrepareSplitRequest,
    ) -> Result<ConnectorPreparedScanUnitSet, ConnectorError> {
        Err(starrocks_read_unsupported())
    }

    fn open_unit_reader(
        &self,
        _unit: &ConnectorPreparedScanUnit,
        _request: ConnectorOpenReaderRequest,
    ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
        Err(starrocks_read_unsupported())
    }
}

fn ensure_active(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "StarRocks connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "StarRocks connector request deadline elapsed",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorBatchBudget, ConnectorCancellation, ConnectorInstanceId,
        ConnectorPreparedScanUnitDescriptor, ConnectorScanUnitDomainFacts,
        ConnectorScanUnitFactsMissingReason,
    };

    use super::*;
    use crate::STARROCKS_READ_UNSUPPORTED;

    struct NeverCancelled;
    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(2),
            Arc::new(NeverCancelled),
            64 * 1024,
            128 * 1024,
        )
        .expect("request context")
    }

    fn installed() -> ConnectorExecutionBinding {
        StarRocksExecutionInstaller::new()
            .install(
                &ConnectorExecutionDeclaration::starrocks("catalog.starrocks", [7; 16], "default")
                    .expect("valid StarRocks declaration"),
                &context(),
            )
            .expect("a well-formed StarRocks declaration installs")
    }

    /// Fabricates the split a refusing entry point is called with. StarRocks
    /// scan planning never produces one, so a test has to mint it through the
    /// generic contract to reach the entry point at all.
    fn split(binding: &ConnectorExecutionBinding) -> ConnectorSplit {
        ConnectorSplit::try_new(
            binding.key().instance_id.clone(),
            "fabricated",
            Bytes::from_static(b"not a StarRocks split"),
            None,
        )
        .expect("fabricated split")
    }

    #[test]
    fn a_declaration_for_another_provider_kind_is_rejected() {
        let error = match StarRocksExecutionInstaller::new().install(
            &ConnectorExecutionDeclaration::iceberg("catalog", [7; 16], "default")
                .expect("valid foreign declaration"),
            &context(),
        ) {
            Ok(_) => panic!("a foreign declaration must not install a StarRocks binding"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("another provider kind"));
    }

    #[test]
    fn split_preparation_refuses_with_the_stable_message_without_decoding_the_split() {
        let installed = installed();
        let read = installed.read().expect("read capability");

        // The payload below is not a StarRocks split. A decode-first
        // implementation would answer CorruptData; the refusal must precede it.
        let error = read
            .prepare_split(
                &split(&installed),
                ConnectorPrepareSplitRequest { context: context() },
            )
            .expect_err("StarRocks must not prepare a split");

        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(error.message(), STARROCKS_READ_UNSUPPORTED);
    }

    #[test]
    fn opening_a_unit_reader_refuses_with_the_stable_message_before_any_reader_is_opened() {
        let installed = installed();
        let read = installed.read().expect("read capability");
        let split = split(&installed);
        // The set is minted here for the same reason as the split: preparation
        // refuses, so no unit of this binding can otherwise exist.
        let prepared = ConnectorPreparedScanUnitSet::try_new(
            installed.key().clone(),
            &split,
            Bytes::new(),
            vec![
                ConnectorPreparedScanUnitDescriptor::try_new(
                    Bytes::from_static(b"not a StarRocks scan unit"),
                    None,
                    ConnectorScanUnitDomainFacts::missing(
                        ConnectorScanUnitFactsMissingReason::NoPinnedStatistics,
                    ),
                )
                .expect("fabricated unit descriptor"),
            ],
            &ConnectorPrepareSplitRequest { context: context() },
        )
        .expect("fabricated unit set");
        let unit = prepared.units().next().expect("one fabricated unit");

        let error = match read.open_unit_reader(
            &unit,
            ConnectorOpenReaderRequest {
                expected_schema: Arc::new(Schema::new(vec![Field::new(
                    "id",
                    DataType::Int64,
                    false,
                )])),
                batch: ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(32).expect("batch rows"),
                    max_bytes: NonZeroUsize::new(4096).expect("batch bytes"),
                },
                options: Default::default(),
                context: context(),
            },
        ) {
            Ok(_) => panic!("StarRocks must not open a reader"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(error.message(), STARROCKS_READ_UNSUPPORTED);
    }

    #[test]
    fn an_installed_binding_owns_its_declared_generation() {
        let installed = installed();

        assert_eq!(
            installed.key().instance_id,
            ConnectorInstanceId::parse("catalog.starrocks").expect("instance ID")
        );
        assert_eq!(
            installed.read().expect("read capability").binding_key(),
            installed.key()
        );
        assert!(installed.write().is_none());
    }
}
