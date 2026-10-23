// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! FE schema authors observe the original operation without memory admission.
//!
//! FE has no byte quota, grant, or capacity wait. The shared provider interface
//! observes construction before allocations and bounded work while constructing;
//! here those observations check only the original cancellation and deadline.
//! Immutable returned objects own their backing through ordinary references.
//! BE callers supply their own acquired construction responsibility instead.

use std::alloc::Layout;

use novarocks_connector_contract::PureProviderCompileError;
use novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMetadata, ConnectorOperationControl,
    ConnectorOperationControlView, ConnectorRequestContext, ConnectorTableMetadata,
    ConnectorTableRequest,
};
use novarocks_type_contract::{CompileControlError, ValueTypeStorageLifetime};

pub(crate) fn with_schema_creation<T>(
    context: &ConnectorRequestContext,
    author: impl FnOnce(
        &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<T, PureProviderCompileError<ConnectorError>>,
) -> Result<T, PureProviderCompileError<ConnectorError>> {
    with_original_control(context.original_operation_control(), author)
}

fn with_original_control<T>(
    control: ConnectorOperationControlView,
    author: impl FnOnce(
        &mut ConnectorReadSchemaCreation<'_>,
    ) -> Result<T, PureProviderCompileError<ConnectorError>>,
) -> Result<T, PureProviderCompileError<ConnectorError>> {
    let observe = || {
        control.check_active().map_err(|cause| match cause.kind() {
            ConnectorErrorKind::Cancelled => CompileControlError::Cancelled,
            ConnectorErrorKind::DeadlineExceeded => CompileControlError::DeadlineExceeded,
            // This view exposes only the original stop and deadline. It has
            // no capacity or other failure-producing operation.
            _ => unreachable!("operation control exposed a non-control failure"),
        })
    };
    observe()?;
    let mut construction = |_: Layout| observe();
    let mut work = |_: usize| observe();
    let mut creation = ConnectorReadSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut construction,
        &mut work,
    );
    let result = author(&mut creation);
    // The author's first refusal is final, including its original provider
    // classification and retry evidence. Only success observes the tail.
    let value = result?;
    observe()?;
    Ok(value)
}

/// The FE role's explicit no-quota catalog preparation path.
pub(crate) trait FrontendSchemaLoad {
    fn load_frontend_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError>;
}

impl<T: ConnectorMetadata + ?Sized> FrontendSchemaLoad for T {
    fn load_frontend_table(
        &self,
        request: ConnectorTableRequest,
    ) -> Result<ConnectorTableMetadata, ConnectorError> {
        let control = request.context.original_operation_control();
        with_original_control(control, |creation| {
            ConnectorMetadata::load_table(self, request, creation)
        })
        .map_err(connector_schema_error)
    }
}

pub(crate) fn load_write_table(
    lease: &novarocks_spi::connector::ConnectorWriteLease,
    request: ConnectorTableRequest,
) -> Result<ConnectorTableMetadata, ConnectorError> {
    let control = request.context.original_operation_control();
    with_original_control(control, |creation| lease.load_table(request, creation))
        .map_err(connector_schema_error)
}

/// Project a typed preparation refusal at an existing ConnectorError boundary.
/// Provider classifications and retry evidence survive unchanged.
pub(crate) fn connector_schema_error(
    error: PureProviderCompileError<ConnectorError>,
) -> ConnectorError {
    match error {
        PureProviderCompileError::Provider(error) => error,
        PureProviderCompileError::Control(cause) => ConnectorError::compile_interruption(cause),
    }
}

/// Test provider authors supply complete declarations; source Fields carry
/// only the provider's physical/attribute facts, never semantic defaults.
#[cfg(test)]
pub(crate) fn declared_test_metadata(
    header: novarocks_spi::connector::ConnectorTableMetadataHeader,
    source: arrow::datatypes::SchemaRef,
    bindings: Vec<novarocks_type_contract::FunctionValueType>,
) -> ConnectorTableMetadata {
    let context = crate::connector::application::test_request_context();
    with_schema_creation(&context, |creation| {
        declared_test_metadata_with_creation(header, source, bindings, creation)
    })
    .expect("explicit complete test provider metadata")
}

#[cfg(test)]
pub(crate) fn declared_test_metadata_with_creation(
    header: novarocks_spi::connector::ConnectorTableMetadataHeader,
    source: arrow::datatypes::SchemaRef,
    bindings: Vec<novarocks_type_contract::FunctionValueType>,
    creation: &mut ConnectorReadSchemaCreation<'_>,
) -> Result<ConnectorTableMetadata, PureProviderCompileError<ConnectorError>> {
    assert_eq!(source.fields().len(), bindings.len());
    let schema = creation.arc(arrow::datatypes::Schema::new(
        source
            .fields()
            .iter()
            .zip(&bindings)
            .map(|(field, value)| {
                arrow::datatypes::Field::new(
                    field.name(),
                    value.data_type().clone(),
                    value.nullable(),
                )
            })
            .collect::<Vec<_>>(),
    ))?;
    let storage = creation.storage();
    let execution = {
        let (request, work) = creation.observers();
        novarocks_connector_contract::ConnectorReadExecutionSchema::try_new_observed(
            schema,
            bindings.into_boxed_slice(),
            storage,
            |layout| request(layout).map_err(PureProviderCompileError::Control),
            |units| work(units).map_err(PureProviderCompileError::Control),
        )?
    };
    ConnectorTableMetadata::try_new_typed(header, source, execution, creation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorStopOwner;

    #[test]
    fn original_schema_control_source_keeps_its_author_and_interruption_role() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
        let raw = LayoutResourceError::SourceModel;
        let expected = raw.failure_fact();
        let cause = CompileControlError::from_domain(raw);
        let error = connector_schema_error(PureProviderCompileError::Control(cause.clone()));
        assert_eq!(error.kind(), ConnectorErrorKind::Interrupted);
        assert_eq!(error.failure_fact(), expected);
        assert!(!error.retryable_before_progress());
    }

    #[test]
    fn a_provider_refusal_keeps_its_original_classification_when_stop_arrives_at_return() {
        let stop = ConnectorStopOwner::new();
        let context = crate::connector::connector_request_context(None, stop.view()).unwrap();
        let failure = with_schema_creation::<()>(&context, |_| {
            stop.request_stop();
            Err(PureProviderCompileError::Provider(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "original provider schema refusal",
            )))
        })
        .unwrap_err();
        let failure = connector_schema_error(failure);
        assert_eq!(failure.kind(), ConnectorErrorKind::InvalidRequest);
        assert_eq!(failure.message(), "original provider schema refusal");
    }

    #[test]
    fn successful_preparation_observes_the_original_stop_before_returning() {
        let stop = ConnectorStopOwner::new();
        let context = crate::connector::connector_request_context(None, stop.view()).unwrap();
        let failure = with_schema_creation(&context, |_| {
            stop.request_stop();
            Ok(())
        })
        .unwrap_err();
        assert!(matches!(
            failure,
            PureProviderCompileError::Control(CompileControlError::Cancelled,)
        ));
    }
}
