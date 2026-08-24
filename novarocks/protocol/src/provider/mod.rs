//! Validated provider-scoped native carriers.

mod execution_binding;

pub use execution_binding::{
    ConnectorExecutionBindingDeclaration, ConnectorExecutionBindingKey,
    ConnectorExecutionBindingProvider, ConnectorExecutionProviderKind,
    EnsureConnectorExecutionBindingOutcome, EnsureConnectorExecutionBindingRejection,
    EnsureConnectorExecutionBindingRejectionReason, EnsureConnectorExecutionBindingResult,
    RetireConnectorExecutionBindingOutcome, RetireConnectorExecutionBindingResult,
};
