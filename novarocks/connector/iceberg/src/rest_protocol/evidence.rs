
//! Dispatch observations and the D13 evidence classification.
//!
//! The observation types below are shared by the HTTP layer, which records
//! where one send reached, and by the pure classifier, which turns an
//! observation plus a decoded minimal view into evidence. Observation never
//! interprets status codes; classification never performs I/O.

use reqwest::header::HeaderName;

/// The establishment step that failed while acquiring the session binding.
///
/// It belongs to exactly one establishment attempt: every participant of that
/// attempt observes the same step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BindingFailure {
    /// The token endpoint answered with a 4xx status.
    TokenRejected,
    /// The token exchange failed in any other way.
    TokenUnavailable,
    /// `/v1/config` failed, or upstream never reached `catalog_session`.
    ConfigFetch,
    /// The merged configuration carried server-sent authentication material
    /// or authentication-class headers.
    InvalidServerConfig,
}

/// Why a request was never written to the wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NotWrittenCause {
    /// No session binding could be acquired.
    Binding(BindingFailure),
    /// The request URL could not be formed.
    Url,
    /// The request body could not be serialized.
    Serialize,
    /// The HTTP request could not be built.
    Build,
    /// The client failed while connecting (DNS, TCP, TLS, proxy tunnel,
    /// connect timeout).
    Connect,
}

/// Why a written request produced no status line.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NoStatusCause {
    /// The read timeout elapsed before a status line arrived.
    ReadTimeout,
    /// The connection closed before a status line arrived.
    Closed,
    /// Any other post-write transport failure.
    Other,
}

/// Where one send of one request reached.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DispatchStage {
    NotWritten(NotWrittenCause),
    WrittenNoStatus(NoStatusCause),
    /// A status line arrived. Only header names are recorded: header values
    /// may carry credentials.
    Status {
        code: u16,
        header_names: Vec<HeaderName>,
    },
}
