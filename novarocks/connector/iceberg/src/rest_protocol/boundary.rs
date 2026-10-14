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

//! The upstream boundary (L29): every error from an upstream REST call is
//! reduced to its `ErrorKind` and a fixed per-operation text. Upstream may put
//! response bodies, server messages and, with `disable-header-redaction`,
//! header values into its errors; none of that crosses this boundary.

use std::fmt;

use crate::iceberg::{Error, ErrorKind};

/// The private source marking the session probe's vetoed request (L28).
#[derive(Debug)]
struct ProbeVeto;

impl fmt::Display for ProbeVeto {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("NovaRocks session probe request vetoed")
    }
}

impl std::error::Error for ProbeVeto {}

/// The error the auth owner returns for the session probe's request, so the
/// request is never sent.
pub(crate) fn probe_veto() -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "NovaRocks session probe request vetoed",
    )
    .with_source(ProbeVeto)
}

/// Whether `error` is the probe veto, recognized by its private source type.
pub(crate) fn is_probe_veto(error: &Error) -> bool {
    std::error::Error::source(error).is_some_and(|source| source.is::<ProbeVeto>())
}

/// Converts an upstream error into one that carries only its kind and the
/// fixed text `operation` (for example "load table"): no message, context or
/// source of the upstream error survives.
pub(crate) fn sanitize_upstream_error(error: &Error, operation: &'static str) -> Error {
    Error::new(
        error.kind(),
        UpstreamFailure(operation, error.kind()).to_string(),
    )
}

/// The fixed text of a sanitized upstream failure. A not-found kind keeps a
/// fixed statement of absence, which is all the error kind already says.
struct UpstreamFailure(&'static str, ErrorKind);

impl fmt::Display for UpstreamFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let absent = match self.1 {
            ErrorKind::TableNotFound => Some("table"),
            ErrorKind::NamespaceNotFound => Some("namespace"),
            _ => None,
        };
        match absent {
            Some(object) => write!(
                formatter,
                "REST catalog {} failed: the {object} does not exist",
                self.0
            ),
            None => write!(formatter, "REST catalog {} failed", self.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CANARIES: [&str; 4] = [
        "Bearer canary-token",
        "client-secret-canary",
        "server-message-canary",
        "response-body-canary",
    ];

    fn assert_free_of_canaries(error: &Error) {
        let display = error.to_string();
        let debug = format!("{error:?}");
        let alternate = format!("{error:#?}");
        for canary in CANARIES {
            assert!(!display.contains(canary), "{display}");
            assert!(!debug.contains(canary), "{debug}");
            assert!(!alternate.contains(canary), "{alternate}");
        }
    }

    #[test]
    fn sanitized_errors_keep_only_kind_and_fixed_text() {
        let upstream = Error::new(
            ErrorKind::CatalogCommitConflicts,
            "server-message-canary: Bearer canary-token",
        )
        .with_context("response_body", "response-body-canary")
        .with_context("credential", "client-secret-canary")
        .with_source(std::io::Error::other("client-secret-canary"));
        let sanitized = sanitize_upstream_error(&upstream, "commit table");
        assert_eq!(sanitized.kind(), ErrorKind::CatalogCommitConflicts);
        assert_eq!(sanitized.message(), "REST catalog commit table failed");
        assert!(std::error::Error::source(&sanitized).is_none());
        assert_free_of_canaries(&sanitized);
    }

    #[test]
    fn the_probe_veto_is_recognized_only_by_its_source_type() {
        assert!(is_probe_veto(&probe_veto()));
        let same_text = Error::new(
            ErrorKind::Unexpected,
            "NovaRocks session probe request vetoed",
        );
        assert!(!is_probe_veto(&same_text));
        let other_source = Error::new(ErrorKind::Unexpected, "x").with_source(
            std::io::Error::other("NovaRocks session probe request vetoed"),
        );
        assert!(!is_probe_veto(&other_source));
        assert!(!is_probe_veto(&sanitize_upstream_error(
            &probe_veto(),
            "probe"
        )));
    }

    #[test]
    fn a_sanitized_not_found_states_absence_with_fixed_text() {
        let upstream = Error::new(ErrorKind::TableNotFound, "server-message-canary");
        let sanitized = sanitize_upstream_error(&upstream, "table load");
        assert_eq!(sanitized.kind(), ErrorKind::TableNotFound);
        assert_eq!(
            sanitized.message(),
            "REST catalog table load failed: the table does not exist"
        );
        assert_free_of_canaries(&sanitized);
    }
}
