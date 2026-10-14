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

//! The session binding captured while upstream establishes its session, and
//! the address and header composition every NovaRocks request derives from it.

/// Header names only the per-role auth owner may set (L5).
///
/// This is a fail-fast guard against overriding the role's authentication,
/// not the rule that decides whether a header is public: publicity is decided
/// by provenance and explicit declaration (L15).
pub(crate) fn is_authentication_header(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    matches!(
        name.as_str(),
        "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
    ) || ["token", "secret", "password", "credential", "access_key"]
        .iter()
        .any(|marker| name.contains(marker))
}

/// Header names the client sets per request (L26); `header.*` never overrides
/// them on a NovaRocks request.
pub(crate) fn is_protocol_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "content-type" | "accept" | "user-agent" | "x-client-version" | "x-iceberg-access-delegation"
    )
}

#[cfg(test)]
mod header_class_tests {
    use super::*;

    #[test]
    fn authentication_headers_are_recognized_case_insensitively() {
        for name in [
            "Authorization",
            "proxy-authorization",
            "Cookie",
            "SET-COOKIE",
            "x-api-token",
            "X-Client-Secret",
            "x-password",
            "x-credential",
            "x-access_key",
        ] {
            assert!(is_authentication_header(name), "{name}");
        }
        for name in ["x-tenant", "polaris-realm", "x-project-id", "x-goog-user-project"] {
            assert!(!is_authentication_header(name), "{name}");
        }
    }

    #[test]
    fn protocol_headers_are_recognized_case_insensitively() {
        for name in [
            "Content-Type",
            "accept",
            "User-Agent",
            "x-client-version",
            "X-Iceberg-Access-Delegation",
        ] {
            assert!(is_protocol_header(name), "{name}");
        }
        assert!(!is_protocol_header("x-tenant"));
    }
}
