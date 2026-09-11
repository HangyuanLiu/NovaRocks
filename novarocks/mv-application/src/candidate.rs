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

//! Bounded, item-isolated candidate discovery.

/// Maximum retained diagnostic bytes for one optional candidate.
pub const MAX_CANDIDATE_DIAGNOSTIC_BYTES: usize = 4096;

/// A rejected optional candidate. A rejection never represents failure of a
/// required query input; the caller decides how to surface its diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateDiagnostic {
    identity: String,
    message: String,
}

impl CandidateDiagnostic {
    pub fn new(identity: impl Into<String>, message: impl Into<String>) -> Self {
        let identity = identity.into();
        let mut message = message.into();
        if message.len() > MAX_CANDIDATE_DIAGNOSTIC_BYTES {
            message.truncate(MAX_CANDIDATE_DIAGNOSTIC_BYTES);
        }
        Self { identity, message }
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Result of inspecting an optional candidate inventory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateReadReport<T> {
    accepted: Vec<T>,
    diagnostics: Vec<CandidateDiagnostic>,
}

impl<T> CandidateReadReport<T> {
    pub fn accepted(&self) -> &[T] {
        &self.accepted
    }

    pub fn diagnostics(&self) -> &[CandidateDiagnostic] {
        &self.diagnostics
    }

    pub fn into_accepted(self) -> Vec<T> {
        self.accepted
    }
}

/// Inspect every discovered candidate independently.
///
/// Discovery itself is intentionally outside this helper: an unavailable
/// inventory is one service-level condition, while a malformed or unreadable
/// member is an optional per-candidate rejection. This prevents a single bad
/// MV from discarding healthy candidates through `collect::<Result<_, _>>()`.
pub fn inspect_candidates<I, C, T, E>(
    candidates: I,
    identity: impl Fn(&C) -> String,
    mut inspect: impl FnMut(C) -> Result<T, E>,
) -> CandidateReadReport<T>
where
    I: IntoIterator<Item = C>,
    E: std::fmt::Display,
{
    let mut accepted = Vec::new();
    let mut diagnostics = Vec::new();
    for candidate in candidates {
        let candidate_identity = identity(&candidate);
        match inspect(candidate) {
            Ok(frozen) => accepted.push(frozen),
            Err(error) => diagnostics.push(CandidateDiagnostic::new(
                candidate_identity,
                error.to_string(),
            )),
        }
    }
    CandidateReadReport {
        accepted,
        diagnostics,
    }
}

#[cfg(test)]
mod tests {
    use super::inspect_candidates;

    #[test]
    fn one_bad_candidate_does_not_discard_verified_candidates() {
        let report = inspect_candidates(
            ["orders_mv", "broken_mv", "customers_mv"],
            |name| (*name).to_string(),
            |name| match name {
                "broken_mv" => Err("published target M1 is unreadable"),
                name => Ok(name.to_string()),
            },
        );

        assert_eq!(report.accepted(), ["orders_mv", "customers_mv"]);
        assert_eq!(report.diagnostics().len(), 1);
        assert_eq!(report.diagnostics()[0].identity(), "broken_mv");
    }
}
