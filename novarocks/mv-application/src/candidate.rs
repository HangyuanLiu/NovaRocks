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

use novarocks_query_application::api::{ExactObjectBinding, MvCandidateFactInput, MvPublicationId};

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
            let mut boundary = MAX_CANDIDATE_DIAGNOSTIC_BYTES;
            while !message.is_char_boundary(boundary) {
                boundary -= 1;
            }
            message.truncate(boundary);
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

/// Product-owned immutable evidence for one published MV candidate.  A
/// reader may construct it only with exact query bindings that already carry
/// stable semantic revisions; it has no API for substituting a Current read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedCandidatePublication {
    publication_id: MvPublicationId,
    definition_fingerprint: [u8; 32],
    definition_revision: [u8; 32],
    interpretation_revision: [u8; 32],
    definition_provenance: String,
    definition_occurrences: Vec<novarocks_sql::compiler::SqlMvRelationOccurrenceId>,
    inputs: Vec<ExactObjectBinding>,
    output: ExactObjectBinding,
}

impl VerifiedCandidatePublication {
    pub fn try_new(
        publication_id: MvPublicationId,
        definition_fingerprint: [u8; 32],
        definition_revision: [u8; 32],
        interpretation_revision: [u8; 32],
        definition_provenance: impl Into<String>,
        definition_occurrences: Vec<novarocks_sql::compiler::SqlMvRelationOccurrenceId>,
        inputs: Vec<ExactObjectBinding>,
        output: ExactObjectBinding,
    ) -> Option<Self> {
        let definition_provenance = definition_provenance.into();
        MvCandidateFactInput::try_new(
            publication_id,
            definition_fingerprint,
            definition_revision,
            interpretation_revision,
            &definition_provenance,
            &definition_occurrences,
            &inputs,
            &output,
        )?;
        Some(Self {
            publication_id,
            definition_fingerprint,
            definition_revision,
            interpretation_revision,
            definition_provenance,
            definition_occurrences,
            inputs,
            output,
        })
    }

    pub fn as_query_fact(&self) -> MvCandidateFactInput<'_> {
        MvCandidateFactInput::try_new(
            self.publication_id,
            self.definition_fingerprint,
            self.definition_revision,
            self.interpretation_revision,
            &self.definition_provenance,
            &self.definition_occurrences,
            &self.inputs,
            &self.output,
        )
        .expect("verified candidate publication keeps its query fact valid")
    }
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
    use super::{CandidateDiagnostic, MAX_CANDIDATE_DIAGNOSTIC_BYTES, inspect_candidates};

    #[test]
    fn diagnostic_truncation_preserves_utf8_boundary() {
        let diagnostic = CandidateDiagnostic::new("mv", "界".repeat(2_000));

        assert!(diagnostic.message().len() <= MAX_CANDIDATE_DIAGNOSTIC_BYTES);
        assert_eq!(diagnostic.message().len(), 4_095);
        assert!(
            diagnostic
                .message()
                .chars()
                .all(|character| character == '界')
        );
    }

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
