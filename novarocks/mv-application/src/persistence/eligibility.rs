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

//! Exact, lake-authoritative maintenance eligibility. Configuration, process
//! incarnation and a new physical metadata version cannot discharge this fact.

use super::codec::{
    EncodedDocument, PersistenceCodecError, encode_message, ensure_canonical, require_version,
    required, wire,
};
use super::generated as proto;
use super::identity::{
    ComputationIdentity, DocumentRevision, NativeDataVersion, ObjectIdentity, PublicationIdentity,
};
use super::validation::{PersistenceDecodeBudget, ValidationError};
use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
use prost::Message;

pub const MAX_ELIGIBILITY_SAMPLES: usize = 16;
pub const MAX_ELIGIBILITY_SAMPLE_BYTES: usize = 256;

/// Stable identity of the NativeResultContentV1 contract, independent of SQL
/// value equality, a physical snapshot or Arrow storage representation.
pub fn native_result_content_v1_identity() -> DocumentRevision {
    DocumentRevision::from_canonical_bytes(
        novarocks_type_contract::NATIVE_RESULT_CONTENT_V1_CANONICAL_BYTES,
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EligibilityBinding {
    pub object_id: ObjectIdentity,
    pub publication_id: PublicationIdentity,
    pub publication_revision: DocumentRevision,
    pub computation_identity: ComputationIdentity,
    pub content_contract_identity: DocumentRevision,
    pub generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EligibilityEvidence {
    pub requested: u64,
    pub matched: u64,
    /// Bounded content samples for diagnostics; never an application key.
    pub samples: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EligibilityState {
    Eligible,
    ValidationPending {
        attempt: QueryExecutionId,
        target_snapshot: NativeDataVersion,
    },
    Invalid {
        evidence: EligibilityEvidence,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EligibilityDocument {
    pub binding: EligibilityBinding,
    pub state: EligibilityState,
}

impl EligibilityDocument {
    pub fn is_eligible_for(&self, binding: &EligibilityBinding) -> bool {
        self.validate().is_ok()
            && &self.binding == binding
            && self.state == EligibilityState::Eligible
    }

    pub fn validate(&self) -> Result<(), PersistenceCodecError> {
        if self.binding.generation == 0
            || self.binding.content_contract_identity != native_result_content_v1_identity()
        {
            return Err(invalid(
                "eligibility binding requires a nonzero generation and NativeResultContentV1",
            ));
        }
        if let EligibilityState::Invalid { evidence } = &self.state {
            if evidence.matched >= evidence.requested {
                return Err(invalid(
                    "invalid eligibility evidence must report a positive retraction shortage",
                ));
            }
            if evidence.samples.len() > MAX_ELIGIBILITY_SAMPLES {
                return Err(PersistenceCodecError::ResourceBudget {
                    resource: "eligibility samples",
                    maximum: MAX_ELIGIBILITY_SAMPLES,
                    actual: evidence.samples.len(),
                });
            }
            for sample in &evidence.samples {
                if sample.len() > MAX_ELIGIBILITY_SAMPLE_BYTES {
                    return Err(PersistenceCodecError::ResourceBudget {
                        resource: "eligibility sample bytes",
                        maximum: MAX_ELIGIBILITY_SAMPLE_BYTES,
                        actual: sample.len(),
                    });
                }
            }
        }
        Ok(())
    }
}

pub fn encode_eligibility(
    document: &EligibilityDocument,
) -> Result<EncodedDocument, PersistenceCodecError> {
    document.validate()?;
    encode_message(to_proto(document))
}

pub fn decode_eligibility(
    bytes: &[u8],
    budget: PersistenceDecodeBudget,
) -> Result<EligibilityDocument, PersistenceCodecError> {
    wire::preflight(bytes, wire::Schema::EligibilityDocument, budget)?;
    let dto =
        proto::EligibilityDocument::decode(bytes).map_err(PersistenceCodecError::ProtobufDecode)?;
    require_version("eligibility", dto.format_version)?;
    let state = match required(dto.state, "eligibility.state")? {
        1 => {
            if dto.attempt.is_some()
                || dto.target_snapshot.is_some()
                || dto.requested.is_some()
                || dto.matched.is_some()
                || !dto.samples.is_empty()
            {
                return Err(invalid(
                    "Eligible must not contain pending or invalid facts",
                ));
            }
            EligibilityState::Eligible
        }
        2 => {
            if dto.requested.is_some() || dto.matched.is_some() || !dto.samples.is_empty() {
                return Err(invalid(
                    "ValidationPending must not contain invalid evidence",
                ));
            }
            let raw: [u8; 24] = required(dto.attempt, "eligibility.attempt")?
                .try_into()
                .map_err(|_| invalid("pending attempt must have exactly 24 bytes"))?;
            let query = QueryId::new(
                i64::from_be_bytes(raw[0..8].try_into().unwrap()),
                i64::from_be_bytes(raw[8..16].try_into().unwrap()),
            );
            let attempt = AttemptId::new(u64::from_be_bytes(raw[16..24].try_into().unwrap()))
                .map_err(|_| invalid("pending attempt ordinal must be nonzero"))?;
            EligibilityState::ValidationPending {
                attempt: QueryExecutionId::new(query, attempt)
                    .map_err(|_| invalid("pending query identity must be nonzero"))?,
                target_snapshot: NativeDataVersion::try_new(required(
                    dto.target_snapshot,
                    "eligibility.target_snapshot",
                )?)?,
            }
        }
        3 => {
            if dto.attempt.is_some() || dto.target_snapshot.is_some() {
                return Err(invalid("Invalid must not contain pending facts"));
            }
            EligibilityState::Invalid {
                evidence: EligibilityEvidence {
                    requested: required(dto.requested, "eligibility.requested")?,
                    matched: required(dto.matched, "eligibility.matched")?,
                    samples: dto.samples,
                },
            }
        }
        value => {
            return Err(PersistenceCodecError::UnknownEnum {
                field: "eligibility.state",
                value,
            });
        }
    };
    let document = EligibilityDocument {
        binding: EligibilityBinding {
            object_id: ObjectIdentity::try_new(required(dto.object_id, "eligibility.object_id")?)?,
            publication_id: PublicationIdentity::try_new(required(
                dto.publication_id,
                "eligibility.publication_id",
            )?)?,
            publication_revision: DocumentRevision::try_from_bytes(&required(
                dto.publication_revision,
                "eligibility.publication_revision",
            )?)?,
            computation_identity: ComputationIdentity::try_from_bytes(&required(
                dto.computation_identity,
                "eligibility.computation_identity",
            )?)?,
            content_contract_identity: DocumentRevision::try_from_bytes(&required(
                dto.content_contract_identity,
                "eligibility.content_contract_identity",
            )?)?,
            generation: required(dto.generation, "eligibility.generation")?,
        },
        state,
    };
    document.validate()?;
    ensure_canonical(bytes, to_proto(&document))?;
    Ok(document)
}

fn to_proto(document: &EligibilityDocument) -> proto::EligibilityDocument {
    let mut dto = proto::EligibilityDocument {
        format_version: Some(1),
        object_id: Some(document.binding.object_id.as_bytes().to_vec()),
        publication_id: Some(document.binding.publication_id.as_bytes().to_vec()),
        publication_revision: Some(document.binding.publication_revision.as_bytes().to_vec()),
        computation_identity: Some(document.binding.computation_identity.as_bytes().to_vec()),
        content_contract_identity: Some(
            document
                .binding
                .content_contract_identity
                .as_bytes()
                .to_vec(),
        ),
        generation: Some(document.binding.generation),
        state: None,
        attempt: None,
        target_snapshot: None,
        requested: None,
        matched: None,
        samples: Vec::new(),
    };
    match &document.state {
        EligibilityState::Eligible => dto.state = Some(1),
        EligibilityState::ValidationPending {
            attempt,
            target_snapshot,
        } => {
            dto.state = Some(2);
            let mut bytes = Vec::with_capacity(24);
            bytes.extend_from_slice(&attempt.query_id().high().to_be_bytes());
            bytes.extend_from_slice(&attempt.query_id().low().to_be_bytes());
            bytes.extend_from_slice(&attempt.attempt_id().get().to_be_bytes());
            dto.attempt = Some(bytes);
            dto.target_snapshot = Some(target_snapshot.as_bytes().to_vec());
        }
        EligibilityState::Invalid { evidence } => {
            dto.state = Some(3);
            dto.requested = Some(evidence.requested);
            dto.matched = Some(evidence.matched);
            dto.samples = evidence.samples.clone();
        }
    }
    dto
}

fn invalid(message: &str) -> PersistenceCodecError {
    PersistenceCodecError::InvalidDocument(ValidationError::new("eligibility", message))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> EligibilityDocument {
        EligibilityDocument {
            binding: EligibilityBinding {
                object_id: ObjectIdentity::try_new(vec![1]).unwrap(),
                publication_id: PublicationIdentity::try_new(vec![2]).unwrap(),
                publication_revision: DocumentRevision::from_canonical_bytes(b"P"),
                computation_identity: ComputationIdentity::from_canonical_bytes(b"D"),
                content_contract_identity: native_result_content_v1_identity(),
                generation: 1,
            },
            state: EligibilityState::Eligible,
        }
    }
    #[test]
    fn eligibility_roundtrips_all_states_and_binds_exact_basis() {
        let mut doc = fixture();
        for state in [
            EligibilityState::Eligible,
            EligibilityState::ValidationPending {
                attempt: QueryExecutionId::new(QueryId::new(-1, 3), AttemptId::new(1).unwrap())
                    .unwrap(),
                target_snapshot: NativeDataVersion::try_new(vec![7]).unwrap(),
            },
            EligibilityState::Invalid {
                evidence: EligibilityEvidence {
                    requested: 5,
                    matched: 3,
                    samples: vec![vec![9]],
                },
            },
        ] {
            doc.state = state;
            let encoded = encode_eligibility(&doc).unwrap();
            assert_eq!(
                decode_eligibility(encoded.as_bytes(), PersistenceDecodeBudget::default()).unwrap(),
                doc
            );
        }
        let mut other = doc.binding.clone();
        other.generation += 1;
        assert!(!doc.is_eligible_for(&other));
    }
    #[test]
    fn eligibility_enforces_bounded_evidence_and_strict_wire() {
        let mut doc = fixture();
        doc.state = EligibilityState::Invalid {
            evidence: EligibilityEvidence {
                requested: 2,
                matched: 1,
                samples: vec![vec![1]; MAX_ELIGIBILITY_SAMPLES + 1],
            },
        };
        assert!(matches!(
            encode_eligibility(&doc),
            Err(PersistenceCodecError::ResourceBudget { .. })
        ));
        doc.state = EligibilityState::Eligible;
        let mut bytes = encode_eligibility(&doc).unwrap().into_bytes();
        bytes.extend_from_slice(&[112, 1]);
        assert!(matches!(
            decode_eligibility(&bytes, PersistenceDecodeBudget::default()),
            Err(PersistenceCodecError::MalformedWire(_))
        ));
        let mut dto = to_proto(&doc);
        dto.requested = Some(1);
        assert!(
            decode_eligibility(&dto.encode_to_vec(), PersistenceDecodeBudget::default()).is_err()
        );
    }
    #[test]
    fn eligibility_rejects_unknown_content_generation_and_oversized_samples() {
        let mut doc = fixture();
        doc.binding.generation = 0;
        assert!(encode_eligibility(&doc).is_err());
        doc.binding.generation = 1;
        doc.binding.content_contract_identity = DocumentRevision::from_canonical_bytes(b"unknown");
        assert!(encode_eligibility(&doc).is_err());
        doc = fixture();
        doc.state = EligibilityState::Invalid {
            evidence: EligibilityEvidence {
                requested: 2,
                matched: 1,
                samples: vec![vec![0; MAX_ELIGIBILITY_SAMPLE_BYTES + 1]],
            },
        };
        assert!(matches!(
            encode_eligibility(&doc),
            Err(PersistenceCodecError::ResourceBudget { .. })
        ));
        doc.state = EligibilityState::Invalid {
            evidence: EligibilityEvidence {
                requested: 1,
                matched: 1,
                samples: Vec::new(),
            },
        };
        assert!(encode_eligibility(&doc).is_err());
        let mut dto = to_proto(&fixture());
        dto.state = Some(2);
        assert!(
            decode_eligibility(&dto.encode_to_vec(), PersistenceDecodeBudget::default()).is_err()
        );
    }
}
