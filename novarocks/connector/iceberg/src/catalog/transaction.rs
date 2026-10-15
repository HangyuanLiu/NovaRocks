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

//! Immutable proof facts shared by prepared adapters and the sole journal.
use novarocks_spi::connector::ExternalMutationEffect;
use std::sync::Arc;

/// Proof that a specific publication is present in the catalog.
///
/// One flat shape, with the fields a publication may or may not produce left
/// optional. Different operations genuinely observe different things -- a
/// snapshot append lands on a snapshot id, a conditional metadata create lands
/// on a metadata file whose digest is re-read afterwards -- and this records
/// whichever of them the publisher saw.
///
/// It is deliberately not a variant per operation. A variant would put the
/// question "which kind of catalog published this?" back in front of every
/// caller, which is the thing this owner exists to remove; a caller reads the
/// fields it needs and ignores the rest.
///
/// There is deliberately no `Default`: the effect is the one field a proof can
/// never be vague about, so every proof states it.
/// A response clue for locating the real committed predecessor. It never
/// grants deletion authority; reclamation validates the metadata chain itself.
#[derive(Clone, Default, Eq, PartialEq)]
pub(crate) enum MetadataPredecessorObservation {
    #[default]
    NotReported,
    Candidate {
        location: String,
        timestamp_ms: i64,
    },
    Unusable,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CommitProof {
    /// Snapshot the publication landed on, when the operation produces one.
    pub(crate) snapshot_id: Option<i64>,
    /// Table UUID observed at proof time.
    pub(crate) table_uuid: Option<Arc<str>>,
    /// Metadata file the target now resolves to, when the operation publishes
    /// one directly.
    pub(crate) metadata_location: Option<Arc<str>>,
    /// Digest of the exact metadata bytes acknowledged by the storage request
    /// or positively read back. This fact does not itself decide the effect.
    pub(crate) metadata_digest: Option<Arc<str>>,
    pub(crate) metadata_predecessor: MetadataPredecessorObservation,
    /// Whether the catalog state changed or the request was already satisfied.
    pub(crate) effect: ExternalMutationEffect,
}

impl std::fmt::Debug for MetadataPredecessorObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotReported => f.write_str("NotReported"),
            Self::Candidate { timestamp_ms, .. } => f
                .debug_struct("Candidate")
                .field("timestamp_ms", timestamp_ms)
                .finish_non_exhaustive(),
            Self::Unusable => f.write_str("Unusable"),
        }
    }
}
impl std::fmt::Debug for CommitProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitProof")
            .field("snapshot_id", &self.snapshot_id)
            .field("table_uuid", &self.table_uuid)
            .field(
                "metadata_location_present",
                &self.metadata_location.is_some(),
            )
            .field("metadata_digest_present", &self.metadata_digest.is_some())
            .field("metadata_predecessor", &self.metadata_predecessor)
            .field("effect", &self.effect)
            .finish()
    }
}

impl CommitProof {
    pub(crate) fn new(effect: ExternalMutationEffect) -> Self {
        Self {
            snapshot_id: None,
            table_uuid: None,
            metadata_location: None,
            metadata_digest: None,
            metadata_predecessor: MetadataPredecessorObservation::NotReported,
            effect,
        }
    }

    pub(crate) fn applied(snapshot_id: Option<i64>) -> Self {
        Self {
            snapshot_id,
            ..Self::new(ExternalMutationEffect::Applied)
        }
    }

    pub(crate) fn no_op() -> Self {
        Self::new(ExternalMutationEffect::NoOp)
    }

    pub(crate) fn with_table_uuid(mut self, uuid: impl Into<Arc<str>>) -> Self {
        self.table_uuid = Some(uuid.into());
        self
    }

    pub(crate) fn with_metadata(
        mut self,
        location: impl Into<Arc<str>>,
        digest: impl Into<Arc<str>>,
    ) -> Self {
        self.metadata_location = Some(location.into());
        self.metadata_digest = Some(digest.into());
        self
    }
}
