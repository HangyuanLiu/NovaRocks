// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

pub(crate) const DOCUMENT_ENVELOPE_VERSION: u16 = 1;
pub(crate) const DOCUMENT_MANIFEST_VERSION: u16 = 1;
pub(crate) const DOCUMENT_MANIFEST_PROPERTY: &str = "novarocks.documents.v1";
pub(crate) const DOCUMENT_SIDECAR_DIRECTORY: &str = "metadata/novarocks-documents/v1";
pub(crate) const MAX_DOCUMENT_MANIFEST_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergDocumentManifestV1 {
    pub(crate) version: u16,
    pub(crate) documents: Vec<IcebergDocumentEnvelopeV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergDocumentEnvelopeV1 {
    pub(crate) version: u16,
    pub(crate) owner: String,
    pub(crate) name: String,
    pub(crate) format_owner: String,
    pub(crate) format_name: String,
    pub(crate) format_version: u32,
    pub(crate) revision: [u8; 32],
    pub(crate) encoded_len: u64,
    pub(crate) references: Vec<IcebergDocumentReferenceV1>,
    pub(crate) attachment: IcebergDocumentAttachmentV1,
    pub(crate) carrier: IcebergDocumentCarrierV1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IcebergDocumentReferenceV1 {
    pub(crate) relationship: String,
    pub(crate) owner: String,
    pub(crate) name: String,
    pub(crate) revision: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum IcebergDocumentAttachmentV1 {
    TableMetadata,
    ExactOutput {
        committed_version: Vec<u8>,
        snapshot_id: Option<i64>,
    },
    CommitOutput,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum IcebergDocumentCarrierV1 {
    Available { content: Vec<u8> },
    Deferred { location: String },
}
