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

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;

use typed_builder::TypedBuilder;

use super::{FormatVersion, ManifestContentType, PartitionSpec, Schema};
use crate::error::Result;
use crate::spec::{PartitionField, SchemaId, SchemaRef};
use crate::{Error, ErrorKind};

/// Meta data of a manifest that is stored in the key-value metadata of the Avro file
#[derive(Debug, PartialEq, Clone, Eq, TypedBuilder)]
pub struct ManifestMetadata {
    /// The table schema at the time the manifest
    /// was written
    pub schema: SchemaRef,
    /// ID of the schema used to write the manifest as a string
    pub schema_id: SchemaId,
    /// The partition spec used to write the manifest
    pub partition_spec: PartitionSpec,
    /// Table format version number of the manifest as a string
    pub format_version: FormatVersion,
    /// Type of content files tracked by the manifest: “data” or “deletes”
    pub content: ManifestContentType,
}

/// JSON nesting admitted for the schema embedded in a manifest. A legitimate
/// 64-level logical type needs about 193 JSON levels (a struct level is an
/// object, its `fields` array and the field object), which serde's default
/// recursion limit of 128 refuses. Decoding without that limit is safe only
/// after a structural scan has bounded the nesting, which keeps the decoder's
/// stack bounded; the table's logical type budget is enforced where the
/// provider admits table metadata, not here.
const MAX_MANIFEST_SCHEMA_JSON_DEPTH: usize = 256;

/// Whether `bytes` never nests JSON objects or arrays deeper than `max`.
/// Brackets inside strings, including escaped quotes, are not structure.
fn json_nesting_within(bytes: &[u8], max: usize) -> bool {
    let mut depth = 0_usize;
    let mut in_string = false;
    let mut escaped = false;
    for byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max {
                    return false;
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
}

fn parse_manifest_schema(bytes: &[u8]) -> Result<Schema> {
    if !json_nesting_within(bytes, MAX_MANIFEST_SCHEMA_JSON_DEPTH) {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "Manifest schema exceeds the JSON nesting depth limit",
        ));
    }
    let invalid = |err: serde_json::Error| {
        Error::new(
            ErrorKind::DataInvalid,
            "Fail to parse schema in manifest metadata",
        )
        .with_source(err)
    };
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let schema = Schema::deserialize(&mut decoder).map_err(invalid)?;
    decoder.end().map_err(invalid)?;
    Ok(schema)
}

impl ManifestMetadata {
    /// Parse from metadata in avro file.
    pub fn parse(meta: &HashMap<String, Vec<u8>>) -> Result<Self> {
        let schema = Arc::new({
            let bs = meta.get("schema").ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "schema is required in manifest metadata but not found",
                )
            })?;
            parse_manifest_schema(bs)?
        });
        let schema_id: i32 = meta
            .get("schema-id")
            .map(|bs| {
                String::from_utf8_lossy(bs).parse().map_err(|err| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Fail to parse schema id in manifest metadata",
                    )
                    .with_source(err)
                })
            })
            .transpose()?
            .unwrap_or(0);
        let partition_spec = {
            let fields = {
                let bs = meta.get("partition-spec").ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "partition-spec is required in manifest metadata but not found",
                    )
                })?;
                serde_json::from_slice::<Vec<PartitionField>>(bs).map_err(|err| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        "Fail to parse partition spec in manifest metadata",
                    )
                    .with_source(err)
                })?
            };
            let spec_id = meta
                .get("partition-spec-id")
                .map(|bs| {
                    String::from_utf8_lossy(bs).parse().map_err(|err| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            "Fail to parse partition spec id in manifest metadata",
                        )
                        .with_source(err)
                    })
                })
                .transpose()?
                .unwrap_or(0);
            PartitionSpec::builder(schema.clone())
                .with_spec_id(spec_id)
                .add_unbound_fields(fields.into_iter().map(|f| f.into_unbound()))?
                .build()?
        };
        let format_version = if let Some(bs) = meta.get("format-version") {
            serde_json::from_slice::<FormatVersion>(bs).map_err(|err| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Fail to parse format version in manifest metadata",
                )
                .with_source(err)
            })?
        } else {
            FormatVersion::V1
        };
        let content = if let Some(v) = meta.get("content") {
            let v = String::from_utf8_lossy(v);
            v.parse()?
        } else {
            ManifestContentType::Data
        };
        Ok(ManifestMetadata {
            schema,
            schema_id,
            partition_spec,
            format_version,
            content,
        })
    }

    /// Get the schema of table at the time manifest was written
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Get the ID of schema used to write the manifest
    pub fn schema_id(&self) -> SchemaId {
        self.schema_id
    }

    /// Get the partition spec used to write manifest
    pub fn partition_spec(&self) -> &PartitionSpec {
        &self.partition_spec
    }

    /// Get the table format version
    pub fn format_version(&self) -> &FormatVersion {
        &self.format_version
    }

    /// Get the type of content files tracked by manifest
    pub fn content(&self) -> &ManifestContentType {
        &self.content
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Iceberg schema JSON with `levels` nested struct containers around an
    /// int leaf: logical depth `levels + 1`.
    fn nested_struct_schema_json(levels: usize) -> String {
        let mut field_type = "\"int\"".to_string();
        for level in (1..=levels).rev() {
            field_type = format!(
                "{{\"type\":\"struct\",\"fields\":[{{\"id\":{},\"name\":\"n{level}\",\"required\":false,\"type\":{field_type}}}]}}",
                level + 1
            );
        }
        format!(
            "{{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{{\"id\":1,\"name\":\"deep\",\"required\":false,\"type\":{field_type}}}]}}"
        )
    }

    #[test]
    fn manifest_schema_at_logical_depth_64_decodes() {
        let json = nested_struct_schema_json(63);
        assert!(serde_json::from_str::<Schema>(&json).is_err(), "default serde limit refuses it");
        let schema = parse_manifest_schema(json.as_bytes()).expect("bounded decode admits depth 64");
        assert!(schema.field_by_name("deep").is_some());
    }

    #[test]
    fn manifest_schema_nesting_beyond_the_bound_is_refused() {
        let json = nested_struct_schema_json(MAX_MANIFEST_SCHEMA_JSON_DEPTH);
        let error = parse_manifest_schema(json.as_bytes()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert!(error.to_string().contains("nesting depth limit"), "{error}");
    }

    #[test]
    fn brackets_inside_strings_are_not_structure() {
        let nested = "[".repeat(MAX_MANIFEST_SCHEMA_JSON_DEPTH + 1);
        let json = format!("{{\"doc\":\"{nested}\\\"{nested}\"}}");
        assert!(json_nesting_within(json.as_bytes(), 1));
        assert!(!json_nesting_within(b"[[[]]]", 2));
        assert!(json_nesting_within(b"[[[]]]", 3));
    }
}
