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

//! Allocation-free schema shape preflight before SDK serde builds indexes.

use novarocks_types::logical_type::LogicalTypeLimits;

const MAX_JSON_DEPTH: usize = 256;
const MAX_JSON_VALUES: usize = 131_072;

#[derive(Clone, Copy)]
enum Role {
    ProviderEnvelope,
    ProviderTableValue,
    ProviderTableInfo,
    ProviderSchema,
    ProviderSchemaRoot,
    ProviderFields(usize),
    ProviderField(usize),
    RestTable,
    Metadata,
    Schemas,
    OptionalSchema,
    Root,
    Type(usize),
    Field(usize),
    Fields(usize),
    Name,
    Opaque,
}

struct Scan<'a> {
    bytes: &'a [u8],
    offset: usize,
    nodes: usize,
    names: usize,
    values: usize,
    json_depth_limit: usize,
    json_value_limit: Option<usize>,
    limits: LogicalTypeLimits,
}

pub(crate) fn preflight_schema(json: &str) -> Result<(), String> {
    preflight(json, Role::Root, false)
}

pub(crate) fn preflight_type(json: &str) -> Result<(), String> {
    preflight(json, Role::Type(1), true)
}

/// Check each retained schema independently before the SDK constructs its
/// field indexes. Metadata history is not one logical schema budget.
pub(crate) fn preflight_table_metadata(json: &str) -> Result<(), String> {
    preflight(json, Role::Metadata, false)
}

/// REST load/create/register/commit responses carry one `metadata` envelope.
/// This callback runs before SDK response materialization, not after it.
pub(crate) fn preflight_rest_table_response(bytes: &[u8]) -> Result<(), String> {
    let json = std::str::from_utf8(bytes)
        .map_err(|_| "Iceberg table response preflight: invalid UTF-8".to_string())?;
    preflight(json, Role::RestTable, false)
}

/// Frozen provider handles carry a field-ID/name tree independently of the
/// serialized SDK metadata. Check that tree before serde allocates its nodes.
/// Ordinary envelope values retain serde's existing recursion boundary.
pub(crate) fn decode_provider_payload<T: for<'de> serde::Deserialize<'de>>(
    bytes: &[u8],
) -> Result<T, String> {
    let json = std::str::from_utf8(bytes)
        .map_err(|_| "Iceberg provider payload preflight: invalid UTF-8".to_string())?;
    preflight(json, Role::ProviderEnvelope, false)?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let payload = T::deserialize(&mut decoder).map_err(|error| error.to_string())?;
    decoder.end().map_err(|error| error.to_string())?;
    Ok(payload)
}

pub(crate) fn decode_table_metadata(
    json: &str,
) -> Result<crate::iceberg::spec::TableMetadata, String> {
    use serde::Deserialize;
    preflight_table_metadata(json)?;
    let mut decoder = serde_json::Deserializer::from_str(json);
    decoder.disable_recursion_limit();
    let metadata = crate::iceberg::spec::TableMetadata::deserialize(&mut decoder)
        .map_err(|error| format!("decode Iceberg table metadata: {error}"))?;
    decoder
        .end()
        .map_err(|error| format!("decode Iceberg table metadata: {error}"))?;
    Ok(metadata)
}

pub(crate) fn decode_table_metadata_bytes(
    bytes: &[u8],
) -> Result<crate::iceberg::spec::TableMetadata, String> {
    let json = std::str::from_utf8(bytes)
        .map_err(|error| format!("decode Iceberg table metadata UTF-8: {error}"))?;
    decode_table_metadata(json)
}

pub(crate) async fn read_table_metadata(
    file_io: &crate::iceberg::io::FileIO,
    location: impl AsRef<str>,
) -> crate::iceberg::Result<crate::iceberg::spec::TableMetadata> {
    crate::iceberg::spec::TableMetadata::read_from_with_decoder(
        file_io,
        location,
        decode_sdk_table_metadata,
    )
    .await
}

pub(crate) fn decode_sdk_table_metadata(
    bytes: &[u8],
) -> crate::iceberg::Result<crate::iceberg::spec::TableMetadata> {
    decode_table_metadata_bytes(bytes).map_err(|message| {
        crate::iceberg::Error::new(crate::iceberg::ErrorKind::DataInvalid, message)
    })
}

fn preflight(json: &str, root: Role, count_root: bool) -> Result<(), String> {
    let mut scan = Scan {
        bytes: json.as_bytes(),
        offset: 0,
        nodes: 0,
        names: 0,
        values: 0,
        json_depth_limit: if matches!(
            root,
            Role::Metadata | Role::RestTable | Role::ProviderEnvelope
        ) {
            127
        } else {
            MAX_JSON_DEPTH
        },
        json_value_limit: if matches!(
            root,
            Role::Metadata | Role::RestTable | Role::ProviderEnvelope
        ) {
            None
        } else {
            Some(MAX_JSON_VALUES)
        },
        limits: LogicalTypeLimits::default(),
    };
    if count_root {
        scan.node(1, 0)?;
    }
    scan.value(root, 0)?;
    scan.space();
    if scan.offset != scan.bytes.len() {
        return Err("Iceberg schema preflight: trailing JSON".into());
    }
    Ok(())
}

impl<'a> Scan<'a> {
    // Inspect the discriminator without materializing values or depending on
    // object member order. The semantic pass below remains the JSON authority.
    fn metadata_has_v1_schema(&self) -> Result<bool, String> {
        let mut scan = Scan {
            offset: self.offset,
            ..*self
        };
        scan.expect(b'{')?;
        let mut legacy = false;
        loop {
            scan.space();
            if scan.bytes.get(scan.offset) == Some(&b'}') {
                return Ok(legacy);
            }
            let (key, _, _) = scan.string()?;
            scan.expect(b':')?;
            scan.space();
            let start = scan.offset;
            scan.skip_value()?;
            if metadata_key_eq(key, b"format-version") {
                legacy = &scan.bytes[start..scan.offset] == b"1";
            }
            scan.space();
            match scan.bytes.get(scan.offset) {
                Some(b',') => scan.offset += 1,
                Some(b'}') => return Ok(legacy),
                _ => return Err("Iceberg metadata preflight: invalid object separator".into()),
            }
        }
    }

    // A lexical walk needs only a depth counter, even for a schema that the
    // next pass will reject. It cannot construct SDK vectors or field indexes.
    fn skip_value(&mut self) -> Result<(), String> {
        match self.bytes.get(self.offset) {
            Some(b'"') => {
                self.string()?;
            }
            Some(b'{' | b'[') => {
                let mut depth = 0usize;
                loop {
                    match self.bytes.get(self.offset) {
                        Some(b'"') => {
                            self.string()?;
                            continue;
                        }
                        Some(b'{' | b'[') => depth += 1,
                        Some(b'}' | b']') => depth -= 1,
                        Some(_) => (),
                        None => return Err("Iceberg metadata preflight: truncated value".into()),
                    }
                    self.offset += 1;
                    if depth == 0 {
                        break;
                    }
                }
            }
            _ => {
                let start = self.offset;
                while self
                    .bytes
                    .get(self.offset)
                    .is_some_and(|b| !b.is_ascii_whitespace() && !b",]}".contains(b))
                {
                    self.offset += 1;
                }
                if start == self.offset {
                    return Err("Iceberg metadata preflight: missing value".into());
                }
            }
        }
        Ok(())
    }

    fn space(&mut self) {
        while self
            .bytes
            .get(self.offset)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.offset += 1;
        }
    }
    fn expect(&mut self, byte: u8) -> Result<(), String> {
        self.space();
        if self.bytes.get(self.offset) != Some(&byte) {
            return Err("Iceberg schema preflight: invalid JSON shape".into());
        }
        self.offset += 1;
        Ok(())
    }
    fn node(&mut self, depth: usize, name_bytes: usize) -> Result<(), String> {
        self.nodes += 1;
        self.names = self
            .names
            .checked_add(name_bytes)
            .ok_or("Iceberg schema preflight: name overflow")?;
        if depth > self.limits.max_depth
            || self.nodes > self.limits.max_nodes
            || self.names > self.limits.max_text_bytes
        {
            return Err(
                "Iceberg schema preflight: semantic budget exceeded before SDK decode".into(),
            );
        }
        Ok(())
    }
    fn hex(&mut self) -> Result<u32, String> {
        let mut value = 0;
        for _ in 0..4 {
            let byte = *self
                .bytes
                .get(self.offset)
                .ok_or("Iceberg schema preflight: truncated escape")?;
            self.offset += 1;
            value = value * 16
                + match byte {
                    b'0'..=b'9' => u32::from(byte - b'0'),
                    b'a'..=b'f' => u32::from(byte - b'a' + 10),
                    b'A'..=b'F' => u32::from(byte - b'A' + 10),
                    _ => return Err("Iceberg schema preflight: invalid escape".into()),
                };
        }
        Ok(value)
    }
    // Return a borrowed raw key plus its decoded UTF-8 byte count. Escaped
    // schema grammar keys are noncanonical; opaque defaults may contain them.
    fn string(&mut self) -> Result<(&'a [u8], bool, usize), String> {
        self.expect(b'"')?;
        let start = self.offset;
        let mut escaped = false;
        let mut decoded = 0;
        loop {
            let byte = *self
                .bytes
                .get(self.offset)
                .ok_or("Iceberg schema preflight: unterminated string")?;
            if byte == b'"' {
                let end = self.offset;
                self.offset += 1;
                return Ok((&self.bytes[start..end], escaped, decoded));
            }
            self.offset += 1;
            match byte {
                0..=31 => return Err("Iceberg schema preflight: invalid string byte".into()),
                b'\\' => {
                    escaped = true;
                    let escape = *self
                        .bytes
                        .get(self.offset)
                        .ok_or("Iceberg schema preflight: truncated escape")?;
                    self.offset += 1;
                    decoded += match escape {
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => 1,
                        b'u' => {
                            let mut scalar = self.hex()?;
                            if (0xd800..=0xdbff).contains(&scalar) {
                                if self.bytes.get(self.offset..self.offset + 2) != Some(b"\\u") {
                                    return Err(
                                        "Iceberg schema preflight: missing low surrogate".into()
                                    );
                                }
                                self.offset += 2;
                                let low = self.hex()?;
                                if !(0xdc00..=0xdfff).contains(&low) {
                                    return Err(
                                        "Iceberg schema preflight: invalid low surrogate".into()
                                    );
                                }
                                scalar = 0x10000 + ((scalar - 0xd800) << 10) + low - 0xdc00;
                            }
                            char::from_u32(scalar)
                                .ok_or("Iceberg schema preflight: invalid Unicode scalar")?
                                .len_utf8()
                        }
                        _ => return Err("Iceberg schema preflight: invalid escape".into()),
                    };
                }
                _ => decoded += 1,
            }
        }
    }
    fn value(&mut self, role: Role, json_depth: usize) -> Result<(), String> {
        // The envelope may contain optional V1 `schema`/`schemas`. Every
        // non-null schema restarts the existing independent shape budgets.
        if matches!(role, Role::OptionalSchema | Role::ProviderSchema) {
            self.space();
            if self.bytes.get(self.offset) != Some(&b'n') {
                let saved = (
                    self.nodes,
                    self.names,
                    self.values,
                    self.json_depth_limit,
                    self.json_value_limit,
                );
                self.nodes = 0;
                self.names = 0;
                self.values = 0;
                self.json_depth_limit = MAX_JSON_DEPTH;
                self.json_value_limit = Some(MAX_JSON_VALUES);
                let root = if matches!(role, Role::ProviderSchema) {
                    Role::ProviderSchemaRoot
                } else {
                    Role::Root
                };
                let result = self.value(root, 0);
                (
                    self.nodes,
                    self.names,
                    self.values,
                    self.json_depth_limit,
                    self.json_value_limit,
                ) = saved;
                return result;
            }
        }
        if self.json_value_limit.is_some() {
            self.values += 1;
        }
        if json_depth > self.json_depth_limit
            || self
                .json_value_limit
                .is_some_and(|limit| self.values > limit)
        {
            return Err("Iceberg schema preflight: independent JSON budget exceeded".into());
        }
        if let Role::Field(depth) | Role::ProviderField(depth) = role {
            self.node(depth, 0)?;
        }
        self.space();
        let byte = *self
            .bytes
            .get(self.offset)
            .ok_or("Iceberg schema preflight: missing value")?;
        let role = match (role, byte) {
            (Role::ProviderTableValue, b'{') => Role::ProviderEnvelope,
            (Role::ProviderTableValue, b'"') | (Role::ProviderTableInfo, b'n') => Role::Opaque,
            (Role::ProviderTableValue, _) => {
                return Err("Iceberg provider payload preflight: invalid table shape".into());
            }
            _ => role,
        };
        if self.json_value_limit.is_none()
            && matches!(byte, b'{' | b'[')
            && json_depth >= self.json_depth_limit
        {
            return Err(
                "Iceberg metadata preflight: ordinary JSON recursion limit exceeded".into(),
            );
        }
        if matches!(
            role,
            Role::RestTable
                | Role::Metadata
                | Role::Root
                | Role::Field(_)
                | Role::ProviderSchemaRoot
                | Role::ProviderField(_)
                | Role::ProviderEnvelope
                | Role::ProviderTableInfo
        ) && byte != b'{'
            || matches!(role, Role::Fields(_) | Role::ProviderFields(_)) && byte != b'['
            || matches!(role, Role::Schemas) && byte != b'[' && byte != b'n'
        {
            return Err("Iceberg schema preflight: invalid semantic shape".into());
        }
        match byte {
            b'{' => {
                let legacy_schema =
                    matches!(role, Role::Metadata) && self.metadata_has_v1_schema()?;
                self.offset += 1;
                self.space();
                if self.bytes.get(self.offset) == Some(&b'}') {
                    self.offset += 1;
                    return Ok(());
                }
                loop {
                    let (key, escaped, _) = self.string()?;
                    if escaped
                        && !matches!(
                            role,
                            Role::Opaque
                                | Role::Metadata
                                | Role::RestTable
                                | Role::ProviderEnvelope
                                | Role::ProviderTableInfo
                                | Role::ProviderSchemaRoot
                                | Role::ProviderField(_)
                        )
                    {
                        return Err("Iceberg schema preflight: noncanonical grammar key".into());
                    }
                    // Keys borrow the input, so no field vector or SDK index
                    // exists while semantic child budgets are being checked.
                    let next = match (role, key) {
                        (Role::ProviderEnvelope, _) if metadata_key_eq(key, b"table_info") => {
                            Role::ProviderTableInfo
                        }
                        (Role::ProviderEnvelope, _) if metadata_key_eq(key, b"table") => {
                            Role::ProviderTableValue
                        }
                        (Role::ProviderTableInfo, _) if metadata_key_eq(key, b"schema") => {
                            Role::ProviderSchema
                        }
                        (Role::ProviderSchemaRoot, _) if metadata_key_eq(key, b"fields") => {
                            Role::ProviderFields(1)
                        }
                        (Role::ProviderField(depth), _) if metadata_key_eq(key, b"children") => {
                            Role::ProviderFields(depth + 1)
                        }
                        (Role::ProviderField(_), _) if metadata_key_eq(key, b"name") => Role::Name,
                        (Role::RestTable, _) if metadata_key_eq(key, b"metadata") => Role::Metadata,
                        (Role::Metadata, _) if legacy_schema && metadata_key_eq(key, b"schema") => {
                            Role::OptionalSchema
                        }
                        (Role::Metadata, _) if metadata_key_eq(key, b"schemas") => Role::Schemas,
                        (Role::Root, b"fields") => Role::Fields(1),
                        (Role::Type(depth), b"fields") => Role::Fields(depth + 1),
                        (Role::Field(depth), b"type") => Role::Type(depth),
                        (Role::Field(_), b"name") => Role::Name,
                        (Role::Type(depth), b"element") => {
                            self.node(depth + 1, 7)?;
                            Role::Type(depth + 1)
                        }
                        (Role::Type(depth), b"key") => {
                            self.node(depth + 1, 3)?;
                            Role::Type(depth + 1)
                        }
                        (Role::Type(depth), b"value") => {
                            self.node(depth + 1, 5)?;
                            Role::Type(depth + 1)
                        }
                        _ => Role::Opaque,
                    };
                    self.expect(b':')?;
                    self.value(next, json_depth + 1)?;
                    self.space();
                    match self.bytes.get(self.offset) {
                        Some(b',') => self.offset += 1,
                        Some(b'}') => {
                            self.offset += 1;
                            break;
                        }
                        _ => {
                            return Err("Iceberg schema preflight: invalid object separator".into());
                        }
                    }
                }
            }
            b'[' => {
                self.offset += 1;
                self.space();
                if self.bytes.get(self.offset) == Some(&b']') {
                    self.offset += 1;
                    return Ok(());
                }
                loop {
                    let next = match role {
                        Role::Schemas => Role::OptionalSchema,
                        Role::Fields(depth) => Role::Field(depth),
                        Role::ProviderFields(depth) => Role::ProviderField(depth),
                        _ => Role::Opaque,
                    };
                    self.value(next, json_depth + 1)?;
                    self.space();
                    match self.bytes.get(self.offset) {
                        Some(b',') => self.offset += 1,
                        Some(b']') => {
                            self.offset += 1;
                            break;
                        }
                        _ => return Err("Iceberg schema preflight: invalid array separator".into()),
                    }
                }
            }
            b'"' => {
                let (_, _, length) = self.string()?;
                if matches!(role, Role::Name) {
                    self.names = self
                        .names
                        .checked_add(length)
                        .ok_or("Iceberg schema preflight: name overflow")?;
                    if self.names > self.limits.max_text_bytes {
                        return Err("Iceberg schema preflight: semantic name budget exceeded before SDK decode".into());
                    }
                }
            }
            _ => {
                if matches!(role, Role::Name) {
                    return Err("Iceberg schema preflight: invalid name".into());
                }
                let start = self.offset;
                while self
                    .bytes
                    .get(self.offset)
                    .is_some_and(|b| !b.is_ascii_whitespace() && !b",]}".contains(b))
                {
                    self.offset += 1;
                }
                if start == self.offset {
                    return Err("Iceberg schema preflight: invalid primitive".into());
                }
                // This scalar-only parse has no recursive allocation. SDK serde
                // remains the authority for primitive type names and values.
                serde_json::from_slice::<serde::de::IgnoredAny>(&self.bytes[start..self.offset])
                    .map_err(|_| "Iceberg schema preflight: invalid primitive".to_string())?;
            }
        }
        Ok(())
    }
}

// The JSON envelope accepts escaped member names just as the SDK does. Only
// two ASCII names matter here; no decoded key allocation is needed.
fn metadata_key_eq(raw: &[u8], expected: &[u8]) -> bool {
    let mut offset = 0;
    for &byte in expected {
        let actual = match raw.get(offset) {
            Some(b'\\') if raw.get(offset + 1) == Some(&b'u') => {
                let Some(digits) = raw.get(offset + 2..offset + 6) else {
                    return false;
                };
                let mut scalar = 0u32;
                for digit in digits {
                    let Some(hex) = (*digit as char).to_digit(16) else {
                        return false;
                    };
                    scalar = scalar * 16 + hex;
                }
                offset += 6;
                scalar
            }
            Some(&plain) => {
                offset += 1;
                u32::from(plain)
            }
            None => return false,
        };
        if actual != u32::from(byte) {
            return false;
        }
    }
    offset == raw.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider_info(depth: usize) -> String {
        let mut field = r#"{"field_id":64,"name":"leaf","initial_default_json":null,"write_default_json":null,"children":[]}"#.to_string();
        for id in (1..depth).rev() {
            field = format!(
                r#"{{"field_id":{id},"name":"n{id}","initial_default_json":null,"write_default_json":null,"children":[{field}]}}"#
            );
        }
        format!(
            r#"{{"catalog":"c","namespace":"n","table":"t","table_uuid":null,"current_snapshot_id":null,"schema_id":0,"location":"s3://bucket/table","schema":{{"fields":[{field}]}},"serialized_metadata":null,"serialized_metadata_rows":null}}"#
        )
    }

    #[test]
    fn provider_payload_preflight_accepts_exact_depth64_table_and_scan_trees() {
        #[derive(serde::Deserialize)]
        struct Table {
            table_info: crate::scan_model::IcebergTableInfo,
        }
        #[derive(serde::Deserialize)]
        struct Scan {
            table: Table,
        }
        let table = format!(r#"{{"table":"t","table_info":{}}}"#, provider_info(64));
        assert!(serde_json::from_str::<Table>(&table).is_err());
        let decoded: Table = decode_provider_payload(table.as_bytes()).unwrap();
        assert_eq!(decoded.table_info.schema.fields[0].field_id, 1);
        let scan = format!(r#"{{"table":{table}}}"#);
        let decoded: Scan = decode_provider_payload(scan.as_bytes()).unwrap();
        assert_eq!(decoded.table.table_info.schema.fields[0].name, "n1");
        for payload in [
            format!(r#"{{"table_info":{}}}"#, provider_info(65)),
            format!(r#"{{"table":{{"table_info":{}}}}}"#, provider_info(65)),
        ] {
            assert!(
                decode_provider_payload::<serde_json::Value>(payload.as_bytes())
                    .unwrap_err()
                    .contains("semantic budget")
            );
        }
    }

    #[test]
    fn provider_payload_preflight_bounds_fields_and_preserves_opaque_recursion() {
        let leaf = r#"{"field_id":1,"name":"n","children":[]}"#;
        for (count, accepted) in [(4096, true), (4097, false)] {
            let fields = vec![leaf; count].join(",");
            let payload = format!(r#"{{"table_info":{{"schema":{{"fields":[{fields}]}}}}}}"#);
            assert_eq!(
                preflight(&payload, Role::ProviderEnvelope, false).is_ok(),
                accepted
            );
        }
        for (depth, accepted) in [(126, true), (127, false)] {
            let opaque = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
            let payload = format!(r#"{{"ignored":{opaque}}}"#);
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&payload).is_ok(),
                accepted
            );
            assert_eq!(
                decode_provider_payload::<serde_json::Value>(payload.as_bytes()).is_ok(),
                accepted
            );
        }
        let escaped = format!(r#"{{"table_\u0069nfo":{}}}"#, provider_info(64));
        assert!(decode_provider_payload::<serde_json::Value>(escaped.as_bytes()).is_ok());
        let mut trailing = escaped.into_bytes();
        trailing.extend_from_slice(b" false");
        assert!(decode_provider_payload::<serde_json::Value>(&trailing).is_err());
        assert!(decode_provider_payload::<serde_json::Value>(b"\xff").is_err());
    }

    #[test]
    fn provider_payload_preflight_rejects_sequence_form_budget_bypasses() {
        let field = r#"{"field_id":1,"name":"n","initial_default_json":null,"write_default_json":null,"children":[]}"#;
        let fields = vec![field; 4097].join(",");
        let info =
            format!(r#"["c","n","t",null,null,0,"s3://b/t",{{"fields":[{fields}]}},null,null]"#);
        // Derived serde permits positional structs, but our frozen producer
        // emits named objects. Positional data must not evade shape budgets.
        assert!(serde_json::from_str::<crate::scan_model::IcebergTableInfo>(&info).is_ok());
        for payload in [
            format!(r#"{{"table_info":{info}}}"#),
            format!(r#"{{"table":{{"table_info":{info}}}}}"#),
            format!(r#"[{info}]"#),
            format!(r#"{{"table":[{info}]}}"#),
            r#"{"table_info":{"schema":[[]]}}"#.to_string(),
            r#"{"table_info":{"schema":{"fields":[[1,"n",null,null,[]]]}}}"#.to_string(),
        ] {
            assert!(decode_provider_payload::<serde_json::Value>(payload.as_bytes()).is_err());
        }
        assert!(decode_provider_payload::<serde_json::Value>(b"{\"table_info\":null}").is_ok());
    }

    fn table_metadata_json(depth: usize) -> String {
        table_metadata_json_version(depth, crate::iceberg::spec::FormatVersion::V2)
    }

    fn table_metadata_json_version(
        depth: usize,
        version: crate::iceberg::spec::FormatVersion,
    ) -> String {
        use crate::iceberg::spec::{
            NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, StructType,
            TableMetadataBuilder, Type,
        };
        use std::sync::Arc;
        let mut field = NestedField::optional(
            depth as i32,
            format!("n{depth}"),
            Type::Primitive(PrimitiveType::Int),
        );
        for id in (1..depth).rev() {
            field = NestedField::optional(
                id as i32,
                format!("n{id}"),
                Type::Struct(StructType::new(vec![Arc::new(field)])),
            );
        }
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "s3://warehouse/deep".into(),
            version,
            std::collections::HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        serde_json::to_string(&metadata).unwrap()
    }

    #[test]
    fn full_table_metadata_preflight_accepts_64_before_sdk_and_rejects_65() {
        let exact = table_metadata_json(64);
        assert!(serde_json::from_str::<crate::iceberg::spec::TableMetadata>(&exact).is_err());
        let decoded = decode_table_metadata(&exact).unwrap();
        assert_eq!(decoded.current_schema().highest_field_id(), 64);
        assert!(
            decode_table_metadata(&table_metadata_json(65))
                .unwrap_err()
                .contains("semantic budget")
        );
        assert!(decode_table_metadata(&(exact.clone() + " null")).is_err());
        assert!(decode_table_metadata_bytes(&[0xff]).is_err());
    }

    #[test]
    fn full_table_metadata_preflight_checks_every_schema_with_independent_budgets() {
        let fields = (1..=3000)
            .map(|id| {
                format!("{{\"id\":{id},\"name\":\"n{id}\",\"required\":false,\"type\":\"int\"}}")
            })
            .collect::<Vec<_>>()
            .join(",");
        let schema = format!("{{\"type\":\"struct\",\"fields\":[{fields}]}}");
        assert!(
            preflight_table_metadata(&format!(
                "{{\"schemas\":[{schema},{schema}],\"schema\":null}}"
            ))
            .is_ok()
        );
        let invalid = format!(
            "{{\"type\":\"struct\",\"fields\":[{{\"id\":1,\"name\":\"n\",\"required\":false,\"type\":{}}}]}}",
            nested_type(65)
        );
        assert!(
            preflight_table_metadata(&format!("{{\"schemas\":[{schema},{invalid}]}}")).is_err()
        );
        assert!(
            preflight_table_metadata(&format!(
                "{{\"schema\":{invalid},\"schemas\":[{schema}],\"format-version\":1}}"
            ))
            .is_err()
        );
        assert!(preflight_table_metadata("{\"schema\":null,\"schemas\":null}").is_ok());
    }

    #[test]
    fn full_table_metadata_preflight_preserves_versioned_legacy_schema_semantics() {
        use crate::iceberg::spec::FormatVersion;
        let v1 = table_metadata_json_version(64, FormatVersion::V1);
        assert_eq!(
            decode_table_metadata(&v1)
                .unwrap()
                .current_schema()
                .highest_field_id(),
            64
        );
        assert!(
            decode_table_metadata(&table_metadata_json_version(65, FormatVersion::V1)).is_err()
        );

        let v2 = table_metadata_json(1);
        for extension in ["0", "[]", "\"ignored\""] {
            let extended = format!("{{\"schema\":{extension},{}", &v2[1..]);
            assert!(serde_json::from_str::<crate::iceberg::spec::TableMetadata>(&extended).is_ok());
            assert!(decode_table_metadata(&extended).is_ok());
        }
        let mut unknown = "0".to_owned();
        for _ in 0..65 {
            unknown = format!("{{\"unknown\":{unknown}}}");
        }
        let extended = format!("{{\"schema\":{unknown},{}", &v2[1..]);
        assert!(serde_json::from_str::<crate::iceberg::spec::TableMetadata>(&extended).is_ok());
        assert!(decode_table_metadata(&extended).is_ok());
        let invalid_v1 = format!(
            "{{\"schema\":0,{}",
            &table_metadata_json_version(1, FormatVersion::V1)[1..]
        );
        assert!(decode_table_metadata(&invalid_v1).is_err());
    }

    #[test]
    fn full_table_metadata_preflight_recognizes_escaped_envelope_keys() {
        let json = table_metadata_json(64).replacen("\"schemas\"", "\"\\u0073chemas\"", 1);
        assert!(decode_table_metadata(&json).is_ok());
        let invalid = table_metadata_json(65).replacen("\"schemas\"", "\"schem\\u0061s\"", 1);
        assert!(
            preflight_table_metadata(&invalid)
                .unwrap_err()
                .contains("semantic budget")
        );
    }

    #[test]
    fn full_table_metadata_preflight_covers_rest_response_before_sdk() {
        let metadata = table_metadata_json(64);
        let response = format!(
            "{{\"metadata\":{metadata},\"metadata-location\":\"s3://warehouse/metadata.json\",\"config\":{{}}}}"
        );
        assert!(preflight_rest_table_response(response.as_bytes()).is_ok());
        assert!(serde_json::from_str::<serde_json::Value>(&response).is_err());
        let mut decoder = serde_json::Deserializer::from_str(&response);
        decoder.disable_recursion_limit();
        let decoded: serde_json::Value = serde::Deserialize::deserialize(&mut decoder).unwrap();
        let table: crate::iceberg::spec::TableMetadata =
            serde_json::from_value(decoded["metadata"].clone()).unwrap();
        assert_eq!(table.current_schema().highest_field_id(), 64);
        assert!(
            preflight_rest_table_response(
                response
                    .replacen("\"metadata\"", "\"\\u006detadata\"", 1)
                    .as_bytes()
            )
            .is_ok()
        );
        let over = format!("{{\"metadata\":{}}}", table_metadata_json(65));
        assert!(
            preflight_rest_table_response(over.as_bytes())
                .unwrap_err()
                .contains("semantic budget")
        );
        let unrelated = format!(
            "{{\"config\":{}0{},\"metadata\":{metadata}}}",
            "[".repeat(127),
            "]".repeat(127)
        );
        assert!(preflight_rest_table_response(unrelated.as_bytes()).is_err());
    }

    #[test]
    fn full_table_metadata_preflight_preserves_ordinary_json_recursion_boundary() {
        for depth in [125, 126, 127, 128] {
            let unknown = format!(
                "{{\"unknown\":{}0{}}}",
                "[".repeat(depth),
                "]".repeat(depth)
            );
            assert_eq!(
                preflight_table_metadata(&unknown).is_ok(),
                serde_json::from_str::<serde_json::Value>(&unknown).is_ok(),
                "ordinary envelope depth {depth}"
            );
        }
        let large = format!("{{\"unknown\":[{}]}}", "0,".repeat(MAX_JSON_VALUES) + "0");
        assert!(
            preflight_table_metadata(&large).is_ok(),
            "metadata-wide value cap must not replace per-schema limits"
        );
    }

    #[tokio::test]
    async fn full_table_metadata_preflight_file_reads_preserve_sdk_compression() {
        use crate::iceberg::io::FileIO;
        use std::io::Write;
        let io = FileIO::new_with_memory();
        for compressed in [false, true] {
            for depth in [64, 65] {
                let json = table_metadata_json(depth);
                let bytes = if compressed {
                    let mut encoder =
                        flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                    encoder.write_all(json.as_bytes()).unwrap();
                    encoder.finish().unwrap()
                } else {
                    json.into_bytes()
                };
                let path = format!("/schema-{depth}-{compressed}.metadata.json");
                io.new_output(&path)
                    .unwrap()
                    .write(bytes.into())
                    .await
                    .unwrap();
                let result = read_table_metadata(&io, &path).await;
                if depth == 64 {
                    assert_eq!(result.unwrap().current_schema().highest_field_id(), 64);
                } else {
                    assert!(result.unwrap_err().to_string().contains("semantic budget"));
                }
            }
        }
    }

    fn nested_type(depth: usize) -> String {
        let mut ty = "\"int\"".to_string();
        for _ in 1..depth {
            ty = format!(
                "{{\"type\":\"list\",\"element-id\":1,\"element-required\":false,\"element\":{ty}}}"
            );
        }
        ty
    }

    #[test]
    fn borrowed_type_preflight_counts_root_and_collection_children_before_sdk_decode() {
        let limits = LogicalTypeLimits::default();
        assert!(preflight_type(&nested_type(limits.max_depth)).is_ok());
        assert!(preflight_type(&nested_type(limits.max_depth + 1)).is_err());
        let fields = (1..limits.max_nodes)
            .map(|id| {
                format!("{{\"id\":{id},\"name\":\"n{id}\",\"required\":false,\"type\":\"int\"}}")
            })
            .collect::<Vec<_>>()
            .join(",");
        let exact = format!("{{\"type\":\"struct\",\"fields\":[{fields}]}}");
        assert!(preflight_type(&exact).is_ok());
        let over = exact.replacen(
            "]}",
            ", {\"id\":99999,\"name\":\"extra\",\"required\":false,\"type\":\"int\"}]}",
            1,
        );
        assert!(preflight_type(&over).is_err());
    }
}
