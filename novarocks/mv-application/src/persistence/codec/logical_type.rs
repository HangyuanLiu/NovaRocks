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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The MV-owned canonical type payload. No provider identity lives in this codec.

use arrow_schema::TimeUnit;
use bytes::Bytes;
use novarocks_type_contract::{LogicalField, LogicalType, LogicalTypeLimits, LogicalValue};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::hash::{Hash, Hasher};

const PREFIX: &str = "mvtype:1:";

/// A semantic type with its closed historical leaf spelling, when decoded from
/// an already published document. Encoding provenance is not semantic equality.
#[derive(Clone)]
pub struct MvLogicalType {
    data_type: LogicalType,
    legacy: Option<LegacyLeaf>,
    provider_binding: Option<Bytes>,
}

#[derive(Clone, Copy, Debug)]
enum LegacyLeaf {
    Boolean,
    Bool,
    TinyInt,
    SmallInt,
    Int,
    Integer,
    Long,
    BigInt,
    Float,
    Double,
    Date,
    Timestamp,
    String,
    Varchar,
    Char,
    Binary,
    Varbinary,
    Decimal(u8, i8),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeCodecError {
    RetiredComplex,
    Invalid(String),
}
impl fmt::Display for TypeCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RetiredComplex => f.write_str("legacy complex MV type lacks its complete child contract; DROP and recreate the materialized view"),
            Self::Invalid(message) => write!(f, "invalid MV logical type: {message}"),
        }
    }
}
impl std::error::Error for TypeCodecError {}
fn invalid(message: impl Into<String>) -> TypeCodecError {
    TypeCodecError::Invalid(message.into())
}

impl fmt::Debug for MvLogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MvLogicalType")
            .field("data_type", &self.data_type)
            .field("provider_bound", &self.provider_binding.is_some())
            .finish()
    }
}
impl PartialEq for MvLogicalType {
    fn eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type
    }
}
impl Eq for MvLogicalType {}
impl Hash for MvLogicalType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data_type.hash(state);
    }
}
impl fmt::Display for MvLogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut text = String::new();
        encode_type(&self.data_type, &mut text);
        f.write_str(&text)
    }
}
impl Serialize for MvLogicalType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.encode_signature())
    }
}
impl<'de> Deserialize<'de> for MvLogicalType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        Self::decode_signature(&text).map_err(serde::de::Error::custom)
    }
}
impl MvLogicalType {
    pub fn logical_type(&self) -> &LogicalType {
        &self.data_type
    }
    pub fn from_logical_type(data_type: LogicalType) -> Result<Self, TypeCodecError> {
        data_type
            .validate(LogicalTypeLimits::default())
            .map_err(invalid)?;
        // Only actual provider leaf spellings are chosen for new documents.
        let legacy = match data_type {
            LogicalType::Boolean => Some(LegacyLeaf::Boolean),
            LogicalType::Int32 => Some(LegacyLeaf::Int),
            LogicalType::Int64 => Some(LegacyLeaf::Long),
            LogicalType::Float32 => Some(LegacyLeaf::Float),
            LogicalType::Float64 => Some(LegacyLeaf::Double),
            LogicalType::Date32 => Some(LegacyLeaf::Date),
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            } => Some(LegacyLeaf::Timestamp),
            LogicalType::Utf8 => Some(LegacyLeaf::String),
            LogicalType::Binary => Some(LegacyLeaf::Binary),
            LogicalType::Decimal {
                bits: 128,
                precision,
                scale,
            } if scale >= 0 => Some(LegacyLeaf::Decimal(precision, scale)),
            _ => None,
        };
        let result = Self {
            data_type,
            legacy,
            provider_binding: None,
        };
        if result.encoded_len() > LogicalTypeLimits::default().max_text_bytes {
            return Err(invalid("canonical type payload exceeds its byte budget"));
        }
        Ok(result)
    }
    /// Freeze provider-owned schema identity in the sole persisted type payload.
    pub fn from_schema_type(
        data_type: LogicalType,
        binding: Bytes,
    ) -> Result<Self, TypeCodecError> {
        let mut result = Self::from_logical_type(data_type)?;
        if binding.is_empty() || binding.len() > LogicalTypeLimits::default().max_text_bytes / 2 {
            return Err(invalid("missing or over-budget provider type binding"));
        }
        result.legacy = None;
        result.provider_binding = Some(binding);
        if result.encoded_len() > LogicalTypeLimits::default().max_text_bytes {
            return Err(invalid("bound type payload exceeds its byte budget"));
        }
        Ok(result)
    }
    pub(crate) fn legacy_scalar_type(&self) -> Option<&LogicalType> {
        self.legacy.map(|_| &self.data_type)
    }
    pub fn provider_type_binding(&self) -> Option<&Bytes> {
        self.provider_binding.as_ref()
    }
    /// Semantic equality alone never proves provider schema continuity.
    pub fn matches_schema(
        &self,
        actual: &LogicalType,
        binding: &[u8],
        legacy_scalar_type: Option<&LogicalType>,
    ) -> bool {
        &self.data_type == actual
            && match &self.provider_binding {
                Some(expected) => expected.as_ref() == binding,
                None => self.legacy.is_some() && legacy_scalar_type == Some(&self.data_type),
            }
    }
    pub fn validate_schema_type(&self) -> Result<(), TypeCodecError> {
        self.validate()?;
        if self.provider_binding.is_none() && self.legacy.is_none() {
            return Err(invalid("schema type has no exact provider binding"));
        }
        Ok(())
    }
    pub fn decode_signature(text: &str) -> Result<Self, TypeCodecError> {
        Self::decode_with_limits(text, LogicalTypeLimits::default())
    }
    pub fn decode_with_limits(
        text: &str,
        limits: LogicalTypeLimits,
    ) -> Result<Self, TypeCodecError> {
        preflight_signature(text, limits)?;
        if let Some(body) = text.strip_prefix(PREFIX) {
            let mut parser = Parser::new(body, limits);
            let (data_type, binding) = parser.payload(true)?;
            let data_type = data_type.expect("construction pass");
            parser.finish()?;
            data_type.validate(limits).map_err(invalid)?;
            if binding.is_none() && Self::from_logical_type(data_type.clone())?.legacy.is_some() {
                return Err(invalid(
                    "versioned scalar must use its canonical legacy leaf encoding",
                ));
            }
            return Ok(Self {
                data_type,
                legacy: None,
                provider_binding: binding
                    .map(|hex| Bytes::from(hex::decode(hex).expect("preflight lowercase hex"))),
            });
        }
        let (data_type, legacy) = legacy_leaf(text)?;
        data_type.validate(limits).map_err(invalid)?;
        Ok(Self {
            data_type,
            legacy: Some(legacy),
            provider_binding: None,
        })
    }
    pub fn encode_signature(&self) -> String {
        if let Some(leaf) = self.legacy {
            return leaf.encode();
        }
        let mut text = String::from(PREFIX);
        if let Some(binding) = &self.provider_binding {
            text.push_str("bound(");
            encode_type(&self.data_type, &mut text);
            text.push_str(&format!(",{}:{}", binding.len(), hex::encode(binding)));
            text.push(')');
        } else {
            encode_type(&self.data_type, &mut text);
        }
        text
    }
    pub fn encoded_len(&self) -> usize {
        fn digits(mut n: u32) -> usize {
            let mut width = 1;
            while n >= 10 {
                n /= 10;
                width += 1;
            }
            width
        }
        fn text_len(text: &str) -> usize {
            digits(text.len() as u32) + 1 + text.len()
        }
        fn value_len(value: &LogicalValue) -> usize {
            2 + type_len(&value.data_type)
        }
        fn type_len(ty: &LogicalType) -> usize {
            match ty {
                LogicalType::Decimal {
                    bits,
                    precision,
                    scale,
                } => {
                    11 + digits(u32::from(*bits))
                        + digits(u32::from(*precision))
                        + digits(u32::from(scale.unsigned_abs()))
                        + usize::from(*scale < 0)
                }
                LogicalType::FixedSizeBinary(n) => 7 + digits(*n),
                LogicalType::Time { bits, unit } => {
                    7 + digits(u32::from(*bits)) + unit_text(unit).len()
                }
                LogicalType::Timestamp { unit, timezone } => {
                    12 + unit_text(unit).len() + timezone.as_ref().map_or(1, |zone| text_len(zone))
                }
                LogicalType::Array {
                    element,
                    fixed_length,
                } => 8 + fixed_length.map_or(1, digits) + value_len(element),
                LogicalType::Map { key, value } => 6 + value_len(key) + value_len(value),
                LogicalType::Struct(fields) => {
                    8 + digits(fields.len() as u32)
                        + fields
                            .iter()
                            .map(|field| 4 + text_len(&field.name) + type_len(&field.data_type))
                            .sum::<usize>()
                }
                _ => {
                    let mut leaf = String::new();
                    encode_type(ty, &mut leaf);
                    leaf.len()
                }
            }
        }
        if let Some(leaf) = self.legacy {
            match leaf {
                LegacyLeaf::Decimal(p, s) => {
                    10 + digits(u32::from(p))
                        + digits(u32::from(s.unsigned_abs()))
                        + usize::from(s < 0)
                }
                _ => leaf.encode().len(),
            }
        } else {
            PREFIX.len()
                + type_len(&self.data_type)
                + self.provider_binding.as_ref().map_or(0, |binding| {
                    9 + digits(binding.len() as u32) + 2 * binding.len()
                })
        }
    }
    pub fn validate(&self) -> Result<(), TypeCodecError> {
        self.data_type
            .validate(LogicalTypeLimits::default())
            .map(|_| ())
            .map_err(invalid)
    }
}
/// Borrowed preflight accounting for the document's aggregate allocation budget.
pub(crate) fn signature_nodes(
    text: &str,
    limits: LogicalTypeLimits,
) -> Result<usize, TypeCodecError> {
    match preflight_signature(text, limits) {
        Ok(()) => {}
        Err(TypeCodecError::RetiredComplex) => return Ok(1),
        Err(error) => return Err(error),
    }
    if let Some(body) = text.strip_prefix(PREFIX) {
        let mut parser = Parser::new(body, limits);
        parser.payload(false)?;
        Ok(parser.nodes)
    } else {
        Ok(1)
    }
}

impl LegacyLeaf {
    fn encode(self) -> String {
        match self {
            Self::Boolean => "boolean",
            Self::Bool => "bool",
            Self::TinyInt => "tinyint",
            Self::SmallInt => "smallint",
            Self::Int => "int",
            Self::Integer => "integer",
            Self::Long => "long",
            Self::BigInt => "bigint",
            Self::Float => "float",
            Self::Double => "double",
            Self::Date => "date",
            Self::Timestamp => "timestamp",
            Self::String => "string",
            Self::Varchar => "varchar",
            Self::Char => "char",
            Self::Binary => "binary",
            Self::Varbinary => "varbinary",
            Self::Decimal(p, s) => return format!("decimal({p},{s})"),
        }
        .into()
    }
}
fn legacy_leaf(text: &str) -> Result<(LogicalType, LegacyLeaf), TypeCodecError> {
    use LegacyLeaf as L;
    let pair = match text {
        "boolean" => (LogicalType::Boolean, L::Boolean),
        "bool" => (LogicalType::Boolean, L::Bool),
        "tinyint" => (LogicalType::Int8, L::TinyInt),
        "smallint" => (LogicalType::Int16, L::SmallInt),
        "int" => (LogicalType::Int32, L::Int),
        "integer" => (LogicalType::Int32, L::Integer),
        "long" => (LogicalType::Int64, L::Long),
        "bigint" => (LogicalType::Int64, L::BigInt),
        "float" => (LogicalType::Float32, L::Float),
        "double" => (LogicalType::Float64, L::Double),
        "date" => (LogicalType::Date32, L::Date),
        "timestamp" => (
            LogicalType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            },
            L::Timestamp,
        ),
        "string" => (LogicalType::Utf8, L::String),
        "varchar" => (LogicalType::Utf8, L::Varchar),
        "char" => (LogicalType::Utf8, L::Char),
        "binary" => (LogicalType::Binary, L::Binary),
        "varbinary" => (LogicalType::Binary, L::Varbinary),
        _ if text.starts_with("decimal(") => {
            let mut parser = Parser::new(text, LogicalTypeLimits::default());
            parser.word("decimal(")?;
            let precision = u8::try_from(parser.number()?)
                .map_err(|_| invalid("decimal precision overflow"))?;
            parser.word(",")?;
            let scale =
                i8::try_from(parser.signed()?).map_err(|_| invalid("decimal scale overflow"))?;
            parser.word(")")?;
            parser.finish()?;
            (
                LogicalType::Decimal {
                    bits: 128,
                    precision,
                    scale,
                },
                L::Decimal(precision, scale),
            )
        }
        _ => {
            return Err(invalid(
                "unknown legacy scalar spelling or missing parameters",
            ));
        }
    };
    Ok(pair)
}

/// Scan borrowed bytes before any recursive tree or owned text is allocated.
/// The same grammar is used for validation and construction; there is no parser fallback.
pub fn preflight_signature(text: &str, limits: LogicalTypeLimits) -> Result<(), TypeCodecError> {
    if text.len() > limits.max_text_bytes || limits.max_depth == 0 || limits.max_nodes == 0 {
        return Err(invalid("type payload exceeds its byte/depth/node budget"));
    }
    if let Some(body) = text.strip_prefix(PREFIX) {
        let mut parser = Parser::new(body, limits);
        parser.payload(false)?;
        parser.finish()?;
    } else if text.starts_with("mvtype:") {
        return Err(invalid("unknown type payload version"));
    } else if known_retired_complex(text, limits)? {
        return Err(TypeCodecError::RetiredComplex);
    } else {
        let (ty, _) = legacy_leaf(text)?;
        ty.validate(limits).map_err(invalid)?;
    }
    Ok(())
}

// Recognize only producer spellings, including its incomplete recursive Struct
// Display. Unknown children are not a retirement authority.
fn known_retired_complex(text: &str, limits: LogicalTypeLimits) -> Result<bool, TypeCodecError> {
    if matches!(text, "list" | "map" | "struct") {
        return Ok(true);
    }
    if !text.starts_with("struct<") {
        return Ok(false);
    }
    fn scan(
        text: &str,
        pos: &mut usize,
        depth: usize,
        nodes: &mut usize,
        limits: LogicalTypeLimits,
    ) -> Result<(), TypeCodecError> {
        if depth > limits.max_depth || *nodes >= limits.max_nodes {
            return Err(invalid("retired type exceeds its structure budget"));
        }
        *nodes += 1;
        let rest = &text[*pos..];
        if rest.starts_with("struct<") {
            *pos += 7;
            while !text[*pos..].starts_with('>') {
                scan(text, pos, depth + 1, nodes, limits)?;
            }
            *pos += 1;
            return Ok(());
        }
        for token in [
            "timestamp_ns",
            "timestamptz_ns",
            "timestamptz",
            "timestamp",
            "boolean",
            "double",
            "string",
            "binary",
            "variant",
            "float",
            "long",
            "time",
            "date",
            "uuid",
            "list",
            "map",
            "int",
        ] {
            if rest.starts_with(token) {
                *pos += token.len();
                return Ok(());
            }
        }
        for prefix in ["decimal(", "fixed("] {
            if rest.starts_with(prefix) {
                let end = rest
                    .find(')')
                    .ok_or_else(|| invalid("incomplete retired type parameter"))?;
                let inner = &rest[prefix.len()..end];
                if prefix == "decimal(" {
                    let (ty, _) = legacy_leaf(&rest[..=end])?;
                    ty.validate(limits).map_err(invalid)?;
                } else {
                    let n: u64 = inner
                        .parse()
                        .map_err(|_| invalid("unknown retired fixed-width parameter"))?;
                    if n.to_string() != inner {
                        return Err(invalid("noncanonical retired fixed-width parameter"));
                    }
                }
                *pos += end + 1;
                return Ok(());
            }
        }
        Err(invalid("unknown retired complex child"))
    }
    let mut pos = 0;
    let mut nodes = 0;
    scan(text, &mut pos, 1, &mut nodes, limits)?;
    if pos != text.len() {
        return Err(invalid("trailing retired type bytes"));
    }
    Ok(true)
}

struct Parser<'a> {
    text: &'a str,
    pos: usize,
    nodes: usize,
    text_bytes: usize,
    limits: LogicalTypeLimits,
}
impl<'a> Parser<'a> {
    fn new(text: &'a str, limits: LogicalTypeLimits) -> Self {
        Self {
            text,
            pos: 0,
            nodes: 0,
            text_bytes: 0,
            limits,
        }
    }
    fn payload(
        &mut self,
        owned: bool,
    ) -> Result<(Option<LogicalType>, Option<&'a str>), TypeCodecError> {
        if !self.take("bound(") {
            return Ok((self.ty(1, owned)?, None));
        }
        let ty = self.ty(1, owned)?;
        self.word(",")?;
        let bytes = self.number()? as usize;
        self.word(":")?;
        let width = bytes
            .checked_mul(2)
            .ok_or_else(|| invalid("provider binding width overflow"))?;
        if bytes == 0 || width > self.limits.max_text_bytes {
            return Err(invalid("missing or over-budget provider binding"));
        }
        let end = self
            .pos
            .checked_add(width)
            .ok_or_else(|| invalid("provider binding width overflow"))?;
        let hex = self
            .text
            .get(self.pos..end)
            .ok_or_else(|| invalid("truncated provider binding"))?;
        if !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid("provider binding must use canonical lowercase hex"));
        }
        self.pos = end;
        self.word(")")?;
        Ok((ty, Some(hex)))
    }
    fn word(&mut self, word: &str) -> Result<(), TypeCodecError> {
        if self.text[self.pos..].starts_with(word) {
            self.pos += word.len();
            Ok(())
        } else {
            Err(invalid("unexpected type token"))
        }
    }
    fn take(&mut self, word: &str) -> bool {
        if self.text[self.pos..].starts_with(word) {
            self.pos += word.len();
            true
        } else {
            false
        }
    }
    fn finish(&self) -> Result<(), TypeCodecError> {
        if self.pos == self.text.len() {
            Ok(())
        } else {
            Err(invalid("trailing type payload"))
        }
    }
    fn number(&mut self) -> Result<u32, TypeCodecError> {
        let start = self.pos;
        while self
            .text
            .as_bytes()
            .get(self.pos)
            .is_some_and(u8::is_ascii_digit)
        {
            self.pos += 1;
        }
        let digits = &self.text[start..self.pos];
        if digits.is_empty() || (digits.len() > 1 && digits.starts_with('0')) {
            return Err(invalid("noncanonical or missing unsigned parameter"));
        }
        digits
            .parse()
            .map_err(|_| invalid("unsigned parameter overflow"))
    }
    fn signed(&mut self) -> Result<i32, TypeCodecError> {
        let negative = self.take("-");
        let n = self.number()?;
        if negative && n == 0 {
            return Err(invalid("negative zero parameter"));
        }
        let n = i32::try_from(n).map_err(|_| invalid("signed parameter overflow"))?;
        Ok(if negative { -n } else { n })
    }
    fn text(&mut self) -> Result<&'a str, TypeCodecError> {
        let size = self.number()? as usize;
        self.word(":")?;
        let end = self
            .pos
            .checked_add(size)
            .ok_or_else(|| invalid("text length overflow"))?;
        let value = self
            .text
            .get(self.pos..end)
            .ok_or_else(|| invalid("invalid UTF-8 text boundary or missing bytes"))?;
        self.text_bytes = self
            .text_bytes
            .checked_add(size)
            .ok_or_else(|| invalid("type text budget overflow"))?;
        if value.is_empty() || value.contains('\0') || self.text_bytes > self.limits.max_text_bytes
        {
            return Err(invalid("invalid or over-budget type text"));
        }
        self.pos = end;
        Ok(value)
    }
    fn unit(&mut self) -> Result<TimeUnit, TypeCodecError> {
        for (token, unit) in [
            ("s", TimeUnit::Second),
            ("ms", TimeUnit::Millisecond),
            ("us", TimeUnit::Microsecond),
            ("ns", TimeUnit::Nanosecond),
        ] {
            if self.take(token) {
                return Ok(unit);
            }
        }
        Err(invalid("missing or unknown time unit"))
    }
    fn value(&mut self, depth: usize, build: bool) -> Result<Option<LogicalValue>, TypeCodecError> {
        let nullable = if self.take("0:") {
            false
        } else if self.take("1:") {
            true
        } else {
            return Err(invalid("missing child NULL constraint"));
        };
        Ok(self.ty(depth, build)?.map(|data_type| LogicalValue {
            data_type,
            nullable,
        }))
    }
    fn ty(&mut self, depth: usize, build: bool) -> Result<Option<LogicalType>, TypeCodecError> {
        if depth > self.limits.max_depth || self.nodes >= self.limits.max_nodes {
            return Err(invalid("type exceeds its structure budget"));
        }
        self.nodes += 1;
        let leaf = if self.take("decimal(") {
            let bits =
                u16::try_from(self.number()?).map_err(|_| invalid("decimal width overflow"))?;
            self.word(",")?;
            let precision =
                u8::try_from(self.number()?).map_err(|_| invalid("decimal precision overflow"))?;
            self.word(",")?;
            let scale =
                i8::try_from(self.signed()?).map_err(|_| invalid("decimal scale overflow"))?;
            self.word(")")?;
            Some(LogicalType::Decimal {
                bits,
                precision,
                scale,
            })
        } else if self.take("fixed(") {
            let n = self.number()?;
            self.word(")")?;
            Some(LogicalType::FixedSizeBinary(n))
        } else if self.take("time(") {
            let bits = u8::try_from(self.number()?).map_err(|_| invalid("time width overflow"))?;
            self.word(",")?;
            let unit = self.unit()?;
            self.word(")")?;
            Some(LogicalType::Time { bits, unit })
        } else if self.take("timestamp(") {
            let unit = self.unit()?;
            self.word(",")?;
            let timezone = if self.take("-") {
                None
            } else {
                let value = self.text()?;
                if build { Some(value.to_owned()) } else { None }
            };
            self.word(")")?;
            Some(LogicalType::Timestamp { unit, timezone })
        } else if self.take("array(") {
            let fixed_length = if self.take("-") {
                None
            } else {
                Some(self.number()?)
            };
            if fixed_length.is_some_and(|n| n > i32::MAX as u32) {
                return Err(invalid("fixed array length overflow"));
            }
            self.word(",")?;
            let child = self.value(depth + 1, build)?;
            self.word(")")?;
            return Ok(child.map(|element| LogicalType::Array {
                element: Box::new(element),
                fixed_length,
            }));
        } else if self.take("map(") {
            let key = self.value(depth + 1, build)?;
            self.word(",")?;
            let value = self.value(depth + 1, build)?;
            self.word(")")?;
            return Ok(key.zip(value).map(|(key, value)| LogicalType::Map {
                key: Box::new(key),
                value: Box::new(value),
            }));
        } else if self.take("struct(") {
            let count = self.number()? as usize;
            if count > self.limits.max_nodes.saturating_sub(self.nodes) {
                return Err(invalid("struct exceeds node budget"));
            }
            let mut fields = if build {
                Vec::with_capacity(count)
            } else {
                Vec::new()
            };
            for _ in 0..count {
                self.word(",")?;
                let name = self.text()?;
                self.word(",")?;
                let value = self.value(depth + 1, build)?;
                if let Some(value) = value {
                    fields.push(LogicalField {
                        name: name.to_owned(),
                        data_type: value.data_type,
                        nullable: value.nullable,
                    });
                }
            }
            self.word(")")?;
            return Ok(build.then_some(LogicalType::Struct(fields)));
        } else {
            let mut found = None;
            for (token, ty) in [
                ("null", LogicalType::Null),
                ("boolean", LogicalType::Boolean),
                ("largeint", LogicalType::LargeInt),
                ("i8", LogicalType::Int8),
                ("i16", LogicalType::Int16),
                ("i32", LogicalType::Int32),
                ("i64", LogicalType::Int64),
                ("u8", LogicalType::UInt8),
                ("u16", LogicalType::UInt16),
                ("u32", LogicalType::UInt32),
                ("u64", LogicalType::UInt64),
                ("f32", LogicalType::Float32),
                ("f64", LogicalType::Float64),
                ("utf8", LogicalType::Utf8),
                ("binary", LogicalType::Binary),
                ("uuid", LogicalType::Uuid),
                ("json", LogicalType::Json),
                ("bitmap", LogicalType::Bitmap),
                ("hll", LogicalType::Hll),
                ("object", LogicalType::Object),
                ("percentile", LogicalType::Percentile),
                ("variant", LogicalType::Variant),
                ("date32", LogicalType::Date32),
                ("date64", LogicalType::Date64),
            ] {
                if self.take(token) {
                    found = Some(ty);
                    break;
                }
            }
            found
        };
        let leaf = leaf.ok_or_else(|| invalid("unknown or incomplete type node"))?;
        leaf.validate(self.limits).map_err(invalid)?;
        Ok(build.then_some(leaf))
    }
}
fn unit_text(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}
fn encode_text(value: &str, out: &mut String) {
    out.push_str(&value.len().to_string());
    out.push(':');
    out.push_str(value);
}
fn encode_value(value: &LogicalValue, out: &mut String) {
    out.push_str(if value.nullable { "1:" } else { "0:" });
    encode_type(&value.data_type, out);
}
fn encode_type(ty: &LogicalType, out: &mut String) {
    use LogicalType as T;
    match ty {
        T::Decimal {
            bits,
            precision,
            scale,
        } => out.push_str(&format!("decimal({bits},{precision},{scale})")),
        T::FixedSizeBinary(n) => out.push_str(&format!("fixed({n})")),
        T::Time { bits, unit } => out.push_str(&format!("time({bits},{})", unit_text(unit))),
        T::Timestamp { unit, timezone } => {
            out.push_str("timestamp(");
            out.push_str(unit_text(unit));
            out.push(',');
            if let Some(zone) = timezone {
                encode_text(zone, out)
            } else {
                out.push('-')
            }
            out.push(')');
        }
        T::Array {
            element,
            fixed_length,
        } => {
            out.push_str("array(");
            if let Some(n) = fixed_length {
                out.push_str(&n.to_string())
            } else {
                out.push('-')
            }
            out.push(',');
            encode_value(element, out);
            out.push(')');
        }
        T::Map { key, value } => {
            out.push_str("map(");
            encode_value(key, out);
            out.push(',');
            encode_value(value, out);
            out.push(')');
        }
        T::Struct(fields) => {
            out.push_str("struct(");
            out.push_str(&fields.len().to_string());
            for field in fields {
                out.push(',');
                encode_text(&field.name, out);
                out.push(',');
                out.push_str(if field.nullable { "1:" } else { "0:" });
                encode_type(&field.data_type, out);
            }
            out.push(')');
        }
        leaf => out.push_str(match leaf {
            T::Null => "null",
            T::Boolean => "boolean",
            T::Int8 => "i8",
            T::Int16 => "i16",
            T::Int32 => "i32",
            T::Int64 => "i64",
            T::UInt8 => "u8",
            T::UInt16 => "u16",
            T::UInt32 => "u32",
            T::UInt64 => "u64",
            T::LargeInt => "largeint",
            T::Float32 => "f32",
            T::Float64 => "f64",
            T::Utf8 => "utf8",
            T::Binary => "binary",
            T::Uuid => "uuid",
            T::Json => "json",
            T::Bitmap => "bitmap",
            T::Hll => "hll",
            T::Object => "object",
            T::Percentile => "percentile",
            T::Variant => "variant",
            T::Date32 => "date32",
            T::Date64 => "date64",
            _ => unreachable!("parameterized type handled above"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nested_contract_roundtrip_is_exact_and_named_only_for_struct() {
        let ty = LogicalType::Struct(vec![LogicalField {
            name: "with,:雪".into(),
            nullable: false,
            data_type: LogicalType::Array {
                fixed_length: Some(3),
                element: Box::new(LogicalValue {
                    nullable: true,
                    data_type: LogicalType::Map {
                        key: Box::new(LogicalValue {
                            nullable: false,
                            data_type: LogicalType::Utf8,
                        }),
                        value: Box::new(LogicalValue {
                            nullable: true,
                            data_type: LogicalType::Timestamp {
                                unit: TimeUnit::Nanosecond,
                                timezone: Some("Asia/Shanghai".into()),
                            },
                        }),
                    },
                }),
            },
        }]);
        let encoded = MvLogicalType::from_logical_type(ty.clone())
            .unwrap()
            .encode_signature();
        let decoded = MvLogicalType::decode_signature(&encoded).unwrap();
        assert_eq!(decoded.logical_type(), &ty);
        assert_eq!(decoded.encode_signature(), encoded);
        let mut changed = ty;
        let LogicalType::Struct(fields) = &mut changed else {
            unreachable!()
        };
        fields[0].nullable = true;
        assert_ne!(decoded, MvLogicalType::from_logical_type(changed).unwrap());
    }
    #[test]
    fn opaque_schema_binding_is_exact_but_not_content_equality_or_diagnostics() {
        let tree = LogicalType::Struct(vec![LogicalField {
            name: "child".into(),
            data_type: LogicalType::Int64,
            nullable: false,
        }]);
        let a = MvLogicalType::from_schema_type(tree.clone(), Bytes::from_static(b"nested-id-11"))
            .unwrap();
        let b = MvLogicalType::from_schema_type(tree.clone(), Bytes::from_static(b"nested-id-12"))
            .unwrap();
        assert_eq!(a, b);
        let text = a.encode_signature();
        assert_eq!(a.encoded_len(), text.len());
        let decoded = MvLogicalType::decode_signature(&text).unwrap();
        assert_eq!(decoded.encode_signature(), text);
        assert_eq!(decoded.provider_type_binding(), a.provider_type_binding());
        assert!(decoded.matches_schema(&tree, b"nested-id-11", None));
        assert!(!decoded.matches_schema(&tree, b"nested-id-12", None));
        assert!(!decoded.matches_schema(&tree, &[], Some(&tree)));
        assert!(!decoded.to_string().contains("6e657374"));
        assert!(!format!("{decoded:?}").contains("nested-id"));
        assert!(
            MvLogicalType::from_logical_type(tree)
                .unwrap()
                .validate_schema_type()
                .is_err()
        );
        let scalar =
            MvLogicalType::from_schema_type(LogicalType::Int64, Bytes::from_static(b"ids"))
                .unwrap();
        assert_eq!(scalar.encoded_len(), scalar.encode_signature().len());
        assert_eq!(
            MvLogicalType::decode_signature(&scalar.encode_signature())
                .unwrap()
                .provider_type_binding(),
            scalar.provider_type_binding()
        );
        assert!(scalar.validate_schema_type().is_ok());
    }
    #[test]
    fn historical_bound_narrow_signatures_keep_exact_source_derived_bytes() {
        // Independently frozen from the 8c04a1d5a producer source, not an
        // execution capture: v1 exact-field bytes and its bound type grammar.
        for (logical, binding, signature) in [
            (
                LogicalType::Int8,
                b"novarocks.iceberg.exact-field.v1:{\"id\":1,\"required\":false,\"type\":\"int\"}"
                    .as_slice(),
                "mvtype:1:bound(i8,71:6e6f7661726f636b732e696365626572672e65786163742d6669656c642e76313a7b226964223a312c227265717569726564223a66616c73652c2274797065223a22696e74227d)",
            ),
            (
                LogicalType::Int16,
                b"novarocks.iceberg.exact-field.v1:{\"id\":2,\"required\":true,\"type\":\"int\"}"
                    .as_slice(),
                "mvtype:1:bound(i16,70:6e6f7661726f636b732e696365626572672e65786163742d6669656c642e76313a7b226964223a322c227265717569726564223a747275652c2274797065223a22696e74227d)",
            ),
        ] {
            let frozen = MvLogicalType::decode_signature(signature).unwrap();
            assert_eq!(frozen.encode_signature(), signature);
            assert_eq!(frozen.provider_type_binding().unwrap().as_ref(), binding);
            assert!(frozen.matches_schema(&logical, binding, Some(&LogicalType::Int32)));
            assert!(!frozen.matches_schema(
                &LogicalType::Int32,
                binding,
                Some(&LogicalType::Int32)
            ));
            assert!(!frozen.matches_schema(&logical, b"changed", Some(&logical)));
            assert_eq!(
                MvLogicalType::from_schema_type(logical, Bytes::copy_from_slice(binding))
                    .unwrap()
                    .encode_signature(),
                signature
            );
        }
        let raw_int = MvLogicalType::decode_signature("int").unwrap();
        assert!(!raw_int.matches_schema(
            &LogicalType::Int8,
            b"same-physical-int",
            Some(&LogicalType::Int32)
        ));
        assert!(!raw_int.matches_schema(
            &LogicalType::Int16,
            b"same-physical-int",
            Some(&LogicalType::Int32)
        ));
    }

    #[test]
    fn historical_plain_timestamp_requires_provider_exact_legacy_domain() {
        let old = MvLogicalType::decode_signature("timestamp").unwrap();
        let projected = LogicalType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        };
        assert!(old.matches_schema(&projected, b"provider", Some(&projected)));
        // An Iceberg timestamptz projects to the same Arrow read type, but is
        // outside the original plain-timestamp provider domain.
        assert!(!old.matches_schema(&projected, b"provider-tz", None));
        assert!(!old.matches_schema(
            &LogicalType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None
            },
            b"provider-ns",
            None
        ));
        assert!(old.validate_schema_type().is_ok());
    }
    #[test]
    fn bound_envelope_preflight_rejects_hex_width_versions_and_budgets() {
        for text in [
            "mvtype:1:bound(i64,0:)",
            "mvtype:1:bound(i64,1:AA)",
            "mvtype:1:bound(i64,2:aa)",
            "mvtype:1:bound(i64,01:aa)",
            "mvtype:1:bound(i64,4294967295:aa)",
            "mvtype:1:bound(i64,1:aa)extra",
            "mvtype:1:bound(bound(i64,1:aa),1:aa)",
        ] {
            assert!(
                preflight_signature(text, LogicalTypeLimits::default()).is_err(),
                "{text}"
            );
        }
        let text = "mvtype:1:bound(struct(1,1:x,0:array(-,1:i64)),1:aa)";
        for limits in [
            LogicalTypeLimits {
                max_depth: 2,
                ..Default::default()
            },
            LogicalTypeLimits {
                max_nodes: 2,
                ..Default::default()
            },
            LogicalTypeLimits {
                max_text_bytes: text.len() - 1,
                ..Default::default()
            },
        ] {
            assert!(preflight_signature(text, limits).is_err());
        }
        assert!(MvLogicalType::from_schema_type(LogicalType::Int64, Bytes::new()).is_err());
        assert!(
            MvLogicalType::from_schema_type(LogicalType::Int64, Bytes::from(vec![1; 32768]))
                .is_err()
        );
    }
    #[test]
    fn legacy_spelling_is_preserved_while_equality_and_hash_are_semantic() {
        use std::collections::hash_map::DefaultHasher;
        let a = MvLogicalType::decode_signature("long").unwrap();
        let b = MvLogicalType::decode_signature("bigint").unwrap();
        assert_eq!(a, b);
        assert_eq!(a.encode_signature(), "long");
        assert_eq!(b.encode_signature(), "bigint");
        let mut x = DefaultHasher::new();
        a.hash(&mut x);
        let mut y = DefaultHasher::new();
        b.hash(&mut y);
        assert_eq!(x.finish(), y.finish());
        assert_eq!(
            MvLogicalType::decode_signature("int")
                .unwrap()
                .logical_type(),
            &LogicalType::Int32
        );
    }
    #[test]
    fn strict_grammar_rejects_unknown_incomplete_noncanonical_and_retired_payloads() {
        for text in [
            "mvtype:2:i64",
            "mvtype:1:array(-,i64)",
            "mvtype:1:decimal(128,01,0)",
            "mvtype:1:decimal(128,1,-0)",
            "mvtype:1:time(32,ns)",
            "mvtype:1:timestamp(us,0:)",
            "mvtype:1:struct(1,5:short,0:unknown)",
            "mvtype:1:fixed(4294967295)",
            "decimal(18,2,3)",
            " long",
            "LONG",
            "struct<unknown>",
        ] {
            assert!(MvLogicalType::decode_signature(text).is_err(), "{text}");
        }
        for text in ["list", "map", "struct", "struct<longlist>"] {
            assert_eq!(
                MvLogicalType::decode_signature(text).unwrap_err(),
                TypeCodecError::RetiredComplex
            );
        }
    }
    #[test]
    fn budgets_are_checked_by_borrowed_scan_before_tree_construction() {
        let text = "mvtype:1:struct(1,4:name,0:array(-,1:i64))";
        for limits in [
            LogicalTypeLimits {
                max_depth: 2,
                ..Default::default()
            },
            LogicalTypeLimits {
                max_nodes: 2,
                ..Default::default()
            },
            LogicalTypeLimits {
                max_text_bytes: 8,
                ..Default::default()
            },
        ] {
            assert!(MvLogicalType::decode_with_limits(text, limits).is_err());
        }
    }
}
