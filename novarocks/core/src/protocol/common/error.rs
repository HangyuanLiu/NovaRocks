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

use std::error::Error;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProtocolFamily {
    Native,
    StarRocks,
}

impl fmt::Display for ProtocolFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native => f.write_str("native"),
            Self::StarRocks => f.write_str("starrocks"),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FieldPath(Vec<FieldPathSegment>);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum FieldPathSegment {
    Field(&'static str),
    Index(usize),
    MapKey(String),
}

impl FieldPath {
    pub fn root(name: &'static str) -> Self {
        Self(vec![FieldPathSegment::Field(name)])
    }

    pub fn field(mut self, name: &'static str) -> Self {
        self.0.push(FieldPathSegment::Field(name));
        self
    }

    pub(crate) fn index(mut self, index: usize) -> Self {
        self.0.push(FieldPathSegment::Index(index));
        self
    }

    pub(crate) fn map_key(mut self, key: impl Into<String>) -> Self {
        self.0.push(FieldPathSegment::MapKey(key.into()));
        self
    }

    pub(crate) fn segments(&self) -> &[FieldPathSegment] {
        &self.0
    }

    pub(crate) fn append_segments(
        mut self,
        segments: impl IntoIterator<Item = FieldPathSegment>,
    ) -> Self {
        self.0.extend(segments);
        self
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, segment) in self.0.iter().enumerate() {
            match (index, segment) {
                (0, FieldPathSegment::Field(name)) => f.write_str(name)?,
                (_, FieldPathSegment::Field(name)) => write!(f, ".{name}")?,
                (_, FieldPathSegment::Index(value)) => write!(f, "[{value}]")?,
                (_, FieldPathSegment::MapKey(key)) => write!(f, "[{key:?}]")?,
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportDecodeErrorKind {
    MalformedPayload,
    TruncatedPayload,
    UnsupportedEncoding,
}

impl fmt::Display for TransportDecodeErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedPayload => f.write_str("malformed payload"),
            Self::TruncatedPayload => f.write_str("truncated payload"),
            Self::UnsupportedEncoding => f.write_str("unsupported encoding"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransportDecodeError {
    family: ProtocolFamily,
    kind: TransportDecodeErrorKind,
    detail: String,
}

impl TransportDecodeError {
    pub(crate) fn new(
        family: ProtocolFamily,
        kind: TransportDecodeErrorKind,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            family,
            kind,
            detail: detail.into(),
        }
    }

    pub(crate) fn family(&self) -> ProtocolFamily {
        self.family
    }

    pub(crate) fn kind(&self) -> TransportDecodeErrorKind {
        self.kind
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for TransportDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} transport decode error ({}): {}",
            self.family, self.kind, self.detail
        )
    }
}

impl Error for TransportDecodeError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProtocolErrorKind {
    MissingField,
    InvalidEnum,
    InvalidValue,
    OutOfRange,
    DuplicateField,
    InconsistentFields,
    Unsupported,
}

impl fmt::Display for ProtocolErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField => f.write_str("missing field"),
            Self::InvalidEnum => f.write_str("invalid enum"),
            Self::InvalidValue => f.write_str("invalid value"),
            Self::OutOfRange => f.write_str("out of range"),
            Self::DuplicateField => f.write_str("duplicate field"),
            Self::InconsistentFields => f.write_str("inconsistent fields"),
            Self::Unsupported => f.write_str("unsupported"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProtocolError {
    family: ProtocolFamily,
    path: FieldPath,
    kind: ProtocolErrorKind,
    detail: String,
}

impl ProtocolError {
    pub(crate) fn new(
        family: ProtocolFamily,
        path: FieldPath,
        kind: ProtocolErrorKind,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            family,
            path,
            kind,
            detail: detail.into(),
        }
    }

    pub(crate) fn family(&self) -> ProtocolFamily {
        self.family
    }

    pub(crate) fn path(&self) -> &FieldPath {
        &self.path
    }

    pub(crate) fn kind(&self) -> ProtocolErrorKind {
        self.kind
    }

    pub(crate) fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} protocol error at {} ({}): {}",
            self.family, self.path, self.kind, self.detail
        )
    }
}

impl Error for ProtocolError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_path_formats_fields_indexes_and_escaped_map_keys() {
        let path = FieldPath::root("plan_fragment")
            .field("root")
            .field("children")
            .index(2)
            .field("assignments")
            .map_key("scan\"owner");
        assert_eq!(
            path.to_string(),
            "plan_fragment.root.children[2].assignments[\"scan\\\"owner\"]"
        );
        assert_eq!(
            path.segments(),
            &[
                FieldPathSegment::Field("plan_fragment"),
                FieldPathSegment::Field("root"),
                FieldPathSegment::Field("children"),
                FieldPathSegment::Index(2),
                FieldPathSegment::Field("assignments"),
                FieldPathSegment::MapKey("scan\"owner".to_string()),
            ]
        );

        let escaped = FieldPath::root("root").map_key("slash\\line\n\t\u{7}");
        assert_eq!(escaped.to_string(), "root[\"slash\\\\line\\n\\t\\u{7}\"]");
    }

    #[test]
    fn protocol_vocabulary_labels_are_stable() {
        for (family, expected) in [
            (ProtocolFamily::Native, "native"),
            (ProtocolFamily::StarRocks, "starrocks"),
        ] {
            assert_eq!(family.to_string(), expected);
        }
        for (kind, expected) in [
            (
                TransportDecodeErrorKind::MalformedPayload,
                "malformed payload",
            ),
            (
                TransportDecodeErrorKind::TruncatedPayload,
                "truncated payload",
            ),
            (
                TransportDecodeErrorKind::UnsupportedEncoding,
                "unsupported encoding",
            ),
        ] {
            assert_eq!(kind.to_string(), expected);
        }
        for (kind, expected) in [
            (ProtocolErrorKind::MissingField, "missing field"),
            (ProtocolErrorKind::InvalidEnum, "invalid enum"),
            (ProtocolErrorKind::InvalidValue, "invalid value"),
            (ProtocolErrorKind::OutOfRange, "out of range"),
            (ProtocolErrorKind::DuplicateField, "duplicate field"),
            (ProtocolErrorKind::InconsistentFields, "inconsistent fields"),
            (ProtocolErrorKind::Unsupported, "unsupported"),
        ] {
            assert_eq!(kind.to_string(), expected);
        }
    }

    #[test]
    fn protocol_errors_expose_typed_state_and_stable_display() {
        let path = FieldPath::root("plan_fragment").field("root");
        let error = ProtocolError::new(
            ProtocolFamily::Native,
            path.clone(),
            ProtocolErrorKind::MissingField,
            "root is required",
        );
        assert_eq!(error.family(), ProtocolFamily::Native);
        assert_eq!(error.path(), &path);
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(error.detail(), "root is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.root (missing field): root is required"
        );
    }

    #[test]
    fn transport_decode_error_is_distinct_from_dto_validation() {
        let error = TransportDecodeError::new(
            ProtocolFamily::StarRocks,
            TransportDecodeErrorKind::TruncatedPayload,
            "compact payload ended early",
        );
        assert_eq!(error.family(), ProtocolFamily::StarRocks);
        assert_eq!(error.kind(), TransportDecodeErrorKind::TruncatedPayload);
        assert_eq!(error.detail(), "compact payload ended early");
        assert_eq!(
            error.to_string(),
            "starrocks transport decode error (truncated payload): compact payload ended early"
        );
    }
}
