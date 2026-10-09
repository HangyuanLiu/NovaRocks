// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Borrowed, allocation-free traversal of a complete typed scalar record.
//! The wire ceiling and frozen schema depth bound the walk. The caller keeps
//! the record backing alive; no owned nested tree or second allowance exists.

use crate::scalar_leaf::{ABSENT, NULL};
use crate::{
    BorrowedScalarLeaf, SCALAR_LEAF_HEADER_BYTES, ScalarField, ScalarLeafError, ScalarLeafHeader,
    ScalarSchema, ScalarValueType,
};

/// Events in frozen field order. Root depth is one; each child adds one.
pub enum ScalarRecordEvent<'schema, 'record> {
    NoRows,
    Leaf {
        field: &'schema ScalarField,
        value: BorrowedScalarLeaf<'record>,
        depth: usize,
    },
    Begin {
        field: &'schema ScalarField,
        depth: usize,
    },
    End {
        field: &'schema ScalarField,
        depth: usize,
    },
}

#[derive(Debug)]
pub enum ScalarVisitError<E> {
    Record(ScalarLeafError),
    Callback(E),
}
impl<E> From<ScalarLeafError> for ScalarVisitError<E> {
    fn from(error: ScalarLeafError) -> Self {
        Self::Record(error)
    }
}
impl<E: std::fmt::Display> std::fmt::Display for ScalarVisitError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Record(error) => error.fmt(f),
            Self::Callback(error) => error.fmt(f),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for ScalarVisitError<E> {}

/// Validated view, retaining only borrows of the exact schema and body.
pub struct BorrowedScalarRecord<'schema, 'record> {
    schema: &'schema ScalarSchema,
    record: &'record [u8],
    rows: u64,
}
impl<'schema, 'record> BorrowedScalarRecord<'schema, 'record> {
    pub fn try_decode(
        schema: &'schema ScalarSchema,
        record: &'record [u8],
    ) -> Result<Self, ScalarLeafError> {
        let header = ScalarLeafHeader::decode(
            schema,
            record
                .get(..SCALAR_LEAF_HEADER_BYTES)
                .ok_or(ScalarLeafError::MalformedRecord)?,
        )?;
        if record.len() != header.record_bytes() {
            return Err(ScalarLeafError::MalformedRecord);
        }
        let value = Self {
            schema,
            record,
            rows: header.rows(),
        };
        match value.walk(|_| Ok::<(), std::convert::Infallible>(())) {
            Ok(()) => Ok(value),
            Err(ScalarVisitError::Record(error)) => Err(error),
            Err(ScalarVisitError::Callback(never)) => match never {},
        }
    }
    pub const fn rows(&self) -> u64 {
        self.rows
    }

    /// Visit a previously validated record without copying any value payload.
    /// A consumer's own output limit remains independent of the wire ceiling.
    pub fn walk<E>(
        &self,
        mut visit: impl FnMut(ScalarRecordEvent<'schema, 'record>) -> Result<(), E>,
    ) -> Result<(), ScalarVisitError<E>> {
        let field = self.schema.field();
        let flags = self.record[13];
        if flags == NULL | ABSENT {
            return visit(ScalarRecordEvent::NoRows).map_err(ScalarVisitError::Callback);
        }
        if flags == NULL {
            return visit(ScalarRecordEvent::Leaf {
                field,
                value: BorrowedScalarLeaf::Null,
                depth: 1,
            })
            .map_err(ScalarVisitError::Callback);
        }
        if !matches!(
            field.value_type,
            ScalarValueType::List(_) | ScalarValueType::Map { .. } | ScalarValueType::Struct(_)
        ) {
            let value = BorrowedScalarLeaf::decode(self.schema, self.record)?;
            return visit(ScalarRecordEvent::Leaf {
                field,
                value,
                depth: 1,
            })
            .map_err(ScalarVisitError::Callback);
        }
        let mut reader = Reader {
            bytes: &self.record[SCALAR_LEAF_HEADER_BYTES..],
            position: 0,
        };
        reader.body(field, 1, &mut visit)?;
        if reader.position != reader.bytes.len() {
            return Err(ScalarLeafError::MalformedRecord.into());
        }
        Ok(())
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'record> Reader<'record> {
    fn take(&mut self, len: usize) -> Result<&'record [u8], ScalarLeafError> {
        let end = self
            .position
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(ScalarLeafError::MalformedRecord)?;
        let bytes = &self.bytes[self.position..end];
        self.position = end;
        Ok(bytes)
    }
    fn count(&mut self) -> Result<usize, ScalarLeafError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("four bytes")) as usize)
    }
    fn node<'schema, E>(
        &mut self,
        field: &'schema ScalarField,
        depth: usize,
        visit: &mut impl FnMut(ScalarRecordEvent<'schema, 'record>) -> Result<(), E>,
    ) -> Result<(), ScalarVisitError<E>> {
        match self.take(1)?[0] {
            0 => self.body(field, depth, visit),
            1 if field.nullable || matches!(field.value_type, ScalarValueType::Null) => {
                visit(ScalarRecordEvent::Leaf {
                    field,
                    value: BorrowedScalarLeaf::Null,
                    depth,
                })
                .map_err(ScalarVisitError::Callback)
            }
            1 => Err(ScalarLeafError::Nullability.into()),
            _ => Err(ScalarLeafError::MalformedRecord.into()),
        }
    }
    fn body<'schema, E>(
        &mut self,
        field: &'schema ScalarField,
        depth: usize,
        visit: &mut impl FnMut(ScalarRecordEvent<'schema, 'record>) -> Result<(), E>,
    ) -> Result<(), ScalarVisitError<E>> {
        use ScalarValueType as T;
        if depth > crate::RootProfileV1::MAX_DEPTH {
            return Err(ScalarLeafError::MalformedRecord.into());
        }
        match &field.value_type {
            T::List(item) => {
                let count = self.count()?;
                if count > self.bytes.len() - self.position {
                    return Err(ScalarLeafError::MalformedRecord.into());
                }
                visit(ScalarRecordEvent::Begin { field, depth })
                    .map_err(ScalarVisitError::Callback)?;
                for _ in 0..count {
                    self.node(item, depth + 1, visit)?;
                }
                visit(ScalarRecordEvent::End { field, depth }).map_err(ScalarVisitError::Callback)
            }
            T::Map { key, value } => {
                let count = self.count()?;
                if count > (self.bytes.len() - self.position) / 2 {
                    return Err(ScalarLeafError::MalformedRecord.into());
                }
                visit(ScalarRecordEvent::Begin { field, depth })
                    .map_err(ScalarVisitError::Callback)?;
                for _ in 0..count {
                    self.node(key, depth + 1, visit)?;
                    self.node(value, depth + 1, visit)?;
                }
                visit(ScalarRecordEvent::End { field, depth }).map_err(ScalarVisitError::Callback)
            }
            T::Struct(fields) => {
                visit(ScalarRecordEvent::Begin { field, depth })
                    .map_err(ScalarVisitError::Callback)?;
                for named in fields {
                    self.node(&named.field, depth + 1, visit)?;
                }
                visit(ScalarRecordEvent::End { field, depth }).map_err(ScalarVisitError::Callback)
            }
            _ => {
                let len = match field.value_type {
                    T::Boolean => 1,
                    T::SignedInteger(bits) => usize::from(bits / 8),
                    T::LargeInt | T::Decimal { bits: 128, .. } => 16,
                    T::Decimal { bits: 256, .. } => 32,
                    T::Float32 | T::Date => 4,
                    T::Float64 | T::TimeMicros | T::Timestamp { .. } => 8,
                    T::String | T::Json | T::Binary | T::Variant | T::Opaque(_) => self.count()?,
                    _ => return Err(ScalarLeafError::MalformedRecord.into()),
                };
                let value = BorrowedScalarLeaf::decode_payload(field, self.take(len)?)?;
                visit(ScalarRecordEvent::Leaf {
                    field,
                    value,
                    depth,
                })
                .map_err(ScalarVisitError::Callback)
            }
        }
    }
}
