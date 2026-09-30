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

//! Pure provider-issued input identities and role-preserving field shapes.
use crate::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorWriteFieldToken,
};
use arrow_schema::Field;
use std::collections::HashSet;

/// The exact provider generation a write value belongs to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteBinding {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
}

impl ConnectorWriteBinding {
    pub const fn new(
        descriptor: ConnectorInstanceDescriptor,
        catalog_handle: CatalogHandle,
    ) -> Self {
        Self {
            descriptor,
            catalog_handle,
        }
    }

    pub const fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorWriteFieldBinding {
    token: ConnectorWriteFieldToken,
    field: Field,
}

impl PartialEq for ConnectorWriteFieldBinding {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token && crate::arrow_fields_exact(&self.field, &other.field)
    }
}
impl Eq for ConnectorWriteFieldBinding {}

impl ConnectorWriteFieldBinding {
    pub fn new(token: ConnectorWriteFieldToken, field: Field) -> Self {
        Self { token, field }
    }

    pub const fn token(&self) -> ConnectorWriteFieldToken {
        self.token
    }

    pub fn field(&self) -> &Field {
        &self.field
    }
}

/// Provider-signed counterpart to the SQL admission input request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorWriteInputShape {
    Data {
        fields: Vec<ConnectorWriteFieldBinding>,
    },
    RowLineage {
        data_fields: Vec<ConnectorWriteFieldBinding>,
        row_identity_fields: Vec<ConnectorWriteFieldBinding>,
    },
    PositionDelete {
        identity_fields: Vec<ConnectorWriteFieldBinding>,
        partition_source_fields: Vec<ConnectorWriteFieldBinding>,
    },
    DeletionVector {
        identity_fields: Vec<ConnectorWriteFieldBinding>,
        partition_source_fields: Vec<ConnectorWriteFieldBinding>,
    },
    EqualityDelete {
        equality_fields: Vec<ConnectorWriteFieldBinding>,
    },
}

impl ConnectorWriteInputShape {
    pub(crate) fn owned_bounded(&self) -> Result<Self, ConnectorError> {
        let copy = |fields: &[ConnectorWriteFieldBinding]| {
            fields
                .iter()
                .map(|binding| {
                    crate::schema::owned_field(binding.field())
                        .map(|field| ConnectorWriteFieldBinding::new(binding.token(), field))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(|fields| fields.into_boxed_slice().into_vec())
        };
        Ok(match self {
            Self::Data { fields } => Self::Data {
                fields: copy(fields)?,
            },
            Self::RowLineage {
                data_fields,
                row_identity_fields,
            } => Self::RowLineage {
                data_fields: copy(data_fields)?,
                row_identity_fields: copy(row_identity_fields)?,
            },
            Self::PositionDelete {
                identity_fields,
                partition_source_fields,
            } => Self::PositionDelete {
                identity_fields: copy(identity_fields)?,
                partition_source_fields: copy(partition_source_fields)?,
            },
            Self::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => Self::DeletionVector {
                identity_fields: copy(identity_fields)?,
                partition_source_fields: copy(partition_source_fields)?,
            },
            Self::EqualityDelete { equality_fields } => Self::EqualityDelete {
                equality_fields: copy(equality_fields)?,
            },
        })
    }

    pub fn field_count(&self) -> usize {
        match self {
            Self::Data { fields } => fields.len(),
            Self::RowLineage {
                data_fields,
                row_identity_fields,
            } => data_fields.len().saturating_add(row_identity_fields.len()),
            Self::PositionDelete {
                identity_fields,
                partition_source_fields,
            }
            | Self::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => identity_fields
                .len()
                .saturating_add(partition_source_fields.len()),
            Self::EqualityDelete { equality_fields } => equality_fields.len(),
        }
    }

    pub fn fields_iter(&self) -> impl Iterator<Item = &ConnectorWriteFieldBinding> {
        let groups: [&[ConnectorWriteFieldBinding]; 2] = match self {
            Self::Data { fields } => [fields, &[]],
            Self::RowLineage {
                data_fields,
                row_identity_fields,
            } => [data_fields, row_identity_fields],
            Self::PositionDelete {
                identity_fields,
                partition_source_fields,
            }
            | Self::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => [identity_fields, partition_source_fields],
            Self::EqualityDelete { equality_fields } => [equality_fields, &[]],
        };
        groups.into_iter().flatten()
    }

    pub fn validate(&self) -> Result<(), ConnectorError> {
        let mut tokens = HashSet::new();
        let mut names = HashSet::new();
        if self.field_count() == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write input shape must contain at least one field",
            ));
        }
        for binding in self.fields_iter() {
            if !tokens.insert(binding.token) || !names.insert(binding.field.name().to_owned()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write input shape contains a duplicate field token or name",
                ));
            }
        }
        Ok(())
    }

    pub fn fields(&self) -> Vec<&ConnectorWriteFieldBinding> {
        self.fields_iter().collect()
    }
}
