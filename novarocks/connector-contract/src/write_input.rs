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
use crate::owned_copy::{OwnedCopy, PlainCopy};
use crate::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorWriteFieldToken,
};
use arrow_schema::Field;
use novarocks_type_contract::owned_resources::hashmap;
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
    /// Exact roles, order, field tokens and complete Arrow fields, with observed
    /// nested schema/metadata work and no field copying or packed-role guess.
    /// Both shapes come from validated writer drafts, preserving the writer
    /// owner's depth/aggregate-byte domain rather than a new type-node gate.
    pub(crate) fn same_layout_observed<E>(
        &self,
        other: &Self,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        let same_roles = match (self, other) {
            (Self::Data { fields: left }, Self::Data { fields: right }) => {
                left.len() == right.len()
            }
            (
                Self::EqualityDelete {
                    equality_fields: left,
                },
                Self::EqualityDelete {
                    equality_fields: right,
                },
            ) => left.len() == right.len(),
            (
                Self::RowLineage {
                    data_fields: left,
                    row_identity_fields: left_identity,
                },
                Self::RowLineage {
                    data_fields: right,
                    row_identity_fields: right_identity,
                },
            ) => left.len() == right.len() && left_identity.len() == right_identity.len(),
            (
                Self::PositionDelete {
                    identity_fields: left,
                    partition_source_fields: left_partition,
                },
                Self::PositionDelete {
                    identity_fields: right,
                    partition_source_fields: right_partition,
                },
            )
            | (
                Self::DeletionVector {
                    identity_fields: left,
                    partition_source_fields: left_partition,
                },
                Self::DeletionVector {
                    identity_fields: right,
                    partition_source_fields: right_partition,
                },
            ) => left.len() == right.len() && left_partition.len() == right_partition.len(),
            _ => false,
        };
        if !same_roles {
            return Ok(false);
        }
        for (left, right) in self.fields_iter().zip(other.fields_iter()) {
            observe()?;
            if left.token() != right.token()
                || !novarocks_type_contract::arrow_fields_exact_observed::<E>(
                    left.field(),
                    right.field(),
                    &mut observe,
                )?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn role_vectors(&self) -> [Option<&Vec<ConnectorWriteFieldBinding>>; 2] {
        match self {
            Self::Data { fields } => [Some(fields), None],
            Self::RowLineage {
                data_fields,
                row_identity_fields,
            } => [Some(data_fields), Some(row_identity_fields)],
            Self::PositionDelete {
                identity_fields,
                partition_source_fields,
            }
            | Self::DeletionVector {
                identity_fields,
                partition_source_fields,
            } => [Some(identity_fields), Some(partition_source_fields)],
            Self::EqualityDelete { equality_fields } => [Some(equality_fields), None],
        }
    }

    /// Admit each original role's reserve and possible Vec-to-Box shrink before
    /// validation. Role Vec backings are independently owned; nested shared
    /// fields are deliberately absent from this necessary source lower floor.
    pub(crate) fn preflight_owned_roles<O: OwnedCopy>(
        &self,
        context: &mut O,
    ) -> Result<(), O::Error> {
        let mut floor = size_of::<Self>();
        let mut fields = 0;
        for role in self.role_vectors().into_iter().flatten() {
            floor = context.add(
                floor,
                context.mul(role.capacity(), size_of::<ConnectorWriteFieldBinding>())?,
            )?;
            fields = context.add(fields, role.len())?;
            context.array::<ConnectorWriteFieldBinding>(role.len(), 2)?;
        }
        context.source_floor(floor)?;
        // The two passes move inline outputs and the materializing pass may
        // also move each retained binding during the original trim operation.
        context.work(context.add(
            size_of::<Self>(),
            context.mul(
                context.mul(fields, size_of::<ConnectorWriteFieldBinding>())?,
                3,
            )?,
        )?)
    }

    /// The constructor calls the O(1) role preflight once before validation.
    /// Counting visits the original field-copy grammar without output objects.
    pub(crate) fn owned_bounded_core<O: OwnedCopy>(
        &self,
        context: &mut O,
    ) -> Result<Option<Self>, O::Error> {
        fn role<O: OwnedCopy>(
            fields: &[ConnectorWriteFieldBinding],
            context: &mut O,
        ) -> Result<Option<Vec<ConnectorWriteFieldBinding>>, O::Error> {
            let materializes = context.materializes();
            let mut output = Vec::new();
            if materializes {
                context.flush()?;
                let reserved = output.try_reserve_exact(fields.len());
                context.reserve_exit(reserved)?;
            }
            for binding in fields {
                let field = crate::schema::owned_field_core(binding.field(), context)?;
                if materializes {
                    let field = field.ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::Internal,
                            "writer owned copy did not materialize a field",
                        )
                    })?;
                    output.push(ConnectorWriteFieldBinding::new(binding.token(), field));
                }
                context.step()?;
            }
            if !materializes {
                return Ok(None);
            }
            context.flush()?;
            // Preserve the original role-local trim; its conservative second
            // request and inline moves were admitted by preflight_owned_roles.
            let output = output.into_boxed_slice().into_vec();
            context.step()?;
            context.flush()?;
            Ok(Some(output))
        }
        let [first, second] = self.role_vectors();
        let first = role(
            first.expect("every write input has its first role"),
            context,
        )?;
        let second = match second {
            Some(fields) => role(fields, context)?,
            None => None,
        };
        let Some(first) = first else {
            return Ok(None);
        };
        let result = match self {
            Self::Data { .. } => Self::Data { fields: first },
            Self::EqualityDelete { .. } => Self::EqualityDelete {
                equality_fields: first,
            },
            Self::RowLineage { .. } => Self::RowLineage {
                data_fields: first,
                row_identity_fields: second.expect("second role was copied"),
            },
            Self::PositionDelete { .. } => Self::PositionDelete {
                identity_fields: first,
                partition_source_fields: second.expect("second role was copied"),
            },
            Self::DeletionVector { .. } => Self::DeletionVector {
                identity_fields: first,
                partition_source_fields: second.expect("second role was copied"),
            },
        };
        context.step()?;
        Ok(Some(result))
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
        self.validate_with(&mut PlainCopy)
    }

    pub(crate) fn validate_with<O: OwnedCopy>(&self, context: &mut O) -> Result<(), O::Error> {
        let [first, second] = self.role_vectors();
        let count = context.add(
            first.expect("first role exists").len(),
            second.map_or(0, Vec::len),
        )?;
        if count == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write input shape must contain at least one field",
            )
            .into());
        }
        let token_buckets = context.table::<ConnectorWriteFieldToken, ()>(count)?;
        let name_buckets = context.table::<String, ()>(count)?;
        // Both temporary sets are destroyed before schema validation, also
        // after a duplicate or interruption. These are closed Token/String
        // destructors; allocator release cost is not a cooperative CPU grant.
        context.table_cleanup::<ConnectorWriteFieldToken, ()>(count, 64)?;
        context.table_cleanup::<String, ()>(count, 128)?;
        context.flush()?;
        let mut tokens = HashSet::new();
        let reserved = tokens.try_reserve(count);
        context.reserve_exit(reserved)?;
        context.flush()?;
        let mut names = HashSet::new();
        let reserved = names.try_reserve(count);
        context.reserve_exit(reserved)?;
        let mut longest_name = 0;
        for binding in self.fields_iter() {
            // Token Hash is the derived transparent [u8;32] body. Each table
            // had one fresh reserve, so its admitted bucket bound stays valid.
            if context.source_invoice().is_some() {
                let token_work =
                    hashmap::byte_array32_operations_work_upper_bound(token_buckets, 1)
                        .map_err(|error| context.hash_error(error))?;
                context.work(token_work)?;
            }
            context.flush()?;
            let unique_token = tokens.insert(binding.token);
            context.step()?;
            context.flush()?;
            // Preserve the original short circuit: a duplicate token never
            // copies the name, even before the later writer name-length law.
            if !unique_token {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write input shape contains a duplicate field token or name",
                )
                .into());
            }
            let name = binding.field.name();
            longest_name = longest_name.max(name.len());
            if context.source_invoice().is_some() {
                let name_work = hashmap::string_operations_work_upper_bound(
                    name_buckets,
                    1,
                    name.len(),
                    longest_name,
                )
                .map_err(|error| context.hash_error(error))?;
                context.work(name_work)?;
            }
            let name = context.real_string(name)?;
            context.flush()?;
            let unique_name = names.insert(name);
            context.step()?;
            context.flush()?;
            if !unique_name {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector write input shape contains a duplicate field token or name",
                )
                .into());
            }
        }
        Ok(())
    }

    pub fn fields(&self) -> Vec<&ConnectorWriteFieldBinding> {
        self.fields_iter().collect()
    }
}

#[cfg(test)]
#[path = "write_input/tests.rs"]
mod tests;
