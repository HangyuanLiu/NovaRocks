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

//! Borrowed actual reader ports; this adapter owns no scope or policy.
use arrow::{array::ArrayData, datatypes::Field};
use novarocks_constant_contract::{
    ConstantCreationFailure, ConstantOwnerResourceFacts, ConstantPolicy, ConstantPool,
    ConstantSourceOwner,
};
use novarocks_type_contract::{CompileCheckpoints, ValueSchemaCreationScope};
use std::sync::Arc;

pub(crate) enum ReaderCreation<'a, E> {
    Single(&'a mut dyn ValueSchemaCreationScope<E>),
    Paired {
        temporary: &'a mut dyn ValueSchemaCreationScope<E>,
        retained: &'a mut dyn ValueSchemaCreationScope<E>,
        pool: &'a mut dyn ValueSchemaCreationScope<E>,
    },
}
impl<'a, E> ReaderCreation<'a, E> {
    pub(crate) fn operation(
        &mut self,
        complete: usize,
        payload: usize,
        result_headers: usize,
        pool_data_headers: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), E>
    where
        E: From<novarocks_type_contract::CompileControlError>,
    {
        match self {
            Self::Single(port) => port.observe_opaque_upper_bound(complete),
            Self::Paired {
                temporary,
                retained,
                pool,
            } => {
                let result = payload
                    .checked_add(result_headers)
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
                let scratch = complete
                    .checked_sub(result)
                    .and_then(|bytes| bytes.checked_sub(pool_data_headers))
                    .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
                work.flush()?;
                if scratch != 0 {
                    temporary.observe_opaque_upper_bound(scratch)?;
                }
                work.flush()?;
                if result != 0 {
                    retained.observe_opaque_upper_bound(result)?;
                }
                work.flush()?;
                if pool_data_headers != 0 {
                    pool.observe_opaque_upper_bound(pool_data_headers)?;
                }
                Ok(())
            }
        }
    }
    pub(crate) fn pool(
        self,
        field: Arc<Field>,
        value_type: novarocks_type_contract::FunctionValueType,
        data: ArrayData,
        policy: ConstantPolicy,
        source: ConstantSourceOwner,
        admit: &mut impl FnMut(&ConstantOwnerResourceFacts) -> Result<(), ConstantCreationFailure<E>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ConstantPool, ConstantCreationFailure<E>> {
        match self {
            Self::Single(creation) => ConstantPool::try_new_with_creator_in(
                field, value_type, data, policy, source, creation, admit, work,
            ),
            Self::Paired {
                temporary,
                retained,
                pool,
            } => ConstantPool::try_new_with_paired_creator_in(
                field,
                value_type,
                data,
                policy,
                source,
                temporary,
                pool,
                retained.storage_lifetime(),
                admit,
                work,
            ),
        }
    }
}

/// Stack-only nominal mapping. It reborrows the same actual scope and owns no
/// factory, controller, receipt, counter or failure side channel.
pub(crate) struct MappedScope<'a, H>(pub &'a mut dyn ValueSchemaCreationScope<H>);
impl<H>
    ValueSchemaCreationScope<
        crate::host_projection_v2::ProjectionFailure<super::FlatReaderError, H>,
    > for MappedScope<'_, H>
{
    fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        self.0.storage_lifetime()
    }
    fn request_layout(
        &mut self,
        layout: std::alloc::Layout,
    ) -> Result<(), crate::host_projection_v2::ProjectionFailure<super::FlatReaderError, H>> {
        self.0
            .request_layout(layout)
            .map_err(crate::host_projection_v2::ProjectionFailure::Host)
    }
    fn observe_opaque_upper_bound(
        &mut self,
        bytes: usize,
    ) -> Result<(), crate::host_projection_v2::ProjectionFailure<super::FlatReaderError, H>> {
        self.0
            .observe_opaque_upper_bound(bytes)
            .map_err(crate::host_projection_v2::ProjectionFailure::Host)
    }
}
