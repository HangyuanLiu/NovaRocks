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

//! Exact copies of the closed scan-source DTO family. These provider facts
//! retain their original bytes and never supply execution type semantics.
//! Every actual Vec/String/Box construction uses the encoding caller's request.

use super::{TypeEncodingResources, TypeEncodingResult};
use novarocks_proto_models::{catalog, connector_common, connector_read, plan};

pub(crate) trait CopySourceObserved: Sized {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H>;
}
macro_rules! copy_scalars {
    ($($ty:ty),*) => {$(impl CopySourceObserved for $ty {
        fn copy_source_observed<H>(&self, resources: &mut TypeEncodingResources<'_,H>) -> TypeEncodingResult<Self,H> {
            resources.work.step()?;
            Ok(*self)
        }
    })*};
}
copy_scalars!(u8, u16, u32, u64, i8, i16, i32, i64, bool, f32, f64);
impl CopySourceObserved for String {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.copy_string(self)
    }
}
impl<T: CopySourceObserved> CopySourceObserved for Option<T> {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        self.as_ref()
            .map(|value| value.copy_source_observed(resources))
            .transpose()
    }
}
impl<T: CopySourceObserved> CopySourceObserved for Vec<T> {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        let mut output = resources.vector(self.len())?;
        for value in self {
            output.push(value.copy_source_observed(resources)?);
        }
        Ok(output)
    }
}
impl<T: CopySourceObserved> CopySourceObserved for Box<T> {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.request_fixed::<T>()?;
        let value = self.as_ref().copy_source_observed(resources)?;
        let output = Box::new(value);
        resources.work.step()?;
        Ok(output)
    }
}

impl CopySourceObserved for plan::ScanSource {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            kind: self.kind.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for plan::scan_source::Kind {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(match self {
            Self::TypedConnectorRead(value) => {
                Self::TypedConnectorRead(value.copy_source_observed(resources)?)
            }
        })
    }
}

impl CopySourceObserved for connector_read::ConnectorTableScanSource {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            table: self.table.copy_source_observed(resources)?,
            assignments: self.assignments.copy_source_observed(resources)?,
            enforced_predicate: self.enforced_predicate.copy_source_observed(resources)?,
            unenforced_predicate: self.unenforced_predicate.copy_source_observed(resources)?,
            remaining_expression: self.remaining_expression.copy_source_observed(resources)?,
            dynamic_filters: self.dynamic_filters.copy_source_observed(resources)?,
            max_batch_rows: self.max_batch_rows.copy_source_observed(resources)?,
            max_batch_bytes: self.max_batch_bytes.copy_source_observed(resources)?,
            work_source: self.work_source.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::CatalogTableHandle {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            catalog_handle: self.catalog_handle.copy_source_observed(resources)?,
            transaction: self.transaction.copy_source_observed(resources)?,
            relation: self.relation.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for catalog::CatalogHandle {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            catalog_name: self.catalog_name.copy_source_observed(resources)?,
            version: self.version.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ConnectorTransactionHandle {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            provider_payload: self.provider_payload.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_common::ConnectorEncodedPayload {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            header: self.header.copy_source_observed(resources)?,
            payload: self.payload.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_common::ConnectorEnvelopeHeader {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            provider_id: self.provider_id.copy_source_observed(resources)?,
            catalog: self.catalog.copy_source_observed(resources)?,
            category: self.category.copy_source_observed(resources)?,
            codec_revision: self.codec_revision.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::catalog_table_handle::Relation {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(match self {
            Self::Table(value) => Self::Table(value.copy_source_observed(resources)?),
            Self::TableFunction(value) => {
                Self::TableFunction(value.copy_source_observed(resources)?)
            }
            Self::ChangeWindow(value) => Self::ChangeWindow(value.copy_source_observed(resources)?),
            Self::SystemTable(value) => Self::SystemTable(value.copy_source_observed(resources)?),
            Self::TableExecute(value) => Self::TableExecute(value.copy_source_observed(resources)?),
            Self::MergeTable(value) => Self::MergeTable(value.copy_source_observed(resources)?),
        })
    }
}

impl CopySourceObserved for connector_read::ConnectorTableHandle {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            provider_payload: self.provider_payload.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ConnectorTableFunctionHandle {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            provider_payload: self.provider_payload.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ConnectorChangeWindowHandle {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            provider_payload: self.provider_payload.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ConnectorSystemTableReference {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            provider_payload: self.provider_payload.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ConnectorTableExecuteHandle {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            provider_payload: self.provider_payload.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ConnectorMergeTableHandle {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            provider_payload: self.provider_payload.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ScanAssignment {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            variable: self.variable.copy_source_observed(resources)?,
            column: self.column.copy_source_observed(resources)?,
            value_type: self.value_type.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ColumnHandle {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            provider_payload: self.provider_payload.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ValueType {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            kind: self.kind.copy_source_observed(resources)?,
            decimal_precision: self.decimal_precision.copy_source_observed(resources)?,
            decimal_scale: self.decimal_scale.copy_source_observed(resources)?,
            fixed_length: self.fixed_length.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::TupleDomain {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            none: self.none.copy_source_observed(resources)?,
            column_domains: self.column_domains.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ColumnDomain {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            column: self.column.copy_source_observed(resources)?,
            domain: self.domain.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::Domain {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            values: self.values.copy_source_observed(resources)?,
            null_allowed: self.null_allowed.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ValueSet {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            value_type: self.value_type.copy_source_observed(resources)?,
            ranges: self.ranges.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::Range {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            low: self.low.copy_source_observed(resources)?,
            high: self.high.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::Bound {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            kind: self.kind.copy_source_observed(resources)?,
            value: self.value.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::Value {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            value: self.value.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::value::Value {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(match self {
            Self::Boolean(value) => Self::Boolean(value.copy_source_observed(resources)?),
            Self::TinyInt(value) => Self::TinyInt(value.copy_source_observed(resources)?),
            Self::SmallInt(value) => Self::SmallInt(value.copy_source_observed(resources)?),
            Self::Integer(value) => Self::Integer(value.copy_source_observed(resources)?),
            Self::BigInt(value) => Self::BigInt(value.copy_source_observed(resources)?),
            Self::Real(value) => Self::Real(value.copy_source_observed(resources)?),
            Self::DoubleValue(value) => Self::DoubleValue(value.copy_source_observed(resources)?),
            Self::Decimal(value) => Self::Decimal(value.copy_source_observed(resources)?),
            Self::Date(value) => Self::Date(value.copy_source_observed(resources)?),
            Self::TimeMicros(value) => Self::TimeMicros(value.copy_source_observed(resources)?),
            Self::TimestampMicros(value) => {
                Self::TimestampMicros(value.copy_source_observed(resources)?)
            }
            Self::TimestampTzMicros(value) => {
                Self::TimestampTzMicros(value.copy_source_observed(resources)?)
            }
            Self::TimestampNanos(value) => {
                Self::TimestampNanos(value.copy_source_observed(resources)?)
            }
            Self::TimestampTzNanos(value) => {
                Self::TimestampTzNanos(value.copy_source_observed(resources)?)
            }
            Self::TimestampMillis(value) => {
                Self::TimestampMillis(value.copy_source_observed(resources)?)
            }
            Self::Varchar(value) => Self::Varchar(value.copy_source_observed(resources)?),
            Self::Varbinary(value) => Self::Varbinary(value.copy_source_observed(resources)?),
            Self::Uuid(value) => Self::Uuid(value.copy_source_observed(resources)?),
            Self::Fixed(value) => Self::Fixed(value.copy_source_observed(resources)?),
        })
    }
}

impl CopySourceObserved for connector_read::DecimalValue {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            unscaled: self.unscaled.copy_source_observed(resources)?,
            precision: self.precision.copy_source_observed(resources)?,
            scale: self.scale.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::ConnectorExpression {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            node: self.node.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::connector_expression::Node {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(match self {
            Self::Constant(value) => Self::Constant(value.copy_source_observed(resources)?),
            Self::Variable(value) => Self::Variable(value.copy_source_observed(resources)?),
            Self::FieldDereference(value) => {
                Self::FieldDereference(value.copy_source_observed(resources)?)
            }
            Self::Call(value) => Self::Call(value.copy_source_observed(resources)?),
        })
    }
}

impl CopySourceObserved for connector_read::ConstantExpression {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            value_type: self.value_type.copy_source_observed(resources)?,
            value: self.value.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::VariableExpression {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            name: self.name.copy_source_observed(resources)?,
            value_type: self.value_type.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::FieldDereferenceExpression {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            target: self.target.copy_source_observed(resources)?,
            field_index: self.field_index.copy_source_observed(resources)?,
            value_type: self.value_type.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::CallExpression {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            function_name: self.function_name.copy_source_observed(resources)?,
            value_type: self.value_type.copy_source_observed(resources)?,
            arguments: self.arguments.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for connector_read::DynamicFilterBinding {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            filter_id: self.filter_id.copy_source_observed(resources)?,
            variable: self.variable.copy_source_observed(resources)?,
        })
    }
}

impl CopySourceObserved for novarocks_proto_models::connector_write::ConnectorWriterHandle {
    fn copy_source_observed<H>(
        &self,
        resources: &mut TypeEncodingResources<'_, H>,
    ) -> TypeEncodingResult<Self, H> {
        resources.work.step()?;
        Ok(Self {
            provider_payload: self.provider_payload.copy_source_observed(resources)?,
        })
    }
}
