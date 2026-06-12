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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::type_relation::{CompatibilityPolicy, relate, retag_column};

pub(crate) fn is_execution_data_type_compatible(expected: &DataType, actual: &DataType) -> bool {
    // The one type relation governs execution-layer compatibility: same-scale
    // decimal (precision may differ), timestamp, utf8<->binary, and recursion into
    // list/largelist/map/struct. The former List<->Struct<1> arm was a StarRocks-FE
    // intermediate-state bridge (added by #295 for 1FE3BE); NovaRocks owns its own
    // array_agg intermediate shape, so it is no longer needed.
    relate(expected, actual, CompatibilityPolicy::SameScaleWiden).is_ok()
}

fn align_field_to_data_type(
    field: &Field,
    actual_type: &DataType,
    actual_nullable: bool,
    context: &str,
) -> Result<Field, String> {
    if !is_execution_data_type_compatible(field.data_type(), actual_type) {
        return Err(format!(
            "{context} schema field type mismatch: expected {:?}, got {:?}",
            field.data_type(),
            actual_type
        ));
    }
    if field.data_type() == actual_type && (field.is_nullable() || !actual_nullable) {
        return Ok(field.clone());
    }
    Ok(Field::new(
        field.name(),
        actual_type.clone(),
        field.is_nullable() || actual_nullable,
    )
    .with_metadata(field.metadata().clone()))
}

pub(crate) fn align_fields_to_arrays(
    fields: &Fields,
    arrays: &[ArrayRef],
    context: &str,
) -> Result<Fields, String> {
    if fields.len() != arrays.len() {
        return Err(format!(
            "{context} schema/array length mismatch: schema_fields={} arrays={}",
            fields.len(),
            arrays.len()
        ));
    }
    let fields = fields
        .iter()
        .zip(arrays.iter())
        .map(|(field, array)| {
            align_field_to_data_type(
                field.as_ref(),
                array.data_type(),
                array.null_count() > 0,
                context,
            )
            .map(Arc::new)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(fields.into())
}

pub(crate) fn align_schema_to_arrays(
    schema: &SchemaRef,
    arrays: &[ArrayRef],
    context: &str,
) -> Result<SchemaRef, String> {
    let fields = align_fields_to_arrays(schema.fields(), arrays, context)?;
    if fields == *schema.fields() {
        return Ok(Arc::clone(schema));
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

pub(crate) fn align_schema_to_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    context: &str,
) -> Result<SchemaRef, String> {
    let mut aligned = Arc::clone(schema);
    for batch in batches {
        aligned = align_schema_to_arrays(&aligned, batch.columns(), context)?;
    }
    Ok(aligned)
}

pub(crate) fn normalize_array_to_data_type(
    array: &ArrayRef,
    target_type: &DataType,
    context: &str,
) -> Result<ArrayRef, String> {
    // Metadata-only retag of the column to target_type, via the one type-relation
    // primitive (same-scale decimal / utf8<->binary / recursive). Replaces the
    // schema_compat-local decimal retag helpers.
    retag_column(array, target_type).map_err(|m| {
        format!(
            "{context} array type mismatch: expected {:?}, got {:?} ({:?})",
            target_type,
            array.data_type(),
            m.kind
        )
    })
}

pub(crate) fn normalize_batch_to_schema(
    schema: &SchemaRef,
    batch: &RecordBatch,
    context: &str,
) -> Result<RecordBatch, String> {
    if schema.fields().len() != batch.num_columns() {
        return Err(format!(
            "{context} schema/batch length mismatch: schema_fields={} batch_columns={}",
            schema.fields().len(),
            batch.num_columns()
        ));
    }
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns().iter())
        .map(|(field, array)| normalize_array_to_data_type(array, field.data_type(), context))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Decimal128Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use arrow::compute::concat_batches;

    use super::{
        align_schema_to_arrays, align_schema_to_batches, is_execution_data_type_compatible,
        normalize_batch_to_schema,
    };

    #[test]
    fn decimal_precision_is_compatible_only_when_scale_matches() {
        assert!(is_execution_data_type_compatible(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(38, 2)
        ));
        assert!(!is_execution_data_type_compatible(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(38, 3)
        ));
    }

    #[test]
    fn align_schema_to_arrays_uses_actual_decimal_precision() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(10, 2),
            false,
        )]));
        let array = Arc::new(
            Decimal128Array::from(vec![Some(123_i128)])
                .with_precision_and_scale(38, 2)
                .expect("decimal type"),
        ) as ArrayRef;

        let aligned = align_schema_to_arrays(&schema, &[array], "test").expect("align schema");
        assert_eq!(aligned.field(0).data_type(), &DataType::Decimal128(38, 2));
    }

    #[test]
    fn normalize_batch_to_schema_retags_decimal_for_concat() {
        let narrow_schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(8, 2),
            true,
        )]));
        let wide_schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            DataType::Decimal128(38, 2),
            true,
        )]));
        let narrow = RecordBatch::try_new(
            narrow_schema.clone(),
            vec![Arc::new(
                Decimal128Array::from(vec![Some(123_i128)])
                    .with_precision_and_scale(8, 2)
                    .expect("decimal type"),
            ) as ArrayRef],
        )
        .expect("narrow batch");
        let wide = RecordBatch::try_new(
            wide_schema,
            vec![Arc::new(
                Decimal128Array::from(vec![Some(456_i128)])
                    .with_precision_and_scale(38, 2)
                    .expect("decimal type"),
            ) as ArrayRef],
        )
        .expect("wide batch");

        let schema =
            align_schema_to_batches(&narrow_schema, &[narrow.clone(), wide.clone()], "test")
                .expect("align schema");
        let narrow = normalize_batch_to_schema(&schema, &narrow, "test").expect("normalize");
        let wide = normalize_batch_to_schema(&schema, &wide, "test").expect("normalize");
        concat_batches(&schema, [&narrow, &wide]).expect("concat");
    }
}
