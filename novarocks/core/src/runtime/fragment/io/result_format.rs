use arrow::array::ArrayRef;
use novarocks_types::PrimitiveType;
use novarocks_types::arrow_primitive::arrow_field_to_primitive;

use crate::common::result_batch::ResultBatch;
use crate::common::util::{
    FieldRenderSchema, http_json_row_from_arrays_with_primitives,
    mysql_text_row_from_arrays_with_primitives,
};
use crate::exec::chunk::Chunk;

use super::{ResultPresentation, ResultProjection};

fn columns_for_projections(
    chunk: &Chunk,
    projections: &[ResultProjection],
) -> Result<Vec<ArrayRef>, String> {
    projections
        .iter()
        .map(|projection| chunk.column_by_slot_id(projection.slot_id()))
        .collect()
}

fn primitives_for_projections(projections: &[ResultProjection]) -> Vec<PrimitiveType> {
    projections
        .iter()
        .map(|projection| projection.primitive())
        .collect()
}

fn field_schemas_for_projections(projections: &[ResultProjection]) -> Vec<FieldRenderSchema> {
    projections
        .iter()
        .map(|projection| projection.field_schema().clone())
        .collect()
}

fn primitives_for_chunk_fields(chunk: &Chunk) -> Vec<PrimitiveType> {
    chunk
        .chunk_schema()
        .slots()
        .iter()
        .map(|slot| arrow_field_to_primitive(slot.field()).unwrap_or(PrimitiveType::Invalid))
        .collect()
}

fn field_schemas_for_chunk_fields(chunk: &Chunk) -> Vec<FieldRenderSchema> {
    chunk
        .chunk_schema()
        .slots()
        .iter()
        .map(|slot| FieldRenderSchema::from_field(slot.field()))
        .collect()
}

pub fn build_result_batch(
    chunk: &Chunk,
    projections: Option<&[ResultProjection]>,
    presentation: ResultPresentation,
) -> Result<ResultBatch, String> {
    if presentation == ResultPresentation::Statistic {
        return Err(
            "STATISTIC result presentation is owned by the Compat result adapter".to_string(),
        );
    }

    let (columns, primitives, field_schemas) = match projections.filter(|value| !value.is_empty()) {
        Some(projections) => (
            columns_for_projections(chunk, projections)?,
            primitives_for_projections(projections),
            field_schemas_for_projections(projections),
        ),
        None => (
            chunk.columns().to_vec(),
            primitives_for_chunk_fields(chunk),
            field_schemas_for_chunk_fields(chunk),
        ),
    };
    let mut batch = ResultBatch::empty();
    for row in 0..chunk.len() {
        let encoded = match presentation {
            ResultPresentation::MysqlText => mysql_text_row_from_arrays_with_primitives(
                &columns,
                row,
                Some(&primitives),
                Some(&field_schemas),
            )?,
            ResultPresentation::HttpJson => http_json_row_from_arrays_with_primitives(
                &columns,
                row,
                Some(&primitives),
                Some(&field_schemas),
            )?,
            ResultPresentation::Statistic => unreachable!("checked above"),
        };
        batch.rows.push(encoded);
    }
    Ok(batch)
}

pub fn empty_result_batch(presentation: ResultPresentation) -> Result<ResultBatch, String> {
    match presentation {
        ResultPresentation::MysqlText | ResultPresentation::HttpJson => Ok(ResultBatch::empty()),
        ResultPresentation::Statistic => {
            Err("STATISTIC result presentation is owned by the Compat result adapter".to_string())
        }
    }
}
