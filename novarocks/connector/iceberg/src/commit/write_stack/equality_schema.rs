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

//! One schema-binding algorithm for legacy equality writers and pure preparation.

use arrow::datatypes::SchemaRef;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use super::domain::IcebergEqualityDeleteRecipe;
use crate::commit::EqualityDeleteColumn;

#[derive(Debug)]
pub(crate) enum EqualitySchemaError {
    Source(ConnectorError),
    Control(CompileControlError),
}
impl std::fmt::Display for EqualitySchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source(error) => std::fmt::Display::fmt(error, f),
            Self::Control(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for EqualitySchemaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}
impl From<CompileControlError> for EqualitySchemaError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

struct Work<'a>(Option<CompileCheckpoints<'a>>);
impl Work<'_> {
    fn step(&mut self) -> Result<(), EqualitySchemaError> {
        if let Some(work) = &mut self.0 {
            work.step()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), EqualitySchemaError> {
        if let Some(work) = &mut self.0 {
            work.flush()?;
        }
        Ok(())
    }
    fn opaque<T>(&mut self, operation: impl FnOnce() -> T) -> Result<T, EqualitySchemaError> {
        self.flush()?;
        let result = operation();
        self.flush()?;
        Ok(result)
    }
    fn equal(&mut self, left: &str, right: &str) -> Result<bool, EqualitySchemaError> {
        let same_length = left.len() == right.len();
        self.step()?;
        if !same_length {
            return Ok(false);
        }
        for (left, right) in left
            .as_bytes()
            .chunks(1024)
            .zip(right.as_bytes().chunks(1024))
        {
            let equal = left == right;
            self.step()?;
            if !equal {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn append(&mut self, output: &mut String, text: &str) -> Result<(), EqualitySchemaError> {
        let mut position = 0;
        while position < text.len() {
            let mut end = text.len().min(position.saturating_add(1024));
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            output.push_str(&text[position..end]);
            position = end;
            self.step()?;
        }
        Ok(())
    }
    fn copy(&mut self, text: &str) -> Result<String, EqualitySchemaError> {
        // This allocation is not a host grant. Only the completed copies are
        // cooperative; allocator behavior remains the caller's obligation.
        let mut output = self.opaque(|| String::with_capacity(text.len()))?;
        self.append(&mut output, text)?;
        Ok(output)
    }
}

/// Preserve the legacy writer's exact schema-binding and diagnostics.
pub(crate) fn resolve_equality_columns(
    recipe: &IcebergEqualityDeleteRecipe,
    expected_schema: &SchemaRef,
) -> Result<Vec<EqualityDeleteColumn>, ConnectorError> {
    match resolve_core(recipe, expected_schema, &mut Work(None)) {
        Ok(columns) => Ok(columns),
        Err(EqualitySchemaError::Source(error)) => Err(error),
        Err(EqualitySchemaError::Control(_)) => {
            unreachable!("legacy schema binding has no control port")
        }
    }
}

/// The pure compiler borrows the original control and never retries through
/// the legacy path. Debug rendering and Arrow cloning are opaque, finite
/// operations on the caller's admitted schema, observed before and after;
/// this does not prove their internal cooperation or allocation governance.
pub(crate) fn resolve_equality_columns_for_compile(
    recipe: &IcebergEqualityDeleteRecipe,
    expected_schema: &SchemaRef,
    control: &dyn PureCompileControl,
) -> Result<Vec<EqualityDeleteColumn>, EqualitySchemaError> {
    let mut work = Work(Some(CompileCheckpoints::try_new(
        control,
        CompilePhase::ProviderValidation,
    )?));
    let result = resolve_core(recipe, expected_schema, &mut work);
    if matches!(&result, Err(EqualitySchemaError::Control(_))) {
        return result;
    }
    work.flush()?;
    result
}

fn resolve_core(
    recipe: &IcebergEqualityDeleteRecipe,
    expected_schema: &SchemaRef,
    work: &mut Work<'_>,
) -> Result<Vec<EqualityDeleteColumn>, EqualitySchemaError> {
    let same_count = recipe.columns().len() == expected_schema.fields().len();
    work.step()?;
    if !same_count {
        let message = work.opaque(|| {
            format!(
                "Iceberg equality-delete handle names {} columns but its fragment input carries {}",
                recipe.columns().len(),
                expected_schema.fields().len()
            )
        })?;
        return Err(EqualitySchemaError::Source(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            message,
        )));
    }
    let mut columns = Vec::new();
    for (frozen, actual) in recipe.columns().iter().zip(expected_schema.fields()) {
        let name_matches = work.equal(frozen.name(), actual.name())?;
        let type_matches = if name_matches {
            let rendering = work.opaque(|| format!("{:?}", actual.data_type()))?;
            work.equal(frozen.data_type(), &rendering)?
        } else {
            false
        };
        let matches = name_matches && type_matches && frozen.nullable() == actual.is_nullable();
        work.step()?;
        if !matches {
            let mut message = String::new();
            work.append(&mut message, "Iceberg equality-delete column `")?;
            work.append(&mut message, frozen.name())?;
            work.append(&mut message, "` does not match fragment input `")?;
            work.append(&mut message, actual.name())?;
            work.append(&mut message, "`")?;
            return Err(EqualitySchemaError::Source(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                message,
            )));
        }
        let name = work.copy(frozen.name())?;
        let data_type = work.opaque(|| actual.data_type().clone())?;
        // Public input field metadata is not the target field-ID authority.
        // Retain the exact recipe ID and source order, including reordered keys.
        columns.push(EqualityDeleteColumn {
            name,
            field_id: frozen.field_id(),
            data_type,
            nullable: actual.is_nullable(),
        });
        work.step()?;
    }
    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use arrow::datatypes::{DataType, Field, Schema};

    use super::super::domain::IcebergEqualityDeleteColumnFacts;

    struct Control {
        calls: Mutex<Vec<u32>>,
        failure: Option<(usize, CompileControlError)>,
        positive: Option<CompileControlError>,
    }
    impl Control {
        fn accept() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                failure: None,
                positive: None,
            }
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::ProviderValidation);
            let mut calls = self.calls.lock().unwrap();
            calls.push(units);
            if let Some((call, error)) = self.failure
                && calls.len() == call
            {
                return Err(error);
            }
            if units == 256
                && let Some(error) = self.positive
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn causes() -> [CompileControlError; 3] {
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }
    fn recipe(fields: &[(&str, i32, DataType, bool)]) -> IcebergEqualityDeleteRecipe {
        IcebergEqualityDeleteRecipe::try_new(
            fields
                .iter()
                .map(|(name, id, ty, nullable)| {
                    IcebergEqualityDeleteColumnFacts::try_new(
                        (*name).to_string(),
                        *id,
                        format!("{ty:?}"),
                        *nullable,
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap()
    }
    fn schema(fields: &[(&str, DataType, bool)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, ty, nullable)| Field::new(*name, ty.clone(), *nullable))
                .collect::<Vec<_>>(),
        ))
    }
    fn source(error: EqualitySchemaError) -> ConnectorError {
        match error {
            EqualitySchemaError::Source(error) => error,
            EqualitySchemaError::Control(error) => panic!("unexpected control: {error}"),
        }
    }
    fn assert_control(
        result: Result<Vec<EqualityDeleteColumn>, EqualitySchemaError>,
        expected: CompileControlError,
    ) {
        assert!(matches!(result, Err(EqualitySchemaError::Control(actual)) if actual == expected));
    }

    #[test]
    fn equality_schema_preserves_recipe_order_ids_and_complete_arrow_types() {
        let child = Field::new("provider_child", DataType::Utf8, true).with_metadata(
            HashMap::from([("provider.key".to_string(), "value".to_string())]),
        );
        let nested = DataType::Struct(vec![Arc::new(child)].into());
        let recipe = recipe(&[
            ("nested", 90, nested.clone(), true),
            ("key", 3, DataType::Int64, false),
        ]);
        let actual = Arc::new(Schema::new(vec![
            Field::new("nested", nested.clone(), true).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "999".to_string(),
            )])),
            Field::new("key", DataType::Int64, false).with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "1000".to_string(),
            )])),
        ]));
        let legacy = resolve_equality_columns(&recipe, &actual).unwrap();
        let pure =
            resolve_equality_columns_for_compile(&recipe, &actual, &Control::accept()).unwrap();
        assert_eq!(pure, legacy);
        assert_eq!(
            pure.iter()
                .map(|column| column.field_id)
                .collect::<Vec<_>>(),
            vec![90, 3]
        );
        assert_eq!(
            pure.iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["nested", "key"]
        );
        assert_eq!(pure[0].data_type, nested);
        assert!(pure[0].nullable);
        assert!(!pure[1].nullable);
        assert_eq!(recipe.field_ids(), vec![3, 90]);
    }

    #[test]
    fn equality_schema_retains_dictionary_debug_contract_and_nullable_exactness() {
        let dictionary = DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
        let recipe = recipe(&[("dict", 0, dictionary.clone(), false)]);
        let actual = schema(&[("dict", dictionary.clone(), false)]);
        let output =
            resolve_equality_columns_for_compile(&recipe, &actual, &Control::accept()).unwrap();
        assert_eq!(output, resolve_equality_columns(&recipe, &actual).unwrap());
        assert_eq!(output[0].field_id, 0);
        assert_eq!(output[0].data_type, dictionary);
        // The legacy equality binder checks the frozen rendering, not storage
        // writer support. Later owners remain responsible for that support.
        let changed = schema(&[("dict", DataType::Utf8, false)]);
        assert_eq!(
            source(
                resolve_equality_columns_for_compile(&recipe, &changed, &Control::accept())
                    .unwrap_err()
            ),
            resolve_equality_columns(&recipe, &changed).unwrap_err()
        );
    }

    #[test]
    fn equality_schema_count_name_type_nullable_and_order_fail_with_legacy_diagnostics() {
        let recipe = recipe(&[
            ("a", 1, DataType::Int64, false),
            ("b", 2, DataType::Utf8, true),
        ]);
        for actual in [
            schema(&[("a", DataType::Int64, false)]),
            schema(&[
                ("renamed", DataType::Int64, false),
                ("b", DataType::Utf8, true),
            ]),
            schema(&[("a", DataType::Int32, false), ("b", DataType::Utf8, true)]),
            schema(&[("a", DataType::Int64, true), ("b", DataType::Utf8, true)]),
            schema(&[("b", DataType::Utf8, true), ("a", DataType::Int64, false)]),
        ] {
            let legacy = resolve_equality_columns(&recipe, &actual).unwrap_err();
            let pure = source(
                resolve_equality_columns_for_compile(&recipe, &actual, &Control::accept())
                    .unwrap_err(),
            );
            assert_eq!(pure, legacy);
            assert_eq!(pure.kind(), ConnectorErrorKind::InvalidRequest);
        }
        assert_eq!(
            resolve_equality_columns(&recipe, &schema(&[("a", DataType::Int64, false)]))
                .unwrap_err()
                .message(),
            "Iceberg equality-delete handle names 2 columns but its fragment input carries 1"
        );
        assert_eq!(
            resolve_equality_columns(
                &recipe,
                &schema(&[
                    ("changed", DataType::Int64, false),
                    ("b", DataType::Utf8, true)
                ])
            )
            .unwrap_err()
            .message(),
            "Iceberg equality-delete column `a` does not match fragment input `changed`"
        );
    }

    #[test]
    fn equality_schema_chunked_unicode_copy_preserves_values_without_a_new_name_cap() {
        let name = "é🙂".repeat(900);
        let recipe = recipe(&[(&name, 12, DataType::Int64, true)]);
        let actual = schema(&[(&name, DataType::Int64, true)]);
        let output =
            resolve_equality_columns_for_compile(&recipe, &actual, &Control::accept()).unwrap();
        assert_eq!(output, resolve_equality_columns(&recipe, &actual).unwrap());
        assert_eq!(output[0].name, name);
    }

    #[test]
    fn equality_schema_original_control_entry_refusal_precedes_source_validation() {
        let recipe = recipe(&[("a", 1, DataType::Int64, false)]);
        let wrong = Arc::new(Schema::empty());
        for error in causes() {
            let control = Control {
                failure: Some((1, error)),
                ..Control::accept()
            };
            assert_control(
                resolve_equality_columns_for_compile(&recipe, &wrong, &control),
                error,
            );
            assert_eq!(*control.calls.lock().unwrap(), vec![0]);
        }
    }

    #[test]
    fn equality_schema_real_long_name_comparison_stops_at_first_256_quantum() {
        // The leaf does not invent a name admission limit. This already-built
        // schema exercises actual byte comparisons, not anticipated byte work.
        let name = "key".repeat(100_000);
        let recipe = recipe(&[(&name, 77, DataType::Int64, false)]);
        let actual = schema(&[(&name, DataType::Int64, false)]);
        for error in causes() {
            let control = Control {
                positive: Some(error),
                ..Control::accept()
            };
            assert_control(
                resolve_equality_columns_for_compile(&recipe, &actual, &control),
                error,
            );
            assert_eq!(*control.calls.lock().unwrap(), vec![0, 256]);
        }
    }

    #[test]
    fn equality_schema_success_tail_refusal_does_not_publish_columns_or_recheck() {
        let recipe = recipe(&[("a", 1, DataType::Int64, false)]);
        let actual = schema(&[("a", DataType::Int64, false)]);
        let reference = Control::accept();
        resolve_equality_columns_for_compile(&recipe, &actual, &reference).unwrap();
        let calls = reference.calls.lock().unwrap().clone();
        assert!(calls.last().is_some_and(|units| *units > 0 && *units < 256));
        for error in causes() {
            let control = Control {
                failure: Some((calls.len(), error)),
                ..Control::accept()
            };
            assert_control(
                resolve_equality_columns_for_compile(&recipe, &actual, &control),
                error,
            );
            assert_eq!(*control.calls.lock().unwrap(), calls);
        }
    }

    #[test]
    fn equality_schema_ordinary_failure_tail_preserves_primary_typed_control() {
        let recipe = recipe(&[("a", 1, DataType::Int64, false)]);
        let wrong = schema(&[("b", DataType::Int64, false)]);
        let reference = Control::accept();
        let expected =
            source(resolve_equality_columns_for_compile(&recipe, &wrong, &reference).unwrap_err());
        assert_eq!(
            expected,
            resolve_equality_columns(&recipe, &wrong).unwrap_err()
        );
        let calls = reference.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert!(calls[1] > 0);
        for error in causes() {
            let control = Control {
                failure: Some((2, error)),
                ..Control::accept()
            };
            assert_control(
                resolve_equality_columns_for_compile(&recipe, &wrong, &control),
                error,
            );
            assert_eq!(*control.calls.lock().unwrap(), calls);
        }
    }
}
