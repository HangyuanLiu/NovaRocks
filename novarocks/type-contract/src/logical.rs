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

use arrow_schema::{DataType, Field};
use std::fmt;

/// Semantic identity is explicit; an Arrow carrier never establishes it.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ValueLogicalType {
    #[default]
    Physical,
    Json,
    Variant,
    Hll,
    Bitmap,
    Object,
    Percentile,
    LargeInt,
    Uuid,
}

pub const NR_LOGICAL_TYPE_KEY: &str = "nr_logical_type";
pub const MAX_VALUE_TYPE_DEPTH: usize = 64;
pub const MAX_VALUE_TYPE_NODES: usize = 4096;

impl ValueLogicalType {
    pub const fn metadata_value(self) -> Option<&'static str> {
        match self {
            Self::Physical => None,
            Self::Json => Some("json"),
            Self::Variant => Some("variant"),
            Self::Hll => Some("hll"),
            Self::Bitmap => Some("bitmap"),
            Self::Object => Some("object"),
            Self::Percentile => Some("percentile"),
            Self::LargeInt => Some("largeint"),
            Self::Uuid => Some("uuid"),
        }
    }

    pub fn from_metadata_value(value: &str) -> Result<Self, ValueTypeError> {
        match value {
            "json" => Ok(Self::Json),
            "variant" => Ok(Self::Variant),
            "hll" => Ok(Self::Hll),
            "bitmap" => Ok(Self::Bitmap),
            "object" => Ok(Self::Object),
            "percentile" => Ok(Self::Percentile),
            "largeint" => Ok(Self::LargeInt),
            "uuid" => Ok(Self::Uuid),
            _ => Err(ValueTypeError::UnknownLogicalMetadata),
        }
    }

    pub fn validate_carrier(self, data_type: &DataType) -> Result<(), ValueTypeError> {
        let valid = match self {
            Self::Physical => true,
            Self::Json => matches!(data_type, DataType::Utf8),
            Self::Variant => matches!(data_type, DataType::LargeBinary),
            Self::Hll | Self::Bitmap | Self::Object | Self::Percentile => {
                matches!(data_type, DataType::Binary | DataType::LargeBinary)
            }
            Self::LargeInt | Self::Uuid => matches!(data_type, DataType::FixedSizeBinary(16)),
        };
        if valid {
            Ok(())
        } else {
            Err(ValueTypeError::InvalidLogicalCarrier(self))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueTypeError {
    UnknownLogicalMetadata,
    InvalidLogicalCarrier(ValueLogicalType),
    TooDeep,
    TooManyNodes,
}
impl fmt::Display for ValueTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownLogicalMetadata => f.write_str("unknown logical type metadata"),
            Self::InvalidLogicalCarrier(logical) => {
                write!(f, "invalid Arrow carrier for {logical:?}")
            }
            Self::TooDeep => f.write_str("value type exceeds the nesting depth limit"),
            Self::TooManyNodes => f.write_str("value type exceeds the node count limit"),
        }
    }
}
impl std::error::Error for ValueTypeError {}

pub fn field_logical_type(field: &Field) -> Result<ValueLogicalType, ValueTypeError> {
    let logical = match field.metadata().get(NR_LOGICAL_TYPE_KEY) {
        None => ValueLogicalType::Physical,
        Some(value) => ValueLogicalType::from_metadata_value(value)?,
    };
    logical.validate_carrier(field.data_type())?;
    Ok(logical)
}

/// This bound protects direct pure-catalog callers too, before recursive
/// domain comparisons. The carrier's own structural/resource rules remain
/// with its plan/codec owner.
pub fn validate_nested_logical_types(root: &DataType) -> Result<(), ValueTypeError> {
    validate_nested_logical_types_observed(root, || Ok(()))
}

/// Same bounded structural validation with a caller-owned work/control
/// observer. Every type node and child edge is observed without a shadow
/// validator or recursive type copy.
pub fn validate_nested_logical_types_observed<E>(
    root: &DataType,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<(), E>
where
    E: From<ValueTypeError>,
{
    validate_value_type_structure_observed(root, |visit| match visit {
        ValueTypeVisit::Field(_) => Ok(()),
        _ => observe(),
    })
}

/// One walk exposes exact borrowed attributes to the caller's carrier-resource
/// checks; logical validation and node/depth bounds remain defined here.
#[derive(Clone, Copy, Debug)]
pub enum ValueTypeVisit<'a> {
    TypeNode(&'a DataType),
    ChildEdge(&'a DataType),
    Field(&'a Field),
}

pub fn validate_value_type_structure_observed<'a, E: From<ValueTypeError>>(
    root: &'a DataType,
    mut observe: impl FnMut(ValueTypeVisit<'a>) -> Result<(), E>,
) -> Result<(), E> {
    let mut pending = vec![(root, 1usize)];
    let mut visited = 0usize;
    while let Some((ty, depth)) = pending.pop() {
        observe(ValueTypeVisit::TypeNode(ty))?;
        visited += 1;
        if depth > MAX_VALUE_TYPE_DEPTH {
            return Err(ValueTypeError::TooDeep.into());
        }
        if visited > MAX_VALUE_TYPE_NODES {
            return Err(ValueTypeError::TooManyNodes.into());
        }
        let mut push = |child: &'a DataType, field: Option<&'a Field>| -> Result<(), E> {
            if let Some(field) = field {
                observe(ValueTypeVisit::Field(field))?;
                field_logical_type(field)?;
            }
            observe(ValueTypeVisit::ChildEdge(child))?;
            if visited + pending.len() >= MAX_VALUE_TYPE_NODES {
                return Err(ValueTypeError::TooManyNodes.into());
            }
            pending.push((child, depth + 1));
            Ok(())
        };
        match ty {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => {
                push(field.data_type(), Some(field))?;
            }
            DataType::Struct(fields) => {
                for field in fields {
                    push(field.data_type(), Some(field))?;
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    push(field.data_type(), Some(field))?;
                }
            }
            DataType::Dictionary(key, value) => {
                push(key, None)?;
                push(value, None)?;
            }
            DataType::RunEndEncoded(ends, values) => {
                push(ends.data_type(), Some(ends))?;
                push(values.data_type(), Some(values))?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// A carrier cast has no authority to establish or erase semantic identities.
/// Ordinary unlabelled structures may change through the existing cast rules.
pub fn preserves_nested_logical_identity(actual: &DataType, expected: &DataType) -> bool {
    if validate_nested_logical_types(actual).is_err()
        || validate_nested_logical_types(expected).is_err()
    {
        return false;
    }
    fn field_has(field: &Field) -> bool {
        field_logical_type(field) != Ok(ValueLogicalType::Physical) || has(field.data_type())
    }
    fn has(ty: &DataType) -> bool {
        match ty {
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => field_has(f),
            DataType::Struct(fields) => fields.iter().any(|f| field_has(f)),
            DataType::Union(fields, _) => fields.iter().any(|(_, f)| field_has(f)),
            DataType::Dictionary(key, value) => has(key) || has(value),
            DataType::RunEndEncoded(ends, values) => field_has(ends) || field_has(values),
            _ => false,
        }
    }
    fn field_preserves(a: &Field, e: &Field) -> bool {
        field_logical_type(a) == field_logical_type(e) && preserves(a.data_type(), e.data_type())
    }
    fn preserves(a: &DataType, e: &DataType) -> bool {
        match (a, e) {
            (DataType::List(a), DataType::List(e))
            | (DataType::LargeList(a), DataType::LargeList(e))
            | (DataType::ListView(a), DataType::ListView(e))
            | (DataType::LargeListView(a), DataType::LargeListView(e))
            | (DataType::FixedSizeList(a, _), DataType::FixedSizeList(e, _))
            | (DataType::Map(a, _), DataType::Map(e, _)) => field_preserves(a, e),
            (DataType::Struct(a), DataType::Struct(e)) if a.len() == e.len() => {
                a.iter().zip(e.iter()).all(|(a, e)| field_preserves(a, e))
            }
            (DataType::Union(a, _), DataType::Union(e, _)) if a.len() == e.len() => a
                .iter()
                .zip(e.iter())
                .all(|((ai, af), (ei, ef))| ai == ei && field_preserves(af, ef)),
            (DataType::Dictionary(ak, av), DataType::Dictionary(ek, ev)) => {
                preserves(ak, ek) && preserves(av, ev)
            }
            (DataType::RunEndEncoded(ar, av), DataType::RunEndEncoded(er, ev)) => {
                field_preserves(ar, er) && field_preserves(av, ev)
            }
            _ => !has(a) && !has(e),
        }
    }
    preserves(actual, expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FunctionValueType, fits_nested_nullability};
    use std::sync::Arc;

    #[test]
    fn logical_domains_require_explicit_identity_and_exact_carriers() {
        for (logical, carrier) in [
            (ValueLogicalType::Json, DataType::Utf8),
            (ValueLogicalType::Variant, DataType::LargeBinary),
            (ValueLogicalType::Hll, DataType::Binary),
            (ValueLogicalType::Bitmap, DataType::Binary),
            (ValueLogicalType::Object, DataType::LargeBinary),
            (ValueLogicalType::Percentile, DataType::Binary),
            (ValueLogicalType::LargeInt, DataType::FixedSizeBinary(16)),
            (ValueLogicalType::Uuid, DataType::FixedSizeBinary(16)),
        ] {
            let typed =
                FunctionValueType::try_with_logical_type(carrier.clone(), false, logical).unwrap();
            let plain = FunctionValueType::new(carrier, false);
            assert!(!typed.same_value_domain(&plain));
            assert!(!typed.fits_value_type(&plain));
            assert!(!plain.fits_value_type(&typed));
            assert!(
                FunctionValueType::try_with_logical_type(DataType::Int64, false, logical).is_err()
            );
        }
        let variant = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Variant,
        )
        .unwrap();
        let object = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Object,
        )
        .unwrap();
        assert!(!variant.same_value_domain(&object));
        assert!(
            FunctionValueType::try_with_logical_type(
                DataType::Binary,
                false,
                ValueLogicalType::Variant
            )
            .is_err()
        );
    }

    #[test]
    fn nested_logical_identity_survives_nullable_widening() {
        let field = |nullable, label: Option<&str>| {
            let field = Field::new("item", DataType::Utf8, nullable);
            match label {
                None => field,
                Some(label) => {
                    field.with_metadata([(NR_LOGICAL_TYPE_KEY.into(), label.into())].into())
                }
            }
        };
        let actual = DataType::List(Arc::new(field(false, Some("json"))));
        let expected = DataType::List(Arc::new(field(true, Some("json"))));
        assert!(fits_nested_nullability(&actual, &expected));
        assert!(!fits_nested_nullability(&expected, &actual));
        assert!(!fits_nested_nullability(
            &actual,
            &DataType::List(Arc::new(field(true, None)))
        ));
        let unknown = DataType::List(Arc::new(field(true, Some("unknown"))));
        assert_eq!(
            validate_nested_logical_types(&unknown),
            Err(ValueTypeError::UnknownLogicalMetadata)
        );
        assert!(!fits_nested_nullability(&unknown, &unknown));
        assert!(preserves_nested_logical_identity(&actual, &expected));
        let plain = DataType::List(Arc::new(field(true, None)));
        assert!(!preserves_nested_logical_identity(&actual, &plain));
        assert!(!preserves_nested_logical_identity(&plain, &actual));
        assert!(!preserves_nested_logical_identity(&actual, &DataType::Utf8));
        assert!(preserves_nested_logical_identity(&plain, &DataType::Utf8));
    }

    #[test]
    fn direct_catalog_types_have_near_and_over_bounds() {
        let mut ty = DataType::Int64;
        for _ in 1..MAX_VALUE_TYPE_DEPTH {
            ty = DataType::List(Arc::new(Field::new("item", ty, true)));
        }
        validate_nested_logical_types(&ty).unwrap();
        let over = DataType::List(Arc::new(Field::new("item", ty, true)));
        assert_eq!(
            validate_nested_logical_types(&over),
            Err(ValueTypeError::TooDeep)
        );
        let fields = (0..MAX_VALUE_TYPE_NODES - 1)
            .map(|i| Field::new(format!("v{i}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        validate_nested_logical_types(&DataType::Struct(fields.clone().into())).unwrap();
        let mut over = fields;
        over.push(Field::new("over", DataType::Int64, false));
        assert_eq!(
            validate_nested_logical_types(&DataType::Struct(over.into())),
            Err(ValueTypeError::TooManyNodes)
        );
    }

    #[test]
    fn typed_equality_keeps_nested_dictionary_ids() {
        #[allow(deprecated)]
        let value = |id| {
            FunctionValueType::new(
                DataType::Struct(
                    vec![Field::new_dict(
                        "dict",
                        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                        false,
                        id,
                        false,
                    )]
                    .into(),
                ),
                false,
            )
        };
        assert_ne!(value(1), value(2));
        assert!(!value(1).same_value_domain(&value(2)));
    }

    #[test]
    fn observed_validation_checks_child_edges_and_nodes_with_bounded_work() {
        use crate::{CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl};
        use std::sync::Mutex;
        struct Control {
            units: Mutex<Vec<u32>>,
            cancel: bool,
        }
        impl PureCompileControl for Control {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                self.units.lock().unwrap().push(units);
                if self.cancel && units > 0 {
                    Err(CompileControlError::Cancelled)
                } else {
                    Ok(())
                }
            }
        }
        #[derive(Debug, PartialEq)]
        enum Failure {
            Type(ValueTypeError),
            Control(CompileControlError),
        }
        impl From<ValueTypeError> for Failure {
            fn from(error: ValueTypeError) -> Self {
                Self::Type(error)
            }
        }
        let data_type = DataType::Struct(
            (0..300)
                .map(|n| std::sync::Arc::new(Field::new(n.to_string(), DataType::Int64, false)))
                .collect(),
        );
        for cancel in [false, true] {
            let control = Control {
                units: Mutex::default(),
                cancel,
            };
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                    .unwrap();
            let result = validate_nested_logical_types_observed::<Failure>(&data_type, || {
                work.step().map_err(Failure::Control)
            });
            if cancel {
                assert_eq!(
                    result,
                    Err(Failure::Control(CompileControlError::Cancelled))
                );
                assert_eq!(control.units.lock().unwrap().iter().sum::<u32>(), 256);
            } else {
                result.unwrap();
                work.finish().unwrap();
                let units = control.units.lock().unwrap();
                assert_eq!(units.iter().sum::<u32>(), 601);
                assert!(units.iter().all(|n| *n <= 256));
            }
        }
    }
}
