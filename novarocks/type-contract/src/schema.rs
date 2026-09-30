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

use arrow_schema::{DataType, Field, Schema};

pub const MAX_ARROW_FIELD_NAME_BYTES: usize = 1024;
pub const MAX_ARROW_FIELD_METADATA_ENTRIES: usize = 256;
pub const MAX_ARROW_FIELD_METADATA_KEY_BYTES: usize = 1024;
pub const MAX_ARROW_FIELD_METADATA_VALUE_BYTES: usize = 16 * 1024;
pub const MAX_ARROW_FIELD_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES: usize = 1024;

/// Compare every Arrow physical field attribute recursively. Arrow's built-in
/// `Field::eq` deliberately ignores dictionary ids and dictionary ordering,
/// which is appropriate for logical schema compatibility but not for a frozen
/// internal-relation contract.
pub fn arrow_schemas_exact(left: &Schema, right: &Schema) -> bool {
    left.metadata() == right.metadata()
        && left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| arrow_fields_exact(left, right))
}

pub fn arrow_fields_exact(left: &Field, right: &Field) -> bool {
    #[allow(deprecated)]
    let dictionary_ids_equal = left.dict_id() == right.dict_id();
    left.name() == right.name()
        && left.is_nullable() == right.is_nullable()
        && left.metadata() == right.metadata()
        && dictionary_ids_equal
        && left.dict_is_ordered() == right.dict_is_ordered()
        && arrow_data_types_exact(left.data_type(), right.data_type())
}

pub fn arrow_data_types_exact(left: &DataType, right: &DataType) -> bool {
    compare_types::<crate::ValueTypeError>(left, right, || Ok(()), false).unwrap_or(false)
}

/// The same exact comparison with bounded depth/node traversal and an observer
/// at every visited type and field. Frozen metadata bounds are validated by
/// the type owner; this comparison does not allocate a recursive schema copy.
pub fn arrow_data_types_exact_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types(left, right, observe, true)
}

fn compare_types<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    mut observe: impl FnMut() -> Result<(), E>,
    bounded: bool,
) -> Result<bool, E> {
    struct Walk<'a, F> {
        observe: &'a mut F,
        nodes: usize,
        bounded: bool,
    }
    impl<E: From<crate::ValueTypeError>, F: FnMut() -> Result<(), E>> Walk<'_, F> {
        fn field(&mut self, left: &Field, right: &Field, depth: usize) -> Result<bool, E> {
            (self.observe)()?;
            #[allow(deprecated)]
            let ids_equal = left.dict_id() == right.dict_id();
            if left.name() != right.name()
                || left.is_nullable() != right.is_nullable()
                || left.metadata() != right.metadata()
                || !ids_equal
                || left.dict_is_ordered() != right.dict_is_ordered()
            {
                return Ok(false);
            }
            self.ty(left.data_type(), right.data_type(), depth)
        }
        fn ty(&mut self, left: &DataType, right: &DataType, depth: usize) -> Result<bool, E> {
            (self.observe)()?;
            if self.bounded && depth > crate::MAX_VALUE_TYPE_DEPTH {
                return Err(crate::ValueTypeError::TooDeep.into());
            }
            self.nodes += 1;
            if self.bounded && self.nodes > crate::MAX_VALUE_TYPE_NODES {
                return Err(crate::ValueTypeError::TooManyNodes.into());
            }
            match (left, right) {
                (DataType::List(left), DataType::List(right))
                | (DataType::ListView(left), DataType::ListView(right))
                | (DataType::LargeList(left), DataType::LargeList(right))
                | (DataType::LargeListView(left), DataType::LargeListView(right)) => {
                    self.field(left, right, depth + 1)
                }
                (DataType::FixedSizeList(left, ls), DataType::FixedSizeList(right, rs)) => {
                    if ls != rs {
                        Ok(false)
                    } else {
                        self.field(left, right, depth + 1)
                    }
                }
                (DataType::Struct(left), DataType::Struct(right)) => {
                    if left.len() != right.len() {
                        return Ok(false);
                    }
                    for (left, right) in left.iter().zip(right) {
                        if !self.field(left, right, depth + 1)? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                (DataType::Union(left, lm), DataType::Union(right, rm)) => {
                    if lm != rm || left.len() != right.len() {
                        return Ok(false);
                    }
                    for ((li, left), (ri, right)) in left.iter().zip(right.iter()) {
                        if li != ri || !self.field(left, right, depth + 1)? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                (DataType::Dictionary(lk, lv), DataType::Dictionary(rk, rv)) => {
                    Ok(self.ty(lk, rk, depth + 1)? && self.ty(lv, rv, depth + 1)?)
                }
                (DataType::Map(left, ls), DataType::Map(right, rs)) => {
                    if ls != rs {
                        Ok(false)
                    } else {
                        self.field(left, right, depth + 1)
                    }
                }
                (DataType::RunEndEncoded(lr, lv), DataType::RunEndEncoded(rr, rv)) => {
                    Ok(self.field(lr, rr, depth + 1)? && self.field(lv, rv, depth + 1)?)
                }
                _ => Ok(left == right),
            }
        }
    }
    Walk {
        observe: &mut observe,
        nodes: 0,
        bounded,
    }
    .ty(left, right, 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueTypeError,
    };
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Eq, PartialEq)]
    enum Failure {
        Type(ValueTypeError),
        Control(CompileControlError),
    }
    impl From<ValueTypeError> for Failure {
        fn from(value: ValueTypeError) -> Self {
            Self::Type(value)
        }
    }
    struct Control {
        failure: Option<CompileControlError>,
        work: Mutex<Vec<u32>>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.work.lock().unwrap().push(units);
            if units > 0
                && let Some(failure) = self.failure
            {
                Err(failure)
            } else {
                Ok(())
            }
        }
    }
    #[test]
    fn wide_exact_comparison_observes_each_edge_without_copying_the_schema() {
        let ty = DataType::Struct(
            (0..300)
                .map(|i| Field::new(i.to_string(), DataType::Int64, false))
                .collect(),
        );
        for failure in [
            None,
            Some(CompileControlError::Cancelled),
            Some(CompileControlError::DeadlineExceeded),
            Some(CompileControlError::ResourceExhausted),
        ] {
            let control = Control {
                failure,
                work: Mutex::default(),
            };
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                    .unwrap();
            let result = arrow_data_types_exact_observed::<Failure>(&ty, &ty, || {
                work.step().map_err(Failure::Control)
            });
            if let Some(failure) = failure {
                assert_eq!(result, Err(Failure::Control(failure)));
                assert_eq!(*control.work.lock().unwrap(), [0, 256]);
            } else {
                assert_eq!(result, Ok(true));
                work.finish().unwrap();
                assert_eq!(*control.work.lock().unwrap(), [0, 256, 256, 89]);
            }
        }
    }
    #[test]
    fn observed_exact_comparison_bounds_types_without_breaking_plain_equality() {
        let mut ty = DataType::Int64;
        for _ in 1..crate::MAX_VALUE_TYPE_DEPTH {
            ty = DataType::List(Arc::new(Field::new("item", ty, true)));
        }
        assert_eq!(
            arrow_data_types_exact_observed::<ValueTypeError>(&ty, &ty, || Ok(())),
            Ok(true)
        );
        let over = DataType::List(Arc::new(Field::new("item", ty, true)));
        assert_eq!(
            arrow_data_types_exact_observed::<ValueTypeError>(&over, &over, || Ok(())),
            Err(ValueTypeError::TooDeep)
        );
        assert!(arrow_data_types_exact(&over, &over));
        let fields: Vec<_> = (0..crate::MAX_VALUE_TYPE_NODES)
            .map(|_| Field::new("x", DataType::Int64, false))
            .collect();
        let over = DataType::Struct(fields.into());
        assert_eq!(
            arrow_data_types_exact_observed::<ValueTypeError>(&over, &over, || Ok(())),
            Err(ValueTypeError::TooManyNodes)
        );
        assert!(arrow_data_types_exact(&over, &over));
    }
    #[test]
    fn observed_comparison_preserves_dictionary_and_field_metadata_identity() {
        #[allow(deprecated)]
        let make = |id, label: &str| {
            DataType::Struct(
                vec![
                    Field::new_dict(
                        "v",
                        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                        true,
                        id,
                        false,
                    )
                    .with_metadata([("annotation".into(), label.into())].into()),
                ]
                .into(),
            )
        };
        let first = make(7, "first");
        for second in [make(8, "first"), make(7, "second")] {
            assert_eq!(
                arrow_data_types_exact_observed::<ValueTypeError>(&first, &second, || Ok(())),
                Ok(false)
            );
            assert!(!arrow_data_types_exact(&first, &second));
        }
    }
}
