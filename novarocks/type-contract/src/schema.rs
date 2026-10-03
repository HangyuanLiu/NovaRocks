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
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::Hasher;

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
    compare_types::<std::convert::Infallible>(
        left,
        right,
        FieldMode::Exact,
        || Ok(()),
        |_, _| Ok(()),
    )
    .unwrap_or(false)
}

/// The same exact comparison with bounded depth/node traversal and an observer
/// at every visited type and field. Frozen metadata bounds are validated by
/// the type owner; this comparison does not allocate a recursive schema copy.
pub fn arrow_data_types_exact_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types(left, right, FieldMode::Exact, observe, |depth, nodes| {
        if depth > crate::MAX_VALUE_TYPE_DEPTH {
            Err(crate::ValueTypeError::TooDeep.into())
        } else if nodes > crate::MAX_VALUE_TYPE_NODES {
            Err(crate::ValueTypeError::TooManyNodes.into())
        } else {
            Ok(())
        }
    })
}

/// Compare exact types with the same traversal and bounds as the observed
/// exact port, borrowing metadata instead of allocating fingerprint buckets.
/// Callers must admit quadratic metadata candidate work and retained map
/// iteration capacity. Long names, keys, and values retain chunk observation;
/// this port introduces no additional metadata admission limit.
pub fn arrow_data_types_exact_borrowed_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    Walk {
        observe: &mut observe,
        validate: &mut |depth, nodes| {
            if depth > crate::MAX_VALUE_TYPE_DEPTH {
                Err(crate::ValueTypeError::TooDeep.into())
            } else if nodes > crate::MAX_VALUE_TYPE_NODES {
                Err(crate::ValueTypeError::TooManyNodes.into())
            } else {
                Ok(())
            }
        },
        nodes: 0,
        mode: FieldMode::Exact,
        metadata_mode: MetadataMode::BorrowedLinear,
    }
    .ty(left, right, 1)
}

/// Exact Field comparison using borrowed metadata and the sole type traversal.
/// Like the existing observed Field port, this relies on caller-owned schema
/// bounds and does not introduce a new depth, node, or metadata limit.
pub fn arrow_fields_exact_borrowed_observed<E>(
    left: &Field,
    right: &Field,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    Walk {
        observe: &mut observe,
        validate: &mut |_, _| Ok(()),
        nodes: 0,
        mode: FieldMode::Exact,
        metadata_mode: MetadataMode::BorrowedLinear,
    }
    .field(left, right, 1)
}

/// Observe Arrow schema compatibility equality. Unlike the frozen exact
/// comparison, this deliberately ignores Field dictionary ids/order exactly
/// as Arrow's PartialEq does; all other attributes use the same comparison
/// engine. This is not a frozen internal-relation identity proof.
pub fn arrow_data_types_equal_observed<E: From<crate::ValueTypeError>>(
    left: &DataType,
    right: &DataType,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    compare_types(
        left,
        right,
        FieldMode::Compatibility,
        observe,
        |depth, nodes| {
            if depth > crate::MAX_VALUE_TYPE_DEPTH {
                Err(crate::ValueTypeError::TooDeep.into())
            } else if nodes > crate::MAX_VALUE_TYPE_NODES {
                Err(crate::ValueTypeError::TooManyNodes.into())
            } else {
                Ok(())
            }
        },
    )
}

/// Observe exact field comparison within the caller's already validated schema
/// domain. This adds no type-node admission bound: owners such as writer
/// relations have their own depth and aggregate schema-byte limits.
/// Callers must validate those limits before entering this borrowed traversal.
pub fn arrow_fields_exact_observed<E>(
    left: &Field,
    right: &Field,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    Walk {
        observe: &mut observe,
        validate: &mut |_, _| Ok(()),
        nodes: 0,
        mode: FieldMode::Exact,
        metadata_mode: MetadataMode::Bucketed,
    }
    .field(left, right, 1)
}

fn compare_types<E>(
    left: &DataType,
    right: &DataType,
    mode: FieldMode,
    mut observe: impl FnMut() -> Result<(), E>,
    mut validate: impl FnMut(usize, usize) -> Result<(), E>,
) -> Result<bool, E> {
    Walk {
        observe: &mut observe,
        validate: &mut validate,
        nodes: 0,
        mode,
        metadata_mode: MetadataMode::Bucketed,
    }
    .ty(left, right, 1)
}

#[derive(Clone, Copy)]
enum MetadataMode {
    Bucketed,
    BorrowedLinear,
}

#[derive(Clone, Copy)]
enum FieldMode {
    Exact,
    Compatibility,
}

struct Walk<'a, F, B> {
    observe: &'a mut F,
    validate: &'a mut B,
    nodes: usize,
    mode: FieldMode,
    metadata_mode: MetadataMode,
}
impl<E, F: FnMut() -> Result<(), E>, B: FnMut(usize, usize) -> Result<(), E>> Walk<'_, F, B> {
    fn bytes(&mut self, left: &[u8], right: &[u8]) -> Result<bool, E> {
        if left.len() != right.len() {
            return Ok(false);
        }
        for (left, right) in left.chunks(1024).zip(right.chunks(1024)) {
            (self.observe)()?;
            if left != right {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn key_hash(&mut self, key: &[u8]) -> Result<u64, E> {
        (self.observe)()?;
        let mut hash = DefaultHasher::new();
        hash.write_usize(key.len());
        for chunk in key.chunks(1024) {
            (self.observe)()?;
            hash.write(chunk);
        }
        Ok(hash.finish())
    }
    fn metadata(&mut self, left: &Field, right: &Field) -> Result<bool, E> {
        match self.metadata_mode {
            MetadataMode::Bucketed => self.metadata_with_key_hash(left, right, Self::key_hash),
            MetadataMode::BorrowedLinear => self.metadata_borrowed(left, right),
        }
    }
    fn metadata_borrowed(&mut self, left: &Field, right: &Field) -> Result<bool, E> {
        if left.metadata().len() != right.metadata().len() {
            return Ok(false);
        }
        // Borrow the original maps and compare candidate bytes cooperatively.
        // No hash computation, bucket table, or copied key is required. A caller
        // must admit the K right-map scans and K^2 candidates, including opaque
        // iteration over retained hash-table capacity, before entering here.
        for (key, value) in left.metadata() {
            (self.observe)()?;
            let mut matched = false;
            for (other_key, other_value) in right.metadata() {
                (self.observe)()?;
                if self.bytes(key.as_bytes(), other_key.as_bytes())? {
                    if !self.bytes(value.as_bytes(), other_value.as_bytes())? {
                        return Ok(false);
                    }
                    matched = true;
                    break;
                }
            }
            if !matched {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn metadata_with_key_hash(
        &mut self,
        left: &Field,
        right: &Field,
        mut key_hash: impl FnMut(&mut Self, &[u8]) -> Result<u64, E>,
    ) -> Result<bool, E> {
        if left.metadata().len() != right.metadata().len() {
            return Ok(false);
        }
        if left.metadata().is_empty() {
            return Ok(true);
        }
        // Fixed-size fingerprints only select borrowed candidates. Exact
        // chunk comparisons retain unordered metadata equality even on a
        // collision. Scratch bucket allocations still need host admission;
        // neither key strings nor recursive field/type facts are copied.
        let mut buckets: HashMap<u64, Vec<(&str, &str)>> = HashMap::new();
        for (key, value) in right.metadata() {
            let fingerprint = key_hash(self, key.as_bytes())?;
            buckets.entry(fingerprint).or_default().push((key, value));
        }
        for (key, value) in left.metadata() {
            let fingerprint = key_hash(self, key.as_bytes())?;
            (self.observe)()?;
            let Some(candidates) = buckets.get(&fingerprint) else {
                return Ok(false);
            };
            let mut matched = false;
            for (other_key, other_value) in candidates {
                (self.observe)()?;
                if self.bytes(key.as_bytes(), other_key.as_bytes())? {
                    if !self.bytes(value.as_bytes(), other_value.as_bytes())? {
                        return Ok(false);
                    }
                    matched = true;
                    break;
                }
            }
            if !matched {
                return Ok(false);
            }
        }
        Ok(true)
    }
    fn field(&mut self, left: &Field, right: &Field, depth: usize) -> Result<bool, E> {
        (self.observe)()?;
        #[allow(deprecated)]
        let dictionary_equal = match self.mode {
            FieldMode::Exact => {
                left.dict_id() == right.dict_id()
                    && left.dict_is_ordered() == right.dict_is_ordered()
            }
            FieldMode::Compatibility => true,
        };
        if !self.bytes(left.name().as_bytes(), right.name().as_bytes())?
            || left.is_nullable() != right.is_nullable()
            || !self.metadata(left, right)?
            || !dictionary_equal
        {
            return Ok(false);
        }
        self.ty(left.data_type(), right.data_type(), depth)
    }
    fn ty(&mut self, left: &DataType, right: &DataType, depth: usize) -> Result<bool, E> {
        (self.observe)()?;
        self.nodes += 1;
        (self.validate)(depth, self.nodes)?;
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
            (DataType::Timestamp(lu, lt), DataType::Timestamp(ru, rt)) => {
                if lu != ru {
                    return Ok(false);
                }
                match (lt, rt) {
                    (Some(left), Some(right)) => self.bytes(left.as_bytes(), right.as_bytes()),
                    (None, None) => Ok(true),
                    _ => Ok(false),
                }
            }
            _ => Ok(left == right),
        }
    }
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
                assert_eq!(*control.work.lock().unwrap(), [0, 256, 256, 256, 133]);
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
    #[test]
    fn owner_validated_field_comparison_does_not_add_default_type_admission_bounds() {
        let field = Field::new(
            "writer",
            DataType::Struct(
                (0..5000)
                    .map(|i| Field::new(i.to_string(), DataType::Int32, false))
                    .collect(),
            ),
            false,
        );
        let mut visits = 0;
        assert_eq!(
            arrow_fields_exact_observed::<ValueTypeError>(&field, &field, || {
                visits += 1;
                Ok(())
            }),
            Ok(true)
        );
        assert!(visits > 15000);
        assert_eq!(
            arrow_data_types_exact_observed::<ValueTypeError>(
                field.data_type(),
                field.data_type(),
                || Ok(())
            ),
            Err(ValueTypeError::TooManyNodes)
        );
    }

    #[test]
    fn owner_bounded_long_timezone_comparison_observes_bytes_before_completion() {
        let field = Field::new(
            "writer_time",
            DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("x".repeat(300 * 1024).into()),
            ),
            true,
        );
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                failure: Some(failure),
                work: Mutex::default(),
            };
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
            assert_eq!(
                arrow_fields_exact_observed::<Failure>(&field, &field, || work
                    .step()
                    .map_err(Failure::Control)),
                Err(Failure::Control(failure))
            );
            assert_eq!(*control.work.lock().unwrap(), [0, 256]);
        }
    }
    #[test]
    fn actual_long_metadata_key_hashing_preserves_control_at_first_quantum() {
        let key = "k".repeat(320 * 1024);
        let left = Field::new("x", DataType::Int64, false)
            .with_metadata(HashMap::from([(key.clone(), "v".to_owned())]));
        let right = Field::new("x", DataType::Int64, false)
            .with_metadata(HashMap::from([(key, "v".to_owned())]));
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                failure: Some(failure),
                work: Mutex::default(),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
            let result = arrow_fields_exact_observed(&left, &right, || {
                work.step().map_err(Failure::Control)
            });
            assert_eq!(result, Err(Failure::Control(failure)));
            assert_eq!(*control.work.lock().unwrap(), [0, 256]);
        }
        assert_eq!(
            arrow_fields_exact_observed::<std::convert::Infallible>(&left, &right, || Ok(())),
            Ok(true)
        );
    }

    #[test]
    fn borrowed_metadata_buckets_preserve_unordered_exact_equality() {
        let field = |entries: &[(&str, &str)]| {
            Field::new("x", DataType::Int64, false).with_metadata(
                entries
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            )
        };
        let left = field(&[("a", "first"), ("b", "second"), ("c", "third")]);
        let reordered = field(&[("c", "third"), ("b", "second"), ("a", "first")]);
        let missing = field(&[("c", "third"), ("b", "second"), ("d", "first")]);
        let changed = field(&[("c", "third"), ("b", "different"), ("a", "first")]);
        for (right, expected) in [(&reordered, true), (&missing, false), (&changed, false)] {
            assert_eq!(
                arrow_fields_exact_observed::<std::convert::Infallible>(&left, right, || Ok(())),
                Ok(expected)
            );
        }
        let empty = field(&[]);
        let mut observed = 0;
        let mut observe = || {
            observed += 1;
            Ok::<_, std::convert::Infallible>(())
        };
        assert_eq!(
            Walk {
                observe: &mut observe,
                validate: &mut |_, _| Ok(()),
                nodes: 0,
                mode: FieldMode::Exact,
                metadata_mode: MetadataMode::Bucketed,
            }
            .metadata(&empty, &empty),
            Ok(true)
        );
        assert_eq!(observed, 0, "empty metadata needs no bucket or key work");
    }

    #[test]
    fn forced_metadata_fingerprint_collision_requires_exact_key_and_value() {
        let field = |entries: &[(&str, &str)]| {
            Field::new("x", DataType::Int64, false).with_metadata(
                entries
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            )
        };
        let left = field(&[("same-prefix-a", "first"), ("same-prefix-b", "second")]);
        let equal = field(&[("same-prefix-b", "second"), ("same-prefix-a", "first")]);
        let missing = field(&[("same-prefix-c", "first"), ("same-prefix-b", "second")]);
        let swapped = field(&[("same-prefix-a", "second"), ("same-prefix-b", "first")]);
        for (right, expected) in [(&equal, true), (&missing, false), (&swapped, false)] {
            let mut observed = 0;
            let mut observe = || {
                observed += 1;
                Ok::<_, std::convert::Infallible>(())
            };
            let result = Walk {
                observe: &mut observe,
                validate: &mut |_, _| Ok(()),
                nodes: 0,
                mode: FieldMode::Exact,
                metadata_mode: MetadataMode::Bucketed,
            }
            .metadata_with_key_hash(&left, right, |walk, _| {
                (walk.observe)()?;
                Ok(0)
            });
            assert_eq!(result, Ok(expected));
            assert!(
                observed > 4,
                "collisions still inspect actual key/value bytes"
            );
        }
    }

    #[test]
    fn observed_compatibility_equals_arrow_for_dictionary_bits_and_all_other_field_attributes() {
        use arrow_schema::{UnionFields, UnionMode};
        #[allow(deprecated)]
        let field = |id, ordered| {
            Arc::new(
                Field::new_dict(
                    "item",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                    id,
                    ordered,
                )
                .with_metadata([("provider.id".into(), "7".into())].into()),
            )
        };
        let wrap = |f: Arc<Field>| {
            vec![
                DataType::List(f.clone()),
                DataType::LargeList(f.clone()),
                DataType::ListView(f.clone()),
                DataType::LargeListView(f.clone()),
                DataType::FixedSizeList(f.clone(), 2),
                DataType::Struct(vec![f.clone()].into()),
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Arc::new(Field::new("key", DataType::Int64, false)),
                                f.clone(),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                DataType::Union(
                    UnionFields::try_new(vec![41], vec![f.clone()]).unwrap(),
                    UnionMode::Dense,
                ),
                DataType::Dictionary(
                    Box::new(DataType::Int16),
                    Box::new(DataType::List(f.clone())),
                ),
                DataType::RunEndEncoded(Arc::new(Field::new("ends", DataType::Int32, false)), f),
            ]
        };
        let left = field(0, false);
        let compatible = field(i64::MAX, true);
        let variants = [
            compatible.clone(),
            Arc::new(compatible.as_ref().clone().with_name("different")),
            Arc::new(compatible.as_ref().clone().with_nullable(false)),
            Arc::new(
                compatible
                    .as_ref()
                    .clone()
                    .with_metadata([("provider.id".into(), "8".into())].into()),
            ),
            Arc::new(
                compatible
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Dictionary(
                        Box::new(DataType::Int16),
                        Box::new(DataType::Utf8),
                    )),
            ),
        ];
        for right in variants {
            for (left, right) in wrap(left.clone()).iter().zip(wrap(right).iter()) {
                assert_eq!(
                    arrow_data_types_equal_observed::<ValueTypeError>(left, right, || Ok(())),
                    Ok(left == right)
                );
                if left == right {
                    assert_eq!(
                        arrow_data_types_exact_observed::<ValueTypeError>(left, right, || Ok(())),
                        Ok(false)
                    );
                }
            }
        }
    }

    #[test]
    fn observed_compatibility_retains_union_ids_mode_and_timestamp_timezone_equality() {
        use arrow_schema::{TimeUnit, UnionFields, UnionMode};
        let union = |id, mode| {
            DataType::Union(
                UnionFields::try_new(vec![id], vec![Field::new("v", DataType::Int64, false)])
                    .unwrap(),
                mode,
            )
        };
        let corpus = [
            union(7, UnionMode::Dense),
            union(41, UnionMode::Dense),
            union(7, UnionMode::Sparse),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        ];
        for left in &corpus {
            for right in &corpus {
                assert_eq!(
                    arrow_data_types_equal_observed::<ValueTypeError>(left, right, || Ok(())),
                    Ok(left == right)
                );
            }
        }
    }
}
