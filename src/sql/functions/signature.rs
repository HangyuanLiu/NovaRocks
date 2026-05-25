//! [`Signature`] / [`TypeSpec`] — the data the registry stores per function.
//!
//! `TypeSpec` is a structural description used at registration time. It
//! resembles `arrow::datatypes::DataType` but adds a `Any(name)` variant for
//! type variables (the equivalent of StarRocks' `ANY_ELEMENT`, `ANY_ARRAY`
//! etc.), so a single record can stand in for a family of concrete
//! signatures like `array_append(List<T>, T) -> List<T>`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

/// Structural type used in registered function signatures.
///
/// The variants split into three groups:
///
/// 1. Anchor variants that name a concrete `DataType` family (`Boolean`,
///    `Int64`, `Float64`, `Utf8`, ...). At resolution time these must match
///    the concrete argument type exactly (strict match) or via implicit
///    widening (cast match, not yet implemented).
/// 2. Container variants (`List`, `Map`) that recurse into a child
///    `TypeSpec`. Used to express `List<T>` / `Map<K, V>`.
/// 3. The `Any(name)` variant — a type variable that binds to whatever
///    concrete type the caller passes for that argument position. Every
///    occurrence of `Any("T")` in a single signature must bind to the same
///    concrete type for the match to succeed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TypeSpec {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Date,
    Datetime,
    /// Decimal128 with unspecified precision/scale. Strict match accepts any
    /// `DataType::Decimal128(_, _)`. (We don't yet propagate decimal scale
    /// derivation through the registry — the legacy `infer_*` path is still
    /// responsible for that until Step B.)
    AnyDecimal128,
    /// `List<inner>`. `inner` may itself be `Any(...)` for polymorphic
    /// signatures such as `array_append(List<T>, T) -> List<T>`.
    List(Box<TypeSpec>),
    /// `Map<key, value>`.
    Map(Box<TypeSpec>, Box<TypeSpec>),
    /// Type variable, e.g. `Any("T")`. Binds to the corresponding concrete
    /// argument type during polymorphic resolution.
    Any(&'static str),
}

/// A single function signature record.
///
/// `args` is the parameter list. If `variadic` is true, the last element of
/// `args` is repeated to absorb extra positional arguments (matches the
/// `concat(str, str, ...)` style).
#[derive(Clone, Debug)]
pub(crate) struct Signature {
    pub(crate) args: Vec<TypeSpec>,
    pub(crate) ret: TypeSpec,
    pub(crate) variadic: bool,
}

impl Signature {
    pub(crate) fn new(args: Vec<TypeSpec>, ret: TypeSpec) -> Self {
        Self {
            args,
            ret,
            variadic: false,
        }
    }

    pub(crate) fn variadic(args: Vec<TypeSpec>, ret: TypeSpec) -> Self {
        Self {
            args,
            ret,
            variadic: true,
        }
    }
}

/// Check whether a concrete `DataType` matches a `TypeSpec` *anchor*
/// (everything except `Any`). Returns `false` when called on `Any` —
/// polymorphic matching is handled separately by the resolver because it
/// needs to manage type-variable bindings.
pub(crate) fn anchor_matches(spec: &TypeSpec, dt: &DataType) -> bool {
    match (spec, dt) {
        (TypeSpec::Boolean, DataType::Boolean) => true,
        (TypeSpec::Int8, DataType::Int8) => true,
        (TypeSpec::Int16, DataType::Int16) => true,
        (TypeSpec::Int32, DataType::Int32) => true,
        (TypeSpec::Int64, DataType::Int64) => true,
        (TypeSpec::Float32, DataType::Float32) => true,
        (TypeSpec::Float64, DataType::Float64) => true,
        (TypeSpec::Utf8, DataType::Utf8) => true,
        (TypeSpec::Utf8, DataType::LargeUtf8) => true,
        (TypeSpec::Binary, DataType::Binary) => true,
        (TypeSpec::Binary, DataType::LargeBinary) => true,
        (TypeSpec::Date, DataType::Date32) => true,
        (TypeSpec::Datetime, DataType::Timestamp(_, _)) => true,
        (TypeSpec::AnyDecimal128, DataType::Decimal128(_, _)) => true,
        (TypeSpec::List(inner_spec), DataType::List(field)) => {
            anchor_matches(inner_spec, field.data_type())
        }
        (TypeSpec::List(inner_spec), DataType::LargeList(field)) => {
            anchor_matches(inner_spec, field.data_type())
        }
        (TypeSpec::Map(key_spec, value_spec), DataType::Map(entries, _)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return false;
            };
            if fields.len() != 2 {
                return false;
            }
            anchor_matches(key_spec, fields[0].data_type())
                && anchor_matches(value_spec, fields[1].data_type())
        }
        _ => false,
    }
}

/// Realize a `TypeSpec` (the return-type slot of a matched signature) into
/// a concrete `DataType`. `bindings` carries the type-variable assignments
/// produced by the polymorphic match — looking up `Any("T")` returns the
/// concrete type bound to it.
///
/// Returns `Err` if the spec references an unbound type variable, which is
/// a bug in the registry (return type referencing a name that does not
/// appear in `args`).
pub(crate) fn realize(
    spec: &TypeSpec,
    bindings: &Bindings,
) -> Result<DataType, String> {
    Ok(match spec {
        TypeSpec::Boolean => DataType::Boolean,
        TypeSpec::Int8 => DataType::Int8,
        TypeSpec::Int16 => DataType::Int16,
        TypeSpec::Int32 => DataType::Int32,
        TypeSpec::Int64 => DataType::Int64,
        TypeSpec::Float32 => DataType::Float32,
        TypeSpec::Float64 => DataType::Float64,
        TypeSpec::Utf8 => DataType::Utf8,
        TypeSpec::Binary => DataType::Binary,
        TypeSpec::Date => DataType::Date32,
        TypeSpec::Datetime => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        TypeSpec::AnyDecimal128 => {
            return Err("AnyDecimal128 cannot appear as a return type — \
                       precision/scale propagation is not yet handled by \
                       the registry"
                .to_string());
        }
        TypeSpec::List(inner) => {
            let item = realize(inner, bindings)?;
            DataType::List(Arc::new(Field::new("item", item, true)))
        }
        TypeSpec::Map(key, value) => {
            let k = realize(key, bindings)?;
            let v = realize(value, bindings)?;
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", k, true)),
                            Arc::new(Field::new("value", v, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }
        TypeSpec::Any(name) => bindings
            .lookup(name)
            .ok_or_else(|| format!("unbound type variable `{name}` in signature return type"))?,
    })
}

/// Type-variable bindings produced by polymorphic matching.
#[derive(Default, Debug)]
pub(crate) struct Bindings {
    entries: Vec<(&'static str, DataType)>,
}

impl Bindings {
    pub(crate) fn lookup(&self, name: &str) -> Option<DataType> {
        self.entries
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, dt)| dt.clone())
    }

    /// Try to bind `name` to `dt`. If `name` was already bound, require the
    /// existing binding to equal `dt` (so `T` is consistent across all
    /// occurrences). Returns `false` on a conflicting bind.
    pub(crate) fn bind(&mut self, name: &'static str, dt: &DataType) -> bool {
        if let Some(existing) = self.lookup(name) {
            return &existing == dt;
        }
        self.entries.push((name, dt.clone()));
        true
    }
}

/// Polymorphic match: try to unify each `spec` against `dt`, recording any
/// type-variable bindings into `bindings`. Returns `false` on the first
/// concrete mismatch.
///
/// Anchor variants behave like `anchor_matches`; `Any(name)` binds the
/// variable; container variants recurse.
pub(crate) fn unify(
    spec: &TypeSpec,
    dt: &DataType,
    bindings: &mut Bindings,
) -> bool {
    match spec {
        TypeSpec::Any(name) => bindings.bind(name, dt),
        TypeSpec::List(inner_spec) => match dt {
            DataType::List(field) | DataType::LargeList(field) => {
                unify(inner_spec, field.data_type(), bindings)
            }
            _ => false,
        },
        TypeSpec::Map(key_spec, value_spec) => match dt {
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return false;
                };
                if fields.len() != 2 {
                    return false;
                }
                unify(key_spec, fields[0].data_type(), bindings)
                    && unify(value_spec, fields[1].data_type(), bindings)
            }
            _ => false,
        },
        // For anchor specs, fall back to the no-bindings matcher.
        _ => anchor_matches(spec, dt),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit;

    fn list_of(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item, true)))
    }

    fn map_of(k: DataType, v: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", k, true)),
                        Arc::new(Field::new("value", v, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    #[test]
    fn anchor_matches_primitive_types() {
        assert!(anchor_matches(&TypeSpec::Int64, &DataType::Int64));
        assert!(!anchor_matches(&TypeSpec::Int64, &DataType::Int32));
        assert!(anchor_matches(&TypeSpec::Utf8, &DataType::Utf8));
        assert!(anchor_matches(&TypeSpec::Utf8, &DataType::LargeUtf8));
        assert!(anchor_matches(
            &TypeSpec::Datetime,
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        ));
        assert!(anchor_matches(
            &TypeSpec::AnyDecimal128,
            &DataType::Decimal128(38, 9)
        ));
    }

    #[test]
    fn anchor_matches_list_recursively() {
        let spec = TypeSpec::List(Box::new(TypeSpec::Int64));
        assert!(anchor_matches(&spec, &list_of(DataType::Int64)));
        assert!(!anchor_matches(&spec, &list_of(DataType::Int32)));
    }

    #[test]
    fn anchor_matches_map_recursively() {
        let spec = TypeSpec::Map(Box::new(TypeSpec::Utf8), Box::new(TypeSpec::Int64));
        assert!(anchor_matches(
            &spec,
            &map_of(DataType::Utf8, DataType::Int64)
        ));
        assert!(!anchor_matches(
            &spec,
            &map_of(DataType::Utf8, DataType::Int32)
        ));
    }

    #[test]
    fn unify_binds_type_variable() {
        let spec = TypeSpec::Any("T");
        let mut b = Bindings::default();
        assert!(unify(&spec, &DataType::Int64, &mut b));
        assert_eq!(b.lookup("T"), Some(DataType::Int64));
    }

    #[test]
    fn unify_rejects_inconsistent_binding() {
        // `f(T, T)` called with `(Int64, Utf8)` must fail.
        let arg_spec = TypeSpec::Any("T");
        let mut b = Bindings::default();
        assert!(unify(&arg_spec, &DataType::Int64, &mut b));
        assert!(!unify(&arg_spec, &DataType::Utf8, &mut b));
    }

    #[test]
    fn unify_list_with_type_variable() {
        // `array_append(List<T>, T) -> List<T>`: bind T from List<Int64>.
        let arg0 = TypeSpec::List(Box::new(TypeSpec::Any("T")));
        let arg1 = TypeSpec::Any("T");
        let mut b = Bindings::default();
        assert!(unify(&arg0, &list_of(DataType::Int64), &mut b));
        assert!(unify(&arg1, &DataType::Int64, &mut b));
        assert_eq!(b.lookup("T"), Some(DataType::Int64));
    }

    #[test]
    fn realize_returns_concrete_type_for_anchor() {
        let b = Bindings::default();
        assert_eq!(realize(&TypeSpec::Int64, &b).unwrap(), DataType::Int64);
        assert_eq!(realize(&TypeSpec::Utf8, &b).unwrap(), DataType::Utf8);
        assert_eq!(
            realize(&TypeSpec::Datetime, &b).unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn realize_returns_list_with_bound_type_variable() {
        let mut b = Bindings::default();
        b.bind("T", &DataType::Int64);
        let spec = TypeSpec::List(Box::new(TypeSpec::Any("T")));
        assert_eq!(realize(&spec, &b).unwrap(), list_of(DataType::Int64));
    }

    #[test]
    fn realize_returns_err_for_unbound_type_variable() {
        let b = Bindings::default();
        let spec = TypeSpec::Any("T");
        assert!(realize(&spec, &b).is_err());
    }
}
