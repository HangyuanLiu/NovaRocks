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
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    AnyDecimal128,
    /// Decimal128 with unspecified precision/scale, bound to a name. Strict
    /// match accepts any `DataType::Decimal128(_, _)` and records the exact
    /// one, so a function that returns the decimal it was given -- `abs`,
    /// `negative` -- can name it on both sides. `AnyDecimal128` cannot do
    /// that: it carries no name, so nothing can refer back to it.
    Decimal128Of(&'static str),
    /// LARGEINT, whose physical carrier is `FixedSizeBinary(16)`. It is one
    /// of this engine's integer types; the registry could not name it at all
    /// before, so every numeric family silently refused it.
    LargeInt,
    /// `List<inner>`. `inner` may itself be `Any(...)` for polymorphic
    /// signatures such as `array_append(List<T>, T) -> List<T>`.
    List(Box<TypeSpec>),
    /// `Map<key, value>`.
    Map(Box<TypeSpec>, Box<TypeSpec>),
    /// Type variable, e.g. `Any("T")`. Binds to the corresponding concrete
    /// argument type during polymorphic resolution.
    Any(&'static str),
    /// Any type at all, binding nothing. This is what a position accepts when
    /// the function genuinely does not constrain it -- `json_object`'s
    /// alternating keys and values, for instance. It differs from `Any(name)`
    /// in exactly the way that matters for a variadic tail: a repeated type
    /// *variable* asserts every argument shares one type, which for these
    /// functions is false. Like `AnyDecimal128` it keeps the argument's own
    /// type and cannot be a return type, having nothing to realize.
    AnyType,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArgumentBindingPolicy {
    Legacy,
    CoerceAndEnforce,
}

impl ArgumentBindingPolicy {
    pub(crate) fn is_enforced(self) -> bool {
        matches!(self, Self::CoerceAndEnforce)
    }
}

/// A single function signature record.
///
/// `args` is the parameter list. If `variadic` is true, the last element of
/// `args` is repeated to absorb extra positional arguments (matches the
/// `concat(str, str, ...)` style).
///
/// `widening` opts the signature into the resolver's widening-cast match
/// pass. Default-off: only the `coalesce` / `if` / `ifnull` / `case` /
/// `nvl` / `nullif` family — whose return type is the wider type of all
/// argument types — should enable this. Structural polymorphic
/// signatures like `array_append(List<T>, T) -> List<T>` must stay
/// `widening: false` so a call like `array_append(List<Int64>, Utf8)`
/// is correctly rejected (instead of silently widening `T` to `Utf8`
/// and producing `List<Utf8>`).
#[derive(Clone, Debug)]
pub(crate) struct Signature {
    pub(crate) args: Vec<TypeSpec>,
    pub(crate) ret: TypeSpec,
    pub(crate) variadic: bool,
    pub(crate) widening: bool,
    pub(crate) argument_binding: ArgumentBindingPolicy,
}

impl Signature {
    pub(crate) fn new(args: Vec<TypeSpec>, ret: TypeSpec) -> Self {
        Self {
            args,
            ret,
            variadic: false,
            widening: false,
            argument_binding: ArgumentBindingPolicy::Legacy,
        }
    }

    pub(crate) fn variadic(args: Vec<TypeSpec>, ret: TypeSpec) -> Self {
        Self {
            args,
            ret,
            variadic: true,
            widening: false,
            argument_binding: ArgumentBindingPolicy::Legacy,
        }
    }

    /// Mark this signature as widening: type variables `Any(name)` will be
    /// merged via `wider_type` when they appear at multiple positions with
    /// conflicting concrete types. Use this only for functions like
    /// `coalesce` / `if` / `ifnull` / `case` whose semantics genuinely
    /// produce the wider type — not for structural polymorphism like
    /// `array_append(List<T>, T)`.
    pub(crate) fn with_widening(mut self) -> Self {
        self.widening = true;
        self
    }

    pub(crate) fn with_argument_coercion(mut self) -> Self {
        self.argument_binding = ArgumentBindingPolicy::CoerceAndEnforce;
        self
    }

    pub(crate) fn canonical(&self) -> String {
        let mut value = String::from("(");
        for (index, argument) in self.args.iter().enumerate() {
            if index > 0 {
                value.push(',');
            }
            argument.write_canonical(&mut value);
        }
        if self.variadic {
            value.push_str("...");
        }
        value.push_str(")->");
        self.ret.write_canonical(&mut value);
        value.push_str(if self.widening { ";widen" } else { ";strict" });
        value.push_str(match self.argument_binding {
            ArgumentBindingPolicy::Legacy => ";legacy",
            ArgumentBindingPolicy::CoerceAndEnforce => ";coerce",
        });
        value
    }
}

impl TypeSpec {
    fn write_canonical(&self, output: &mut String) {
        match self {
            Self::Boolean => output.push_str("bool"),
            Self::Int8 => output.push_str("i8"),
            Self::Int16 => output.push_str("i16"),
            Self::Int32 => output.push_str("i32"),
            Self::Int64 => output.push_str("i64"),
            Self::Float32 => output.push_str("f32"),
            Self::Float64 => output.push_str("f64"),
            Self::Utf8 => output.push_str("utf8"),
            Self::Binary => output.push_str("binary"),
            Self::Date => output.push_str("date"),
            Self::Datetime => output.push_str("datetime"),
            Self::AnyDecimal128 => output.push_str("decimal128"),
            Self::List(item) => {
                output.push_str("list<");
                item.write_canonical(output);
                output.push('>');
            }
            Self::Map(key, value) => {
                output.push_str("map<");
                key.write_canonical(output);
                output.push(',');
                value.write_canonical(output);
                output.push('>');
            }
            Self::AnyType => output.push_str("any"),
            Self::LargeInt => output.push_str("largeint"),
            Self::Decimal128Of(name) => {
                output.push_str("decimal128<");
                output.push_str(name);
                output.push('>');
            }
            Self::Any(name) => {
                output.push_str("any<");
                output.push_str(name);
                output.push('>');
            }
        }
    }
}

/// Check whether a concrete `DataType` matches a `TypeSpec` *anchor*
/// (everything except `Any`). Returns `false` when called on `Any` —
/// polymorphic matching is handled separately by the resolver because it
/// needs to manage type-variable bindings.
pub(crate) fn anchor_matches(spec: &TypeSpec, dt: &DataType) -> bool {
    match (spec, dt) {
        // A NULL literal has no type of its own and is a value of whatever
        // type it is used as. Refusing it here would refuse `f(NULL)` for
        // every `f`, which is not a type error but a missing rule.
        //
        // Only a spec that names a type can absorb it this way. A spec with a
        // type variable in it has to go through unification, or the variable
        // would be left for the return type to realize with nothing bound.
        (spec, DataType::Null) if names_a_type(spec) => true,
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
        // `Decimal128Of` is deliberately absent: like `Any`, it must fall
        // through to the polymorphic pass so the concrete decimal is bound
        // before a return type tries to name it.
        (TypeSpec::LargeInt, DataType::FixedSizeBinary(width))
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            true
        }
        (TypeSpec::AnyType, _) => true,
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

/// Record every type variable reachable from `spec` as "seen but undecided",
/// so a NULL at this position does not leave the variable unbound while still
/// letting a later position decide it.
fn bind_nothing_but_open(spec: &TypeSpec, bindings: &mut Bindings) {
    match spec {
        TypeSpec::Any(name) | TypeSpec::Decimal128Of(name) => bindings.bind_null(name),
        TypeSpec::List(inner) => bind_nothing_but_open(inner, bindings),
        TypeSpec::Map(key, value) => {
            bind_nothing_but_open(key, bindings);
            bind_nothing_but_open(value, bindings);
        }
        _ => {}
    }
}

/// Whether this spec names a concrete type rather than standing for one.
fn names_a_type(spec: &TypeSpec) -> bool {
    match spec {
        TypeSpec::Any(_) => false,
        // A wildcard stands for no type in particular, so a NULL literal at
        // this position decides nothing either.
        TypeSpec::AnyType => false,
        TypeSpec::List(inner) => names_a_type(inner),
        TypeSpec::Map(key, value) => names_a_type(key) && names_a_type(value),
        _ => true,
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
pub(crate) fn realize(spec: &TypeSpec, bindings: &Bindings) -> Result<DataType, String> {
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
        TypeSpec::LargeInt => {
            DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
        }
        TypeSpec::Decimal128Of(name) => bindings
            .lookup(name)
            .ok_or_else(|| format!("decimal type variable {name} is unbound in the return type"))?,
        TypeSpec::AnyType => {
            return Err(
                "AnyType cannot appear as a return type — it stands for no type at all".to_string(),
            );
        }
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

/// How `Any(name)` is bound when the same variable shows up at multiple
/// positions in one signature.
///
/// `Strict` requires every occurrence to bind to the same concrete type
/// — used by the polymorphic match pass.
///
/// `Widening` merges conflicting bindings via [`novarocks_types::wider_type`]
/// — used by the widening-cast match pass, which is what makes a call
/// like `coalesce(Int8, Int64)` match the signature
/// `coalesce(Any("T"), Any("T"), ...) -> Any("T")` and yield `Int64`.
#[derive(Clone, Copy, Debug)]
pub(crate) enum BindMode {
    Strict,
    Widening,
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

    /// Try to bind `name` to `dt`. If `name` was already bound, the two
    /// bindings must agree everywhere either of them has decided a type (so
    /// `T` is consistent across all occurrences). Returns `false` on a
    /// conflicting bind.
    pub(crate) fn bind(&mut self, name: &'static str, dt: &DataType) -> bool {
        if let Some(existing) = self.lookup(name) {
            let Some(merged) = merge_undecided_types(&existing, dt) else {
                return false;
            };
            if merged != existing {
                self.replace(name, &merged);
            }
            return true;
        }
        self.entries.push((name, dt.clone()));
        true
    }

    /// Bind `name` to NULL only if nothing has said what it is.
    ///
    /// A NULL argument is a value of whatever the variable turns out to be, so
    /// it never contradicts another position and never decides one. It is
    /// recorded anyway, because a call whose every occurrence is NULL still
    /// has to realize a return type, and NULL is the honest answer there.
    pub(crate) fn bind_null(&mut self, name: &'static str) {
        if self.lookup(name).is_none() {
            self.entries.push((name, DataType::Null));
        }
    }

    fn replace(&mut self, name: &str, dt: &DataType) {
        for entry in self.entries.iter_mut() {
            if entry.0 == name {
                entry.1 = dt.clone();
            }
        }
    }

    /// Widening bind: if `name` is unbound, bind it to `dt`. If `name` is
    /// already bound to some `existing`, replace the binding with
    /// `wider_type(existing, dt)`. Always returns `true` — failure to widen
    /// means the two types have no common supertype, but that yields
    /// `wider_type == Utf8` (a deliberate fall-back in NovaRocks today),
    /// which we accept; downstream codegen / executor will error if the
    /// widened type is actually nonsensical.
    pub(crate) fn bind_widening(&mut self, name: &'static str, dt: &DataType) -> bool {
        if let Some(existing) = self.lookup(name) {
            if existing == DataType::Null {
                self.replace(name, dt);
                return true;
            }
            let widened = novarocks_types::wider_type(&existing, dt);
            for entry in self.entries.iter_mut() {
                if entry.0 == name {
                    entry.1 = widened.clone();
                }
            }
            return true;
        }
        self.entries.push((name, dt.clone()));
        true
    }
}

/// Merge two bindings of one type variable, treating NULL as undecided at
/// every depth.
///
/// A NULL is a value of whatever the variable turns out to be, so it never
/// contradicts another occurrence and never decides one. The type of `[]`
/// says exactly that one level in -- `List<NULL>` is a list whose element
/// type nothing has decided yet -- and `[[]]` says it two levels in. Reading
/// the rule only at the outermost type made `array_concat([[]], [[1]])` a
/// type error while `array_concat([], [1])` was not.
///
/// Returns `None` when the two disagree somewhere both of them have decided.
fn merge_undecided_types(existing: &DataType, incoming: &DataType) -> Option<DataType> {
    if existing == incoming {
        return Some(existing.clone());
    }
    match (existing, incoming) {
        (DataType::Null, decided) | (decided, DataType::Null) => Some(decided.clone()),
        (DataType::List(existing), DataType::List(incoming)) => {
            merge_undecided_fields(existing, incoming).map(DataType::List)
        }
        (DataType::LargeList(existing), DataType::LargeList(incoming)) => {
            merge_undecided_fields(existing, incoming).map(DataType::LargeList)
        }
        (DataType::Map(existing, existing_sorted), DataType::Map(incoming, incoming_sorted))
            if existing_sorted == incoming_sorted =>
        {
            merge_undecided_fields(existing, incoming)
                .map(|entries| DataType::Map(entries, *existing_sorted))
        }
        (DataType::Struct(existing), DataType::Struct(incoming))
            if existing.len() == incoming.len() =>
        {
            let fields = existing
                .iter()
                .zip(incoming.iter())
                .map(|(existing, incoming)| merge_undecided_fields(existing, incoming))
                .collect::<Option<Vec<_>>>()?;
            Some(DataType::Struct(fields.into()))
        }
        _ => None,
    }
}

fn merge_undecided_fields(
    existing: &arrow::datatypes::FieldRef,
    incoming: &arrow::datatypes::FieldRef,
) -> Option<arrow::datatypes::FieldRef> {
    if existing.data_type() == &DataType::Null {
        return Some(incoming.clone());
    }
    if incoming.data_type() == &DataType::Null {
        return Some(existing.clone());
    }
    if existing.name() != incoming.name() {
        return None;
    }
    let data_type = merge_undecided_types(existing.data_type(), incoming.data_type())?;
    Some(Arc::new(
        Field::new(
            existing.name(),
            data_type,
            existing.is_nullable() || incoming.is_nullable(),
        )
        .with_metadata(existing.metadata().clone()),
    ))
}

/// Polymorphic match: try to unify each `spec` against `dt`, recording any
/// type-variable bindings into `bindings`. Returns `false` on the first
/// concrete mismatch.
///
/// Anchor variants behave like `anchor_matches`; `Any(name)` binds the
/// variable; container variants recurse.
///
/// `mode` selects how `Any(name)` handles a repeated occurrence:
/// `BindMode::Strict` rejects different concrete types,
/// `BindMode::Widening` merges them via `wider_type`. The resolver runs
/// this twice: once strict (pass 2) and once widening (pass 3, "cast
/// match") for the few functions like `coalesce` / `if` / `ifnull` /
/// `case` whose return type is the widening of all argument types.
pub(crate) fn unify(
    spec: &TypeSpec,
    dt: &DataType,
    bindings: &mut Bindings,
    mode: BindMode,
) -> bool {
    match spec {
        // A NULL literal is a value of whatever type the variable turns out to
        // be, so it does not decide one. Binding `T` to NULL would make every
        // other position disagree with it and refuse the call - which is how
        // `f(NULL, x)` came to be a type error while `f(x, NULL)` was not.
        TypeSpec::Any(name) if matches!(dt, DataType::Null) => {
            bindings.bind_null(name);
            true
        }
        TypeSpec::Any(name) => match mode {
            BindMode::Strict => bindings.bind(name, dt),
            BindMode::Widening => bindings.bind_widening(name, dt),
        },
        // A named decimal binds like a type variable but only over decimals,
        // so the exact precision and scale travel to the return type.
        TypeSpec::Decimal128Of(name) => match dt {
            DataType::Decimal128(_, _) => match mode {
                BindMode::Strict => bindings.bind(name, dt),
                BindMode::Widening => bindings.bind_widening(name, dt),
            },
            DataType::Null => {
                bindings.bind_null(name);
                true
            }
            _ => false,
        },
        // A NULL literal is a value of whatever list or map the position
        // turns out to hold, exactly as it is for a bare type variable. It
        // decides nothing, so any variable inside the spec is left open for a
        // later position to decide -- `arrays_overlap(a, NULL)` takes its
        // element type from `a`.
        TypeSpec::List(inner_spec) if matches!(dt, DataType::Null) => {
            bind_nothing_but_open(inner_spec, bindings);
            true
        }
        TypeSpec::Map(key_spec, value_spec) if matches!(dt, DataType::Null) => {
            bind_nothing_but_open(key_spec, bindings);
            bind_nothing_but_open(value_spec, bindings);
            true
        }
        TypeSpec::List(inner_spec) => match dt {
            DataType::List(field) | DataType::LargeList(field) => {
                unify(inner_spec, field.data_type(), bindings, mode)
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
                unify(key_spec, fields[0].data_type(), bindings, mode)
                    && unify(value_spec, fields[1].data_type(), bindings, mode)
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
        assert!(unify(&spec, &DataType::Int64, &mut b, BindMode::Strict));
        assert_eq!(b.lookup("T"), Some(DataType::Int64));
    }

    #[test]
    fn unify_strict_rejects_inconsistent_binding() {
        // `f(T, T)` called with `(Int64, Utf8)` must fail in strict mode.
        let arg_spec = TypeSpec::Any("T");
        let mut b = Bindings::default();
        assert!(unify(&arg_spec, &DataType::Int64, &mut b, BindMode::Strict));
        assert!(!unify(&arg_spec, &DataType::Utf8, &mut b, BindMode::Strict));
    }

    #[test]
    fn unify_widening_merges_conflicting_bindings() {
        // `coalesce(T, T)` called with `(Int8, Int64)` in widening mode
        // binds T to the wider type (Int64).
        let arg_spec = TypeSpec::Any("T");
        let mut b = Bindings::default();
        assert!(unify(
            &arg_spec,
            &DataType::Int8,
            &mut b,
            BindMode::Widening
        ));
        assert!(unify(
            &arg_spec,
            &DataType::Int64,
            &mut b,
            BindMode::Widening
        ));
        assert_eq!(b.lookup("T"), Some(DataType::Int64));
    }

    #[test]
    fn unify_list_with_type_variable() {
        // `array_append(List<T>, T) -> List<T>`: bind T from List<Int64>.
        let arg0 = TypeSpec::List(Box::new(TypeSpec::Any("T")));
        let arg1 = TypeSpec::Any("T");
        let mut b = Bindings::default();
        assert!(unify(
            &arg0,
            &list_of(DataType::Int64),
            &mut b,
            BindMode::Strict
        ));
        assert!(unify(&arg1, &DataType::Int64, &mut b, BindMode::Strict));
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
