//! Static registry of scalar function signatures.
//!
//! Each entry maps a function name to one or more [`Signature`]
//! candidates. Resolution iterates candidates in registration order
//! (strict-match first, then polymorphic — see [`super::resolver`]).
//!
//! Step A intentionally covers only high-frequency function families.
//! Anything not listed here returns `ResolveError::UnknownFunction` from
//! the resolver, signalling the caller to fall back to the legacy
//! hand-written `infer_*` path. Step B will register the remaining
//! ~200 functions and retire the fallback.

use std::collections::HashMap;
use std::sync::LazyLock;

use super::signature::{Signature, TypeSpec};

/// Lookup all registered signatures for a function name. Returns `None`
/// when the name is unknown to the registry (caller should fall back to
/// the legacy path).
pub(crate) fn scalar_signatures(name: &str) -> Option<&'static [Signature]> {
    SCALAR_FN_SIGNATURES
        .get(&name.to_ascii_lowercase())
        .map(|v| v.as_slice())
}

static SCALAR_FN_SIGNATURES: LazyLock<HashMap<String, Vec<Signature>>> =
    LazyLock::new(|| {
        let mut m: HashMap<String, Vec<Signature>> = HashMap::new();
        register_string_fns(&mut m);
        register_numeric_fns(&mut m);
        register_datetime_fns(&mut m);
        register_condition_fns(&mut m);
        register_array_fns(&mut m);
        register_map_fns(&mut m);
        m
    });

// ---------------------------------------------------------------------------
// Registration helpers
// ---------------------------------------------------------------------------

fn add(map: &mut HashMap<String, Vec<Signature>>, name: &str, sig: Signature) {
    map.entry(name.to_ascii_lowercase()).or_default().push(sig);
}

fn add_for_every<T>(
    map: &mut HashMap<String, Vec<Signature>>,
    name: &str,
    types: &[T],
    mut build_sig: impl FnMut(&T) -> Signature,
) {
    for t in types {
        add(map, name, build_sig(t));
    }
}

// ---------------------------------------------------------------------------
// String functions
// ---------------------------------------------------------------------------

fn register_string_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // (Utf8) -> Utf8
    for name in [
        "upper", "lower", "trim", "ltrim", "rtrim", "reverse", "initcap",
        "md5", "to_base64", "from_base64", "url_encode", "url_decode",
        "char", "hex", "unhex",
    ] {
        add(m, name, Signature::new(vec![TypeSpec::Utf8], TypeSpec::Utf8));
    }

    // (Utf8) -> Int32 (length family).
    // Matches the legacy infer_scalar_function_return_type — `length`,
    // `char_length`, `bit_length`, etc. all return Int32 (not Int64).
    for name in ["length", "char_length", "character_length", "bit_length", "octet_length"]
    {
        add(m, name, Signature::new(vec![TypeSpec::Utf8], TypeSpec::Int32));
    }

    // (Utf8, ...) -> Utf8 — variadic concat family.
    for name in ["concat", "concat_ws", "elt", "format"] {
        add(
            m,
            name,
            Signature::variadic(vec![TypeSpec::Utf8], TypeSpec::Utf8),
        );
    }

    // (Utf8, Utf8) -> Utf8 — two-arg string transforms.
    for name in [
        "replace",
        "regexp_extract",
        "regexp_replace",
        "split_part",
        "substring_index",
    ] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Utf8, TypeSpec::Utf8], TypeSpec::Utf8),
        );
    }

    // substring(str, start) / substring(str, start, length) — overloaded
    for name in ["substr", "substring", "left", "right"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Utf8, TypeSpec::Int64], TypeSpec::Utf8),
        );
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Utf8, TypeSpec::Int64, TypeSpec::Int64],
                TypeSpec::Utf8,
            ),
        );
    }

    // lpad(str, length, pad) / rpad(str, length, pad)
    for name in ["lpad", "rpad"] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::Utf8, TypeSpec::Int64, TypeSpec::Utf8],
                TypeSpec::Utf8,
            ),
        );
    }

    // repeat(str, n) -> str
    add(
        m,
        "repeat",
        Signature::new(vec![TypeSpec::Utf8, TypeSpec::Int64], TypeSpec::Utf8),
    );

    // space(n) -> str
    add(
        m,
        "space",
        Signature::new(vec![TypeSpec::Int64], TypeSpec::Utf8),
    );

    // sha2(str, bits) -> str
    add(
        m,
        "sha2",
        Signature::new(vec![TypeSpec::Utf8, TypeSpec::Int64], TypeSpec::Utf8),
    );
}

// ---------------------------------------------------------------------------
// Numeric functions
// ---------------------------------------------------------------------------

/// Numeric types `abs`, `negative`, etc. preserve their argument type
/// over.
const NUMERIC_PRESERVING_TYPES: &[TypeSpec] = &[
    TypeSpec::Int8,
    TypeSpec::Int16,
    TypeSpec::Int32,
    TypeSpec::Int64,
    TypeSpec::Float32,
    TypeSpec::Float64,
];

fn register_numeric_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // Preserve-input-type: abs, negative.
    for name in ["abs", "negative"] {
        add_for_every(m, name, NUMERIC_PRESERVING_TYPES, |t| {
            Signature::new(vec![t.clone()], t.clone())
        });
    }

    // ceil / ceiling / floor: arg unconstrained numeric, returns Int64.
    // We register one entry per numeric input type so strict-match picks
    // them up without needing a "numeric supertype" type variable.
    for name in ["ceil", "ceiling", "dceil", "floor", "dfloor"] {
        add_for_every(m, name, NUMERIC_PRESERVING_TYPES, |t| {
            Signature::new(vec![t.clone()], TypeSpec::Int64)
        });
    }

    // Single-arg floating-point math returning Float64.
    for name in [
        "sqrt", "dsqrt", "cbrt", "exp", "dexp", "ln", "log2", "log10", "dlog10", "dlog1",
        "sin", "cos", "tan", "asin", "acos", "atan", "sinh", "cosh", "tanh",
        "radians", "degrees",
    ] {
        for t in NUMERIC_PRESERVING_TYPES {
            add(m, name, Signature::new(vec![t.clone()], TypeSpec::Float64));
        }
    }

    // Two-arg math returning Float64: pow, log (base, x), mod, fmod, pmod.
    for name in [
        "pow", "fpow", "dpow", "power", "log", "mod", "fmod", "pmod",
        "atan2",
    ] {
        for tl in NUMERIC_PRESERVING_TYPES {
            for tr in NUMERIC_PRESERVING_TYPES {
                add(
                    m,
                    name,
                    Signature::new(vec![tl.clone(), tr.clone()], TypeSpec::Float64),
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Datetime functions
// ---------------------------------------------------------------------------

fn register_datetime_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // now / current_timestamp / curdate / current_date all return Datetime
    // (Timestamp(Microsecond, None)) in NovaRocks today, matching the legacy
    // codegen-side inference. `curdate` / `current_date` are *not* Date
    // despite their names; rely on cast at projection if needed.
    for name in [
        "now",
        "current_timestamp",
        "localtimestamp",
        "localtime",
        "curdate",
        "current_date",
    ] {
        add(m, name, Signature::new(vec![], TypeSpec::Datetime));
    }

    // year/month/day/hour/minute/second/dayofweek/dayofmonth/dayofyear (datetime) -> Int
    for name in [
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "dayofmonth",
        "dayofweek",
        "dayofyear",
        "weekofyear",
        "quarter",
    ] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Datetime], TypeSpec::Int32),
        );
        add(m, name, Signature::new(vec![TypeSpec::Date], TypeSpec::Int32));
    }

    // date_trunc(unit, datetime) -> datetime
    add(
        m,
        "date_trunc",
        Signature::new(
            vec![TypeSpec::Utf8, TypeSpec::Datetime],
            TypeSpec::Datetime,
        ),
    );
    add(
        m,
        "date_trunc",
        Signature::new(vec![TypeSpec::Utf8, TypeSpec::Date], TypeSpec::Date),
    );

    // date_format(datetime, varchar) -> Utf8
    add(
        m,
        "date_format",
        Signature::new(
            vec![TypeSpec::Datetime, TypeSpec::Utf8],
            TypeSpec::Utf8,
        ),
    );
    add(
        m,
        "date_format",
        Signature::new(vec![TypeSpec::Date, TypeSpec::Utf8], TypeSpec::Utf8),
    );
}

// ---------------------------------------------------------------------------
// Condition functions
// ---------------------------------------------------------------------------

fn register_condition_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // isnull / isnotnull take anything, return bool. We register T-based
    // signatures so they bind to whatever argument type was passed.
    for name in ["isnull", "isnotnull"] {
        add(
            m,
            name,
            Signature::new(vec![TypeSpec::Any("T")], TypeSpec::Boolean),
        );
    }
}

// ---------------------------------------------------------------------------
// Array functions
// ---------------------------------------------------------------------------

fn register_array_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // cardinality / array_length / array_size : List<T> -> Int32
    // (matches the legacy codegen-side inference; analyzer also returns Int32).
    for name in ["cardinality", "array_length", "array_size"] {
        add(
            m,
            name,
            Signature::new(
                vec![TypeSpec::List(Box::new(TypeSpec::Any("T")))],
                TypeSpec::Int32,
            ),
        );
    }

    // array_append(List<T>, T) -> List<T>
    add(
        m,
        "array_append",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Any("T"),
            ],
            TypeSpec::List(Box::new(TypeSpec::Any("T"))),
        ),
    );

    // array_concat(List<T>, List<T>, ...) -> List<T> — variadic
    add(
        m,
        "array_concat",
        Signature::variadic(
            vec![TypeSpec::List(Box::new(TypeSpec::Any("T")))],
            TypeSpec::List(Box::new(TypeSpec::Any("T"))),
        ),
    );

    // array_contains(List<T>, T) -> bool
    add(
        m,
        "array_contains",
        Signature::new(
            vec![
                TypeSpec::List(Box::new(TypeSpec::Any("T"))),
                TypeSpec::Any("T"),
            ],
            TypeSpec::Boolean,
        ),
    );
}

// ---------------------------------------------------------------------------
// Map functions
// ---------------------------------------------------------------------------

fn register_map_fns(m: &mut HashMap<String, Vec<Signature>>) {
    // map_keys(Map<K, V>) -> List<K>
    add(
        m,
        "map_keys",
        Signature::new(
            vec![TypeSpec::Map(
                Box::new(TypeSpec::Any("K")),
                Box::new(TypeSpec::Any("V")),
            )],
            TypeSpec::List(Box::new(TypeSpec::Any("K"))),
        ),
    );

    // map_values(Map<K, V>) -> List<V>
    add(
        m,
        "map_values",
        Signature::new(
            vec![TypeSpec::Map(
                Box::new(TypeSpec::Any("K")),
                Box::new(TypeSpec::Any("V")),
            )],
            TypeSpec::List(Box::new(TypeSpec::Any("V"))),
        ),
    );

    // map_size(Map<K, V>) -> Int32
    // (matches the legacy codegen-side inference.)
    add(
        m,
        "map_size",
        Signature::new(
            vec![TypeSpec::Map(
                Box::new(TypeSpec::Any("K")),
                Box::new(TypeSpec::Any("V")),
            )],
            TypeSpec::Int32,
        ),
    );
}
