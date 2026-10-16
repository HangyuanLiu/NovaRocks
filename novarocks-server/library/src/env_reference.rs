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

//! Server-owned exact environment-reference resolution for application TOML.

use std::fmt;

/// Resolves exact `${ENV:NAME}` scalar references through the process environment.
pub(crate) fn resolve_env_references(value: &mut toml::Value) -> Result<(), EnvReferenceError> {
    resolve_env_references_with(value, |name| match std::env::var(name) {
        Ok(value) if value.is_empty() => Err(EnvLookupError::Empty),
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Err(EnvLookupError::Missing),
        Err(std::env::VarError::NotUnicode(_)) => Err(EnvLookupError::NonUtf8),
    })
}

/// Resolves exact references with an injected lookup for deterministic tests.
pub(crate) fn resolve_env_references_with<F>(
    value: &mut toml::Value,
    mut lookup: F,
) -> Result<(), EnvReferenceError>
where
    F: FnMut(&str) -> Result<String, EnvLookupError>,
{
    resolve_value(value, "", &mut lookup)
}

fn resolve_value<F>(
    value: &mut toml::Value,
    path: &str,
    lookup: &mut F,
) -> Result<(), EnvReferenceError>
where
    F: FnMut(&str) -> Result<String, EnvLookupError>,
{
    match value {
        toml::Value::Table(table) => {
            let keys = table.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                let child_path = join_key(path, &key);
                resolve_value(
                    table
                        .get_mut(&key)
                        .expect("table key was collected from this table"),
                    &child_path,
                    lookup,
                )?;
            }
        }
        toml::Value::Array(values) => {
            for (index, child) in values.iter_mut().enumerate() {
                resolve_value(child, &format!("{path}[{index}]"), lookup)?;
            }
        }
        toml::Value::String(string) => {
            if let Some(name) = exact_env_reference(string) {
                let replacement = lookup(name).map_err(|kind| EnvReferenceError {
                    path: path.to_owned(),
                    kind: kind.into(),
                })?;
                *string = replacement;
            } else if string.contains("${ENV:") {
                return Err(EnvReferenceError {
                    path: path.to_owned(),
                    kind: EnvReferenceErrorKind::InvalidReference,
                });
            }
        }
        _ => {}
    }
    Ok(())
}

fn join_key(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_owned()
    } else {
        format!("{parent}.{key}")
    }
}

fn exact_env_reference(value: &str) -> Option<&str> {
    let name = value.strip_prefix("${ENV:")?.strip_suffix('}')?;
    is_valid_env_name(name).then_some(name)
}

fn is_valid_env_name(name: &str) -> bool {
    let mut characters = name.bytes();
    matches!(characters.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_'))
        && characters
            .all(|character| matches!(character, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

/// The source lookup category, deliberately without the referenced value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EnvLookupError {
    Missing,
    Empty,
    NonUtf8,
}

/// The public category of a resolution failure, deliberately without secret content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EnvReferenceErrorKind {
    InvalidReference,
    Missing,
    Empty,
    NonUtf8,
}

impl From<EnvLookupError> for EnvReferenceErrorKind {
    fn from(value: EnvLookupError) -> Self {
        match value {
            EnvLookupError::Missing => Self::Missing,
            EnvLookupError::Empty => Self::Empty,
            EnvLookupError::NonUtf8 => Self::NonUtf8,
        }
    }
}

/// A path-aware environment-reference resolution failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EnvReferenceError {
    path: String,
    kind: EnvReferenceErrorKind,
}

impl EnvReferenceError {
    #[cfg(test)]
    fn path(&self) -> &str {
        &self.path
    }

    #[cfg(test)]
    fn kind(&self) -> EnvReferenceErrorKind {
        self.kind
    }
}

impl fmt::Display for EnvReferenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "environment reference at config path `{}` is {}",
            self.path,
            match self.kind {
                EnvReferenceErrorKind::InvalidReference => "not an exact ${ENV:VAR} reference",
                EnvReferenceErrorKind::Missing => "missing",
                EnvReferenceErrorKind::Empty => "empty",
                EnvReferenceErrorKind::NonUtf8 => "not valid UTF-8",
            }
        )
    }
}

impl std::error::Error for EnvReferenceError {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{EnvLookupError, EnvReferenceErrorKind, resolve_env_references_with};

    #[test]
    fn resolves_nested_tables_and_arrays_once() {
        let mut value = r#"
[connector.object_store]
access_key_id = "${ENV:ACCESS_KEY}"

[[connector.object_store.session_tokens]]
value = "${ENV:SESSION_TOKEN}"
"#
        .parse::<toml::Value>()
        .expect("parse test TOML");
        let values = HashMap::from([
            ("ACCESS_KEY", "access-key"),
            ("SESSION_TOKEN", "${ENV:ACCESS_KEY}"),
        ]);

        resolve_env_references_with(&mut value, |name| {
            values
                .get(name)
                .map(|value| (*value).to_owned())
                .ok_or(EnvLookupError::Missing)
        })
        .expect("references resolve");

        assert_eq!(
            value["connector"]["object_store"]["access_key_id"].as_str(),
            Some("access-key")
        );
        assert_eq!(
            value["connector"]["object_store"]["session_tokens"][0]["value"].as_str(),
            Some("${ENV:ACCESS_KEY}")
        );
    }

    #[test]
    fn preserves_plain_strings_but_rejects_interpolation_and_invalid_names() {
        for input in ["prefix-${ENV:VALUE}", "${ENV:NOT-VALID}"] {
            let mut value = format!("value = {input:?}")
                .parse::<toml::Value>()
                .expect("parse test TOML");

            let error = resolve_env_references_with(&mut value, |_| {
                panic!("invalid reference must not invoke lookup")
            })
            .expect_err("invalid reference must fail");

            assert_eq!(error.path(), "value");
            assert_eq!(error.kind(), EnvReferenceErrorKind::InvalidReference);
        }

        let mut plain = "value = 'literal ${NOT_ENV:VALUE}'"
            .parse::<toml::Value>()
            .expect("parse plain string");
        resolve_env_references_with(&mut plain, |_| unreachable!("no reference"))
            .expect("ordinary string remains unchanged");
        assert_eq!(plain["value"].as_str(), Some("literal ${NOT_ENV:VALUE}"));
    }

    #[test]
    fn categorizes_lookup_failures_without_exposing_values() {
        for (lookup_error, expected_kind) in [
            (EnvLookupError::Missing, EnvReferenceErrorKind::Missing),
            (EnvLookupError::Empty, EnvReferenceErrorKind::Empty),
            (EnvLookupError::NonUtf8, EnvReferenceErrorKind::NonUtf8),
        ] {
            let mut value = "secret = '${ENV:SECRET}'"
                .parse::<toml::Value>()
                .expect("parse test TOML");
            let error = resolve_env_references_with(&mut value, |_| Err(lookup_error))
                .expect_err("lookup must fail");

            assert_eq!(error.path(), "secret");
            assert_eq!(error.kind(), expected_kind);
            assert!(!format!("{error:?}").contains("nwt-1-secret-canary"));
            assert!(!error.to_string().contains("nwt-1-secret-canary"));
        }
    }
}
