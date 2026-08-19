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

/// The owner-declared phase of a SQL error code.  The runner consumes this
/// descriptor data; it never derives a phase from a code string or message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorPhase {
    Lex,
    Parse,
    Validate,
    Analyze,
    Admit,
}

impl SqlErrorPhase {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "Lex" => Some(Self::Lex),
            "Parse" => Some(Self::Parse),
            "Validate" => Some(Self::Validate),
            "Analyze" => Some(Self::Analyze),
            "Admit" => Some(Self::Admit),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Lex => "Lex",
            Self::Parse => "Parse",
            Self::Validate => "Validate",
            Self::Analyze => "Analyze",
            Self::Admit => "Admit",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlErrorDescriptor {
    pub code: &'static str,
    pub phase: SqlErrorPhase,
}

/// SQLP-1 replaces this empty compile-time slice with the generated aggregate
/// manifest.  Keeping it empty here is intentional: no production suite may
/// silently start relying on an unregistered SQL error code.
pub const SQL_ERROR_DESCRIPTORS: &[SqlErrorDescriptor] = &[];

pub fn lookup_sql_error_descriptor<'a>(
    descriptors: &'a [SqlErrorDescriptor],
    code: &str,
) -> Option<&'a SqlErrorDescriptor> {
    descriptors
        .iter()
        .find(|descriptor| descriptor.code == code)
}

/// SQL error codes are lower-case dotted tokens.  This separates their wire
/// representation from the existing CamelCase `EngineErrorCode` channel.
pub fn is_sql_error_code_token(candidate: &str) -> bool {
    let mut segments = candidate.split('.');
    let Some(first) = segments.next() else {
        return false;
    };
    if first.is_empty() || !is_sql_error_code_segment(first) {
        return false;
    }

    let mut has_dot = false;
    for segment in segments {
        has_dot = true;
        if segment.is_empty() || !is_sql_error_code_segment(segment) {
            return false;
        }
    }
    has_dot
}

fn is_sql_error_code_segment(segment: &str) -> bool {
    segment
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(test)]
pub const TEST_SQL_ERROR_DESCRIPTORS: &[SqlErrorDescriptor] = &[
    SqlErrorDescriptor {
        code: "sql.test.fixture",
        phase: SqlErrorPhase::Parse,
    },
    SqlErrorDescriptor {
        code: "sql.test.analyze",
        phase: SqlErrorPhase::Analyze,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_lowercase_dotted_sql_error_codes_only() {
        assert!(is_sql_error_code_token("sql.parse.unexpected_token"));
        assert!(is_sql_error_code_token("sql.validate.bad_name_2"));
        assert!(!is_sql_error_code_token("CommitUnknown"));
        assert!(!is_sql_error_code_token("sql"));
        assert!(!is_sql_error_code_token("sql.Parse.unexpected"));
        assert!(!is_sql_error_code_token("sql..unexpected"));
    }
}
