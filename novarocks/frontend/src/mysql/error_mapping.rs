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

//! Frontend-owned mapping from stable domain codes to MySQL wire kinds.

use novarocks_user_error::ErrorCodeId;
use opensrv_mysql::ErrorKind;

const ERROR_KIND_MAPPINGS: &[(&str, ErrorKind)] = &[
    ("sql.lex.unexpected_character", ErrorKind::ER_PARSE_ERROR),
    ("sql.lex.unterminated_string", ErrorKind::ER_PARSE_ERROR),
    (
        "sql.lex.unterminated_quoted_identifier",
        ErrorKind::ER_PARSE_ERROR,
    ),
    ("sql.lex.unterminated_comment", ErrorKind::ER_PARSE_ERROR),
    ("sql.parse.unexpected_token", ErrorKind::ER_PARSE_ERROR),
    ("sql.validate.invalid_structure", ErrorKind::ER_PARSE_ERROR),
    ("sql.validate.duplicate_cte_name", ErrorKind::ER_PARSE_ERROR),
    (
        "sql.validate.duplicate_window_name",
        ErrorKind::ER_PARSE_ERROR,
    ),
    (
        "sql.validate.invalid_window_frame_bounds",
        ErrorKind::ER_PARSE_ERROR,
    ),
    (
        "sql.parse.unsupported_statement",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    (
        "sql.admit.delete_requires_where",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    (
        "sql.admit.delete_unsupported_form",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    (
        "sql.admit.update_unsupported_form",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    (
        "sql.admit.merge_unsupported_form",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    (
        "sql.admit.insert_unsupported_form",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    (
        "sql.admit.create_table_unsupported_form",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    (
        "sql.admit.session_global_scope_unsupported",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    (
        "sql.admit.kill_connection_unsupported",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    (
        "sql.analyze.unsupported_expression",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    (
        "sql.analyze.unsupported_query_shape",
        ErrorKind::ER_NOT_SUPPORTED_YET,
    ),
    ("sql.analyze.unknown_table", ErrorKind::ER_NO_SUCH_TABLE),
    ("sql.analyze.unknown_column", ErrorKind::ER_BAD_FIELD_ERROR),
    (
        "sql.analyze.unknown_function",
        ErrorKind::ER_FUNCTION_NOT_DEFINED,
    ),
    ("sql.analyze.type_mismatch", ErrorKind::ER_WRONG_ARGUMENTS),
    ("sql.analyze.invalid_literal", ErrorKind::ER_WRONG_VALUE),
    (
        "sql.analyze.invalid_argument",
        ErrorKind::ER_WRONG_ARGUMENTS,
    ),
    ("sql.analyze.invalid_query_shape", ErrorKind::ER_WRONG_USAGE),
    ("sql.analyze.internal", ErrorKind::ER_UNKNOWN_ERROR),
];

pub(super) fn error_kind_for_code(code: ErrorCodeId) -> Option<ErrorKind> {
    ERROR_KIND_MAPPINGS
        .iter()
        .find_map(|(mapped_code, kind)| (*mapped_code == code.as_str()).then_some(*kind))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use novarocks_parser::ERROR_CODE_DESCRIPTORS as PARSER_ERROR_CODE_DESCRIPTORS;
    use novarocks_sql::analyze_error::ERROR_CODE_DESCRIPTORS as ANALYZE_ERROR_CODE_DESCRIPTORS;
    use novarocks_user_error::ErrorCodeStatus;

    use super::*;
    use crate::{DML_ERROR_CODE_DESCRIPTORS, SESSION_ERROR_CODE_DESCRIPTORS};

    #[test]
    fn every_active_manifest_descriptor_has_exactly_one_wire_mapping() {
        let mut descriptor_codes = BTreeSet::new();
        for descriptor in PARSER_ERROR_CODE_DESCRIPTORS
            .iter()
            .chain(ANALYZE_ERROR_CODE_DESCRIPTORS)
            .chain(DML_ERROR_CODE_DESCRIPTORS)
            .chain(SESSION_ERROR_CODE_DESCRIPTORS)
            .filter(|descriptor| descriptor.status == ErrorCodeStatus::Active)
        {
            assert!(descriptor_codes.insert(descriptor.code.as_str()));
            assert!(error_kind_for_code(descriptor.code).is_some());
        }
        let mapping_codes = ERROR_KIND_MAPPINGS
            .iter()
            .map(|(code, _)| *code)
            .collect::<BTreeSet<_>>();
        assert_eq!(descriptor_codes.len(), 28);
        assert_eq!(mapping_codes.len(), ERROR_KIND_MAPPINGS.len());
        assert_eq!(mapping_codes, descriptor_codes);
        assert!(error_kind_for_code(ErrorCodeId::new("sql.analyze.unregistered")).is_none());
    }

    #[test]
    fn analyze_codes_use_the_frozen_mysql_kinds() {
        assert_eq!(
            error_kind_for_code(ErrorCodeId::new("sql.analyze.unknown_table")),
            Some(ErrorKind::ER_NO_SUCH_TABLE)
        );
        assert_eq!(
            error_kind_for_code(ErrorCodeId::new("sql.analyze.unknown_column")),
            Some(ErrorKind::ER_BAD_FIELD_ERROR)
        );
        assert_eq!(
            error_kind_for_code(ErrorCodeId::new("sql.analyze.unknown_function")),
            Some(ErrorKind::ER_FUNCTION_NOT_DEFINED)
        );
        assert_eq!(
            error_kind_for_code(ErrorCodeId::new("sql.analyze.type_mismatch")),
            Some(ErrorKind::ER_WRONG_ARGUMENTS)
        );
        assert_eq!(
            error_kind_for_code(ErrorCodeId::new("sql.analyze.invalid_literal")),
            Some(ErrorKind::ER_WRONG_VALUE)
        );
        assert_eq!(
            error_kind_for_code(ErrorCodeId::new("sql.analyze.invalid_argument")),
            Some(ErrorKind::ER_WRONG_ARGUMENTS)
        );
        assert_eq!(
            error_kind_for_code(ErrorCodeId::new("sql.analyze.unsupported_expression")),
            Some(ErrorKind::ER_NOT_SUPPORTED_YET)
        );
        assert_eq!(
            error_kind_for_code(ErrorCodeId::new("sql.analyze.unsupported_query_shape")),
            Some(ErrorKind::ER_NOT_SUPPORTED_YET)
        );
        assert_eq!(
            error_kind_for_code(ErrorCodeId::new("sql.analyze.invalid_query_shape")),
            Some(ErrorKind::ER_WRONG_USAGE)
        );
        assert_eq!(
            error_kind_for_code(ErrorCodeId::new("sql.analyze.internal")),
            Some(ErrorKind::ER_UNKNOWN_ERROR)
        );
    }

    #[test]
    fn session_admit_codes_use_not_supported_yet() {
        for code in [
            "sql.admit.session_global_scope_unsupported",
            "sql.admit.kill_connection_unsupported",
        ] {
            assert_eq!(
                error_kind_for_code(ErrorCodeId::new(code)),
                Some(ErrorKind::ER_NOT_SUPPORTED_YET)
            );
        }
    }
}
