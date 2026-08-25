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

use novarocks_parser::{
    ast::{ShowBackends, Statement},
    parse,
    printer::print_statement,
};

#[test]
fn show_backends_is_the_only_backend_sql_surface() {
    let statements = parse("show backends").expect("SHOW BACKENDS must parse");
    assert!(matches!(
        statements.as_slice(),
        [Statement::ShowBackends(ShowBackends { .. })]
    ));
    assert_eq!(print_statement(&statements[0]), "SHOW BACKENDS");
}

#[test]
fn backend_membership_mutations_are_not_sql_syntax() {
    for source in [
        "ADD BACKEND 'be-1:9030'",
        "DROP BACKEND 'be-1:9030'",
        "DROP BACKEND 'be-1:9030' FORCE",
    ] {
        let error = parse(source).expect_err("legacy membership mutation must not parse");
        assert_eq!(
            error.to_user_error(source).code().as_str(),
            "sql.parse.unsupported_statement"
        );
    }
}
