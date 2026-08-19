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
    ast::{BackendStatement, Statement},
    parse,
    printer::print_statements,
};

#[test]
fn backend_commands_are_case_insensitive_and_round_trip() {
    let source = "add backend 'be-1:9030'; DROP BACKEND 'be-2:9030' force; SHOW BACKENDS;";
    let statements = parse(source).expect("backend commands parse");
    assert!(matches!(
        statements[0],
        Statement::Backend(BackendStatement::AddBackend(_))
    ));
    assert!(matches!(
        statements[1],
        Statement::Backend(BackendStatement::DropBackend(_))
    ));
    assert!(matches!(
        statements[2],
        Statement::Backend(BackendStatement::ShowBackends(_))
    ));
    let printed = print_statements(&statements);
    assert_eq!(
        printed,
        "ADD BACKEND 'be-1:9030'; DROP BACKEND 'be-2:9030' FORCE; SHOW BACKENDS"
    );
    let reparsed = parse(&printed).expect("printed backend commands parse");
    assert_eq!(print_statements(&reparsed), printed);
}

#[test]
fn malformed_owned_backend_command_is_not_a_route_miss() {
    let error = parse("ADD BACKEND be-1:9030").expect_err("address must be quoted");
    assert_eq!(
        error.to_user_error("ADD BACKEND be-1:9030").code().as_str(),
        "sql.parse.unexpected_token"
    );
    let error = parse("DROP BACKEND 'be-1:9030' FORCE extra").expect_err("trailing token");
    assert_eq!(
        error
            .to_user_error("DROP BACKEND 'be-1:9030' FORCE extra")
            .code()
            .as_str(),
        "sql.parse.unexpected_token"
    );
}
