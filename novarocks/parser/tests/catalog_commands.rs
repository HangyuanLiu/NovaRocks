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
    ast::{CatalogStatement, Statement},
    parse,
    printer::print_statements,
};

#[test]
fn catalog_and_truncate_commands_round_trip_with_quoted_identifiers() {
    let source = "TRUNCATE TABLE `ice`.db.`t`; CREATE EXTERNAL CATALOG IF NOT EXISTS `warehouse` COMMENT 'remote' WITH PROPERTIES ('type' = 'iceberg', uri = 'http://rest'); DROP CATALOG IF EXISTS `warehouse`; CREATE DATABASE IF NOT EXISTS `ice`.db; DROP DATABASE IF EXISTS `ice`.db FORCE; DROP TABLE IF EXISTS `ice`.db.`t` FORCE; SHOW CREATE TABLE `ice`.db.`t`;";
    let statements = parse(source).expect("catalog command corpus parse");
    assert!(matches!(
        statements[0],
        Statement::Catalog(CatalogStatement::TruncateTable(_))
    ));
    assert!(matches!(
        statements[1],
        Statement::Catalog(CatalogStatement::CreateCatalog(_))
    ));
    assert!(matches!(
        statements[2],
        Statement::Catalog(CatalogStatement::DropCatalog(_))
    ));
    assert!(matches!(
        statements[3],
        Statement::Catalog(CatalogStatement::CreateDatabase(_))
    ));
    assert!(matches!(
        statements[4],
        Statement::Catalog(CatalogStatement::DropDatabase(_))
    ));
    assert!(matches!(
        statements[5],
        Statement::Catalog(CatalogStatement::DropTable(_))
    ));
    assert!(matches!(
        statements[6],
        Statement::Catalog(CatalogStatement::ShowCreateTable(_))
    ));
    let printed = print_statements(&statements);
    let reparsed = parse(&printed).expect("printed catalog commands parse");
    assert_eq!(print_statements(&reparsed), printed);
}

#[test]
fn malformed_owned_catalog_command_has_location() {
    let source = "CREATE CATALOG c PROPERTIES ('type')";
    let error = parse(source).expect_err("properties require an equals value");
    let user_error = error.to_user_error(source);
    assert_eq!(user_error.code().as_str(), "sql.parse.unexpected_token");
    assert!(user_error.location().is_some());
    assert!(parse("DROP CATALOG").is_err());
}

#[test]
fn truncate_preserves_legacy_branch_target_semantics() {
    let default_target = parse("TRUNCATE TABLE `ice`.db.`t`").expect("default branch parses");
    assert_eq!(
        print_statements(&default_target),
        "TRUNCATE TABLE `ice`.db.`t`"
    );

    let branch_target = parse("truncate table `ice`.db.`t`.branch_dev;").expect("branch parses");
    assert_eq!(
        print_statements(&branch_target),
        "TRUNCATE TABLE `ice`.db.`t`.branch_dev"
    );
    assert_eq!(
        parse(&print_statements(&branch_target)).expect("printed branch parses"),
        branch_target
    );
}

#[test]
fn truncate_rejects_read_only_or_non_bare_legacy_forms() {
    for source in [
        "TRUNCATE TABLE t.tag_v1",
        "TRUNCATE TABLE t PARTITION (p = 1)",
        "TRUNCATE TABLE t WHERE k = 1",
    ] {
        let error = parse(source).expect_err("legacy unsupported truncate form must fail");
        assert_eq!(
            error.to_user_error(source).code().as_str(),
            "sql.parse.unexpected_token"
        );
    }
}
