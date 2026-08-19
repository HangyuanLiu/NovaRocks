// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.

use novarocks_parser::{
    ast::{Ident, Literal, Statement, Visit},
    parse,
    printer::print_statement,
};

fn statement(sql: &str) -> Statement {
    let mut statements = parse(sql).expect("Iceberg command should parse");
    assert_eq!(statements.len(), 1);
    statements.remove(0)
}

#[test]
fn parses_schema_properties_partition_references_and_add_files() {
    let schema = statement(
        "ALTER TABLE ice.db.orders ADD COLUMN address.zip INT DEFAULT 94107 AFTER address.city",
    );
    assert!(matches!(schema, Statement::Iceberg(_)));

    let properties = statement(
        "ALTER TABLE ice.db.orders SET TBLPROPERTIES ('write.format.default' = 'parquet')",
    );
    assert!(matches!(properties, Statement::Iceberg(_)));

    let partition = statement("ALTER TABLE ice.db.orders ADD PARTITION COLUMN bucket(user_id, 32)");
    assert!(matches!(partition, Statement::Iceberg(_)));

    let identity_partition =
        statement("ALTER TABLE ice.db.orders ADD PARTITION COLUMN identity(user_id)");
    assert!(matches!(identity_partition, Statement::Iceberg(_)));

    let reference =
        statement("ALTER TABLE ice.db.orders CREATE OR REPLACE BRANCH dev AS OF VERSION 42");
    assert!(matches!(reference, Statement::Iceberg(_)));

    let files = statement("ALTER TABLE ice.db.orders ADD FILES FROM 's3://warehouse/staged'");
    assert!(matches!(files, Statement::Iceberg(_)));
}

#[test]
fn schema_default_preserves_negative_numeric_literal() {
    let parsed = statement("ALTER TABLE ice.db.orders ADD COLUMN offset INT DEFAULT -7");
    assert_eq!(
        print_statement(&parsed),
        "ALTER TABLE ice.db.orders ADD COLUMN offset INT DEFAULT -7"
    );
}

#[test]
fn quoted_identifiers_case_and_semicolon_round_trip_canonically() {
    let source = "alter table `ice`.`db`.`orders` add partition column TrUnCaTe(`user.id`, 16);";
    let parsed = statement(source);
    let canonical = print_statement(&parsed);
    assert_eq!(
        canonical,
        "ALTER TABLE `ice`.`db`.`orders` ADD PARTITION COLUMN truncate(`user.id`, 16)"
    );
    assert_eq!(print_statement(&statement(&canonical)), canonical);
}

#[test]
fn schema_types_preserve_parameterized_and_nested_shapes() {
    let parsed = statement(
        "ALTER TABLE ice.db.orders ADD COLUMN profile STRUCT<name VARCHAR(32), attrs MAP<STRING, ARRAY<DECIMAL(12, 3)>>>",
    );
    let canonical = print_statement(&parsed);
    assert_eq!(
        canonical,
        "ALTER TABLE ice.db.orders ADD COLUMN profile STRUCT<name VARCHAR(32), attrs MAP<STRING, ARRAY<DECIMAL(12, 3)>>>"
    );
    assert_eq!(print_statement(&statement(&canonical)), canonical);
}

#[test]
fn malformed_owned_iceberg_commands_are_typed_errors() {
    for sql in [
        "ALTER TABLE t ADD FILES FROM not_a_string",
        "ALTER TABLE t ADD PARTITION COLUMN bucket(id)",
        "ALTER TABLE t CREATE BRANCH dev AS OF VERSION",
        "ALTER TABLE t UNSET TBLPROPERTIES ()",
    ] {
        let error = parse(sql).expect_err("owned malformed command must fail");
        assert_eq!(
            error.to_user_error(sql).code().as_str(),
            "sql.parse.unexpected_token",
            "{sql}"
        );
    }
}

#[test]
fn equality_delete_remains_outside_the_iceberg_grammar_scope() {
    let sql = "ALTER TABLE t ADD EQUALITY DELETE (id) VALUES (1)";
    let error = parse(sql).expect_err("outside T4 scope");
    assert_eq!(
        error.to_user_error(sql).code().as_str(),
        "sql.parse.unexpected_token"
    );
}

#[test]
fn visitor_reaches_iceberg_nested_syntax() {
    struct CountVisitor(usize);

    impl Visit for CountVisitor {
        fn visit_ident(&mut self, _: &Ident) {
            self.0 += 1;
        }

        fn visit_literal(&mut self, _: &Literal) {
            self.0 += 1;
        }
    }

    let statement = statement(
        "ALTER TABLE ice.db.orders ADD COLUMN address.zip INT DEFAULT 94107 AFTER address.city",
    );
    let mut visitor = CountVisitor(0);
    visitor.visit_statement(&statement);
    assert_eq!(visitor.0, 9);
}
