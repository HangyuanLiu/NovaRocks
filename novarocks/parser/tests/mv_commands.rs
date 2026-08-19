// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use novarocks_parser::{
    ast::{Fold, MaterializedViewStatement, Statement, Visit},
    parse,
    printer::print_statements,
};

#[test]
fn create_materialized_view_preserves_the_embedded_query_slice() {
    let source = "CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.orders_mv \
        COMMENT 'daily' PARTITION BY (bucket(order_id, 16), order_day) \
        DISTRIBUTED BY HASH (order_id) BUCKETS 3 \
        REFRESH DEFERRED ASYNC EVERY INTERVAL 2 HOURS PRIMARY KEY (order_id) \
        PROPERTIES ('storage_engine' = 'iceberg') AS SELECT order_id FROM orders";
    let statements = parse(source).expect("CREATE MATERIALIZED VIEW should parse");
    let [Statement::MaterializedView(MaterializedViewStatement::Create(create))] =
        statements.as_slice()
    else {
        panic!("expected typed CREATE MATERIALIZED VIEW");
    };
    assert_eq!(create.query.text, "SELECT order_id FROM orders");
    assert_eq!(
        print_statements(&statements),
        "CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.orders_mv COMMENT 'daily' \
         PARTITION BY (bucket(order_id, 16), order_day) DISTRIBUTED BY HASH (order_id) \
         BUCKETS 3 REFRESH DEFERRED ASYNC EVERY INTERVAL 2 HOURS PRIMARY KEY (order_id) \
         PROPERTIES ('storage_engine' = 'iceberg') AS SELECT order_id FROM orders"
    );
}

#[test]
fn mv_command_family_round_trips_through_the_typed_ast() {
    let source = "DROP MATERIALIZED VIEW IF EXISTS analytics.mv; \
        ALTER MATERIALIZED VIEW analytics.mv SET TBLPROPERTIES ('ttl' = '7'); \
        ALTER MATERIALIZED VIEW analytics.mv PAUSE REFRESH; \
        ALTER MATERIALIZED VIEW analytics.mv RESUME REFRESH; \
        ALTER MATERIALIZED VIEW analytics.mv REPARTITION BY (truncate(name, 4)); \
        REFRESH MATERIALIZED VIEW analytics.mv FULL WITH SYNC MODE; \
        SHOW MATERIALIZED VIEWS FROM analytics; \
        EXPLAIN REFRESH MATERIALIZED VIEW analytics.mv";
    let statements = parse(source).expect("MV command family should parse");
    assert_eq!(statements.len(), 8);
    let printed = print_statements(&statements);
    assert_eq!(
        printed,
        "DROP MATERIALIZED VIEW IF EXISTS analytics.mv; \
         ALTER MATERIALIZED VIEW analytics.mv SET TBLPROPERTIES ('ttl' = '7'); \
         ALTER MATERIALIZED VIEW analytics.mv PAUSE REFRESH; \
         ALTER MATERIALIZED VIEW analytics.mv RESUME REFRESH; \
         ALTER MATERIALIZED VIEW analytics.mv REPARTITION BY (truncate(name, 4)); \
         REFRESH MATERIALIZED VIEW analytics.mv FULL WITH SYNC MODE; \
         SHOW MATERIALIZED VIEWS FROM analytics; \
         EXPLAIN REFRESH MATERIALIZED VIEW analytics.mv"
    );
    let reparsed = parse(&printed).expect("printed MV commands should parse");
    assert_eq!(reparsed.len(), statements.len());
    assert!(
        reparsed
            .iter()
            .all(|statement| matches!(statement, Statement::MaterializedView(_)))
    );
}

#[test]
fn mv_visitor_and_folder_reach_the_query_and_nested_names() {
    struct Count {
        raw_queries: usize,
        identifiers: usize,
    }

    impl Visit for Count {
        fn visit_raw_query_slice(&mut self, _: &novarocks_parser::ast::RawQuerySlice) {
            self.raw_queries += 1;
        }

        fn visit_ident(&mut self, _: &novarocks_parser::ast::Ident) {
            self.identifiers += 1;
        }
    }

    struct Rename;

    impl Fold for Rename {
        fn fold_ident(
            &mut self,
            mut ident: novarocks_parser::ast::Ident,
        ) -> novarocks_parser::ast::Ident {
            if ident.value == "orders_mv" {
                ident.value = "renamed_mv".to_owned();
            }
            ident
        }
    }

    let [statement] = parse(
        "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH (id) BUCKETS 1 \
         AS SELECT id FROM orders",
    )
    .expect("MV should parse")
    .try_into()
    .expect("one statement");
    let mut count = Count {
        raw_queries: 0,
        identifiers: 0,
    };
    count.visit_statement(&statement);
    assert_eq!(count.raw_queries, 1);
    assert!(count.identifiers >= 1);

    let renamed = Rename.fold_statement(statement);
    assert_eq!(
        print_statements(&[renamed]),
        "CREATE MATERIALIZED VIEW renamed_mv DISTRIBUTED BY HASH (id) BUCKETS 1 \
         AS SELECT id FROM orders"
    );
}

#[test]
fn mv_drift_corpus_rejections_remain_parser_domain_failures() {
    for source in [
        "CREATE MATERIALIZED VIEW reject_mv AS SELECT 1",
        "CREATE MATERIALIZED VIEW reject_mv DISTRIBUTED BY HASH (k1) AS SELECT 1",
        "DROP MATERIALIZED VIEW reject_mv FORCE",
        "ALTER MATERIALIZED VIEW reject_mv SET FOO",
        "REFRESH MATERIALIZED VIEW reject_mv WITH BROKEN",
        "SHOW MATERIALIZED VIEWS LIKE 'reject%'",
        "CREATE MATERIALIZED VIEW reject_mv DISTRIBUTED BY HASH (k1) BUCKETS 1 PRIMARY KEY () AS SELECT k1 FROM source_table",
    ] {
        let error = parse(source).expect_err(source);
        assert_eq!(
            error.to_user_error(source).code().as_str(),
            "sql.parse.unexpected_token",
            "{source}"
        );
    }
}
