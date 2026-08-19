// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Query grammar vertical-slice contracts. These cases prove that parser-owned
//! syntax can round-trip without making it a frontend execution route.

use novarocks_parser::{ast::Statement, parse, printer::Printer};

#[test]
fn query_forms_parse_and_print_as_typed_syntax() {
    for source in [
        "SELECT a, count(*) AS n FROM db.t AS t WHERE a >= 1 GROUP BY a HAVING count(*) > 0 ORDER BY a DESC LIMIT 5 OFFSET 2 ROWS FETCH FIRST 1 ROW ONLY",
        "SELECT CASE WHEN a IS NULL THEN CAST(0 AS INT) ELSE CAST(a AS INT) END AS normalized FROM t WHERE a NOT BETWEEN 1 AND 3",
        "WITH c AS (SELECT 1 AS a) SELECT a FROM c UNION ALL SELECT 2 ORDER BY a LIMIT 3",
        "SELECT l.id FROM left_table l LEFT JOIN right_table r ON l.id = r.id JOIN third_table s ON s.id = l.id",
        "EXPLAIN ANALYZE VALUES (1), (2)",
    ] {
        let statements = parse(source).unwrap_or_else(|error| panic!("{source}: {error:?}"));
        assert_eq!(statements.len(), 1, "{source}");
        assert!(matches!(
            statements.first(),
            Some(Statement::Query(_) | Statement::ExplainQuery(_))
        ));
        let canonical = Printer::new().statements(&statements);
        let reparsed =
            parse(&canonical).unwrap_or_else(|error| panic!("canonical `{canonical}`: {error:?}"));
        assert!(matches!(
            reparsed.first(),
            Some(Statement::Query(_) | Statement::ExplainQuery(_))
        ));
    }
}
