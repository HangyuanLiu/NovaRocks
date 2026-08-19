<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# SQL Conformance Corpus

`tests/sql/suites/` is NovaRocks' executable SQL conformance corpus.  Its
three manifests are derived from runnable cases rather than maintained as a
separate compatibility spreadsheet.

## Taxonomy

- **accept**: every existing suite is the acceptance baseline.  These cases
  document SQL NovaRocks currently accepts and the resulting behavior.  Do not
  move an existing case merely to classify it.
- **reject**: `sql-reject/` contains statements NovaRocks must reject.  Its
  cases cover malformed syntax, recognized-but-unsupported syntax, and
  capability rejection.  A reject case must fail; an unexpected success is a
  test failure.
- **extension**: a NovaRocks-specific statement stays in the suite that
  exercises it and carries an `@nova_extension` directive.  The runner derives
  the extension manifest from those executable annotations; do not maintain a
  hand-written duplicate list.

## Error assertion tiers

Each reject assertion belongs to one of two mechanically distinct tiers:

- **drift** locks the observed behavior so that a later change is visible.  It
  is not a claim that the observed error is the final user contract.  Omit the
  tier only for legacy cases; new reject cases should declare
  `@expect_error_tier=drift` explicitly.
- **target** is the post-cutover user contract.  It requires both a SQL error
  code and a location in the original user SQL, using
  `@expect_sql_code=<lowercase.dot.code>` and
  `@expect_error_at=<line>:<column>`.  The location is 1-based and the column
  is a byte column in the original SQL text.  Do not use target assertions for
  known normalized-text location drift.

The runner also accepts `@expect_sql_phase=<Lex|Parse|Validate|Analyze|Admit>`
to check the phase registered for the asserted SQL code.  It resolves phase
through the SQL-error descriptor manifest, never from error-message text or a
code-name convention.  SQLP-0 starts with an empty production descriptor
registry, so no published suite may use a target SQL code until the owning
domain registers it.  Unknown SQL codes fail while parsing the suite.

`@expect_error` and `@expect_error_code` remain available for drift assertions
and can coexist with the SQL-specific directives.  Prefer the narrowest
current assertion that captures the observed behavior without inventing a
future contract.

## Layout

Each suite follows the runner convention:

```text
<suite>/
  init.sql       # optional setup hook
  cleanup.sql    # optional teardown hook
  sql/           # one or more runnable cases
  result/        # golden results for successful statements, when needed
```

The `sql-reject` skeleton deliberately contains a parser-only drift case.  It
keeps the suite non-empty and runnable before the broader reject corpus lands.
