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

use novarocks_backend::connector::binding_decode::decode_connector_execution_declaration;
use novarocks_frontend::encode_connector_execution_declaration;
use novarocks_spi::connector::ConnectorExecutionDeclaration;

#[test]
fn production_adapters_preserve_closed_domain_declarations_and_admitted_digest() {
    let declarations = [
        ConnectorExecutionDeclaration::iceberg("catalog.analytics", [7; 16], "iceberg-local")
            .expect("canonical Iceberg declaration"),
        ConnectorExecutionDeclaration::starrocks("catalog.analytics", [8; 16], "starrocks-local")
            .expect("canonical StarRocks declaration"),
    ];

    for declaration in declarations {
        let encoded = encode_connector_execution_declaration(&declaration);
        assert_eq!(
            encoded,
            encode_connector_execution_declaration(&declaration)
        );
        let admitted = decode_connector_execution_declaration(encoded)
            .expect("BE accepts the real FE adapter output");
        let admitted_again = decode_connector_execution_declaration(
            encode_connector_execution_declaration(&declaration),
        )
        .expect("BE accepts a repeated real FE adapter output");
        assert_eq!(admitted.declaration(), &declaration);
        assert_eq!(admitted.digest(), admitted_again.digest());
    }
}

#[test]
fn admitted_digest_distinguishes_provider_binding_and_generation() {
    let first =
        ConnectorExecutionDeclaration::iceberg("catalog", [7; 16], "first").expect("declaration");
    let changed_provider =
        ConnectorExecutionDeclaration::starrocks("catalog", [7; 16], "first").expect("declaration");
    let changed_binding =
        ConnectorExecutionDeclaration::iceberg("catalog", [7; 16], "second").expect("declaration");
    let changed_generation =
        ConnectorExecutionDeclaration::iceberg("catalog", [8; 16], "first").expect("declaration");

    let digest = |declaration: &ConnectorExecutionDeclaration| {
        decode_connector_execution_declaration(encode_connector_execution_declaration(declaration))
            .expect("BE accepts real FE adapter output")
            .digest()
    };
    assert_ne!(digest(&first), digest(&changed_provider));
    assert_ne!(digest(&first), digest(&changed_binding));
    assert_ne!(digest(&first), digest(&changed_generation));
}
