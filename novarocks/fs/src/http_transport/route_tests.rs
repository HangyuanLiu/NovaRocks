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

use super::*;

#[test]
fn admitted_uri_is_preserved_and_debug_omits_secrets() {
    let uri: Uri = "https://origin-user:origin-secret@example.com/a%2Fb?signature=secret-query"
        .parse()
        .unwrap();
    let config = HttpRouteConfig::with_matcher(
        Matcher::builder()
            .all("http://proxy-user:proxy-secret@proxy.example:3128")
            .build(),
    );
    let route = config.prepare(&uri).unwrap();
    assert_eq!(route.original_uri(), &uri);
    assert_eq!(
        route.target().connector_uri().unwrap(),
        "https://example.com:443/"
    );
    assert_eq!(
        route.proxy().unwrap().connector_uri().unwrap(),
        "http://proxy.example:3128/"
    );
    assert!(route.proxy_authorization().unwrap().is_sensitive());
    assert!(route.forward_proxy_authorization().is_none());
    let rendered = format!("{route:?} {config:?}");
    for secret in [
        "origin-user",
        "origin-secret",
        "proxy-user",
        "proxy-secret",
        "secret-query",
        "signature",
        "a%2Fb",
    ] {
        assert!(!rendered.contains(secret), "secret leaked: {secret}");
    }
}

#[test]
fn selected_proxy_and_no_proxy_are_fixed_in_the_prepared_route() {
    let matcher = Matcher::builder()
        .all("http://proxy.example:3128")
        .no("internal.example")
        .build();
    let config = HttpRouteConfig::with_matcher(matcher);
    let bypass = config
        .prepare(&"https://internal.example/object?x=y".parse().unwrap())
        .unwrap();
    assert!(bypass.proxy().is_none());
    let uppercase: Uri = "https://INTERNAL.EXAMPLE/a%2Fb?signature=%25raw"
        .parse()
        .unwrap();
    let bypass = config.prepare(&uppercase).unwrap();
    assert!(bypass.proxy().is_none());
    assert_eq!(bypass.original_uri(), &uppercase);
    assert_eq!(
        bypass.original_uri().path_and_query().unwrap().as_str(),
        "/a%2Fb?signature=%25raw"
    );
    let forwarded = config
        .prepare(&"http://external.example/object?x=y".parse().unwrap())
        .unwrap();
    assert!(forwarded.proxy().is_some());
    drop(config);
    assert_eq!(forwarded.proxy().unwrap().port(), 3128);
    assert_eq!(
        forwarded.original_uri().path_and_query().unwrap().as_str(),
        "/object?x=y"
    );
    assert!(
        HttpRouteConfig::local()
            .prepare(&"https://external.example/object".parse().unwrap())
            .unwrap()
            .proxy()
            .is_none()
    );
}

#[test]
fn literal_origin_keeps_reference_address_and_rejects_unsupported_proxy() {
    let route = HttpRouteConfig::local()
        .prepare(&"https://[::1]:8443/encoded%2Fkey?q=%25".parse().unwrap())
        .unwrap();
    assert_eq!(
        route.target().host(),
        &ReferenceHost::Ip("::1".parse().unwrap())
    );
    assert_eq!(
        route.target().connector_uri().unwrap(),
        "https://[::1]:8443/"
    );
    let config = HttpRouteConfig::with_matcher(
        Matcher::builder()
            .all("socks5h://proxy.example:1080")
            .build(),
    );
    assert_eq!(
        config
            .prepare(&"https://example.com/".parse().unwrap())
            .unwrap_err()
            .kind(),
        crate::FileErrorKind::Unsupported
    );
}
