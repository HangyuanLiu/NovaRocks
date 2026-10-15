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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

fn compare_all_constructors(value: &str) {
    assert_eq!(
        FunctionId::validate_str(value),
        FunctionId::try_new(value).map(|_| ())
    );
    assert_eq!(
        FunctionOverloadId::validate_str(value),
        FunctionOverloadId::try_new(value).map(|_| ())
    );
    assert_eq!(
        AggregateStateFormatId::validate_str(value),
        AggregateStateFormatId::try_new(value).map(|_| ())
    );
}

#[test]
fn borrowed_identity_validation_preserves_empty_and_byte_limit_errors() {
    for value in [
        String::new(),
        "x".repeat(1023),
        "x".repeat(1024),
        "x".repeat(1025),
    ] {
        compare_all_constructors(&value);
    }
    assert_eq!(
        FunctionId::validate_str(""),
        Err(FunctionIdentityError::Empty { kind: "function" })
    );
    assert_eq!(
        FunctionOverloadId::validate_str(""),
        Err(FunctionIdentityError::Empty {
            kind: "function overload"
        })
    );
    assert_eq!(
        AggregateStateFormatId::validate_str(""),
        Err(FunctionIdentityError::Empty {
            kind: "aggregate state format"
        })
    );
    let accepted = "x".repeat(1024);
    assert!(FunctionId::validate_str(&accepted).is_ok());
    assert!(FunctionOverloadId::validate_str(&accepted).is_ok());
    assert!(AggregateStateFormatId::validate_str(&accepted).is_ok());
    let over = "x".repeat(1025);
    assert_eq!(
        FunctionId::validate_str(&over),
        Err(FunctionIdentityError::TooLong {
            kind: "function",
            actual: 1025
        })
    );
    assert_eq!(
        FunctionOverloadId::validate_str(&over),
        Err(FunctionIdentityError::TooLong {
            kind: "function overload",
            actual: 1025
        })
    );
    assert_eq!(
        AggregateStateFormatId::validate_str(&over),
        Err(FunctionIdentityError::TooLong {
            kind: "aggregate state format",
            actual: 1025
        })
    );
}

#[test]
fn borrowed_function_identities_keep_whitespace_nul_delimiters_and_unicode() {
    for value in [
        " ",
        "\t\n\r",
        "\0",
        "a|b,c",
        "函数/重载🙂",
        " prefix\0|,后缀 ",
    ] {
        compare_all_constructors(value);
        assert_eq!(FunctionId::validate_str(value), Ok(()));
        assert_eq!(FunctionOverloadId::validate_str(value), Ok(()));
        assert_eq!(FunctionId::try_new(value).unwrap().as_str(), value);
        assert_eq!(FunctionOverloadId::try_new(value).unwrap().as_str(), value);
        assert_eq!(
            AggregateStateFormatId::validate_str(value),
            Err(FunctionIdentityError::InvalidCharacters {
                kind: "aggregate state format"
            })
        );
    }
}

#[test]
fn borrowed_identity_limits_count_unicode_bytes_and_keep_error_order() {
    let exact = "é".repeat(512);
    assert_eq!(exact.len(), 1024);
    compare_all_constructors(&exact);
    assert_eq!(FunctionId::validate_str(&exact), Ok(()));
    assert_eq!(FunctionOverloadId::validate_str(&exact), Ok(()));
    assert_eq!(
        AggregateStateFormatId::validate_str(&exact),
        Err(FunctionIdentityError::InvalidCharacters {
            kind: "aggregate state format"
        })
    );
    let over = format!("{exact}x");
    compare_all_constructors(&over);
    // Length precedes the stricter state character check, even for Unicode.
    assert_eq!(
        AggregateStateFormatId::validate_str(&over),
        Err(FunctionIdentityError::TooLong {
            kind: "aggregate state format",
            actual: 1025
        })
    );
    assert_eq!(
        FunctionId::validate_str(&over),
        Err(FunctionIdentityError::TooLong {
            kind: "function",
            actual: 1025
        })
    );
    assert_eq!(
        FunctionOverloadId::validate_str(&over),
        Err(FunctionIdentityError::TooLong {
            kind: "function overload",
            actual: 1025
        })
    );
}

#[test]
fn borrowed_state_format_keeps_exact_ascii_graphic_exclusions() {
    let ascii = (b'!'..=b'~')
        .filter(|byte| !matches!(*byte, b'|' | b','))
        .map(char::from)
        .collect::<String>();
    compare_all_constructors(&ascii);
    assert_eq!(AggregateStateFormatId::validate_str(&ascii), Ok(()));
    assert_eq!(
        AggregateStateFormatId::try_new(&ascii).unwrap().as_str(),
        ascii
    );
    for value in [
        "state|v1",
        "state,v1",
        "state with space",
        "state\t",
        "state\n",
        "state\0",
        "state\u{7f}",
        "stateé",
    ] {
        compare_all_constructors(value);
        assert_eq!(
            AggregateStateFormatId::validate_str(value),
            Err(FunctionIdentityError::InvalidCharacters {
                kind: "aggregate state format"
            })
        );
    }
}

#[derive(Debug, PartialEq)]
enum ObservedIdentityError {
    Identity(FunctionIdentityError),
    Refused(usize),
}
impl From<FunctionIdentityError> for ObservedIdentityError {
    fn from(error: FunctionIdentityError) -> Self {
        Self::Identity(error)
    }
}

#[test]
fn observed_state_identity_visits_only_actual_bytes_and_preserves_primary_refusal() {
    for (value, inspected) in [("valid-v1", 8), ("ab|hidden", 3), ("abéhidden", 3)] {
        let mut visits = 0;
        let result =
            AggregateStateFormatId::validate_str_observed::<ObservedIdentityError>(value, || {
                visits += 1;
                Ok(())
            });
        assert_eq!(visits, inspected);
        assert_eq!(
            result,
            AggregateStateFormatId::validate_str(value).map_err(ObservedIdentityError::from)
        );
        for refuse in 1..=inspected {
            let mut visited = 0;
            let result = AggregateStateFormatId::validate_str_observed::<ObservedIdentityError>(
                value,
                || {
                    visited += 1;
                    if visited == refuse {
                        Err(ObservedIdentityError::Refused(refuse))
                    } else {
                        Ok(())
                    }
                },
            );
            assert_eq!(result, Err(ObservedIdentityError::Refused(refuse)));
            assert_eq!(visited, refuse);
        }
    }
    for value in [String::new(), "x".repeat(1025)] {
        let result =
            AggregateStateFormatId::validate_str_observed::<ObservedIdentityError>(&value, || {
                panic!("identity length rejection must precede every byte")
            });
        assert_eq!(
            result,
            AggregateStateFormatId::validate_str(&value).map_err(ObservedIdentityError::from)
        );
    }
    let mut visits = 0;
    AggregateStateFormatId::validate_str_observed::<ObservedIdentityError>(
        &"x".repeat(1024),
        || {
            visits += 1;
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(visits, 1024);
}

#[derive(Debug, Eq, PartialEq)]
enum IdentityCreationFailure {
    Identity(FunctionIdentityError),
    Resource(crate::owned_resources::layout::LayoutResourceError),
    Host(usize),
}
impl From<FunctionIdentityError> for IdentityCreationFailure {
    fn from(error: FunctionIdentityError) -> Self {
        Self::Identity(error)
    }
}
impl From<crate::owned_resources::layout::LayoutResourceError> for IdentityCreationFailure {
    fn from(error: crate::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}

struct IdentityLifetime;
impl crate::ValueTypeStorageHold for IdentityLifetime {}

#[test]
fn independent_identity_clones_retain_text_creator_and_keep_content_identity() {
    use std::{
        hash::{Hash, Hasher},
        sync::Arc,
    };
    fn digest(value: &impl Hash) -> u64 {
        let mut state = std::collections::hash_map::DefaultHasher::new();
        value.hash(&mut state);
        state.finish()
    }
    macro_rules! verify {
        ($identity:ident, $text:literal) => {{
            let owner = Arc::new(IdentityLifetime);
            let weak = Arc::downgrade(&owner);
            let storage = crate::ValueTypeStorageLifetime::held(owner);
            let text: Box<str> = $text.into();
            let original_text = text.as_ptr();
            let value = $identity::try_from_boxed_in(text, storage).unwrap();
            let pure = $identity::try_new($text).unwrap();
            assert_eq!(
                value.as_str().as_ptr(),
                original_text,
                "checked consume does not copy text"
            );
            assert_eq!(value, pure);
            assert_eq!(value.cmp(&pure), std::cmp::Ordering::Equal);
            assert_eq!(digest(&value), digest(&pure));
            assert_eq!(format!("{value:?}"), format!("{pure:?}"));
            let mut visits = 0;
            let retained = value
                .clone_observed(|| {
                    visits += 1;
                    Ok::<_, ()>(())
                })
                .unwrap();
            assert_eq!(visits, 1);
            assert_eq!(
                retained.as_str().as_ptr(),
                original_text,
                "clone shares text backing"
            );
            drop(value);
            assert!(weak.upgrade().is_some());
            assert_eq!(retained.as_str(), $text);
            drop(retained);
            assert!(
                weak.upgrade().is_none(),
                "only the final identity releases its creator"
            );
        }};
    }
    verify!(FunctionId, "provider/full-function-v1");
    verify!(FunctionOverloadId, "builtin.aggregate/count/derived-v1");
    verify!(AggregateStateFormatId, "novarocks/count/state-v1");
}

#[test]
fn identity_creation_preserves_first_host_refusal_without_later_work_or_request() {
    use std::{cell::Cell, sync::Arc};
    for fail_at in [1, 2] {
        let requests = Cell::new(0);
        let visits = Cell::new(0);
        let work_at_refusal = Cell::new(None);
        let owner = Arc::new(IdentityLifetime);
        let weak = Arc::downgrade(&owner);
        let result = AggregateStateFormatId::try_new_observed::<IdentityCreationFailure>(
            "novarocks/count/state-v1",
            crate::ValueTypeStorageLifetime::held(owner),
            |layout| {
                assert!(layout.size() > 0);
                let at = requests.get() + 1;
                requests.set(at);
                if at == fail_at {
                    work_at_refusal.set(Some(visits.get()));
                    Err(IdentityCreationFailure::Host(at))
                } else {
                    Ok(())
                }
            },
            |units| {
                visits.set(visits.get() + units);
                Ok(())
            },
        );
        assert_eq!(result.unwrap_err(), IdentityCreationFailure::Host(fail_at));
        assert_eq!(requests.get(), fail_at);
        assert_eq!(work_at_refusal.get(), Some(visits.get()));
        assert!(
            weak.upgrade().is_none(),
            "refused identity publishes no retained storage"
        );
    }
}

#[test]
fn observed_identity_consume_has_one_actual_backing_request_and_original_grammar() {
    let text: Box<str> = "fixture|unicode🙂,overload".into();
    let ptr = text.as_ptr();
    let mut requests = Vec::new();
    let value = FunctionOverloadId::try_from_boxed_observed::<IdentityCreationFailure>(
        text,
        crate::ValueTypeStorageLifetime::pure(),
        |layout| {
            requests.push(layout);
            Ok(())
        },
        |_| panic!("function overload grammar inspects length only"),
    )
    .unwrap();
    assert_eq!(value.as_str().as_ptr(), ptr);
    assert_eq!(
        requests,
        vec![FunctionOverloadId::storage_allocation_layout().unwrap()]
    );
    let mut requests = 0;
    let result = AggregateStateFormatId::try_from_boxed_observed::<IdentityCreationFailure>(
        "invalid|state".into(),
        crate::ValueTypeStorageLifetime::pure(),
        |_| {
            requests += 1;
            Ok(())
        },
        |_| Ok(()),
    );
    assert_eq!(
        result.unwrap_err(),
        IdentityCreationFailure::Identity(FunctionIdentityError::InvalidCharacters {
            kind: "aggregate state format"
        })
    );
    assert_eq!(requests, 0);
}
