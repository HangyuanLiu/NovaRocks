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
