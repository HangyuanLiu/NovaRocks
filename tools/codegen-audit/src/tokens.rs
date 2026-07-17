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

use proc_macro2::{TokenStream, TokenTree};

pub(crate) fn ident_text(ident: &proc_macro2::Ident) -> String {
    let text = ident.to_string();
    text.strip_prefix("r#").unwrap_or(&text).to_string()
}

fn path_at(tokens: &[TokenTree], start: usize) -> Vec<String> {
    let mut path = Vec::new();
    let mut index = start;
    while let Some(TokenTree::Ident(ident)) = tokens.get(index) {
        path.push(ident_text(ident));
        index += 1;
        if !matches!(
            (tokens.get(index), tokens.get(index + 1)),
            (Some(TokenTree::Punct(left)), Some(TokenTree::Punct(right)))
                if left.as_char() == ':' && right.as_char() == ':'
        ) {
            break;
        }
        index += 2;
    }
    path
}

pub(crate) fn contains_path(tokens: TokenStream, forbidden: &[&str]) -> bool {
    let tokens = tokens.into_iter().collect::<Vec<_>>();
    for (index, token) in tokens.iter().enumerate() {
        if let TokenTree::Group(group) = token
            && contains_path(group.stream(), forbidden)
        {
            return true;
        }
        if !matches!(token, TokenTree::Ident(_)) {
            continue;
        }
        let path = path_at(&tokens, index);
        if path.windows(forbidden.len()).any(|window| {
            window
                .iter()
                .map(String::as_str)
                .eq(forbidden.iter().copied())
        }) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::*;

    #[test]
    fn recursive_groups_find_paths_but_ignore_literals() {
        assert!(contains_path(
            quote!({
                type Leak = optimizer::physical_tree::Node;
            }),
            &["optimizer", "physical_tree"]
        ));
        assert!(!contains_path(
            quote!({ "optimizer::physical_tree" }),
            &["optimizer", "physical_tree"]
        ));
    }
}
