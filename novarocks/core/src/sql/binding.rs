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

//! Query-local opaque identities for SQL table facts.
//!
//! These values deliberately have no serialization implementation. A table
//! binding is meaningful only in the application-owned store that allocated
//! its scope, so forwarding it across a request/process boundary is invalid.

use std::num::{NonZeroU32, NonZeroU64};

/// Process-local identity of one application binding store.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SqlTableBindingScopeId(NonZeroU64);

impl SqlTableBindingScopeId {
    pub(crate) fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    pub(crate) fn get(self) -> NonZeroU64 {
        self.0
    }
}

/// One table fact allocated by a query-local binding store.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SqlTableBindingId {
    scope: SqlTableBindingScopeId,
    ordinal: NonZeroU32,
}

impl SqlTableBindingId {
    pub(crate) fn new(scope: SqlTableBindingScopeId, ordinal: NonZeroU32) -> Self {
        Self { scope, ordinal }
    }

    pub(crate) fn scope(self) -> SqlTableBindingScopeId {
        self.scope
    }

    pub(crate) fn ordinal(self) -> NonZeroU32 {
        self.ordinal
    }

    pub(crate) fn belongs_to(self, scope: SqlTableBindingScopeId) -> bool {
        self.scope == scope
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use super::{SqlTableBindingId, SqlTableBindingScopeId};

    #[test]
    fn sqlx2_binding_token_is_scoped_and_nonzero() {
        let first_scope = SqlTableBindingScopeId::new(NonZeroU64::new(17).unwrap());
        let second_scope = SqlTableBindingScopeId::new(NonZeroU64::new(18).unwrap());
        let binding = SqlTableBindingId::new(first_scope, NonZeroU32::new(1).unwrap());

        assert_eq!(binding.scope(), first_scope);
        assert_eq!(binding.ordinal(), NonZeroU32::new(1).unwrap());
        assert!(binding.belongs_to(first_scope));
        assert!(!binding.belongs_to(second_scope));
        assert_eq!(first_scope.get(), NonZeroU64::new(17).unwrap());
    }
}
