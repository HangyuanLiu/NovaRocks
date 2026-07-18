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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(dead_code)]
pub(crate) enum BaseRowIdentity {
    IcebergRowId(i64),
    Position { file_path: String, pos: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum BaseRowChangeKind {
    Insert,
    Delete,
    Update,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct BaseRowChange {
    pub(crate) identity: BaseRowIdentity,
    pub(crate) kind: BaseRowChangeKind,
}

#[allow(dead_code)]
pub(crate) fn normalize_insert_delete_pairs(
    inserts: impl IntoIterator<Item = BaseRowIdentity>,
    deletes: impl IntoIterator<Item = BaseRowIdentity>,
) -> Vec<BaseRowChange> {
    use std::collections::BTreeSet;

    let insert_set: BTreeSet<_> = inserts.into_iter().collect();
    let delete_set: BTreeSet<_> = deletes.into_iter().collect();
    let mut out = Vec::new();

    for identity in delete_set.intersection(&insert_set) {
        out.push(BaseRowChange {
            identity: identity.clone(),
            kind: BaseRowChangeKind::Update,
        });
    }
    for identity in delete_set.difference(&insert_set) {
        out.push(BaseRowChange {
            identity: identity.clone(),
            kind: BaseRowChangeKind::Delete,
        });
    }
    for identity in insert_set.difference(&delete_set) {
        out.push(BaseRowChange {
            identity: identity.clone(),
            kind: BaseRowChangeKind::Insert,
        });
    }
    out
}
