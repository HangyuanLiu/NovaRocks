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

use std::fmt;

use crate::protocol::starrocks::decode::{
    StarRocksExternalDependency, StarRocksResolvedDependencies, StarRocksResolvedDependencyValue,
};

use super::starrocks_fragment_transport::StarRocksPrelaunchCancellationToken;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StarRocksDependencyResolutionError {
    QueryProfileTransport { dependency_id: u64, source: String },
    LakeMetaStorage { dependency_id: u64, source: String },
    Cancelled { dependency_id: u64 },
}

impl fmt::Display for StarRocksDependencyResolutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueryProfileTransport {
                dependency_id,
                source,
            } => write!(
                f,
                "query-profile dependency {dependency_id} failed: {source}"
            ),
            Self::LakeMetaStorage {
                dependency_id,
                source,
            } => write!(
                f,
                "lake-meta storage dependency {dependency_id} failed: {source}"
            ),
            Self::Cancelled { dependency_id } => {
                write!(f, "dependency {dependency_id} cancelled during preparation")
            }
        }
    }
}

impl std::error::Error for StarRocksDependencyResolutionError {}

pub(crate) fn resolve_dependencies(
    requirements: &[StarRocksExternalDependency],
    token: &StarRocksPrelaunchCancellationToken,
) -> Result<StarRocksResolvedDependencies, StarRocksDependencyResolutionError> {
    resolve_dependencies_with(
        requirements,
        token,
        |endpoint, query_id| {
            let address = crate::thrift::types::TNetworkAddress::new(
                endpoint.host().to_string(),
                endpoint.port(),
            );
            crate::service::fe_report::fetch_query_profile(&address, query_id)
        },
        crate::connector::starrocks::lake_meta_storage::resolve_lake_meta_storage,
    )
}

fn resolve_dependencies_with<QueryProfileResolver, LakeMetaResolver>(
    requirements: &[StarRocksExternalDependency],
    token: &StarRocksPrelaunchCancellationToken,
    mut resolve_query_profile: QueryProfileResolver,
    mut resolve_lake_meta: LakeMetaResolver,
) -> Result<StarRocksResolvedDependencies, StarRocksDependencyResolutionError>
where
    QueryProfileResolver:
        FnMut(&crate::runtime::endpoint::RuntimeEndpoint, &str) -> Result<String, String>,
    LakeMetaResolver:
        FnMut(
            &crate::protocol::starrocks::decode::LakeMetaStorageRequest,
        ) -> Result<crate::protocol::starrocks::decode::LakeMetaStorageFacts, String>,
{
    let mut resolved = StarRocksResolvedDependencies::default();
    for requirement in requirements {
        let dependency_id = requirement.id();
        token.check(dependency_id)?;
        let value = match requirement {
            StarRocksExternalDependency::QueryProfile { query_id, .. } => {
                let endpoint = token.frontend_endpoint().ok_or_else(|| {
                    StarRocksDependencyResolutionError::QueryProfileTransport {
                        dependency_id,
                        source: "frontend endpoint is missing".to_string(),
                    }
                })?;
                let profile = resolve_query_profile(endpoint, query_id).map_err(|source| {
                    StarRocksDependencyResolutionError::QueryProfileTransport {
                        dependency_id,
                        source,
                    }
                })?;
                StarRocksResolvedDependencyValue::QueryProfile(profile)
            }
            StarRocksExternalDependency::LakeMetaStorage { request, .. } => {
                let facts = resolve_lake_meta(request).map_err(|source| {
                    StarRocksDependencyResolutionError::LakeMetaStorage {
                        dependency_id,
                        source,
                    }
                })?;
                StarRocksResolvedDependencyValue::LakeMetaStorage(facts)
            }
        };
        token.check(dependency_id)?;
        resolved.insert(dependency_id, value);
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::common::types::UniqueId;
    use crate::protocol::starrocks::decode::{
        LakeMetaStorageFacts, LakeMetaStorageRequest, StarRocksExternalDependency,
        StarRocksResolvedDependencyValue,
    };
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime::query_context::QueryId;

    use super::{StarRocksDependencyResolutionError, resolve_dependencies_with};
    use crate::service::starrocks_fragment_transport::{
        StarRocksPrelaunchGuard, StarRocksPrelaunchRegistry,
    };

    fn guarded_token(
        finst_id: UniqueId,
    ) -> (
        Arc<StarRocksPrelaunchRegistry>,
        StarRocksPrelaunchGuard,
        super::StarRocksPrelaunchCancellationToken,
    ) {
        let registry = Arc::new(StarRocksPrelaunchRegistry::new());
        let mut guard = registry
            .install(QueryId { hi: 91, lo: 92 }, 1, [finst_id])
            .expect("install prelaunch guard");
        guard.set_frontend_endpoint(Some(
            RuntimeEndpoint::new("fe.test", 9020).expect("frontend endpoint"),
        ));
        let token = guard.cancellation_token();
        (registry, guard, token)
    }

    fn query_profile_dependency(id: u64) -> StarRocksExternalDependency {
        StarRocksExternalDependency::QueryProfile {
            id,
            query_id: "query-1".to_string(),
        }
    }

    fn lake_meta_request() -> LakeMetaStorageRequest {
        LakeMetaStorageRequest::new(
            QueryId { hi: 93, lo: 94 },
            "catalog".to_string(),
            "db".to_string(),
            "table".to_string(),
            1,
            2,
            3,
            Vec::new(),
            Vec::new(),
        )
    }

    #[test]
    fn resolves_query_profile_dependency() {
        let finst_id = UniqueId { hi: 101, lo: 102 };
        let (_registry, _guard, token) = guarded_token(finst_id);
        let result = resolve_dependencies_with(
            &[query_profile_dependency(7)],
            &token,
            |endpoint, query_id| {
                assert_eq!(endpoint.as_host_port(), "fe.test:9020");
                assert_eq!(query_id, "query-1");
                Ok("profile-json".to_string())
            },
            |_| panic!("lake-meta resolver must not be called"),
        )
        .expect("resolve query profile");

        assert!(matches!(
            result.get(7),
            Some(StarRocksResolvedDependencyValue::QueryProfile(profile))
                if profile == "profile-json"
        ));
    }

    #[test]
    fn resolves_lake_meta_storage_facts() {
        let finst_id = UniqueId { hi: 103, lo: 104 };
        let (_registry, _guard, token) = guarded_token(finst_id);
        let request = lake_meta_request();
        let dependency_id = request.id();
        let result = resolve_dependencies_with(
            &[StarRocksExternalDependency::LakeMetaStorage {
                id: dependency_id,
                request,
            }],
            &token,
            |_, _| panic!("query-profile resolver must not be called"),
            |_| {
                Ok(LakeMetaStorageFacts {
                    total_rows: 17,
                    column_arrays: BTreeMap::new(),
                })
            },
        )
        .expect("resolve lake-meta facts");

        assert!(matches!(
            result.get(dependency_id),
            Some(StarRocksResolvedDependencyValue::LakeMetaStorage(facts))
                if facts.total_rows == 17
        ));
    }

    #[test]
    fn resolves_only_declared_dependencies() {
        let finst_id = UniqueId { hi: 105, lo: 106 };
        let (_registry, _guard, token) = guarded_token(finst_id);
        let query_calls = AtomicUsize::new(0);
        let lake_calls = AtomicUsize::new(0);
        let result = resolve_dependencies_with(
            &[query_profile_dependency(8)],
            &token,
            |_, _| {
                query_calls.fetch_add(1, Ordering::SeqCst);
                Ok("profile".to_string())
            },
            |_| {
                lake_calls.fetch_add(1, Ordering::SeqCst);
                unreachable!("undeclared lake-meta dependency")
            },
        )
        .expect("resolve declared dependencies");

        assert!(result.get(8).is_some());
        assert_eq!(query_calls.load(Ordering::SeqCst), 1);
        assert_eq!(lake_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn classifies_transport_and_storage_failures_as_resolution_errors() {
        let finst_id = UniqueId { hi: 107, lo: 108 };
        let (_registry, _guard, token) = guarded_token(finst_id);
        let transport_error = resolve_dependencies_with(
            &[query_profile_dependency(9)],
            &token,
            |_, _| Err("network unavailable".to_string()),
            |_| panic!("lake-meta resolver must not be called"),
        )
        .expect_err("transport failure");
        assert!(matches!(
            transport_error,
            StarRocksDependencyResolutionError::QueryProfileTransport {
                dependency_id: 9,
                ref source,
            } if source == "network unavailable"
        ));

        let request = lake_meta_request();
        let dependency_id = request.id();
        let storage_error = resolve_dependencies_with(
            &[StarRocksExternalDependency::LakeMetaStorage {
                id: dependency_id,
                request,
            }],
            &token,
            |_, _| panic!("query-profile resolver must not be called"),
            |_| Err("object store unavailable".to_string()),
        )
        .expect_err("storage failure");
        assert!(matches!(
            storage_error,
            StarRocksDependencyResolutionError::LakeMetaStorage {
                dependency_id: id,
                ref source,
            } if id == dependency_id && source == "object store unavailable"
        ));
    }

    #[test]
    fn cancellation_before_dependency_wait_prevents_resolution() {
        let finst_id = UniqueId { hi: 109, lo: 110 };
        let (registry, _guard, token) = guarded_token(finst_id);
        assert!(registry.cancel(finst_id));
        let calls = AtomicUsize::new(0);

        let error = resolve_dependencies_with(
            &[query_profile_dependency(10)],
            &token,
            |_, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok("unreachable".to_string())
            },
            |_| panic!("lake-meta resolver must not be called"),
        )
        .expect_err("cancel before wait");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            error,
            StarRocksDependencyResolutionError::Cancelled { dependency_id: 10 }
        );
    }

    #[test]
    fn cancellation_after_dependency_wait_discards_resolution() {
        let finst_id = UniqueId { hi: 111, lo: 112 };
        let (registry, _guard, token) = guarded_token(finst_id);

        let error = resolve_dependencies_with(
            &[query_profile_dependency(11)],
            &token,
            |_, _| {
                assert!(registry.cancel(finst_id));
                Ok("late profile".to_string())
            },
            |_| panic!("lake-meta resolver must not be called"),
        )
        .expect_err("cancel after wait");

        assert_eq!(
            error,
            StarRocksDependencyResolutionError::Cancelled { dependency_id: 11 }
        );
    }
}
