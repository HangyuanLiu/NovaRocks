use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks::mv::persistence::definition::{CreateMvDefinitionRequest, StoredMvRefreshPolicy};
use novarocks::mv::persistence::dependency::CreateMvDependencyRequest;
use novarocks::mv::repository::{
    CreateMvRepositoryRequest, InitialMvRefreshConfiguration, MvRepository, MvTarget,
};
use novarocks_frontend::mv::repository::StateStoreMvRepository;
use novarocks_spi::state_store::FeDeploymentView;
use novarocks_state_store::{
    StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};

pub(crate) fn repository() -> (
    tempfile::TempDir,
    tokio::runtime::Runtime,
    StateStoreHost,
    Arc<StateStoreMvRepository>,
) {
    let temp = tempfile::tempdir().expect("temporary StateStore directory");
    let runtime = tokio::runtime::Runtime::new().expect("repository runtime");
    let registry = builtin_state_store_provider_registry().expect("built-in StateStore providers");
    let host = runtime
        .block_on(StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "mv-repository-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: temp.path().join("state-store.sqlite"),
                            deployment_owner: "mv-repository-test".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).expect("one FE"),
                topology_revision: Bytes::from_static(b"mv-repository-test-r1"),
            },
            Instant::now() + Duration::from_secs(5),
        ))
        .expect("open SQLite StateStore host");
    let store = host.state_store().expect("host exposes StateStore");
    let repository = runtime
        .block_on(StateStoreMvRepository::open(
            store,
            runtime.handle().clone(),
        ))
        .expect("open MV repository");
    (temp, runtime, host, repository)
}

pub(crate) fn create_request(table: &str) -> CreateMvRepositoryRequest {
    CreateMvRepositoryRequest {
        definition: CreateMvDefinitionRequest {
            select_sql: "SELECT 1".to_string(),
            base_table_refs: vec!["ice.sales.orders".to_string()],
            primary_key_columns: vec![],
            storage_engine: "iceberg".to_string(),
            target_catalog: Some("ice".to_string()),
            target_namespace: Some("sales".to_string()),
            target_table: Some(table.to_string()),
            schema_contract: None,
            partition_spec: None,
            created_at_ms: 1,
        },
        refresh: InitialMvRefreshConfiguration {
            policy: StoredMvRefreshPolicy::Manual,
            ..Default::default()
        },
        dependencies: vec![CreateMvDependencyRequest {
            upstream: MvDependencyObjectRef {
                catalog: Some("ice".to_string()),
                database_or_namespace: "sales".to_string(),
                name: "orders".to_string(),
                object_type: MvDependencyObjectType::Table,
                storage_engine: MvDependencyStorageEngine::Iceberg,
            },
            created_at_ms: 1,
        }],
    }
}

#[test]
fn create_allocates_monotonic_ids_and_persists_target_and_dependencies_atomically() {
    let (_temp, _runtime, _host, repository) = repository();
    let first = repository
        .create(uuid::Uuid::now_v7(), create_request("daily_one"))
        .expect("create first definition");
    let second = repository
        .create(uuid::Uuid::now_v7(), create_request("daily_two"))
        .expect("create second definition");

    assert_eq!(first.mv_id, 1);
    assert_eq!(second.mv_id, 2);
    assert_eq!(
        repository
            .find_by_target(&MvTarget {
                catalog: Some("ice".to_string()),
                database: "sales".to_string(),
                name: "daily_one".to_string(),
            })
            .expect("find target")
            .expect("target exists"),
        first
    );
    assert_eq!(
        repository
            .list_dependencies_by_downstream(first.mv_id)
            .expect("list dependencies")
            .len(),
        1
    );
    assert!(
        repository
            .create(uuid::Uuid::now_v7(), create_request("daily_one"))
            .is_err()
    );
    assert_eq!(
        repository
            .list_definitions()
            .expect("definitions remain readable after duplicate target")
            .len(),
        2,
        "a duplicate target must not leave an orphan definition or advance visible state"
    );
}

#[test]
fn reserve_advances_without_decreasing_and_rejects_non_positive_ids() {
    let (_temp, _runtime, _host, repository) = repository();
    repository.reserve_definition_id(9).expect("reserve ID");
    repository
        .reserve_definition_id(3)
        .expect("lower reserve is a no-op");
    let created = repository
        .create(uuid::Uuid::now_v7(), create_request("reserved"))
        .expect("create after reserve");
    assert_eq!(created.mv_id, 10);
    assert!(repository.reserve_definition_id(0).is_err());
}

#[test]
fn concurrent_allocation_has_no_duplicates_and_explicit_bounds_are_checked() {
    let (_temp, _runtime, _host, repository) = repository();
    let mut workers = Vec::new();
    for number in 0..8 {
        let repository = Arc::clone(&repository);
        workers.push(std::thread::spawn(move || {
            for _ in 0..8 {
                match repository.create(
                    uuid::Uuid::now_v7(),
                    create_request(&format!("concurrent_{number}")),
                ) {
                    Ok(definition) => return definition.mv_id,
                    Err(error)
                        if error.kind()
                            == novarocks::mv::repository::MvRepositoryErrorKind::Conflict =>
                    {
                        continue;
                    }
                    Err(error) => panic!("concurrent create: {error:?}"),
                }
            }
            panic!("concurrent create exhausted caller retries")
        }));
    }
    let mut ids = workers
        .into_iter()
        .map(|worker| worker.join().expect("worker joins"))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids, (1..=8).collect::<Vec<_>>());

    assert!(
        repository
            .create_with_id(
                uuid::Uuid::now_v7(),
                novarocks::mv::repository::CreateMvRepositoryWithIdRequest {
                    mv_id: 0,
                    create: create_request("invalid"),
                },
            )
            .is_err()
    );
    repository
        .create_with_id(
            uuid::Uuid::now_v7(),
            novarocks::mv::repository::CreateMvRepositoryWithIdRequest {
                mv_id: i64::MAX,
                create: create_request("maximum"),
            },
        )
        .expect("maximum explicit ID is representable");
    assert!(
        repository
            .create(uuid::Uuid::now_v7(), create_request("overflow"))
            .is_err()
    );
}
