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

#![cfg(feature = "foundationdb-provider")]

use std::num::NonZeroUsize;
use std::path::PathBuf;

use bytes::Bytes;
use foundationdb::Database;
use foundationdb::options::TransactionOption;
use novarocks::state_store::{
    FeDeploymentView, FoundationDbClientConfig, StateStoreConfig, StateStoreErrorKind,
    StateStoreLimitOverrides, StateStoreProviderConfig, StateStoreRuntime, open_state_store,
};
use uuid::Uuid;

fn client_config() -> FoundationDbClientConfig {
    FoundationDbClientConfig {
        disable_multi_version_client: true,
        tls_cert_path: None,
        tls_key_path: None,
        tls_ca_path: None,
        tls_verify_peers: None,
        tls_password_env: None,
    }
}

fn cluster_file() -> PathBuf {
    PathBuf::from(
        std::env::var("NOVAROCKS_FDB_CLUSTER_FILE").expect("FoundationDB fixture cluster file"),
    )
}

fn store_config(cluster_id: &str, keyspace_id: Uuid) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: cluster_id.to_owned(),
        limits: StateStoreLimitOverrides::default(),
        provider: StateStoreProviderConfig::Foundationdb {
            cluster_file: cluster_file(),
            keyspace_id,
        },
    }
}

fn deployment() -> FeDeploymentView {
    FeDeploymentView {
        active_fe_count: NonZeroUsize::new(2).expect("non-zero FE count"),
        topology_revision: Bytes::from_static(b"foundationdb-suite-topology"),
    }
}

async fn write_partial_identity(keyspace_id: Uuid) {
    let path = cluster_file();
    let database = Database::from_path(path.to_str().expect("UTF-8 cluster file"))
        .expect("create direct FoundationDB test handle");
    let transaction = database
        .create_trx()
        .expect("create corruption transaction");
    transaction
        .set_option(TransactionOption::Timeout(4_000))
        .expect("set corruption transaction timeout");
    transaction
        .set_option(TransactionOption::RetryLimit(0))
        .expect("disable corruption transaction retries");
    let schema_key = [
        b"NRSS\x01".as_slice(),
        keyspace_id.as_bytes(),
        &[0x00, 0x00],
    ]
    .concat();
    transaction.set(&schema_key, &[1]);
    transaction
        .commit()
        .await
        .expect("persist partial identity corruption");
}

#[tokio::test(flavor = "multi_thread")]
async fn foundationdb_suite() {
    let mut runtime = StateStoreRuntime::foundationdb(client_config())
        .expect("boot process-owned FoundationDB runtime");

    let keyspace_id = Uuid::new_v4();
    let config = store_config("identity-cluster", keyspace_id);
    let (left, right) = tokio::join!(
        open_state_store(&runtime, config.clone(), deployment()),
        open_state_store(&runtime, config, deployment())
    );
    let left = left.expect("initialize FoundationDB keyspace");
    let right = right.expect("concurrent open converges on keyspace identity");
    let left_identity = left.identity().await.expect("read left identity");
    let right_identity = right.identity().await.expect("read right identity");
    assert_eq!(left_identity, right_identity);
    assert_eq!(left_identity.cluster_id, "identity-cluster");
    assert_eq!(left_identity.initial_incarnation, 1);

    let mismatch = match open_state_store(
        &runtime,
        store_config("different-cluster", keyspace_id),
        deployment(),
    )
    .await
    {
        Ok(_) => panic!("existing keyspace must reject a cluster identity mismatch"),
        Err(error) => error,
    };
    assert_eq!(mismatch.kind(), StateStoreErrorKind::InvalidConfiguration);

    let corrupt_keyspace = Uuid::new_v4();
    write_partial_identity(corrupt_keyspace).await;
    let corruption = match open_state_store(
        &runtime,
        store_config("identity-cluster", corrupt_keyspace),
        deployment(),
    )
    .await
    {
        Ok(_) => panic!("partial identity must fail closed"),
        Err(error) => error,
    };
    assert_eq!(corruption.kind(), StateStoreErrorKind::Corruption);

    drop(right);
    drop(left);
    runtime
        .shutdown()
        .await
        .expect("shutdown FoundationDB runtime after all handles drain");
}
