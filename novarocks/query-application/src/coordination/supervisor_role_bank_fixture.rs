// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Included only inside the original supervisor unit-test module. This is an
// explicit neutral test policy, never a complete Native production profile.
use novarocks_resource_contract::SharedDependency;
use novarocks_workload_control::joint_admission::{
    JointAdmissionHandle, JointCapacityConfig, JointProgress, JointProgressDriver, JointResource,
    JointResourceLimits, JointResources, RequiredJointProgress,
};
use novarocks_workload_control::{ControlStorageConfig, PollRoleGeometry};

type TestRoleFactory<F> = fn(WorkOwner, LogicalRoleInput) -> F;
struct TestGovernance<F: Future> {
    control: Option<WorkloadControl>,
    statement_source_slots: NonZeroUsize,
    // A metadata-only Option move transfers this SAME bank into the root.
    bank: std::sync::Mutex<
        Option<RolePollBank<LogicalRoleInput, F, TestRoleFactory<F>, Arc<OriginalLogicalExit>>>,
    >,
    admission: JointAdmissionHandle,
    progress: Option<TestProgressOwner>,
    finished: bool,
}
impl<F: Future> std::ops::Deref for TestGovernance<F> {
    type Target = WorkloadControl;
    fn deref(&self) -> &Self::Target {
        self.control.as_ref().expect("original test authority")
    }
}
impl<F: Future> TestGovernance<F> {
    async fn begin_root(&self) -> RootWork {
        self.begin_root_request(WorkRequest::new(WorkClass::Query))
            .await
    }
    async fn begin_root_request(&self, request: WorkRequest) -> RootWork {
        let root = self.try_begin_root(request).unwrap();
        let mut proposal = self.admission.begin_paid_proposal(&root.owner).unwrap();
        proposal
            .begin_first_bind(root.owner.control_claim_view().unwrap())
            .unwrap();
        // Every Native port used here is an in-process fake. A production
        // Network action must retain its actual Provider/network dependency.
        proposal
            .push_dependency(&SharedDependency::LocalHost)
            .unwrap();
        proposal.end_change().unwrap();
        let mut request = proposal.submit().unwrap();
        let batch = tokio::time::timeout(Duration::from_secs(5), &mut request)
            .await
            .expect("original test FirstBind did not complete")
            .unwrap();
        assert!(batch.new_claims.is_empty());
        batch
            .adjustment
            .unwrap()
            .install_control(&root.owner, &[])
            .unwrap();
        root
    }

    async fn finish_for_process_exit(&mut self) {
        // This test deliberately proved unresolved Native custody. Stop and
        // observe only the real fixture service; retain the actual unproven
        // ledger/backing to process exit without fabricating WC.drained.
        self.close_admission();
        self.progress.as_mut().unwrap().finish().await;
        std::mem::forget(self.progress.take());
        std::mem::forget(self.control.take());
        let bank = { self.bank.lock().unwrap().take() };
        std::mem::forget(bank);
        self.finished = true;
    }

    async fn finish(&mut self) {
        // Caller returns this owner from an inner test block: every ordinary
        // local View/Bytes/window/source alias has already actually dropped.
        let bank = { self.bank.lock().unwrap().take() };
        drop(bank);
        self.close_admission();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let revision = self.progress_revision();
            let progress = tokio::time::timeout_at(deadline, self.wait_progress(revision))
                .await
                .expect("original test authority did not actually drain");
            if matches!(
                progress,
                novarocks_workload_control::WorkloadProgress::Drained(_)
            ) {
                break;
            }
            assert!(
                !matches!(
                    progress,
                    novarocks_workload_control::WorkloadProgress::ControlReady(_)
                ),
                "test must acknowledge its real controls before fixture finish"
            );
        }
        self.progress.as_mut().unwrap().finish().await;
        drop(self.progress.take());
        let owner = self.control.take().unwrap();
        match owner.shutdown() {
            Ok(_) => {}
            Err(failure) => {
                let (error, owner) = failure.into_parts();
                self.control = Some(owner);
                panic!("original fixed test authority refused shutdown: {error}");
            }
        }
        self.finished = true;
    }
}
impl<F: Future> Drop for TestGovernance<F> {
    fn drop(&mut self) {
        if !self.finished {
            // Preserve original finite owners on a failed assertion. This is
            // NOT cleanup success, abort or a substitute for explicit finish.
            std::mem::forget(self.progress.take());
        }
    }
}

fn governance_empty() -> TestGovernance<impl Future<Output = Result<(), QueryExecutionError>> + Send>
{
    governance_with_client_compute(false)
}
fn governance_with_client_compute(
    client: bool,
) -> TestGovernance<impl Future<Output = Result<(), QueryExecutionError>> + Send> {
    // This function pointer names the SAME actual opaque async factory. Quote
    // its real M too (a pointer here, not the production zero-sized item).
    let factory: TestRoleFactory<_> = make_logical_role;
    let roles = [
        PollRoleGeometry::for_factory_with_context::<_, _, _, Arc<OriginalLogicalExit>>(&factory)
            .unwrap(),
    ];
    // Query covers the original three-owner handoff fixture. Every other
    // class has one fake role plus its declared progress/storage margin.
    let slots = [8usize, 3, 3, 3, 3];
    let count = slots.iter().sum::<usize>();
    let proposals_per_slot = 2;
    let max_changes = if client { 4 } else { 1 };
    let max_dependencies = 1;
    let storage = ControlStorageConfig::new(slots, count - 1, proposals_per_slot, &roles);
    let mut config = JointCapacityConfig {
        global: JointResourceLimits::default(),
        classes: [JointResourceLimits::default(); 5],
        lanes: [JointResourceLimits::default(); 3],
        class_lanes: [[JointResourceLimits::default(); 3]; 5],
        max_dependencies_per_claim: max_dependencies,
        max_registered_dependencies: 1,
        max_claims: count * 2,
        max_requests: count * 2,
        max_changes_per_request: max_changes,
        max_snapshot_bytes: 16 * 1024 * 1024,
        max_scratch_bytes: 16 * 1024 * 1024,
        max_residual_arcs: 10000,
        max_total_proof_work: 10000000,
        work_per_turn: NonZeroUsize::new(7).unwrap(),
    };
    // The original registered-row floor, using this fixture's actual creators.
    config.max_registered_dependencies = (4 * config.max_claims
        + 2 * count * proposals_per_slot * max_changes
        + 2 * max_changes
        + 1)
        * max_dependencies;
    let base = storage.base_charge(&config).unwrap();
    let limit = |n: usize| JointResourceLimits {
        global: JointResources::ZERO
            .with(JointResource::ScopeRecords, n as u64)
            .with(JointResource::ClaimRecords, (n * 2) as u64)
            .with(
                JointResource::MetadataBytes,
                base.get(JointResource::MetadataBytes) * n as u64 * 4,
            ),
        per_dependency: JointResources::ZERO
            .with(JointResource::ScopeRecords, (n - 1) as u64)
            .with(JointResource::ClaimRecords, n as u64)
            .with(
                JointResource::MetadataBytes,
                base.get(JointResource::MetadataBytes) * n as u64 * 2,
            ),
        protected_progress: base,
    };
    config.global = limit(count);
    for (class, n) in slots.into_iter().enumerate() {
        config.classes[class] = limit(n);
        config.class_lanes[class][0] = limit(n);
    }
    config.lanes[0] = limit(count);
    if client {
        // Real Root payload plus one P=1 stage; no Native network protection
        // is asserted by this deliberately in-process finite fixture.
        for limits in [
            &mut config.global,
            &mut config.classes[0],
            &mut config.lanes[0],
            &mut config.class_lanes[0][0],
        ] {
            limits.global = limits
                .global
                .with(JointResource::PayloadBytes, 64 * 1024 * 1024);
            limits.per_dependency = limits
                .per_dependency
                .with(JointResource::PayloadBytes, 64 * 1024 * 1024);
        }
        let foreground = JointResourceLimits {
            global: JointResources::ZERO
                .with(JointResource::ClaimRecords, 4)
                .with(JointResource::Computations, 1),
            per_dependency: JointResources::ZERO
                .with(JointResource::ClaimRecords, 4)
                .with(JointResource::Computations, 1),
            protected_progress: JointResources::ZERO,
        };
        config.lanes[1] = foreground;
        config.class_lanes[0][1] = foreground;
        for limits in [&mut config.global, &mut config.classes[0]] {
            limits.global = limits
                .global
                .with(
                    JointResource::ClaimRecords,
                    limits.global.get(JointResource::ClaimRecords) + 4,
                )
                .with(JointResource::Computations, 1);
            limits.per_dependency = limits
                .per_dependency
                .with(
                    JointResource::ClaimRecords,
                    limits.per_dependency.get(JointResource::ClaimRecords) + 4,
                )
                .with(JointResource::Computations, 1);
        }
    }
    // Every class may need its original base progress at the same time.
    // Keep the actual fixture inventories; only sum their real reservations.
    let simultaneous_progress = [
        JointResource::ScopeRecords,
        JointResource::ClaimRecords,
        JointResource::ReplyRecords,
        JointResource::QueueRecords,
        JointResource::MetadataBytes,
        JointResource::PayloadBytes,
        JointResource::UnreceivedProductions,
        JointResource::Publications,
        JointResource::Residuals,
        JointResource::Computations,
        JointResource::AsyncIo,
        JointResource::BlockingJobs,
        JointResource::InternalCpuJobs,
        JointResource::PhysicalQueueRecords,
        JointResource::PhysicalQueueBytes,
    ]
    .into_iter()
    .fold(JointResources::ZERO, |total, resource| {
        let amount = config
            .classes
            .iter()
            .try_fold(0u64, |sum, class| {
                sum.checked_add(class.protected_progress.get(resource))
            })
            .expect("finite fixture simultaneous progress");
        total.with(resource, amount)
    });
    config.global.protected_progress = simultaneous_progress;
    config.lanes[0].protected_progress = simultaneous_progress;
    let mut required = RequiredJointProgress {
        global: simultaneous_progress,
        classes: [base; 5],
        lanes: [JointResources::ZERO; 3],
        class_lanes: [[JointResources::ZERO; 3]; 5],
    };
    required.lanes[0] = simultaneous_progress;
    for class in &mut required.class_lanes {
        class[0] = base;
    }
    let mut control = WorkloadControl::try_new(
        WorkloadConfig {
            root_limit: count,
            business_limit: count,
            query_concurrency_limit: slots[0],
            scope_records_limit: count,
            ..WorkloadConfig::default()
        },
        ResourceConfig {
            total_bytes: 1 << 20,
            control_bytes: 1 << 12,
            per_scope_bytes: (1 << 20) - (1 << 12),
        },
    )
    .unwrap();
    control
        .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
        .unwrap();
    let (admission, driver) = control
        .configure_joint_capacity(config.validate(&required).unwrap())
        .unwrap();
    control.configure_control_storage(storage).unwrap();
    let bank = RolePollBank::<_, _, _, Arc<OriginalLogicalExit>>::install_with_context(
        &mut control,
        0,
        factory,
    )
    .unwrap();
    control.mark_ready().unwrap();
    let progress = TestProgressOwner::start(driver, control.deadline_expiry_handle());
    TestGovernance {
        control: Some(control),
        statement_source_slots: NonZeroUsize::new(count).unwrap(),
        bank: std::sync::Mutex::new(Some(bank)),
        admission,
        progress: Some(progress),
        finished: false,
    }
}
async fn governance() -> (
    TestGovernance<impl Future<Output = Result<(), QueryExecutionError>> + Send>,
    RootWork,
) {
    let control = governance_empty();
    let root = control.begin_root().await;
    (control, root)
}
fn test_supervisor_with_clock<F>(
    control: &TestGovernance<F>,
    runtime: Handle,
    native: Arc<dyn LogicalExecutionNativePort>,
    namespace: QueryProcessNamespace,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
    clock: Arc<dyn MonotonicClock>,
) -> (LogicalExecutionSupervisor, QueryExecutionClient)
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
{
    let bank = {
        control
            .bank
            .lock()
            .unwrap()
            .take()
            .expect("transfer original installed bank exactly once")
    };
    LogicalExecutionSupervisor::start_installed_bank(
        bank,
        NonZeroUsize::new(7).unwrap(),
        runtime,
        native,
        namespace,
        frontend_process_id,
        config,
        clock,
    )
    .unwrap()
}
fn test_supervisor<F>(
    control: &TestGovernance<F>,
    runtime: Handle,
    native: Arc<dyn LogicalExecutionNativePort>,
    namespace: QueryProcessNamespace,
    frontend_process_id: FrontendProcessId,
    config: LogicalExecutionSupervisorConfig,
) -> (LogicalExecutionSupervisor, QueryExecutionClient)
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
{
    test_supervisor_with_clock(
        control,
        runtime,
        native,
        namespace,
        frontend_process_id,
        config,
        Arc::new(super::super::ProcessMonotonicClock::new()),
    )
}

struct TestDriverCustody(std::sync::Mutex<Option<Box<JointProgressDriver>>>);
struct TestDriverLease<'a>(&'a TestDriverCustody, Option<Box<JointProgressDriver>>);
impl Drop for TestDriverLease<'_> {
    fn drop(&mut self) {
        let previous = self.0.0.lock().unwrap().replace(self.1.take().unwrap());
        assert!(previous.is_none());
    }
}
impl TestDriverCustody {
    fn with<R>(&self, f: impl FnOnce(&mut JointProgressDriver) -> R) -> R {
        let driver = self.0.lock().unwrap().take().unwrap();
        let mut lease = TestDriverLease(self, Some(driver));
        f(lease.1.as_deref_mut().unwrap())
    }
}
struct TestProgressOwner {
    custody: Option<Arc<TestDriverCustody>>,
    stop: Option<watch::Sender<bool>>,
    join: Option<JoinHandle<()>>,
    failure: Option<tokio::task::JoinError>,
    joined: bool,
}
impl TestProgressOwner {
    fn start(
        driver: JointProgressDriver,
        deadlines: novarocks_workload_control::DeadlineExpiryHandle,
    ) -> Self {
        let custody = Arc::new(TestDriverCustody(std::sync::Mutex::new(Some(Box::new(
            driver,
        )))));
        let (stop, mut stopped) = watch::channel(false);
        let mut owner = Self {
            custody: Some(Arc::clone(&custody)),
            stop: Some(stop),
            join: None,
            failure: None,
            joined: false,
        };
        owner.join = Some(Handle::current().spawn(async move {
            let interval = Duration::from_millis(1);
            let mut recheck = tokio::time::Instant::now() + interval;
            let mut timer = Box::pin(tokio::time::sleep_until(recheck));
            loop {
                if *stopped.borrow() {
                    return;
                }
                deadlines.expire_deadlines();
                let now = tokio::time::Instant::now();
                if now >= recheck {
                    custody.with(JointProgressDriver::recheck_retired_backing);
                    recheck = now + interval;
                }
                match custody.with(JointProgressDriver::step) {
                    JointProgress::More => tokio::task::yield_now().await,
                    JointProgress::Idle(marker) => {
                        timer.as_mut().reset(
                            deadlines
                                .next_deadline()
                                .map_or(recheck, |d| d.min(recheck)),
                        );
                        tokio::select! {
                            _ = stopped.changed() => {},
                            _ = deadlines.wait_state_change(marker) => {},
                            _ = timer.as_mut() => {},
                        }
                    }
                }
            }
        }));
        owner
    }
    async fn finish(&mut self) {
        self.stop.as_ref().unwrap().send_replace(true);
        let result = self.join.as_mut().expect("same fixture driver join").await;
        if let Err(error) = result {
            self.failure = Some(error);
        }
        self.join.take();
        if self.failure.is_some() {
            panic!("original fixture progress task failed: {:?}", self.failure);
        }
        self.joined = true;
    }
}
impl Drop for TestProgressOwner {
    fn drop(&mut self) {
        if !self.joined {
            std::mem::forget(self.join.take());
            std::mem::forget(self.custody.take());
            std::mem::forget(self.failure.take());
            std::mem::forget(self.stop.take());
        }
    }
}

// A real producer can be paused before its original root starts polling. The
// test still consumes the production driver and actual factory, not a mailbox.
fn unpolled_test_driver<F>(
    control: &TestGovernance<F>,
    native: Arc<dyn LogicalExecutionNativePort>,
) -> (
    RolePollBank<LogicalRoleInput, F, TestRoleFactory<F>, Arc<OriginalLogicalExit>>,
    watch::Sender<bool>,
    LogicalExecutionRuntimeRegistry,
    QueryExecutionClient,
)
where
    F: Future<Output = Result<(), QueryExecutionError>> + Send + 'static,
{
    let bank = { control.bank.lock().unwrap().take().unwrap() };
    let ingress = bank.ingress().unwrap();
    let registry = LogicalExecutionRuntimeRegistry::new(Handle::current());
    let (shutdown, receiver) = watch::channel(false);
    let driver = QueryExecutionDriverHandle {
        ingress,
        shutdown: receiver,
        query_ids: Arc::new(ProcessQueryIdAllocator::new(QueryProcessNamespace::new(
            0x50,
        ))),
        native,
        frontend_process_id: FrontendProcessId::new_v7(),
        config: supervisor_config(),
        clock: Arc::new(super::super::ProcessMonotonicClock::new()),
        registry: registry.handle(),
    };
    (bank, shutdown, registry, QueryExecutionClient::new(driver))
}
