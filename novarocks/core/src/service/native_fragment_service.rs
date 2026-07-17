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

use std::collections::HashMap;
use std::sync::Arc;

use crate::cache::CacheOptions;
use crate::common::app_config;
use crate::common::types::UniqueId;
use crate::lower::novarocks::execute_fragment_native;
use crate::novarocks_logging::{error, info, warn};
use crate::runtime::exchange;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::native_fragment_wire::{endpoint_from_native, query_options_from_native};
use crate::runtime::profile::{ProfileUnit, Profiler};
use crate::runtime::query_context::{QueryContextManager, QueryId, query_context_manager};
use crate::runtime::query_options::{QueryOptions, query_expire_durations};
use crate::runtime::result_buffer;
use crate::service::fe_report;

fn unique_id_from_native(src: &crate::proto::common::UniqueId) -> UniqueId {
    UniqueId {
        hi: src.hi,
        lo: src.lo,
    }
}

fn query_id_from_native(src: &crate::proto::common::UniqueId) -> QueryId {
    QueryId {
        hi: src.hi,
        lo: src.lo,
    }
}

fn profile_name_for_native_fragment(fragment: &crate::proto::plan::PlanFragment) -> String {
    let plan_node_id = fragment
        .root
        .as_ref()
        .map(|root| root.node_id)
        .unwrap_or(-1);
    if plan_node_id >= 0 {
        format!("execute_fragment_native (plan_node_id={plan_node_id})")
    } else {
        "execute_fragment_native".to_string()
    }
}

fn native_exchange_sender_counts(
    instance_params: &crate::proto::novarocks::InstanceParams,
) -> Result<HashMap<i32, usize>, String> {
    instance_params
        .per_exch_num_senders
        .iter()
        .map(|(node_id, count)| {
            if *count <= 0 {
                return Err(format!(
                    "native InstanceParams per_exch_num_senders node_id={} must be positive, got {}",
                    node_id, count
                ));
            }
            let count = usize::try_from(*count).map_err(|_| {
                format!(
                    "native InstanceParams per_exch_num_senders node_id={} cannot convert {} to usize",
                    node_id, count
                )
            })?;
            Ok((*node_id, count))
        })
        .collect()
}

fn native_fragment_uses_fetch_result_buffer(fragment: &crate::proto::plan::PlanFragment) -> bool {
    matches!(
        fragment.sink.as_ref().and_then(|sink| sink.kind.as_ref()),
        Some(crate::proto::plan::data_sink::Kind::Result(true))
    )
}

fn prepare_native_result_buffer_if_needed(
    fragment: &crate::proto::plan::PlanFragment,
    finst_id: UniqueId,
    typed_result_sink: bool,
    mem_tracker: Option<&Arc<MemTracker>>,
) {
    if !native_fragment_uses_fetch_result_buffer(fragment) {
        return;
    }
    if typed_result_sink {
        result_buffer::create_typed_sender(finst_id);
    } else {
        result_buffer::create_sender(finst_id);
    }
    if let Some(root) = mem_tracker {
        let label = format!("ResultBuffer: finst={}", finst_id);
        let tracker = MemTracker::new_child(label, root);
        result_buffer::set_mem_tracker(finst_id, tracker);
    }
}

fn profile_report_interval_ns(
    enable_profile: bool,
    query_opts: Option<&QueryOptions>,
) -> Option<i64> {
    if !enable_profile {
        return None;
    }
    let from_query = query_opts
        .and_then(|opts| opts.runtime_profile_report_interval)
        .filter(|v| *v > 0)
        .and_then(|v| v.checked_mul(1_000_000_000));
    from_query.or_else(|| {
        app_config::config()
            .ok()
            .map(|cfg| cfg.runtime.profile_report_interval.max(1) * 1_000_000_000)
    })
}

fn spawn_exec_fragment_native(
    fragment: crate::proto::plan::PlanFragment,
    instance_params: crate::proto::novarocks::InstanceParams,
    pipeline_dop: i32,
    finst_id: UniqueId,
    query_id: QueryId,
    profiler: Option<Profiler>,
    mem_tracker: Option<Arc<MemTracker>>,
    mgr: Arc<QueryContextManager>,
) {
    let uses_fetch_result_buffer = native_fragment_uses_fetch_result_buffer(&fragment);
    if uses_fetch_result_buffer {
        prepare_native_result_buffer_if_needed(
            &fragment,
            finst_id,
            instance_params.typed_result_sink,
            mem_tracker.as_ref(),
        );
    }
    mgr.register_finst(finst_id, query_id);
    std::thread::spawn(move || {
        let wall_start = std::time::Instant::now();
        let profiler_for_wall = profiler.clone();
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            execute_fragment_native(
                &fragment,
                &instance_params,
                None,
                pipeline_dop,
                None,
                profiler,
                mem_tracker,
            )
        }))
        .unwrap_or_else(|payload| {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic payload".to_string()
            };
            Err(format!("panic in native fragment execution: {msg}"))
        });
        if let Some(p) = profiler_for_wall.as_ref() {
            let elapsed_ns =
                crate::runtime::profile::clamp_u128_to_i64(wall_start.elapsed().as_nanos());
            p.counter_set("QueryExecutionWallTime", ProfileUnit::TimeNs, elapsed_ns);
        }
        let mut report_error: Option<String> = None;
        if uses_fetch_result_buffer {
            match out {
                Ok(out) => {
                    if let Some(json) = out.profile_json.as_deref() {
                        info!(
                            target: "novarocks::profile",
                            finst_id = %finst_id,
                            profile_bytes = json.len(),
                            "native_fragment_profile"
                        );
                    }
                }
                Err(e) => {
                    report_error = Some(e.clone());
                    error!(
                        target: "novarocks::exec",
                        finst_id = %finst_id,
                        error = %e,
                        "exec_plan_fragment_native failed"
                    );
                    result_buffer::close_error(finst_id, e);
                }
            }
        } else if let Err(e) = out {
            report_error = Some(e.clone());
            error!(
                target: "novarocks::exec",
                finst_id = %finst_id,
                error = %e,
                "exec_plan_fragment_native failed"
            );
        }
        if let Some(ref err_msg) = report_error {
            let finsts = mgr.cancel_query(query_id, err_msg.clone());
            for id in finsts {
                result_buffer::close_error(id, err_msg.clone());
                exchange::cancel_fragment(id.hi, id.lo);
            }
        }
        let report_decision = mgr.finish_fragment_for_report(query_id);
        fe_report::report_fragment_done(
            finst_id,
            report_error,
            report_decision.include_runtime_filter_profile,
        );
        exchange::remove_fragment(finst_id.hi, finst_id.lo);
        mgr.unregister_finst(finst_id);
        mgr.cleanup_after_fragment_report(query_id, report_decision);
    });
}

pub fn submit_exec_plan_fragment_native(
    fragment: crate::proto::plan::PlanFragment,
    instance_params: crate::proto::novarocks::InstanceParams,
) -> Result<(), String> {
    let query_id = instance_params
        .query_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing query_id".to_string())
        .map(query_id_from_native)?;
    let finst_id = instance_params
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native InstanceParams missing fragment_instance_id".to_string())
        .map(unique_id_from_native)?;
    let query_opts = instance_params
        .query_options
        .as_ref()
        .map(query_options_from_native)
        .transpose()?;
    let (delivery_expire, query_expire) = query_expire_durations(query_opts.as_ref());
    let mgr = query_context_manager();
    mgr.ensure_native_context(query_id, false, delivery_expire, query_expire)?;
    if instance_params.runtime_filter_params.is_some() {
        return Err(format!(
            "native fragment query_id={query_id} contains legacy runtime-filter params"
        ));
    }
    mgr.get_or_register_native(query_id, false, delivery_expire, query_expire)?;
    let cache_options = CacheOptions::from_query_options(query_opts.as_ref())?;
    mgr.set_cache_options(query_id, cache_options)?;

    let sender_counts = native_exchange_sender_counts(&instance_params)?;
    if !sender_counts.is_empty() {
        mgr.update_exchange_sender_counts(query_id, sender_counts)?;
    }

    let query_mem_tracker = mgr
        .query_mem_tracker(query_id)
        .ok_or_else(|| "QueryContext missing mem_tracker".to_string())?;
    let fragment_label = format!("fragment_{:x}_{:x}", finst_id.hi, finst_id.lo);
    let fragment_mem_tracker = MemTracker::new_child(fragment_label, &query_mem_tracker);
    let enable_profile = query_opts
        .as_ref()
        .map(|opts| opts.enable_profile)
        .unwrap_or(false);
    let profiler = if enable_profile {
        Some(Profiler::new(profile_name_for_native_fragment(&fragment)))
    } else {
        None
    };
    let report_interval_ns = profile_report_interval_ns(enable_profile, query_opts.as_ref());
    if let Some(report_endpoint) = instance_params
        .report_endpoint
        .as_deref()
        .filter(|endpoint| !endpoint.is_empty())
        .map(endpoint_from_native)
        .transpose()?
    {
        fe_report::register_novarocks_instance(
            finst_id,
            query_id,
            report_endpoint,
            instance_params.backend_num,
            enable_profile,
            profiler.clone(),
            Some(Arc::clone(&fragment_mem_tracker)),
            Some(Arc::clone(&query_mem_tracker)),
            report_interval_ns,
        );
    } else {
        warn!(
            target: "novarocks::report",
            finst_id = %finst_id,
            "missing native report_endpoint for reportExecStatus"
        );
    }

    let pipeline_dop = crate::runtime::exec_env::calc_pipeline_dop(
        query_opts
            .as_ref()
            .and_then(|opts| opts.pipeline_dop)
            .unwrap_or(0),
    );
    spawn_exec_fragment_native(
        fragment,
        instance_params,
        pipeline_dop,
        finst_id,
        query_id,
        profiler,
        Some(fragment_mem_tracker),
        Arc::clone(&mgr),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::exec::pipeline::dependency::DependencyManager;
    use crate::runtime::query_context::LegacyRuntimeFilterExecutionClaim;
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_filter_hub::RuntimeFilterHub;
    use crate::runtime::runtime_filter_params::RuntimeFilterParams;

    fn context_submission_state(
        query_id: QueryId,
        exchange_node_id: i32,
    ) -> Option<(usize, usize, Option<CacheOptions>, Option<usize>)> {
        query_context_manager()
            .with_context_mut(query_id, |ctx| {
                Ok((
                    ctx.num_fragments,
                    ctx.num_active_fragments,
                    ctx.cache_options(),
                    ctx.exchange_sender_count(exchange_node_id),
                ))
            })
            .ok()
    }

    fn native_instance_params(
        query_id: QueryId,
        runtime_filter_params: crate::proto::novarocks::RuntimeFilterParams,
    ) -> crate::proto::novarocks::InstanceParams {
        crate::proto::novarocks::InstanceParams {
            query_id: Some(crate::proto::common::UniqueId {
                hi: query_id.hi,
                lo: query_id.lo,
            }),
            fragment_instance_id: Some(crate::proto::common::UniqueId {
                hi: query_id.hi + 1,
                lo: query_id.lo + 1,
            }),
            runtime_filter_params: Some(runtime_filter_params),
            ..Default::default()
        }
    }

    #[test]
    fn native_fragment_profile_report_interval_uses_query_options_before_config() {
        let query_opts = QueryOptions {
            enable_profile: true,
            runtime_profile_report_interval: Some(7),
            ..Default::default()
        };

        assert_eq!(
            profile_report_interval_ns(true, Some(&query_opts)),
            Some(7_000_000_000)
        );
        assert_eq!(profile_report_interval_ns(false, Some(&query_opts)), None);
    }

    #[test]
    fn legacy_runtime_filter_rejection_claims_native_without_fragment_side_effects() {
        let query_id = QueryId {
            hi: 73_001,
            lo: 73_002,
        };
        let exchange_node_id = 91;
        let mut invalid = native_instance_params(
            query_id,
            crate::proto::novarocks::RuntimeFilterParams {
                runtime_filter_builder_number: HashMap::from([(7, 1)]),
                ..Default::default()
            },
        );
        invalid.per_exch_num_senders.insert(exchange_node_id, 3);

        let error =
            submit_exec_plan_fragment_native(crate::proto::plan::PlanFragment::default(), invalid)
                .expect_err("native fragment must reject non-empty legacy runtime-filter params");
        assert!(
            error.contains("contains legacy runtime-filter params"),
            "{error}"
        );
        assert_eq!(
            context_submission_state(query_id, exchange_node_id),
            Some((0, 0, None, None))
        );

        let manager = query_context_manager();
        manager
            .with_context_mut(query_id, |ctx| {
                assert_eq!(
                    ctx.legacy_runtime_filter_execution_claim(),
                    LegacyRuntimeFilterExecutionClaim::NativeDisabled
                );
                Ok(())
            })
            .expect("inspect retained NativeDisabled claim");
        let params_set_error = manager
            .set_runtime_filter_params(
                query_id,
                RuntimeFilterParams::new(BTreeMap::new(), BTreeMap::new(), None),
            )
            .expect_err("NativeDisabled claim must reject legacy params creation");
        assert!(
            params_set_error.contains("NativeDisabled"),
            "{params_set_error}"
        );
        let params_error = manager
            .get_runtime_filter_params(query_id)
            .expect_err("NativeDisabled claim must reject legacy params access");
        assert!(params_error.contains("NativeDisabled"), "{params_error}");
        let pending_error = manager
            .enqueue_pending_runtime_filter(query_id, 7, 0, vec![1], None)
            .expect_err("NativeDisabled claim must reject pending payload creation");
        assert!(pending_error.contains("NativeDisabled"), "{pending_error}");
        let pending_access_error = manager
            .with_context_mut(query_id, |ctx| ctx.drain_pending_runtime_filters())
            .expect_err("NativeDisabled claim must reject pending payload access");
        assert!(
            pending_access_error.contains("NativeDisabled"),
            "{pending_access_error}"
        );
        let hub_set_error = manager
            .set_runtime_filter_hub(
                query_id,
                Arc::new(RuntimeFilterHub::new(DependencyManager::new())),
            )
            .expect_err("NativeDisabled claim must reject legacy hub creation");
        assert!(hub_set_error.contains("NativeDisabled"), "{hub_set_error}");
        let hub_error = match manager.get_runtime_filter_hub(query_id) {
            Ok(_) => panic!("NativeDisabled claim must reject legacy hub access"),
            Err(error) => error,
        };
        assert!(hub_error.contains("NativeDisabled"), "{hub_error}");
        let worker_error = match manager.get_or_create_runtime_filter_worker(query_id) {
            Ok(_) => panic!("NativeDisabled claim must reject legacy worker creation"),
            Err(error) => error,
        };
        assert!(worker_error.contains("NativeDisabled"), "{worker_error}");
        let worker_access_error = match manager.get_runtime_filter_worker(query_id) {
            Ok(_) => panic!("NativeDisabled claim must reject legacy worker access"),
            Err(error) => error,
        };
        assert!(
            worker_access_error.contains("NativeDisabled"),
            "{worker_access_error}"
        );

        #[cfg(feature = "compat")]
        {
            let compat_error = manager
                .get_or_register_compat(
                    query_id,
                    false,
                    std::time::Duration::from_secs(60),
                    std::time::Duration::from_secs(60),
                )
                .expect_err("Compat fragment must conflict with the retained NativeDisabled claim");
            assert!(compat_error.contains("NativeDisabled"), "{compat_error}");
            assert_eq!(
                context_submission_state(query_id, exchange_node_id),
                Some((0, 0, None, None))
            );
        }

        manager
            .get_or_register_native(
                query_id,
                false,
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(60),
            )
            .expect("legal Native fragment retry must register successfully");
        assert_eq!(
            context_submission_state(query_id, exchange_node_id),
            Some((1, 1, None, None))
        );
    }

    #[test]
    fn rejected_native_fragment_claim_only_context_expires() {
        let query_id = QueryId {
            hi: 73_101,
            lo: 73_102,
        };
        let query_key = crate::runtime::runtime_filter_observability::QueryKey::from_hi_lo(
            query_id.hi,
            query_id.lo,
        );
        let registry =
            crate::runtime::runtime_filter_observability::RuntimeFilterLifecycleRegistry::global();
        registry.remove_query(query_key);
        let invalid = native_instance_params(
            query_id,
            crate::proto::novarocks::RuntimeFilterParams {
                runtime_filter_builder_number: HashMap::from([(7, 1)]),
                ..Default::default()
            },
        );

        let error =
            submit_exec_plan_fragment_native(crate::proto::plan::PlanFragment::default(), invalid)
                .expect_err("native fragment must reject legacy runtime-filter params");
        assert!(
            error.contains("contains legacy runtime-filter params"),
            "{error}"
        );

        let manager = query_context_manager();
        assert!(registry.snapshot(query_key).is_some());
        manager
            .with_context_mut(query_id, |context| {
                assert_eq!(context.num_active_fragments, 0);
                context.query_deadline =
                    std::time::Instant::now() - std::time::Duration::from_millis(1);
                Ok(())
            })
            .expect("claim-only active context");

        manager.clean_expired_for_test();

        assert!(manager.query_mem_tracker(query_id).is_none());
        assert!(registry.snapshot(query_key).is_none());
    }

    #[test]
    fn legacy_runtime_filter_rejection_preserves_existing_context_state() {
        let query_id = QueryId {
            hi: 73_003,
            lo: 73_004,
        };
        let exchange_node_id = 92;
        let manager = query_context_manager();
        manager
            .get_or_register_native(
                query_id,
                false,
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(60),
            )
            .expect("register existing native context");
        manager
            .set_cache_options(
                query_id,
                CacheOptions::from_query_options(None).expect("default cache options"),
            )
            .expect("set existing cache options");
        manager
            .update_exchange_sender_counts(query_id, HashMap::from([(exchange_node_id, 2)]))
            .expect("set existing sender count");
        let before = context_submission_state(query_id, exchange_node_id);

        let mut invalid = native_instance_params(
            query_id,
            crate::proto::novarocks::RuntimeFilterParams {
                runtime_filter_builder_number: HashMap::from([(8, 1)]),
                ..Default::default()
            },
        );
        invalid.query_options = Some(crate::proto::novarocks::QueryOptions {
            enable_scan_datacache: true,
            ..Default::default()
        });
        invalid.per_exch_num_senders.insert(exchange_node_id, 9);

        let error =
            submit_exec_plan_fragment_native(crate::proto::plan::PlanFragment::default(), invalid)
                .expect_err("native fragment must reject non-empty legacy runtime-filter params");
        assert!(
            error.contains("contains legacy runtime-filter params"),
            "{error}"
        );
        assert_eq!(context_submission_state(query_id, exchange_node_id), before);
    }
}
