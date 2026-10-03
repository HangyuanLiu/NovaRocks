from pathlib import Path
import subprocess, os, hashlib, json
root=Path(__file__).resolve().parents[5]
import tempfile
out=Path(tempfile.mkdtemp(prefix='m07-request-negatives-'))
cases=[
('provider-reselection','vendor/tonic-0.12.3/src/transport/channel/service/request_task_pool.rs','''        self.pool
            .core()
            .admission_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::ErrorKind::WouldBlock)?;''','''        let _ = self.pool.core().admission_claimed.load(Ordering::Acquire);''','native_tonic_original_request_tasks','actual_shared_pool_second_attempt_with_fresh_task_tokens_refuses_before_connector'),
('missing-predial-request-bound','vendor/tonic-0.12.3/src/transport/channel/service/request_task_pool.rs','''        if pipe_bound > self.pool.core().bounds[0] || send_bound > self.pool.core().bounds[1] {
            return Err(io::ErrorKind::InvalidInput.into());
        }''','''        let _ = (pipe_bound, send_bound);''','native_tonic_original_request_tasks','actual_final_pipe_and_send_short_bounds_refuse_before_connector_without_fallback'),
('ordinary-pipe-send-spawn','vendor/tonic-0.12.3/src/transport/channel/service/request_task_executor.rs','''            drop(runtime.spawn_with_task_owner(future, owner)?);''','''            drop(runtime.spawn(future));
            drop(owner);''','native_tonic_original_request_tasks','actual_tonic_one_pair_refuses_before_body_headers_and_keeps_last_real_pipe_cell_original'),
('missing-callback-pair','vendor/hyper-1.8.1/src/client/dispatch.rs','''        self.lease = lease;''','''        drop(lease);
        self.lease = None;''','native_hyper_request_task_admission','actual_queued_cancel_drops_body_before_its_original_pair'),
('early-eos-send','vendor/hyper-1.8.1/src/proto/h2/client.rs','''                drop(f.body);
                drop(f.body_tx);''','''                // Deliberate negative: leave body/stream for stack teardown.''','native_hyper_eos_request_lease','actual_eos_body_drop_finishes_before_send_dispatch_after_response_cancel'),
('unsupported-admission-fallback','vendor/hyper-1.8.1/src/rt/bounds.rs','''            if admission.is_some()
                && !<E as Executor<H2ClientFuture<B, T, E>>>::supports_client_request_task_lease()
            {
                return Err(std::io::ErrorKind::Unsupported.into());
            }''','''            // Deliberate negative: accept an unsupported provider.''','native_hyper_request_admission_contract','actual_unsupported_admission_refuses_before_io_and_executor_dispatch')]
env=os.environ.copy();env.update(RUSTUP_TOOLCHAIN='1.92.0',CARGO_BUILD_JOBS='4',CARGO_INCREMENTAL='0')
results=[]
for name,relative,before,after,target,test in cases:
 p=root/relative;original=p.read_bytes();src=original.decode()
 assert src.count(before)==1,(name,src.count(before))
 log=out/(name+'.log')
 try:
  p.write_text(src.replace(before,after,1))
  with log.open('w') as stream:
   result=subprocess.run(['cargo','test','-p','novarocks-native-adapter','--test',target,test,'--','--exact','--test-threads=1'],cwd=root,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=420)
  data=log.read_text()
  assert result.returncode==101 and 'test result: FAILED' in data and 'error[E' not in data,(name,result.returncode,data[-2500:])
  results.append(dict(name=name,test=test,target=target,exit_code=result.returncode,source=relative,restored_sha256=hashlib.sha256(original).hexdigest(),log=str(log)))
  print(name+' COMPILED_RUNTIME_FAILED101',flush=True)
 finally:
  p.write_bytes(original)
  assert p.read_bytes()==original
(out/'summary.json').write_text(json.dumps(results,indent=2)+'\n')
print('ALL_NEGATIVES_BYTE_EXACT_RESTORED',flush=True)
