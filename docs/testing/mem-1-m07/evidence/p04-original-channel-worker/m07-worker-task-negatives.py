from pathlib import Path
import subprocess, os, hashlib, json
root=Path(__file__).resolve().parents[5]
out=Path('/tmp/m07-worker-task-negatives');out.mkdir(exist_ok=True)
cases=[
('missing-worker-predial-bound','vendor/tonic-0.12.3/src/transport/channel/service/connection_driver.rs',"""        if actual_bound > self.core().task_bound {
            return Err(io::ErrorKind::InvalidInput.into());
        }""",'        let _ = actual_bound;',1,'actual_short_task_bound_refuses_before_tcp_connector'),
('worker-reselection','vendor/tonic-0.12.3/src/transport/channel/service/connection_driver.rs',"""        self.core()
            .phase
            .compare_exchange(UNBOUND, RESERVED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::ErrorKind::WouldBlock)?;""",'        self.core().phase.store(RESERVED, Ordering::Release);',1,'actual_cloned_token_second_attempt_refuses_before_tcp_connector'),
('ordinary-worker-dispatch','vendor/tonic-0.12.3/src/transport/channel/mod.rs','            Some(original) => original.spawn(worker).map_err(super::Error::from_source)?,','            Some(original) => { drop(original); executor.execute(worker); }',2,'actual_completed_unpolled_join_and_abort_handles_hold_original_task_credit')]
env=os.environ.copy();env.update(RUSTUP_TOOLCHAIN='1.92.0',CARGO_BUILD_JOBS='4',CARGO_INCREMENTAL='0')
results=[]
for name,relative,before,after,count,test in cases:
 p=root/relative;original=p.read_bytes();src=original.decode()
 assert src.count(before)==count,(name,src.count(before))
 log=out/(name+'.log')
 try:
  p.write_text(src.replace(before,after))
  with log.open('w') as stream:
   result=subprocess.run(['cargo','test','-p','novarocks-native-adapter','--test','native_tonic_original_channel_worker',test,'--','--exact','--test-threads=1'],cwd=root,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=420)
  data=log.read_text()
  assert result.returncode==101 and 'test result: FAILED' in data and 'error[E' not in data,(name,result.returncode,data[-2500:])
  results.append(dict(name=name,test=test,exit_code=result.returncode,source=relative,restored_sha256=hashlib.sha256(original).hexdigest(),log=str(log)))
  print(name+' COMPILED_RUNTIME_FAILED101',flush=True)
 finally:
  p.write_bytes(original)
  assert p.read_bytes()==original
(out/'summary.json').write_text(json.dumps(results,indent=2)+'\n')
print('ALL_NEGATIVES_BYTE_EXACT_RESTORED',flush=True)
