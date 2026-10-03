import difflib,gzip,hashlib,json,os,re,subprocess,time,tempfile
from pathlib import Path
root=Path(__file__).resolve().parents[5]; output=Path(tempfile.mkdtemp(prefix='m07-protocol-negatives-'));output.mkdir(exist_ok=True)
source=root/'vendor/tonic-0.12.3/src/transport/channel/service/connection_driver.rs'
cases=[
 ('task-owner','let task = runtime.spawn_with_task_owner(future, core.original.clone())?;','let task = runtime.spawn(future);','actual_completed_unpolled_join_and_abort_handles_hold_original_task_credit'),
 ('one-shot','compare_exchange(UNBOUND, RESERVED, Ordering::AcqRel, Ordering::Acquire)','compare_exchange(self.position.core().phase.load(Ordering::Acquire), RESERVED, Ordering::AcqRel, Ordering::Acquire)','actual_dial_error_abandons_token_without_reselecting_it'),
 ('pre-dial-bound','if bound > self.position.core().task_bound {','if bound > self.position.core().task_bound && false {','actual_short_task_bound_refuses_before_tcp_connector'),
]
env=dict(os.environ,CARGO_BUILD_JOBS='4',CARGO_INCREMENTAL='0');results=[]
for name,before,after,test in cases:
 original=source.read_bytes();content=original.decode(); prefix,content=content.split('const PROTOCOL_PREPARED: u8 = 4;',1);assert content.count(before)==1,name
 try:
  changed=prefix+'const PROTOCOL_PREPARED: u8 = 4;'+content.replace(before,after,1);source.write_text(changed)
  delta=''.join(difflib.unified_diff(original.decode().splitlines(True),changed.splitlines(True),fromfile='original',tofile='negative'))
  with gzip.open(output/(name+'.diff.gz'),'wb') as f:f.write(delta.encode())
  command=['cargo','+1.92.0','test','-p','novarocks-native-adapter','--test','native_tonic_original_protocol_task',test,'--','--exact','--test-threads=1']
  start=time.time()
  with (output/(name+'.log')).open('wb') as log:rc=subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=420).returncode
  log=(output/(name+'.log')).read_text();valid=rc==101 and 'test result: FAILED. 0 passed; 1 failed;' in log and 'error: could not compile' not in log
  result=dict(name=name,test=test,command=command,exit=rc,runtime_failure=valid,seconds=round(time.time()-start,3),restored_source_sha256=hashlib.sha256(original).hexdigest());results.append(result);print(json.dumps(result),flush=True)
  if not valid:raise RuntimeError(name+' did not produce exact compiled runtime failure')
 finally:
  source.write_bytes(original);assert source.read_bytes()==original
  (output/'results.json').write_text(json.dumps(results,indent=2)+'\n')
