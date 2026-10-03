from pathlib import Path
import subprocess, os, hashlib, json
root=Path(__file__).resolve().parents[5]
out=Path('/tmp/m07-worker-native-negatives');out.mkdir(exist_ok=True)
cases=[
('early-detach-reuse','novarocks/native-adapter/src/native_channel_worker_capacity.rs','            if next == FINAL_EXIT {\n                // Other events refuse FINAL_EXIT.','            if next == FINAL_EXIT || !owner_exit {\n                // Deliberate negative: detach alone prematurely returns stock.','native_channel_cache::worker_tests::actual_230_detached_old_channel_workers_exhaust_original_stock_until_last_cell_exit'),
('missing-cold-eviction-detach','novarocks/native-adapter/src/native_channel_cache.rs','                        retired_worker = entry.worker.take();','                        retired_worker = None::<EntryWorker>;','native_channel_cache::worker_tests::actual_cold_ready_eviction_detaches_worker_without_retiring_escaped_channel')]
env=os.environ.copy();env.update(RUSTUP_TOOLCHAIN='1.92.0',CARGO_BUILD_JOBS='4',CARGO_INCREMENTAL='0')
results=[]
for name,relative,before,after,test in cases:
 p=root/relative;original=p.read_bytes();src=original.decode()
 assert src.count(before)==1,(name,src.count(before))
 log=out/(name+'.log')
 try:
  p.write_text(src.replace(before,after,1))
  with log.open('w') as stream:
   result=subprocess.run(['cargo','test','-p','novarocks-native-adapter','--lib',test,'--','--exact','--test-threads=1'],cwd=root,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=420)
  data=log.read_text()
  assert result.returncode!=0 and ('test result: FAILED' in data or 'panic in a destructor' in data) and 'error[E' not in data,(name,result.returncode,data[-2500:])
  results.append(dict(name=name,test=test,exit_code=result.returncode,source=relative,restored_sha256=hashlib.sha256(original).hexdigest(),log=str(log)))
  print(name+' COMPILED_RUNTIME_FAILED',flush=True)
 finally:
  p.write_bytes(original)
  assert p.read_bytes()==original
(out/'summary.json').write_text(json.dumps(results,indent=2)+'\n')
print('ALL_NEGATIVES_BYTE_EXACT_RESTORED',flush=True)
