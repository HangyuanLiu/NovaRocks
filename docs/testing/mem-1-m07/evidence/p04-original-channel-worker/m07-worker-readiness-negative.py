from pathlib import Path
import subprocess, os, hashlib, json
root=Path(__file__).resolve().parents[5]
out=Path('/tmp/m07-worker-readiness-negative');out.mkdir(exist_ok=True)
p=root/'novarocks/native-adapter/src/backend_readiness.rs'
original=p.read_bytes();src=original.decode()
before='    let original_worker = runtime\n        .channels()\n        .transient_original_channel_worker()\n        .map_err(|error| format!("Native readiness Worker election refused: {error}"))?;'
after='    let original_worker = None::<tonic::transport::OriginalChannelWorker>;'
assert src.count(before)==1
env=os.environ.copy();env.update(RUSTUP_TOOLCHAIN='1.92.0',CARGO_BUILD_JOBS='4',CARGO_INCREMENTAL='0')
log=out/'ordinary-readiness-fallback.log'
try:
 p.write_text(src.replace(before,after,1))
 with log.open('w') as stream:
  result=subprocess.run(['cargo','test','-p','novarocks-native-adapter','--lib','backend_readiness::worker_tests::full_original_worker_stock_refuses_actual_readiness_before_tcp_connector','--','--exact','--test-threads=1'],cwd=root,env=env,stdout=stream,stderr=subprocess.STDOUT,timeout=420)
 data=log.read_text()
 assert result.returncode==101 and 'test result: FAILED' in data and 'error[E' not in data,(result.returncode,data[-2500:])
 (out/'summary.json').write_text(json.dumps(dict(name='ordinary-readiness-fallback',exit_code=101,source='novarocks/native-adapter/src/backend_readiness.rs',restored_sha256=hashlib.sha256(original).hexdigest(),log=str(log)),indent=2)+'\n')
 print('ORDINARY_READINESS_COMPILED_RUNTIME_FAILED101',flush=True)
finally:
 p.write_bytes(original)
 assert p.read_bytes()==original
print('BYTE_EXACT_RESTORED',flush=True)
