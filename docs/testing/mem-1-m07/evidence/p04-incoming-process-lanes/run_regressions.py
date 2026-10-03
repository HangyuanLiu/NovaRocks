import gzip, hashlib, json, os, re, subprocess, time
from pathlib import Path
root=Path(__file__).resolve().parents[5]
import tempfile, difflib
out=Path(tempfile.mkdtemp(prefix='m07-incoming-negatives-'))
print(str(out),flush=True)
cases=[
('head-forward', 'vendor/hyper-1.8.1/src/rt/bounds.rs', '<E as Executor<H2Stream<F, B, E>>>::admit_request_head(self, uri, headers)', 'Ok(())', 'novarocks-native-adapter', 'native_task_executor::tests_incoming::actual_cross_process_or_lane_fails_connection_before_prepare_or_service'),
('incoming-exit', 'novarocks/native-adapter/src/native_transport_capacity.rs', 'self.factory\n                .core()\n                .incoming_keys\n                .exit(token)\n                .expect("exact original incoming key exit");', 'let _ = token;', 'novarocks-native-adapter', 'native_task_executor::tests_incoming::actual_field_and_io_aliases_hold_closing_generations_until_last_original_exit'),
('sealed-drift', 'novarocks/native-adapter/src/native_transport_capacity.rs', 'return if existing == key {\n                Ok(())\n            } else {\n                Err(io::ErrorKind::ConnectionAborted.into())\n            };', 'return Ok(());', 'novarocks-native-adapter', 'native_task_executor::tests_incoming::actual_cross_process_or_lane_fails_connection_before_prepare_or_service'),
('retired-publish', 'novarocks/frontend-application/src/native/data_runtime.rs', 'if !exact || !self.leader || self.is_retired() {', 'if (!exact || !self.leader || self.is_retired()) && false {', 'novarocks-frontend-application', 'native::data_runtime::tests::retired_inflight_dial_cannot_publish_or_delete_its_replacement_generation'),
('heartbeat-retirement','novarocks/frontend-application/src/native/transport.rs','if !self.rpc_completed {','if !self.rpc_completed && false {','novarocks-frontend-application','native::transport::routing_tests::failed_or_cancelled_heartbeat_retires_only_its_acquired_control_generation')]
results=[]
env=dict(os.environ,CARGO_BUILD_JOBS='4',CARGO_INCREMENTAL='0')
for name, rel, before, after, package, test in cases:
    p=root/rel; original=p.read_bytes(); content=original.decode(); assert content.count(before)==1,(name,content.count(before))
    start=time.time()
    try:
        p.write_text(content.replace(before,after,1))
        diff=''.join(difflib.unified_diff(content.splitlines(True),content.replace(before,after,1).splitlines(True),fromfile='a/'+rel,tofile='b/'+rel)).encode()
        with gzip.open(out/(name+'.diff.gz'),'wb') as f: f.write(diff)
        cmd=['cargo','test','-p',package,'--lib',test,'--','--exact']
        with (out/(name+'.log')).open('wb') as f:
            rc=subprocess.run(cmd,cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=420).returncode
        log=(out/(name+'.log')).read_text()
        valid=rc==101 and re.search(r'test result: FAILED\. 0 passed; 1 failed;',log) is not None and 'error: could not compile' not in log
        results.append(dict(name=name,source=rel,test=test,command=cmd,exit=rc,runtime_failure=valid,elapsed_seconds=round(time.time()-start,3),source_restored_sha256=hashlib.sha256(original).hexdigest()))
        print(json.dumps(results[-1]),flush=True)
        if not valid: raise RuntimeError('Mutation did not produce one compiled runtime failure: '+name)
    finally:
        p.write_bytes(original); assert p.read_bytes()==original
        (out/'results.json').write_text(json.dumps(results,indent=2)+'\n')
