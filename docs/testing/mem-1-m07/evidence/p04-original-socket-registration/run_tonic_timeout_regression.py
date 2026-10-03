from pathlib import Path
import difflib,gzip,hashlib,json,os,re,subprocess,tempfile
p=Path('vendor/tonic-0.12.3/src/transport/channel/service/attempt_connector.rs'); test=Path('novarocks/native-adapter/tests/native_tonic_attempt_connector.rs'); original=p.read_bytes(); old=b'.map_err(|elapsed| {\n                    crate::Error::from(io::Error::new(io::ErrorKind::TimedOut, elapsed))\n                })?'
if old not in original:
    old=b'.map_err(|elapsed| crate::Error::from(io::Error::new(io::ErrorKind::TimedOut, elapsed)))?'
assert original.count(old)==1
mutated=original.replace(old,b'.map_err(|_| crate::Error::from(io::Error::from(io::ErrorKind::TimedOut)))?',1)
out=Path(tempfile.mkdtemp(prefix='m07-tonic-timeout-payload-negative-'));print(out,flush=True)
(out/'mutation.diff.gz').write_bytes(gzip.compress(''.join(difflib.unified_diff(original.decode().splitlines(True),mutated.decode().splitlines(True),fromfile=str(p),tofile=str(p))).encode(),mtime=0))
command=['cargo','test','--offline','--locked','-p','novarocks-native-adapter','--test','native_tonic_attempt_connector','typed_pending_connect_deadline_exits_actual_future_before_owner','--','--exact','--test-threads=1']
env=os.environ.copy();env.update(CARGO_BUILD_JOBS='4',CARGO_INCREMENTAL='0')
record={'command':command,'production_source':str(p),'original_sha256':hashlib.sha256(original).hexdigest(),'mutated_sha256':hashlib.sha256(mutated).hexdigest(),'test_sha256':hashlib.sha256(test.read_bytes()).hexdigest()}
try:
    p.write_bytes(mutated)
    with (out/'negative.log').open('w') as log:
        r=subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=600)
    text=(out/'negative.log').read_text()
    assert r.returncode==101 and 'running 1 test' in text and 'test result: FAILED. 0 passed; 1 failed;' in text and 'typed connect timeout must preserve the original Elapsed error payload' in text
    assert not re.search(r'error\[E\d+\]|could not compile|SIGABRT|signal:|running 0 tests',text)
    record.update(exit_code=r.returncode,verdict='compiled exact runtime FAILED with original Elapsed payload oracle')
finally:
    p.write_bytes(original);record['exact_restoration']=p.read_bytes()==original
    (out/'verification.json').write_text(json.dumps(record,indent=2)+'\n')
print(record['verdict'],record['exact_restoration'],flush=True)
