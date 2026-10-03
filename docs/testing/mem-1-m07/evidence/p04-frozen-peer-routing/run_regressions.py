#!/usr/bin/env python3
"""Mutate frozen peer routing; require runtime failure and restore bytes."""
from pathlib import Path
import difflib, hashlib, json, subprocess, tempfile, sys
repo = Path.cwd()
receipt = Path(__file__).parent
pins = receipt / 'product-sha256.json'
if pins.exists():
    for relative, digest in json.loads(pins.read_text()).items():
        assert hashlib.sha256((repo / relative).read_bytes()).hexdigest() == digest, relative
out = Path(tempfile.mkdtemp(prefix='m07-frozen-peer-negatives-'))
cases = [('cache-omits-process', 'novarocks/native-adapter/src/native_client.rs', '=> Ok(Self {\n                backend_process_id,', '=> Ok(Self {\n                backend_process_id: None,', '--lib', 'native_client::peer_key_tests::actual_cached'), ('cache-omits-lane', 'novarocks/native-adapter/src/native_client.rs', '                endpoint,\n                method,', '                endpoint,\n                method: NativeRpcMethod::ExchangeUnary,', '--lib', 'native_client::peer_key_tests::actual_cached'), ('decoder-invents-missing-process', 'novarocks/native-adapter/src/runtime_filter_install.rs', '            let process_bytes = remote\n                .backend_process_id\n                .as_slice()\n                .try_into()\n                .map_err(|_| {\n                    invalid(\n                        process_path.clone(),\n                        "backend process identity must be exactly 16 bytes",\n                    )\n                })?;\n', '            let process_bytes = if remote.backend_process_id.is_empty() {\n                BackendProcessId::new_v7().to_bytes()\n            } else {\n                remote\n                .backend_process_id\n                .as_slice()\n                .try_into()\n                .map_err(|_| {\n                    invalid(\n                        process_path.clone(),\n                        "backend process identity must be exactly 16 bytes",\n                    )\n                })?\n            };\n', '--lib', 'runtime_filter_install::tests::remote_peer_refuses')]
if len(sys.argv) > 1:
    selected = set(sys.argv[1:]); assert selected <= {case[0] for case in cases}
    cases = [case for case in cases if case[0] in selected]
originals = {path: (repo / path).read_bytes() for _,path,*_ in cases}
for label,path,old,*_ in cases:
    assert originals[path].decode().count(old) == 1, (label, 'unique anchor')
try:
    for label,path,old,new,target,filter_ in cases:
        source = originals[path].decode(); mutated = source.replace(old,new)
        (repo/path).write_text(mutated)
        (out/(label+'.diff')).write_text(''.join(difflib.unified_diff(source.splitlines(True),mutated.splitlines(True),fromfile=path,tofile=path)))
        cmd=['cargo','test','--offline','-p','novarocks-native-adapter'] + (['--lib'] if target == '--lib' else ['--test',target]) + [filter_,'--','--test-threads=1']
        log=out/(label+'.log')
        with log.open('w') as handle:
            handle.write('Command: '+' '.join(cmd)+'\n');handle.flush()
            result=subprocess.run(cmd,stdout=handle,stderr=subprocess.STDOUT,timeout=180)
        (repo/path).write_bytes(originals[path])
        assert result.returncode == 101 and 'test result: FAILED.' in log.read_text(), (label,result.returncode,'compiled runtime failure required')
        print(label,'runtime rejected',flush=True)
finally:
    for path,source in originals.items(): (repo/path).write_bytes(source)
    assert all((repo/path).read_bytes()==source for path,source in originals.items())
    print('exact source restoration',flush=True)
    print('evidence directory',out,flush=True)
