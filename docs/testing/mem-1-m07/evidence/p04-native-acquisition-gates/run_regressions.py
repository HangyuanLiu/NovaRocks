#!/usr/bin/env python3
"""Mutate actual acquisition seams; require runtime failure and restore bytes."""
from pathlib import Path
import difflib, hashlib, json, subprocess, tempfile, sys
repo = Path.cwd()
receipt = Path(__file__).parent
pins = receipt / 'product-sha256.json'
if pins.exists():
    for relative, digest in json.loads(pins.read_text()).items():
        assert hashlib.sha256((repo / relative).read_bytes()).hexdigest() == digest, relative
out = Path(tempfile.mkdtemp(prefix='m07-acquisition-negatives-'))
cases = [('gate-permits-one-extra', 'novarocks/native-adapter/src/native_transport_capacity.rs', 'next <= capacity', 'next <= capacity + 1', '--lib', 'native_server::acquisition_tests::data_32'), ('control-borrows-data-counter', 'novarocks/native-adapter/src/native_transport_capacity.rs', 'TransportClass::Control => 1,', 'TransportClass::Control => 0,', '--lib', 'native_server::acquisition_tests::control_8'), ('ready-owner-before-future', 'vendor/tonic-0.12.3/src/transport/mod.rs', '            this.future.set(None);\n            drop(this.owner.take());', '            drop(this.owner.take());\n            this.future.set(None);', 'native_tonic_acquisition_owner', 'actual_acquisition_ready_error'), ('cancel-owner-before-future', 'vendor/tonic-0.12.3/src/transport/mod.rs', '        this.future.set(None);\n        drop(this.owner.take());', '        drop(this.owner.take());\n        this.future.set(None);', 'native_tonic_acquisition_owner', 'unpolled_actual_acquisition_drop'), ('tonic-omits-acquisition-owner', 'vendor/tonic-0.12.3/src/transport/channel/service/connection.rs', 'acquisition_owner = config.acquisition_owner.take();', 'acquisition_owner = None;', 'native_tonic_acquisition_owner', 'canceled_pending_dial'), ('server-delays-complete-to-deadline', 'novarocks/native-adapter/src/native_server.rs', 'Poll::Pending if complete => Poll::Ready(None),', 'Poll::Pending if complete => Poll::Pending,', '--lib', 'native_server::acquisition_tests::applied_settings_returns')]
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
