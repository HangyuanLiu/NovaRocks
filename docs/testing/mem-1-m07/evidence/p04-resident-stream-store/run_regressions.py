#!/usr/bin/env python3
"""Mutate actual resident-store seams; require runtime failure and restore bytes."""
from pathlib import Path
import difflib, hashlib, json, subprocess, tempfile, sys
repo = Path.cwd()
receipt = Path(__file__).parent
pins = receipt / 'product-sha256.json'
if pins.exists():
    for relative, digest in json.loads(pins.read_text()).items():
        assert hashlib.sha256((repo / relative).read_bytes()).hexdigest() == digest, relative
out = Path(tempfile.mkdtemp(prefix='m07-stream-store-negatives-'))
cases = [
    ('drop-original-fixed-store', 'vendor/h2-0.4.12/src/proto/streams/store.rs',
     'Some(storage) => Storage::Fixed(storage),',
     'Some(_storage) => Storage::Default { slab: slab::Slab::new(), ids: IndexMap::new() },',
     'native_h2_resident_store', 'hard_two_survives'),
    ('cancel-keeps-registration', 'vendor/h2-0.4.12/src/client.rs',
     'self.inner\n            .unregister_resident_waiter(&mut self.resident_registration);',
     '// Incorrectly keep a canceled original waiter registration.',
     'native_h2_resident_store', 'canceled_handles_and_repolls'),
    ('wake-under-connection-lock', 'vendor/h2-0.4.12/src/proto/streams/store.rs',
     'waiter.notify = true;\n                    self.resident_notifications_pending = true;',
     'waiter.waker.take().unwrap().wake();',
     'native_h2_resident_store', 'resident_notification_can_drop'),
    ('none-clones-waker', 'vendor/h2-0.4.12/src/proto/streams/streams.rs',
     'self.inner.fixed_store.then(|| cx.waker().clone())',
     'Some(cx.waker().clone())',
     'native_h2_resident_store', 'legacy_none_ready'),
    ('hyper-client-omits-store', 'vendor/hyper-1.8.1/src/proto/h2/client.rs',
     'builder.stream_store_buffer(buffer.clone());',
     'let _ = buffer;',
     'native_h2_stream_store_owner', 'cloned_hyper_client'),
    ('hyper-server-omits-store', 'vendor/hyper-1.8.1/src/proto/h2/server.rs',
     'builder.stream_store_buffer(buffer.clone());',
     'let _ = buffer;',
     'native_h2_stream_store_owner', 'cloned_hyper_server'),
    ('tonic-omits-store', 'vendor/tonic-0.12.3/src/transport/channel/http2_connection.rs',
     'builder.stream_store_buffer(buffer);',
     'let _ = buffer;',
     'native_h2_stream_store_owner', 'actual_tonic_factory'),
]
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
        cmd=['cargo','test','--offline','-p','novarocks-native-adapter','--test',target,filter_,'--','--test-threads=1']
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
