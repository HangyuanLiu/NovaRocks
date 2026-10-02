#!/usr/bin/env python3
"""Require runtime failures for actual initial SETTINGS mutations; restore bytes."""
from pathlib import Path
import difflib, hashlib, json, subprocess, tempfile, sys
repo = Path.cwd()
receipt = Path(__file__).parent
pins = receipt / 'product-sha256.json'
if pins.exists():
    for relative, digest in json.loads(pins.read_text()).items():
        assert hashlib.sha256((repo / relative).read_bytes()).hexdigest() == digest, relative
out = Path(tempfile.mkdtemp(prefix='m07-initial-settings-negatives-'))
cases = [
    ('client-preface-only', 'vendor/hyper-1.8.1/src/proto/h2/client.rs',
     'if config.initial_settings_deadline.is_some() {\n        crate::common::future::poll_fn',
     'if false {\n        crate::common::future::poll_fn',
     'native_server::initial_settings_tests::actual_capacity_endpoint'),
    ('server-skips-settings', 'vendor/hyper-1.8.1/src/proto/h2/server.rs',
     'wait_initial_settings: config.initial_settings_deadline.is_some(),',
     'wait_initial_settings: false,',
     'native_server::initial_settings_tests::actual_listener_partial'),
    ('factory-restarts-deadline', 'vendor/tonic-0.12.3/src/transport/channel/service/connection.rs',
     'let deadline = started.checked_add(timeout)',
     'let deadline = std::time::Instant::now().checked_add(timeout)',
     'initial_settings_allowance_includes_synchronous_factory'),
    ('application-retains-acquisition-deadline', 'vendor/h2-0.4.12/src/proto/connection.rs',
     'self.codec.set_initial_settings_deadline(None);',
     '// Incorrectly keep the acquisition deadline on application IO.',
     'native_server::initial_settings_tests::completed_native_bootstrap'),
    ('skip-real-ack-flush', 'vendor/h2-0.4.12/src/proto/connection.rs',
     'ready!(self.codec.flush(cx))?;\n            self.check_initial_settings_phase()?;',
     '// Incorrectly skip actual ACK flush.\n            self.check_initial_settings_phase()?;',
     'applied_peer_settings_wait_for_real_flush'),
    ('unknown-frames-do-not-yield', 'vendor/h2-0.4.12/src/codec/framed_read.rs',
     'initial_settings_frames == 32',
     'initial_settings_frames == usize::MAX',
     'always_ready_unknown_frames_yield_at_32'),

]

if len(sys.argv) > 1:
    selected = set(sys.argv[1:])
    assert selected <= {case[0] for case in cases}, selected
    cases = [case for case in cases if case[0] in selected]

originals = {p: (repo/p).read_bytes() for _,p,*_ in cases}
for label,p,old,*_ in cases:
    assert originals[p].decode().count(old) == 1, (label, 'unique anchor')
try:
    for label,p,old,new,filter_ in cases:
        source = originals[p].decode()
        mutated = source.replace(old,new)
        (repo/p).write_text(mutated)
        (out/(label+'.diff')).write_text(''.join(difflib.unified_diff(source.splitlines(True),mutated.splitlines(True),fromfile=p,tofile=p)))
        if label == 'factory-restarts-deadline': target = ['--test', 'native_tonic_connection_factory']
        elif label in {'skip-real-ack-flush', 'unknown-frames-do-not-yield'}: target = ['--test', 'native_initial_settings_kernel']
        else: target = ['--lib']
        cmd = ['cargo','test','--offline','-p','novarocks-native-adapter'] + target + [filter_,'--','--test-threads=1']
        log = out/(label+'.log')
        with log.open('w') as handle:
            handle.write('Command: '+' '.join(cmd)+'\n'); handle.flush()
            result = subprocess.run(cmd,stdout=handle,stderr=subprocess.STDOUT)
        (repo/p).write_bytes(originals[p])
        assert result.returncode == 101 and 'test result: FAILED.' in log.read_text(), (label,result.returncode,'compiled runtime failure required')
        print(label,'runtime rejected',flush=True)
finally:
    for p,source in originals.items(): (repo/p).write_bytes(source)
    assert all((repo/p).read_bytes()==source for p,source in originals.items())
    print('exact source restoration',flush=True)
    print('evidence directory',out,flush=True)
