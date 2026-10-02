#!/usr/bin/env python3
"""Require runtime failures for actual capacity wiring mutations; restore bytes."""
from pathlib import Path
import difflib, hashlib, json, subprocess, tempfile
repo = Path.cwd()
receipt = Path(__file__).parent
pins = receipt / 'product-sha256.json'
if pins.exists():
    for relative, digest in json.loads(pins.read_text()).items():
        assert hashlib.sha256((repo / relative).read_bytes()).hexdigest() == digest, relative
out = Path(tempfile.mkdtemp(prefix='m07-native-capacity-negatives-'))
cases = [
    ('uninstalled-actual-listener', 'novarocks/native-adapter/src/native_server.rs',
     'let capacity = match &transport_capacity {',
     'let capacity = match &None::<NativeTransportCapacityFactory> {',
     'native_server::capacity_tests::installed_tcp_listener'),
    ('separate-startup-wallet', 'novarocks/native-adapter/src/backend_application.rs',
     'NativeTransportCapacityFactory::try_new(Arc::clone(\n            &result_retained_budget,\n        ))',
     'NativeTransportCapacityFactory::try_new(novarocks_worker::result_buffer::ResultRetainedBudget::new(std::num::NonZeroUsize::new(4 * 1024 * 1024 * 1024).unwrap()))',
     'backend_application::tests::short_original_transport_budget'),
    ('reuse-unfunded-cache', 'novarocks/native-adapter/src/lib.rs',
     'channels: Arc::new(Mutex::new(HashMap::new())),\n            transport_capacity: Some(capacity),',
     'channels: Arc::clone(&self.channels),\n            transport_capacity: Some(capacity),',
     'backend_application::tests::installing_original_capacity'),
    ('ordinary-outbound-config', 'novarocks/native-adapter/src/native_client.rs',
     '.http2_connection_factory(move || factory.try_config(class))',
     '.http2_connection_factory(move || { let _ = (&factory, class); Ok::<_, std::io::Error>(tonic::transport::Http2ConnectionConfig::default()) })',
     'native_transport_capacity::tests::exhausted_actual_outbound'),
    ('early-slot-release', 'novarocks/native-adapter/src/native_transport_capacity.rs',
     'let owner = self.claim(class)?;',
     'let owner = self.claim(class)?; drop(owner); let owner = Bytes::new();',
     'native_transport_capacity::tests::final_independent_header_value_alias'),
    ('control-borrows-data', 'novarocks/native-adapter/src/native_transport_capacity.rs',
     'TransportClass::Control => d.data_positions..d.data_positions + d.control_positions,',
     'TransportClass::Control => 0..d.data_positions,',
     'native_transport_capacity::tests::control_and_data_stock'),
]
originals = {p: (repo/p).read_bytes() for _,p,*_ in cases}
for label,p,old,*_ in cases:
    assert originals[p].decode().count(old) == 1, (label, 'unique anchor')
try:
    for label,p,old,new,filter_ in cases:
        source = originals[p].decode()
        mutated = source.replace(old,new)
        (repo/p).write_text(mutated)
        (out/(label+'.diff')).write_text(''.join(difflib.unified_diff(source.splitlines(True),mutated.splitlines(True),fromfile=p,tofile=p)))
        cmd = ['cargo','test','--offline','-p','novarocks-native-adapter','--lib',filter_,'--','--test-threads=1']
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
