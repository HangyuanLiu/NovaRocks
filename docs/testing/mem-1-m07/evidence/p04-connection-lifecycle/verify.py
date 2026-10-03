#!/usr/bin/env python3
"""Verify pinned actual lifecycle protocol wiring and scoped production consumers."""
from pathlib import Path
import hashlib, json, re, subprocess, tempfile
repo=Path.cwd(); receipt=Path(__file__).parent
for filename,base in [('product-sha256.json',repo),('vendor-source-sha256.json',repo/'vendor')]:
 for relative,digest in json.loads((receipt/filename).read_text()).items():
  assert hashlib.sha256((base/relative).read_bytes()).hexdigest()==digest, relative
out=Path(tempfile.mkdtemp(prefix='m07-lifecycle-verification-'))
targets=['native_connection_lifecycle','native_initial_settings_kernel','native_tonic_acquisition_owner','native_tonic_connection_factory','native_h2_stream_store_owner','native_h2_resident_store','native_http_header_map_pool','native_tonic_header_map_capacity','native_tonic_status_field_pool','native_tonic_preallocated_status','native_tonic_preallocated_trailers','native_tonic_preallocated_unary','root_result_reader','native_tonic_request_response_headers','native_ingress_response_headers']
cmd=['cargo','test','--offline','-p','novarocks-native-adapter']
for target in targets: cmd+=['--test',target]
commands=[('protocol',cmd+['--','--test-threads=1'])]
filters=['native_response::tests','native_ingress::tests','native_server::','native_transport_capacity::tests','backend_application::','native_client::tests']
for filter_ in filters:
 commands.append((filter_.split('::')[0],['cargo','test','--offline','-p','novarocks-native-adapter','--lib',filter_,'--','--test-threads=1']))
for package in ['h2','hyper','tonic']:
 commands.append((package+'-clippy',['cargo','clippy','--offline','-p',package,'--lib','--no-deps','--','-D','warnings']))
commands += [('native-clippy',['cargo','clippy','--offline','-p','novarocks-native-adapter','--lib','--test','native_connection_lifecycle','--test','native_tonic_connection_factory']),('fmt',['cargo','fmt','--all','--','--check']),('diff',['git','diff','--check'])]
results={}
for label,cmd in commands:
 with (out/(label+'.log')).open('w') as log:
  log.write('Command: '+' '.join(cmd)+'\n'); log.flush()
  result=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT)
 results[label]=result.returncode
 print(label,result.returncode,flush=True)
 if result.returncode: print('evidence directory',out,flush=True); raise SystemExit(result.returncode)
results['protocol_tests']=sum(map(int,re.findall(r'test result: ok\. (\d+) passed;', (out/'protocol.log').read_text())))
results['native_lib_tests']=sum(int(re.search(r'test result: ok\. (\d+) passed;', (out/(f.split('::')[0]+'.log')).read_text()).group(1)) for f in filters)
(out/'verification.json').write_text(json.dumps(results,indent=2)+'\n')
print('evidence directory',out,flush=True)
