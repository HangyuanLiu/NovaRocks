#!/usr/bin/env python3
"""Mutate actual lifecycle wiring, require compiled runtime failure, restore bytes."""
from pathlib import Path
import difflib, json, subprocess, tempfile, sys
repo = Path.cwd()
out = Path(tempfile.mkdtemp(prefix='m07-lifecycle-negatives-'))
capacity = 'novarocks/native-adapter/src/native_transport_capacity.rs'
cases = [
 ('tonic-omits-acquisition-retention', 'vendor/tonic-0.12.3/src/transport/channel/service/connection.rs',
  'lifecycle.retain_acquisition_owner(owner.clone())?;', 'let _ = owner;',
  'native_connection_lifecycle', 'actual_tonic_acquisition_position_waits_for_independent_failed_io_exit'),
 ('ignore-generation', capacity,
  '|| record.generation.load(Ordering::Acquire) != self.generation', '',
  None, 'returned_position_changes_generation_and_refuses_old_observer_events'),
 ('initial-publishes-live', capacity,
  '                ACQUIRING,\n                INITIAL_COMPLETE,', '                ACQUIRING,\n                LIVE,',
  None, 'actual_lifecycle_phases_hold_position_until_final_original_alias_exit'),
 ('hyper-server-omits-yield', 'vendor/hyper-1.8.1/src/proto/h2/server.rs',
  'yield_after_initial_settings: config.connection_lifecycle.is_some(),', 'yield_after_initial_settings: false,',
  'native_connection_lifecycle', 'hyper_server_yields_after_initial_before_ready_headers_dispatch'),
 ('tonic-omits-final-verdict', 'vendor/tonic-0.12.3/src/transport/channel/service/connection.rs',
  'if let Err(error) = lifecycle.on_acquisition_complete() {', 'if let Err(error) = Ok::<(), std::io::Error>(()) {',
  'native_connection_lifecycle', 'tonic_final_verdict_follows_real_initial_phase_and_missing_timeout_never_dials'),
]
if len(sys.argv) > 1:
 selected=set(sys.argv[1:]); assert selected <= {c[0] for c in cases}
 cases=[c for c in cases if c[0] in selected]
originals={path:(repo/path).read_bytes() for _,path,*_ in cases}
for label,path,old,*_ in cases:
 assert originals[path].decode().count(old)==1, (label,'unique source anchor')
results={}
try:
 for label,path,old,new,target,filter_ in cases:
  source=originals[path].decode(); mutated=source.replace(old,new,1)
  (repo/path).write_text(mutated)
  (out/(label+'.diff')).write_text(''.join(difflib.unified_diff(source.splitlines(True),mutated.splitlines(True),fromfile=path,tofile=path)))
  cmd=['cargo','test','--offline','-p','novarocks-native-adapter']
  cmd += ['--test',target] if target else ['--lib']
  cmd += [filter_,'--','--test-threads=1']
  log=out/(label+'.log')
  with log.open('w') as handle:
   handle.write('Command: '+' '.join(cmd)+'\n'); handle.flush()
   result=subprocess.run(cmd,stdout=handle,stderr=subprocess.STDOUT,timeout=180)
  (repo/path).write_bytes(originals[path])
  assert result.returncode==101 and 'test result: FAILED.' in log.read_text(), (label,result.returncode,'compiled runtime failure required')
  results[label]='compiled runtime FAILED'; print(label,results[label],flush=True)
finally:
 for path,source in originals.items(): (repo/path).write_bytes(source)
 assert all((repo/path).read_bytes()==source for path,source in originals.items())
 (out/'verification.json').write_text(json.dumps(results,indent=2)+'\n')
 print('exact source restoration; evidence directory',out,flush=True)
