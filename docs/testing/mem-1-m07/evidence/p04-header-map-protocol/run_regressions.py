#!/usr/bin/env python3
"""Actual production runtime mutants, serialized with all other workspace Cargo work."""
import argparse
import difflib
import hashlib
from pathlib import Path
import subprocess

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--repo',type=Path,default=Path.cwd())
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args();repo=args.repo.resolve(strict=True);args.output.mkdir(parents=True,exist_ok=True)
cases=[
 ('codec-map-install','vendor/h2-0.4.12/src/codec/framed_read.rs','maps: header_map_pool.as_ref(),','maps: None,','h2_both_directions_maps_clones_iterators_and_field_aliases_outlive_io'),
 ('hyper-client-forward','vendor/hyper-1.8.1/src/proto/h2/client.rs','builder.receive_header_map_pool(pool.clone());','let _ = pool;','hyper_both_directions_and_tonic_factory_forward_original_map_pool'),
 ('hyper-server-forward','vendor/hyper-1.8.1/src/proto/h2/server.rs','builder.receive_header_map_pool(pool.clone());','let _ = pool;','hyper_both_directions_and_tonic_factory_forward_original_map_pool'),
 ('tonic-forward','vendor/tonic-0.12.3/src/transport/channel/http2_connection.rs','builder.receive_header_map_pool(pool);','let _ = pool;','hyper_both_directions_and_tonic_factory_forward_original_map_pool'),
 ('tonic-predial-dependency','vendor/tonic-0.12.3/src/transport/channel/http2_connection.rs','self.receive_header_map_pool.is_some() && self.receive_header_field_pool.is_none()','self.receive_header_map_pool.is_some() && false','tonic_map_pool_missing_field_dependency_is_rejected_before_dial'),
 ('client-prebind-capacity','vendor/h2-0.4.12/src/client.rs','if let Some(message) = geometry_error {','if let Some(message) = geometry_error.filter(|_| false) {','invalid_frame_geometry_does_not_burn_any_original_buffer_or_map_binding'),
 ('server-prebind-capacity','vendor/h2-0.4.12/src/server.rs','if let Some(message) = geometry_error {','if let Some(message) = geometry_error.filter(|_| false) {','invalid_frame_geometry_does_not_burn_any_original_buffer_or_map_binding'),
 ('hpack-error-precedence','vendor/h2-0.4.12/src/frame/headers.rs','        if let Err(e) = res {\n            // Preserve a complete HPACK failure.', '        if metadata_exhausted { return Err(Error::HeaderMapCapacityExhausted); }\n        if let Err(e) = res {\n            // Preserve a complete HPACK failure.','real_hpack_errors_take_precedence_but_exhausted_nonfinal_need_more_fails_immediately'),
]
originals={relative:(repo/relative).read_bytes() for _,relative,_,_,_ in cases}
try:
 for name,relative,needle,replacement,test in cases:
  path=repo/relative;original=originals[relative].decode()
  if original.count(needle)!=1:raise SystemExit(f'Unique mutation failed: {name}: {original.count(needle)}')
  changed=original.replace(needle,replacement,1);path.write_text(changed)
  result=subprocess.run(['cargo','test','-p','novarocks-native-adapter','--offline','--test','native_h2_header_map_pool',test,'--','--exact','--test-threads=1'],cwd=repo,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
  (args.output/(name+'.log')).write_text(result.stdout)
  (args.output/(name+'.diff')).write_text(''.join(difflib.unified_diff(original.splitlines(True),changed.splitlines(True),fromfile=relative,tofile=name)))
  if result.returncode!=101 or 'test result: FAILED.' not in result.stdout:raise SystemExit(f'Negative did not fail at runtime: {name}: {result.returncode}')
  print(f'{name}: runtime 101',flush=True)
  path.write_bytes(originals[relative])
finally:
 for relative,original in originals.items():
  path=repo/relative;path.write_bytes(original)
  assert path.read_bytes()==original
  print('Restored '+relative+' SHA256 '+hashlib.sha256(original).hexdigest(),flush=True)
