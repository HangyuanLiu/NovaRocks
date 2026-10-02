#!/usr/bin/env python3
"""Serialized actual-source runtime regressions; always restore exact production bytes."""
from pathlib import Path
import difflib,hashlib,subprocess,tempfile
repo=Path.cwd();output=Path(tempfile.mkdtemp(prefix='m07-metadata-negatives-'))
cases=[
 ('infallible-status-copy','vendor/tonic-0.12.3/src/status.rs','match header_map.try_clone() {','match Ok::<_, http::header::MaxSizeReached>(header_map.clone()) {','native_tonic_header_map_capacity','public_status_parse_exhaustion'),
 ('lost-original-adoption','vendor/http-1.4.0/src/header/map.rs','if self.allocation.is_none() && other.allocation.is_some() {','if false && self.allocation.is_none() && other.allocation.is_some() {','native_http_header_map_pool','owned_merge_adopts'),
 ('partial-trailers-escape','vendor/tonic-0.12.3/src/codec/decode.rs','                                self.trailers.take();','                                // Deliberately omit partial metadata retirement.','native_tonic_header_map_capacity','streaming_failed_partial_trailer_merge'),
 ('unfunded-server-fallback','vendor/tonic-0.12.3/src/server/grpc.rs','''            return crate::status::metadata_failure_response(
                parts.headers,
                crate::metadata::metadata_capacity_exhausted(error),
            );''','''            return crate::metadata::metadata_capacity_exhausted(error).into_http();''','native_tonic_header_map_capacity','actual_server_unary_protocol_shortage'),
 ('infallible-hyper-date','vendor/hyper-1.8.1/src/proto/h2/server.rs','''headers
        .try_entry(http::header::DATE)
        .map_err(|_| crate::Error::new_user_header())?''','''Ok::<_, crate::Error>(headers.entry(http::header::DATE))?''','native_hyper_header_map_mutation','echoed_received_map_date_insertion'),
]
originals={path:(repo/path).read_bytes() for _,path,*_ in cases}
try:
 for label,path,old,new,target,filter_ in cases:
  original=originals[path];source=original.decode();assert source.count(old)==1,(label,'unique anchor')
  mutated=source.replace(old,new);(repo/path).write_text(mutated)
  (output/(label+'.diff')).write_text(''.join(difflib.unified_diff(source.splitlines(True),mutated.splitlines(True),fromfile=path,tofile=path)))
  cmd=['cargo','test','-p','novarocks-native-adapter','--offline','--test',target,filter_,'--','--test-threads=1']
  with (output/(label+'.log')).open('w') as log:r=subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,check=False)
  (repo/path).write_bytes(original)
  log=(output/(label+'.log')).read_text();assert r.returncode==101 and 'test result: FAILED.' in log,(label,r.returncode,'runtime failure required')
  print(label,'runtime failed',r.returncode,flush=True)
finally:
 for path,original in originals.items():(repo/path).write_bytes(original)
 for path,original in originals.items():assert (repo/path).read_bytes()==original
 print('exact source restoration', {p:hashlib.sha256(b).hexdigest() for p,b in originals.items()},flush=True)
 print('negative evidence directory',output,flush=True)
