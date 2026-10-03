from pathlib import Path
import hashlib, json, os, re, subprocess, time
root=Path(__file__).resolve().parents[5]
run=Path('/tmp/m07-scalar-leaf-negatives'); run.mkdir(exist_ok=True)
def replace(old,new):
 def mutate(src):
  assert src.count(old)==1, (old,src.count(old))
  return src.replace(old,new,1)
 return mutate
def local_guard(src):
 pattern=r'contract\s*\.validate_purpose\(\)\s*\.map_err\(\|_\| LocalProgramError::InvalidSink\)\?;'
 result,count=re.subn(pattern,'let _ = contract;',src,count=1)
 assert count==1
 return result
base=['cargo','+1.92.0','test']
cases=[
 ('single-value-before-cursor','novarocks/result-contract/src/scalar_leaf.rs',replace('if bytes.len() > ScalarProfileV1::SINGLE_VALUE_BYTES {','if false {'),['-p','novarocks-result-contract','--test','scalar_leaf','every_variable_leaf_limit_is_inclusive_and_empty_value_is_present']),
 ('leaf-type-parameters-before-assembly','novarocks/result-contract/src/scalar_leaf.rs',replace('if header[12] != expected.header[12] || header[14..24] != expected.header[14..24] {','if header[12] != expected.header[12] {'),['-p','novarocks-result-contract','--test','scalar_leaf','decoder_rejects_each_malformed_declaration_and_incomplete_or_extra_record']),
 ('flat-node-sole-reference','novarocks/proto-codec/src/scalar_result.rs',replace('if *visited {','if false {'),['-p','novarocks-proto-codec','--lib','scalar_result::tests::shared_two_node_tree_refuses_without_cycle_or_unreachable_node']),
 ('dto-neutral-coexistence-before-growth','novarocks/proto-codec/src/scalar_result.rs',replace('add(&mut overlap, neutral, &path)?;','let _ = neutral;'),['-p','novarocks-proto-codec','--lib','scalar_result::tests::dto_neutral_and_seen_overlap_refuses_before_target_allocation']),
 ('actual-frozen-wrapper-raw-preflight','novarocks/task-codec/src/resource_preflight.rs',replace('(4, Value::Bytes(schema)) => self.scan_scalar(schema)?,','(4, Value::Bytes(_schema)) => {},'),['-p','novarocks-task-codec','--lib','resource_preflight::root_preflight_tests::scalar_schema_raw_preflight_follows_every_actual_frozen_root_wrapper']),
 ('canonical-original-field-metadata','novarocks/native-adapter/src/root_scalar_leaf_codec.rs',replace('if logical != expected_logical(&expected.value_type).map(LogicalType::metadata_value) {','if false {'),['-p','novarocks-native-adapter','--lib','root_scalar_leaf_codec::tests::canonical_metadata_and_cached_facts_must_both_match']),
 ('selected-dictionary-value-null','novarocks/native-adapter/src/root_scalar_leaf_codec.rs',lambda src: src.replace('if values.is_null(key) {','if false {',1),['-p','novarocks-native-adapter','--lib','root_scalar_leaf_codec::tests::dictionary_key_and_selected_value_null_are_distinct_physical_cases']),
 ('original-batch-alias-until-cursor-exit','novarocks/native-adapter/src/root_scalar_leaf_codec.rs',replace('batch: chunk.batch.clone(),','batch: RecordBatch::new_empty(chunk.batch.schema()),'),['-p','novarocks-native-adapter','--lib','root_scalar_leaf_codec::tests::immutable_batch_alias_remains_until_cursor_exits']),
 ('direct-local-purpose-validation','novarocks/local-program/src/program.rs',local_guard,['-p','novarocks-native-adapter','--lib','physical_v1_roundtrip::scalar_local_program_rejects_naked_domain_without_wire_guard']),
 ('typed-host-installation-gate','novarocks/native-adapter/src/backend_task_execution/execution_host.rs',replace('return Err(protocol("explicit internal root codec is not installed"));','let _missing_codec = protocol("explicit internal root codec is not installed");'),['-p','novarocks-native-adapter','--lib','backend_task_execution::execution_host::tests::bounded_root_missing_domain_codec_is_protocol_refusal_before_channel_install']),
 ('typed-session-installation-gate','novarocks/native-adapter/src/root_result_session.rs',replace('return Err(io_error("explicit internal root codec is not installed"));','let _missing_codec = io_error("explicit internal root codec is not installed");'),['-p','novarocks-native-adapter','--test','root_producer_session','typed_scalar_session_stays_closed_until_the_complete_domain_producer_is_installed']),
]
env=dict(os.environ,CARGO_BUILD_JOBS='4',CARGO_INCREMENTAL='0')
originals={path:(root/path).read_bytes() for _,path,_,_ in cases}
results=[]
try:
 for name,path,mutate,args in cases:
  source=root/path; original=originals[path]
  assert source.read_bytes()==original
  modified=mutate(original.decode()).encode(); assert modified!=original
  started=time.time(); log=run/(name+'.log')
  try:
   source.write_bytes(modified)
   with log.open('wb') as out:
    result=subprocess.run(base+args+['--','--exact','--test-threads=1'],cwd=root,env=env,stdout=out,stderr=subprocess.STDOUT,timeout=120)
   output=log.read_text(errors='replace')
   record=dict(name=name,path=path,command=base+args+['--','--exact','--test-threads=1'],exit_code=result.returncode,compiled_runtime_failed=result.returncode==101 and 'test result: FAILED.' in output,elapsed_seconds=round(time.time()-started,2),log=str(log))
  finally:
   source.write_bytes(original)
   assert source.read_bytes()==original
  results.append(record); print(json.dumps(record),flush=True)
  (run/'results.json').write_text(json.dumps(results,indent=2)+'\n')
  assert record['compiled_runtime_failed'],record
finally:
 for path,data in originals.items():
  (root/path).write_bytes(data)
  assert (root/path).read_bytes()==data
 (run/'restored-sources.json').write_text(json.dumps({path:hashlib.sha256(data).hexdigest() for path,data in originals.items()},indent=2)+'\n')
