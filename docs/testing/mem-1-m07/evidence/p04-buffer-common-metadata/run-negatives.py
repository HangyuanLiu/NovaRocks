from pathlib import Path
import os,subprocess,json,hashlib,difflib
root=Path.cwd();work=root/'logs/mem-1-m07/p04-buffer-common-metadata/negatives'
e=dict(os.environ,RUSTUP_TOOLCHAIN='1.92.0',CARGO_BUILD_JOBS='4',CARGO_INCREMENTAL='0',CARGO_TARGET_DIR=str(root/'logs/mem-1-m07/p04-response-port/product-target'))
s='vendor/tower-0.4.13/src/buffer/service.rs';w='vendor/tower-0.4.13/src/buffer/worker.rs'
mutations=[
 ('missing-worker-original',s,'        worker.retain_common_metadata_owner(original.clone());','        let _ = original.clone();','actual_worker_retains_common_original_after_buffer_and_all_cells_exit'),
 ('missing-handle-prewarm',s,'        buffer.handle.prewarm_allocation_metadata()?;','        // Negative: omit final Handle first-lock prewarm.','actual_owned_pair_prewarm_delta_matches_both_real_mutex_backings'),
 ('missing-handle-private-pal',w,'        let mutex = std::alloc::Layout::new::<(isize, [u8; 56])>().size();','        let mutex = 0;','actual_owned_pair_prewarm_delta_matches_both_real_mutex_backings'),
]
results=[]
for label,path,old,new,test in mutations:
 p=root/path;original=p.read_bytes();source=original.decode();assert source.count(old)==1
 changed=source.replace(old,new)
 (work/(label+'.patch')).write_text(''.join(difflib.unified_diff(source.splitlines(True),changed.splitlines(True),fromfile=path,tofile=path)))
 p.write_text(changed)
 try:
  with (work/(label+'.log')).open('w') as out:
   r=subprocess.run(['cargo','test','--offline','--locked','-p','novarocks-native-adapter','--test','native_tower_common_metadata',test,'--','--exact','--nocapture'],env=e,stdout=out,stderr=subprocess.STDOUT)
  text=(work/(label+'.log')).read_text()
  row=dict(label=label,source=path,test=test,exit_code=r.returncode,compiled_runtime_failure='test result: FAILED' in text,source_before_sha256=hashlib.sha256(original).hexdigest(),source_mutated_sha256=hashlib.sha256(changed.encode()).hexdigest())
  results.append(row);print(json.dumps(row),flush=True)
 finally:
  p.write_bytes(original)
  assert p.read_bytes()==original
 (work/'results.json').write_text(json.dumps(results,indent=2)+'\n')
 assert r.returncode==101 and row['compiled_runtime_failure']
