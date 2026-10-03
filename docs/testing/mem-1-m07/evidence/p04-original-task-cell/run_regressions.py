#!/usr/bin/env python3
"""Run safe actual-source TaskCell lifetime regressions with exclusive Cargo.

The ordinary public-target negatives must fail their physical lifetime oracle.
The constructor probe copies the actual downstream allocator test source and
links the actual production crates; a safe panic is injected after Cell Box
allocation. Baseline passes, removing the outer original clone fails. No model
TaskCell or altered pointer/list operation is used. Every source is restored.
"""
from pathlib import Path
import argparse,difflib,gzip,hashlib,json,os,re,shutil,subprocess,tempfile,tomllib


def digest(data):
    return hashlib.sha256(data).hexdigest()


def replace(source, old, new, count=1):
    assert source.count(old)==count, (old,source.count(old))
    return source.replace(old,new)


CTOR = r'''
struct ConstructorFuture(ProbeFuture<32768, 0>);
impl Future for ConstructorFuture {
    type Output = Output<0>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(cx)
    }
}
impl Drop for ConstructorFuture {
    fn drop(&mut self) {
        self.0.state.future.store(self as *const Self as usize, Ordering::SeqCst);
    }
}
#[test]
fn post_box_constructor_unwind_exits_actual_backings_before_original_credit() {
    let _serial = SERIAL.lock().unwrap();
    let runtime = runtime(false);
    let state = State::new();
    let (funding, owner) = Funding::new::<ConstructorFuture>(&state);
    // Disable the task allocation tag before the unrelated panic runtime
    // prepares its payload/unwind backing. The two actual task allocations
    // have already happened when this hook runs.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| TRACK.with(|v| v.set(false))));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        spawn(&runtime, ConstructorFuture(ProbeFuture::new(&state, false, false)), owner)
    }));
    std::panic::set_hook(hook);
    let panic = outcome.expect_err("actual post-Box constructor injection must unwind");
    assert_eq!(panic.downcast_ref::<&str>().copied(), Some("actual post-Box constructor probe"));
    assert_eq!(state.polls.load(Ordering::SeqCst), 0);
    assert_eq!(state.future_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(COUNT.load(Ordering::SeqCst), 2, "actual automatic FutureBox and unpublished Cell");
    assert!(!OVERFLOW.load(Ordering::SeqCst));
    assert!(!REALLOCATED.load(Ordering::SeqCst));
    assert_eq!(RECORDS.iter().map(|r|r.bytes.load(Ordering::SeqCst)).sum::<usize>(), funding.bound);
    let future = record(state.future.load(Ordering::SeqCst));
    assert_eq!(future.bytes.load(Ordering::SeqCst), Layout::new::<ConstructorFuture>().size());
    assert_eq!(future.alignment.load(Ordering::SeqCst), Layout::new::<ConstructorFuture>().align());
    let cell = RECORDS.iter().find(|r| r.pointer.load(Ordering::SeqCst)!=0 && !std::ptr::eq(*r,future)).unwrap();
    assert_eq!(cell.bytes.load(Ordering::SeqCst), funding.bound-Layout::new::<ConstructorFuture>().size());
    assert!(cell.alignment.load(Ordering::SeqCst)>=64);
    assert!(all_freed());
    funding.returned();
    drop(runtime);
}
'''


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout',type=int,default=600)
    parser.add_argument('--constructor-only',action='store_true')
    args=parser.parse_args(); assert args.timeout>0
    repo=next(p for p in Path(__file__).resolve().parents if (p/'vendor/tokio-1.52.3/src/lib.rs').exists())
    paths=['vendor/tokio-1.52.3/src/runtime/task/harness.rs','vendor/tokio-1.52.3/src/runtime/task/raw.rs','vendor/tokio-1.52.3/src/runtime/task/core.rs','vendor/tokio-1.52.3/src/runtime/handle.rs']
    original={p:(repo/p).read_bytes() for p in paths}
    out=Path(tempfile.mkdtemp(prefix='m07-original-task-negatives-'));print(out,flush=True)
    env=os.environ.copy();env.update(CARGO_BUILD_JOBS='4',CARGO_INCREMENTAL='0',RUSTUP_TOOLCHAIN=tomllib.loads((repo/'rust-toolchain.toml').read_text())['toolchain']['channel'],CARGO_TARGET_DIR=str(repo/'target'))
    receipt={'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'scope':'actual TaskCell/FutureBox/carrier physical lifetime; external/shared graph open','original_sha256':{p:digest(v) for p,v in original.items()},'cases':[]}
    test='current_thread_small_future_large_output_and_overaligned_cell_keep_original_credit'
    target=['cargo','test','--offline','--locked','-p','novarocks-native-adapter','--test','native_original_task_cell',test,'--','--exact','--test-threads=1']
    hp,rp,cp,op=paths
    ordinary=replace(original[hp],b'            drop(cell);\n            drop(original);',b'            drop(original);\n            drop(cell);')
    early=replace(original[rp],b'        Self::new_inner(task, scheduler, id, spawned_at, Some(owner))',b'''        let task = async move {
            let original = owner;
            let result = task.await;
            drop(original);
            result
        };
        Self::new_inner(task, scheduler, id, spawned_at, None)''')
    injected=replace(original[cp],b'        #[cfg(debug_assertions)]\n        {\n            // Using a separate function',b'''        #[cfg(feature = "io-util")]
        if result.trailer.task_owner.is_some() {
            std::panic::panic_any("actual post-Box constructor probe");
        }
        #[cfg(debug_assertions)]
        {
            // Using a separate function''')
    no_clone=replace(original[op],b'original.clone()',b'original',2)
    no_clone=replace(no_clone,b'            drop(original);\n            Ok(join)',b'            Ok(join)')
    def run(label,changes,command,expect_fail,oracle=None):
        assert all((repo/p).read_bytes()==v for p,v in original.items())
        case={'name':label,'command':command,'expected':'exact compiled runtime FAILED' if expect_fail else 'exact compiled runtime PASS','mutations':{p:digest(v) for p,v in changes.items()}}
        receipt['cases'].append(case)
        try:
            for path,mutated in changes.items():
                delta=''.join(difflib.unified_diff(original[path].decode().splitlines(True),mutated.decode().splitlines(True),fromfile=path,tofile=path))
                (out/(label+'-'+Path(path).name+'.diff.gz')).write_bytes(gzip.compress(delta.encode(),mtime=0));(repo/path).write_bytes(mutated)
            with (out/(label+'.log')).open('w') as log:
                result=subprocess.run(command,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=args.timeout)
            text=(out/(label+'.log')).read_text(errors='replace');case['exit']=result.returncode
            exact=command[command.index('--exact')-2]
            assert 'running 1 test' in text and re.search('^test '+re.escape(exact)+r' \.\.\. '+('FAILED' if expect_fail else 'ok')+r'\s*$',text,re.M)
            assert not re.search(r'error\[E\d+\]|could not compile|SIGABRT|signal:|running 0 tests',text)
            assert result.returncode==(101 if expect_fail else 0)
            if oracle: assert oracle in text
            case['verdict']='expected exact runtime outcome and physical oracle'
            case['log_sha256']=digest((out/(label+'.log')).read_bytes());print(label,case['verdict'],flush=True)
        finally:
            for path,data in original.items(): (repo/path).write_bytes(data)
            case['exact_restoration']=all((repo/p).read_bytes()==v for p,v in original.items())
    try:
        if not args.constructor_only:
            run('owner-before-cell-deallocation',{hp:ordinary},target,True,'actual Cell/FutureBox/carrier deallocation must precede credit release')
            run('future-completion-owner',{rp:early},target,True,'assertion failed: !self.state.owner_exited.load(Ordering::SeqCst)')
        scratch=out/'constructor';scratch.mkdir()
        test_source=(repo/'novarocks/native-adapter/tests/native_original_task_cell.rs').read_bytes()
        (scratch/'probe.rs').write_bytes(test_source+CTOR.encode());shutil.copy2(repo/'Cargo.lock',scratch/'Cargo.lock')
        (scratch/'Cargo.toml').write_text(f'''[package]\nname="m07-task-constructor-probe"\nversion="0.0.0"\nedition="2024"\n[workspace]\n[[test]]\nname="probe"\npath="probe.rs"\n[dependencies]\nbytes="1.11.0"\ntokio={{version="=1.52.3",features=["macros","rt-multi-thread","io-util","net","signal","sync","time"]}}\nnovarocks-worker={{path="{repo}/novarocks/worker",features=["test-support"]}}\nnovarocks-execution={{path="{repo}/novarocks/execution"}}\n[patch.crates-io]\nbytes={{path="{repo}/vendor/bytes-1.11.0"}}\nhttp={{path="{repo}/vendor/http-1.4.0"}}\nh2={{path="{repo}/vendor/h2-0.4.12"}}\nhyper={{path="{repo}/vendor/hyper-1.8.1"}}\ntonic={{path="{repo}/vendor/tonic-0.12.3"}}\ntokio={{path="{repo}/vendor/tokio-1.52.3"}}\n''')
        # Reproduce every actual production patch. Partial patches can silently
        # replace the Arrow path ABI with cached registry packages; reject drift.
        manifest = (scratch/'Cargo.toml').read_text().split('[patch.crates-io]')[0]
        manifest += '[patch.crates-io]\n'
        for name, patch in tomllib.loads((repo/'Cargo.toml').read_text())['patch']['crates-io'].items():
            assert set(patch)=={'path'}, 'This probe requires exact path-only production patches'
            manifest += name+'={path='+json.dumps(str(repo/patch['path']))+'}\n'
        (scratch/'Cargo.toml').write_text(manifest)
        with (out/'constructor-metadata.log').open('w') as log:
            subprocess.run(['cargo','metadata','--offline','--manifest-path',str(scratch/'Cargo.toml'),'--format-version','1'],cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
        identity=lambda p:(p['name'],p['version'],p.get('source'),p.get('checksum'))
        expected={identity(p) for p in tomllib.loads((repo/'Cargo.lock').read_text())['package']}
        observed={identity(p) for p in tomllib.loads((scratch/'Cargo.lock').read_text())['package'] if p['name']!='m07-task-constructor-probe'}
        assert observed<=expected,observed-expected
        receipt['constructor_probe']={'actual_test_source_sha256':digest(test_source),'compound_probe_source_sha256':digest((scratch/'probe.rs').read_bytes()),'production_package_identities':len(observed),'funding':'same production Worker ResultRetainedBudget; copied downstream allocator test code, no model Cell'}
        command=['cargo','test','--offline','--locked','--manifest-path',str(scratch/'Cargo.toml'),'--test','probe','post_box_constructor_unwind_exits_actual_backings_before_original_credit','--','--exact','--test-threads=1']
        run('post-box-constructor-positive',{cp:injected},command,False)
        run('post-box-constructor-missing-outer-clone',{cp:injected,op:no_clone},command,True,'actual Cell/FutureBox/carrier deallocation must precede credit release')
    finally:
        for p,data in original.items(): (repo/p).write_bytes(data)
        receipt['exact_restoration']=all((repo/p).read_bytes()==v for p,v in original.items())
        (out/'verification.json').write_text(json.dumps(receipt,indent=2)+'\n');print('Restored',receipt['exact_restoration'],out,flush=True)


if __name__=='__main__':main()
