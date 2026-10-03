#!/usr/bin/env python3
"""Replay safe actual-source regressions with exclusive Cargo/source access."""
import difflib
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib


def change(source, before, after):
    assert source.count(before) == 1, before
    return source.replace(before, after)


def main():
    repo = next(p for p in Path(__file__).resolve().parents if (p/'novarocks/native-adapter/src/native_task_executor.rs').exists())
    pool = 'novarocks/native-adapter/src/native_task_executor.rs'
    server = 'vendor/hyper-1.8.1/src/proto/h2/server.rs'
    paths = [pool, server]
    original = {p: (repo/p).read_bytes() for p in paths}
    texts = {p: b.decode() for p, b in original.items()}
    out = Path(tempfile.mkdtemp(prefix='m07-original-stream-negatives-'))
    print(out, flush=True)
    h = lambda b: hashlib.sha256(b).hexdigest()
    receipt = {'head': subprocess.check_output(['git','rev-parse','HEAD'], cwd=repo, text=True).strip(), 'original_sha256': {p:h(b) for p,b in original.items()}, 'cases': []}
    protocol = 'real_small_stream_cell_waker_holds_position_refuses_before_service_and_recovers'
    hooks = 'public_prepared_alias_cannot_replay_or_spawn_larger_future_and_cannot_reuse_early'
    connect = 'connect_is_refused_before_preparation_and_service_while_ordinary_default_dispatches'
    cases = [
        ('ordinary-cell-without-original-owner', pool, change(texts[pool], 'let _ = tokio::runtime::Handle::current()\n                    .spawn_with_task_owner(future, prepared.owner.clone());', 'tokio::spawn(future);'), protocol, 'called `Result::unwrap_err()` on an `Ok` value'),
        ('prepared-clone-replay', pool, change(texts[pool], '.is_err()\n                {', '.is_err() && false\n                {'), hooks, 'same prepared lease cannot spawn a second Cell'),
        ('oversized-future-bypass', pool, change(texts[pool], 'if actual > pool.core().task_bound', 'if actual == usize::MAX'), hooks, 'oversized future must not allocate or consume the prepared position'),
        ('pool-original-before-position-vec', pool, change(texts[pool], 'drop(Arc::into_inner(core));', 'if let Some(mut core) = Arc::into_inner(core) {\n                let original = std::mem::replace(&mut core._original, Bytes::new());\n                drop(original);\n                drop(core);\n            }'), protocol, 'Cell/autoBox/carrier/pool requested backing must physically exit before credit'),
        ('hyper-prepare-forwarding-omitted', server, change(texts[server], 'let prepared = match exec.try_prepare_h2stream()', 'let prepared = match Ok::<Option<E>, std::io::Error>(None)'), protocol, 'called `Result::unwrap()` on an `Err` value'),
        ('connect-preallocation-gate-omitted', server, change(texts[server], 'if self.reject_connect_for_preallocated_tasks\n                            && req.method() == Method::CONNECT', 'if self.reject_connect_for_preallocated_tasks && false\n                            && req.method() == Method::CONNECT'), connect, 'called `Result::unwrap_err()` on an `Ok` value'),
    ]
    env = os.environ.copy()
    env.update(CARGO_BUILD_JOBS='4', CARGO_INCREMENTAL='0', RUSTUP_TOOLCHAIN=tomllib.loads((repo/'rust-toolchain.toml').read_text())['toolchain']['channel'])
    try:
        for label, path, mutated, test, oracle in cases:
            assert all((repo/p).read_bytes() == b for p,b in original.items())
            command = ['cargo','test','--offline','--locked','-p','novarocks-native-adapter','--test','native_original_stream_task',test,'--','--exact','--test-threads=1']
            record = {'name':label,'command':command,'mutation_sha256':h(mutated.encode()),'oracle':oracle}
            receipt['cases'].append(record)
            try:
                delta = ''.join(difflib.unified_diff(texts[path].splitlines(True), mutated.splitlines(True), fromfile=path, tofile=path))
                (out/(label+'.diff.gz')).write_bytes(gzip.compress(delta.encode(),mtime=0))
                (repo/path).write_text(mutated)
                with (out/(label+'.log')).open('w') as log:
                    result = subprocess.run(command, cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=600)
                log = (out/(label+'.log')).read_text(errors='replace')
                record['exit'] = result.returncode
                assert result.returncode == 101
                assert 'running 1 test' in log
                assert re.search('^test '+re.escape(test)+r' \.\.\. FAILED\s*$', log, re.M)
                assert not re.search(r'error\[E\d+\]|could not compile|SIGABRT|signal:|running 0 tests', log)
                assert oracle in log
                record['verdict'] = 'exact compiled runtime failure and allocation/lifetime oracle'
                record['log_sha256'] = h((out/(label+'.log')).read_bytes())
                print(label, record['verdict'], flush=True)
            finally:
                for p,b in original.items(): (repo/p).write_bytes(b)
                record['exact_restoration'] = all((repo/p).read_bytes()==b for p,b in original.items())
    finally:
        for p,b in original.items(): (repo/p).write_bytes(b)
        receipt['exact_restoration'] = all((repo/p).read_bytes()==b for p,b in original.items())
        (out/'verification.json').write_text(json.dumps(receipt,indent=2)+'\n')
        print('Restored',receipt['exact_restoration'],out,flush=True)


if __name__ == '__main__':
    main()
